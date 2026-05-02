/*
Copyright 2014 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package portforward

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes/scheme"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/polymorphichelpers"
	"k8s.io/kubectl/pkg/util"
	"k8s.io/kubectl/pkg/util/completion"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
	streamhttp "k8s.io/streaming/pkg/httpstream"
)

// PortForwardOptions contains all the options for running the port-forward cli command.
type PortForwardOptions struct {
	Namespace     string
	PodName       string
	RESTClient    restclient.Interface
	Config        *restclient.Config
	PodClient     corev1client.PodsGetter
	Address       []string
	Ports         []string
	PortForwarder portForwarder
	StopChannel   chan struct{}
	ReadyChannel  chan struct{}

	// RetryConfig holds configuration for resilient port-forwarding with auto-reconnect.
	RetryConfig RetryConfig
	// OriginalObject is the resolved resource (Service, Deployment, Pod, ...) used
	// to re-resolve named ports after a pod switch in the resilient retry loop.
	OriginalObject runtime.Object
	// PodSelector finds replacement pods for service/deployment/etc. targets.
	// Nil for direct pod targets — disables pod re-selection.
	PodSelector labels.Selector
	// RawPorts holds the user-supplied port arguments verbatim. The resilient
	// retry loop re-resolves named ports against the currently selected pod
	// from these raw values on every reconnect.
	RawPorts []string

	// ErrOut sinks the messages the resilient retry loop emits ("Connection
	// lost", "Switching to pod", reconnect notes, the no-pod-selector warning
	// in Complete()). When nil, falls back to os.Stderr — preserving CLI
	// behavior. Library callers (e.g. cmd/multi-fwd) set it to a logger so
	// these operationally-important messages can be tagged per session.
	ErrOut io.Writer
}

// errOut returns ErrOut, defaulting to os.Stderr.
func (o *PortForwardOptions) errOut() io.Writer {
	if o.ErrOut != nil {
		return o.ErrOut
	}
	return os.Stderr
}

var (
	portforwardLong = templates.LongDesc(i18n.T(`
                Forward one or more local ports to a pod.

                Use resource type/name such as deployment/mydeployment to select a pod. Resource type defaults to 'pod' if omitted.

                If there are multiple pods matching the criteria, a pod will be selected automatically. The
                forwarding session ends when the selected pod terminates, and a rerun of the command is needed
                to resume forwarding.`))

	portforwardExample = templates.Examples(i18n.T(`
		# Listen on ports 5000 and 6000 locally, forwarding data to/from ports 5000 and 6000 in the pod
		kubectl port-forward pod/mypod 5000 6000

		# Listen on ports 5000 and 6000 locally, forwarding data to/from ports 5000 and 6000 in a pod selected by the deployment
		kubectl port-forward deployment/mydeployment 5000 6000

		# Listen on port 8443 locally, forwarding to the targetPort of the service's port named "https" in a pod selected by the service
		kubectl port-forward service/myservice 8443:https

		# Listen on port 8888 locally, forwarding to 5000 in the pod
		kubectl port-forward pod/mypod 8888:5000

		# Listen on port 8888 on all addresses, forwarding to 5000 in the pod
		kubectl port-forward --address 0.0.0.0 pod/mypod 8888:5000

		# Listen on port 8888 on localhost and selected IP, forwarding to 5000 in the pod
		kubectl port-forward --address localhost,10.19.21.23 pod/mypod 8888:5000

		# Listen on a random port locally, forwarding to 5000 in the pod
		kubectl port-forward pod/mypod :5000`))
)

const (
	// Amount of time to wait until at least one pod is running
	defaultPodPortForwardWaitTimeout = 60 * time.Second
)

func NewCmdPortForward(f cmdutil.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	opts := NewDefaultPortForwardOptions(streams)
	cmd := &cobra.Command{
		Use:                   "port-forward TYPE/NAME [options] [LOCAL_PORT:]REMOTE_PORT [...[LOCAL_PORT_N:]REMOTE_PORT_N]",
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Forward one or more local ports to a pod"),
		Long:                  portforwardLong,
		Example:               portforwardExample,
		ValidArgsFunction:     completion.ResourceAndPortCompletionFunc(f),
		Run: func(cmd *cobra.Command, args []string) {
			cmdutil.CheckErr(opts.Complete(f, cmd, args))
			cmdutil.CheckErr(opts.Validate())
			cmdutil.CheckErr(opts.RunPortForward())
		},
	}
	cmdutil.AddPodRunningTimeoutFlag(cmd, defaultPodPortForwardWaitTimeout)
	cmd.Flags().StringSliceVar(&opts.Address, "address", []string{"localhost"}, "Addresses to listen on (comma separated). Only accepts IP addresses or localhost as a value. When localhost is supplied, kubectl will try to bind on both 127.0.0.1 and ::1 and will fail if neither of these addresses are available to bind.")

	// Resilient port-forward flags.
	cmd.Flags().BoolVar(&opts.RetryConfig.Enabled, "retry", false, "Enable automatic reconnection when the connection is lost.")
	cmd.Flags().DurationVar(&opts.RetryConfig.InitialDelay, "retry-delay", 5*time.Second, "Initial delay before retrying a failed connection (used as exponential-backoff base).")
	cmd.Flags().DurationVar(&opts.RetryConfig.MaxDelay, "max-retry-delay", 60*time.Second, "Maximum delay between retry attempts (exponential backoff cap).")
	cmd.Flags().IntVar(&opts.RetryConfig.MaxRetries, "max-retries", 0, "Maximum number of reconnect attempts after a failure (0 = unlimited).")

	// TODO support UID
	return cmd
}

func NewDefaultPortForwardOptions(streams genericiooptions.IOStreams) *PortForwardOptions {
	return &PortForwardOptions{
		PortForwarder: &defaultPortForwarder{
			IOStreams: streams,
		},
		RetryConfig: DefaultRetryConfig(),
		ErrOut:      streams.ErrOut,
	}
}

type portForwarder interface {
	ForwardPorts(method string, url *url.URL, opts PortForwardOptions) error
}

type defaultPortForwarder struct {
	genericiooptions.IOStreams

	// boundMu guards bound. The retry loop reads bound to pin local ports
	// across reconnects; the capture goroutine in ForwardPorts writes it.
	boundMu sync.Mutex
	bound   []portforward.ForwardedPort
}

// GetBoundPorts implements boundPortReporter from resilient.go. It returns the
// most recently captured set of locally-bound ports (after a successful Ready
// signal). The slice is copied so callers can hold it without locking.
func (f *defaultPortForwarder) GetBoundPorts() []portforward.ForwardedPort {
	f.boundMu.Lock()
	defer f.boundMu.Unlock()
	if f.bound == nil {
		return nil
	}
	return append([]portforward.ForwardedPort(nil), f.bound...)
}

func createDialer(method string, url *url.URL, opts PortForwardOptions) (streamhttp.Dialer, error) {
	transport, upgrader, err := spdy.RoundTripperFor(opts.Config)
	if err != nil {
		return nil, err
	}
	dialer := spdy.NewDialerForStreaming(upgrader, &http.Client{Transport: transport}, method, url)
	if !cmdutil.PortForwardWebsockets.IsDisabled() {
		tunnelingDialer, err := portforward.NewSPDYOverWebsocketDialerForStreaming(url, opts.Config)
		if err != nil {
			return nil, err
		}
		// First attempt tunneling (websocket) dialer, then fallback to spdy dialer.
		dialer = portforward.NewFallbackDialerForStreaming(tunnelingDialer, dialer, func(err error) bool {
			return streamhttp.IsUpgradeFailure(err) || streamhttp.IsHTTPSProxyError(err)
		})
	}
	return dialer, nil
}

func (f *defaultPortForwarder) ForwardPorts(method string, url *url.URL, opts PortForwardOptions) error {
	dialer, err := createDialer(method, url, opts)
	if err != nil {
		return err
	}
	fw, err := portforward.NewOnAddressesForStreaming(dialer, opts.Address, opts.Ports, opts.StopChannel, opts.ReadyChannel, f.Out, f.ErrOut)
	if err != nil {
		return err
	}

	// Capture bound local ports as soon as fw signals ready, so the resilient
	// retry loop can pin them across reconnects. The goroutine exits when
	// either Ready or Stop fires; both are guaranteed to fire before
	// ForwardPorts returns (Ready on success, Stop via the retry loop's
	// per-iteration cancel).
	if opts.ReadyChannel != nil && opts.StopChannel != nil {
		go func() {
			select {
			case <-opts.ReadyChannel:
				ports, perr := fw.GetPorts()
				if perr != nil {
					return
				}
				f.boundMu.Lock()
				f.bound = ports
				f.boundMu.Unlock()
			case <-opts.StopChannel:
			}
		}()
	}

	return fw.ForwardPorts()
}

// splitPort splits port string which is in form of [LOCAL PORT]:REMOTE PORT
// and returns local and remote ports separately
func splitPort(port string) (local, remote string) {
	parts := strings.Split(port, ":")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}

	return parts[0], parts[0]
}

// Translates service port to target port
// It rewrites ports as needed if the Service port declares targetPort.
// It returns an error when a named targetPort can't find a match in the pod, or the Service did not declare
// the port.
func translateServicePortToTargetPort(ports []string, svc corev1.Service, pod corev1.Pod) ([]string, error) {
	var translated []string
	for _, port := range ports {
		localPort, remotePort := splitPort(port)

		portnum, err := strconv.Atoi(remotePort)
		if err != nil {
			svcPort, err := util.LookupServicePortNumberByName(svc, remotePort)
			if err != nil {
				return nil, err
			}
			portnum = int(svcPort)

			if localPort == remotePort {
				localPort = strconv.Itoa(portnum)
			}
		}
		containerPort, err := util.LookupContainerPortNumberByServicePort(svc, pod, int32(portnum))
		if err != nil {
			// can't resolve a named port, or Service did not declare this port, return an error
			return nil, err
		}

		// convert the resolved target port back to a string
		remotePort = strconv.Itoa(int(containerPort))

		if localPort != remotePort {
			translated = append(translated, fmt.Sprintf("%s:%s", localPort, remotePort))
		} else {
			translated = append(translated, remotePort)
		}
	}
	return translated, nil
}

// convertPodNamedPortToNumber converts named ports into port numbers
// It returns an error when a named port can't be found in the pod containers
func convertPodNamedPortToNumber(ports []string, pod corev1.Pod) ([]string, error) {
	var converted []string
	for _, port := range ports {
		localPort, remotePort := splitPort(port)
		if remotePort == "" {
			return nil, fmt.Errorf("remote port cannot be empty")
		}
		containerPortStr := remotePort
		_, err := strconv.Atoi(remotePort)
		if err != nil {
			containerPort, err := util.LookupContainerPortNumberByName(pod, remotePort)
			if err != nil {
				return nil, err
			}

			containerPortStr = strconv.Itoa(int(containerPort))
		}

		if localPort != remotePort {
			converted = append(converted, fmt.Sprintf("%s:%s", localPort, containerPortStr))
		} else {
			converted = append(converted, containerPortStr)
		}
	}

	return converted, nil
}

func checkUDPPorts(udpOnlyPorts sets.Set[int], ports []string, obj metav1.Object) error {
	for _, port := range ports {
		_, remotePort := splitPort(port)
		portNum, err := strconv.Atoi(remotePort)
		if err != nil {
			switch v := obj.(type) {
			case *corev1.Service:
				svcPort, err := util.LookupServicePortNumberByName(*v, remotePort)
				if err != nil {
					return err
				}
				portNum = int(svcPort)

			case *corev1.Pod:
				ctPort, err := util.LookupContainerPortNumberByName(*v, remotePort)
				if err != nil {
					return err
				}
				portNum = int(ctPort)

			default:
				return fmt.Errorf("unknown object: %v", obj)
			}
		}
		if udpOnlyPorts.Has(portNum) {
			return fmt.Errorf("UDP protocol is not supported for %s", remotePort)
		}
	}
	return nil
}

// checkUDPPortInService returns an error if remote port in Service is a UDP port
// TODO: remove this check after #47862 is solved
func checkUDPPortInService(ports []string, svc *corev1.Service) error {
	udpPorts := sets.New[int]()
	tcpPorts := sets.New[int]()
	for _, port := range svc.Spec.Ports {
		portNum := int(port.Port)
		switch port.Protocol {
		case corev1.ProtocolUDP:
			udpPorts.Insert(portNum)
		case corev1.ProtocolTCP:
			tcpPorts.Insert(portNum)
		}
	}
	return checkUDPPorts(udpPorts.Difference(tcpPorts), ports, svc)
}

// checkUDPPortInPod returns an error if remote port in Pod is a UDP port
// TODO: remove this check after #47862 is solved
func checkUDPPortInPod(ports []string, pod *corev1.Pod) error {
	udpPorts := sets.New[int]()
	tcpPorts := sets.New[int]()
	for _, ct := range pod.Spec.Containers {
		for _, ctPort := range ct.Ports {
			portNum := int(ctPort.ContainerPort)
			switch ctPort.Protocol {
			case corev1.ProtocolUDP:
				udpPorts.Insert(portNum)
			case corev1.ProtocolTCP:
				tcpPorts.Insert(portNum)
			}
		}
	}
	return checkUDPPorts(udpPorts.Difference(tcpPorts), ports, pod)
}

// Complete completes all the required options for port-forward cmd.
func (o *PortForwardOptions) Complete(f cmdutil.Factory, cmd *cobra.Command, args []string) error {
	var err error
	if len(args) < 2 {
		return cmdutil.UsageErrorf(cmd, "TYPE/NAME and list of ports are required for port-forward")
	}

	o.Namespace, _, err = f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	builder := f.NewBuilder().
		WithScheme(scheme.Scheme, scheme.Scheme.PrioritizedVersionsAllGroups()...).
		ContinueOnError().
		NamespaceParam(o.Namespace).DefaultNamespace()

	getPodTimeout, err := cmdutil.GetPodRunningTimeoutFlag(cmd)
	if err != nil {
		return cmdutil.UsageErrorf(cmd, "%s", err.Error())
	}

	resourceName := args[0]
	builder.ResourceNames("pods", resourceName)

	obj, err := builder.Do().Object()
	if err != nil {
		return err
	}

	forwardablePod, err := polymorphichelpers.AttachablePodForObjectFn(f, obj, getPodTimeout)
	if err != nil {
		return err
	}

	o.PodName = forwardablePod.Name

	// When --retry is requested, capture the raw user port arguments and the
	// resolved target object so the retry loop can re-resolve named ports
	// against the freshly selected pod after a reconnect. Selector lookup is
	// best-effort: a missing selector silently disables pod re-selection
	// (single-pod targets only). Surface the error to the user when they
	// asked for resilient mode.
	if o.RetryConfig.Enabled {
		o.RawPorts = append([]string(nil), args[1:]...)
		o.OriginalObject = obj
		if _, selector, selErr := polymorphichelpers.SelectorsForObject(obj); selErr == nil {
			o.PodSelector = selector
		} else {
			fmt.Fprintf(o.errOut(), "Note: %s/%s has no pod selector; --retry will reconnect to the same pod and will not pick a replacement on rollout.\n", obj.GetObjectKind().GroupVersionKind().Kind, resourceName)
		}
	}

	// handle service port mapping to target port if needed
	switch t := obj.(type) {
	case *corev1.Service:
		err = checkUDPPortInService(args[1:], t)
		if err != nil {
			return err
		}
		o.Ports, err = translateServicePortToTargetPort(args[1:], *t, *forwardablePod)
		if err != nil {
			return err
		}
	default:
		err = checkUDPPortInPod(args[1:], forwardablePod)
		if err != nil {
			return err
		}
		o.Ports, err = convertPodNamedPortToNumber(args[1:], *forwardablePod)
		if err != nil {
			return err
		}
	}

	clientset, err := f.KubernetesClientSet()
	if err != nil {
		return err
	}

	o.PodClient = clientset.CoreV1()

	o.Config, err = f.ToRESTConfig()
	if err != nil {
		return err
	}
	o.RESTClient, err = f.RESTClient()
	if err != nil {
		return err
	}

	o.StopChannel = make(chan struct{}, 1)
	o.ReadyChannel = make(chan struct{})
	return nil
}

// Validate validates all the required options for port-forward cmd.
func (o PortForwardOptions) Validate() error {
	if len(o.PodName) == 0 {
		return fmt.Errorf("pod name or resource type/name must be specified")
	}

	if len(o.Ports) < 1 {
		return fmt.Errorf("at least 1 PORT is required for port-forward")
	}

	if o.PortForwarder == nil || o.PodClient == nil || o.RESTClient == nil || o.Config == nil {
		return fmt.Errorf("client, client config, restClient, and portforwarder must be provided")
	}

	if o.RetryConfig.Enabled {
		if err := validateRetryConfig(o.RetryConfig); err != nil {
			return err
		}
	}
	return nil
}

// Deprecated: Use RunPortForwardContext instead, which allows canceling.
func (o *PortForwardOptions) RunPortForward() error {
	return o.RunPortForwardContext(context.Background())
}

// RunPortForwardContext implements all the necessary functionality for port-forward cmd.
// It ends portforwarding when an error is received from the backend, or an os.Interrupt
// signal is received, or the provided context is done.
// When --retry is enabled, it delegates to runResilientPortForward for automatic reconnection.
func (o *PortForwardOptions) RunPortForwardContext(ctx context.Context) error {
	if o.RetryConfig.Enabled {
		return o.runResilientPortForward(ctx)
	}
	return o.runSinglePortForward(ctx)
}

// runSinglePortForward runs a single port-forward session (original behavior).
func (o *PortForwardOptions) runSinglePortForward(ctx context.Context) error {
	pod, err := o.PodClient.Pods(o.Namespace).Get(ctx, o.PodName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	if pod.Status.Phase != corev1.PodRunning {
		return fmt.Errorf("unable to forward port because pod is not running. Current status=%v", pod.Status.Phase)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	returnCtx, returnCtxCancel := context.WithCancel(ctx)
	defer returnCtxCancel()

	go func() {
		select {
		case <-signals:
		case <-returnCtx.Done():
		}
		if o.StopChannel != nil {
			close(o.StopChannel)
		}
	}()

	req := o.RESTClient.Post().
		Resource("pods").
		Namespace(o.Namespace).
		Name(pod.Name).
		SubResource("portforward")

	return o.PortForwarder.ForwardPorts("POST", req.URL(), *o)
}
