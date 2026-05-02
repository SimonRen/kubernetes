/*
Copyright 2024 The Kubernetes Authors.

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
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	kubefake "k8s.io/client-go/kubernetes/fake"
	restclient "k8s.io/client-go/rest"
	fakerest "k8s.io/client-go/rest/fake"
	"k8s.io/client-go/tools/portforward"
)

var restConfigStub = restclient.Config{Host: "https://example.test"}

// --- Test doubles -----------------------------------------------------------

// scriptedPortForwarder returns a sequence of errors from ForwardPorts. Each
// call consumes the next entry; the last entry is reused if the sequence is
// exhausted. It optionally signals iterReady before returning so tests can
// drive the "stable connection" reset path. It also captures the resolved
// o.Ports per call so tests can assert port re-resolution after pod switch.
type scriptedPortForwarder struct {
	mu sync.Mutex

	errs       []error
	calls      int
	signalReady bool
	holdFor    time.Duration

	capturedPorts [][]string
	capturedPods  []string

	bound []portforward.ForwardedPort
}

func (s *scriptedPortForwarder) ForwardPorts(method string, _ *url.URL, opts PortForwardOptions) error {
	s.mu.Lock()
	idx := s.calls
	s.calls++
	s.capturedPorts = append(s.capturedPorts, append([]string(nil), opts.Ports...))
	s.capturedPods = append(s.capturedPods, opts.PodName)
	var err error
	if len(s.errs) == 0 {
		err = nil
	} else if idx >= len(s.errs) {
		err = s.errs[len(s.errs)-1]
	} else {
		err = s.errs[idx]
	}
	signal := s.signalReady
	hold := s.holdFor
	s.mu.Unlock()

	if signal && opts.ReadyChannel != nil {
		// Use a non-blocking close — the channel is per-attempt and unique.
		close(opts.ReadyChannel)
	}
	if hold > 0 {
		select {
		case <-opts.StopChannel:
		case <-time.After(hold):
		}
	}
	return err
}

func (s *scriptedPortForwarder) GetBoundPorts() []portforward.ForwardedPort {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]portforward.ForwardedPort(nil), s.bound...)
}

func (s *scriptedPortForwarder) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *scriptedPortForwarder) capturedPortsCopy() [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]string, len(s.capturedPorts))
	for i, p := range s.capturedPorts {
		out[i] = append([]string(nil), p...)
	}
	return out
}

func makePod(name string, namedPort string, containerPort int32) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: "c",
				Ports: []corev1.ContainerPort{{
					Name:          namedPort,
					ContainerPort: containerPort,
				}},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	return pod
}

func makeRESTClient() *fakerest.RESTClient {
	return &fakerest.RESTClient{
		GroupVersion: schema.GroupVersion{Group: "", Version: "v1"},
	}
}

// --- isRetriableError -------------------------------------------------------

func TestIsRetriableError(t *testing.T) {
	wrappedNotFound := apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, "x")
	wrappedTimeout := apierrors.NewServerTimeout(schema.GroupResource{Resource: "pods"}, "x", 1)
	wrappedTooMany := apierrors.NewTooManyRequestsError("slow down")
	wrappedUnavail := apierrors.NewServiceUnavailable("upstream down")
	wrappedInternal := apierrors.NewInternalError(errors.New("boom"))

	type tc struct {
		name          string
		err           error
		allowReselect bool
		want          bool
	}
	cases := []tc{
		{name: "nil", err: nil, want: false},
		{name: "context.Canceled", err: context.Canceled, want: false},
		{name: "context.DeadlineExceeded", err: context.DeadlineExceeded, want: false},
		{name: "wrapped Canceled", err: fmt.Errorf("wrapper: %w", context.Canceled), want: false},

		{name: "ErrLostConnectionToPod", err: portforward.ErrLostConnectionToPod, want: true},
		{name: "wrapped ErrLostConnectionToPod", err: fmt.Errorf("conn died: %w", portforward.ErrLostConnectionToPod), want: true},

		{name: "errPodNotRunning", err: errPodNotRunning, want: true},
		{name: "wrapped errPodNotRunning", err: fmt.Errorf("%w (status: Pending)", errPodNotRunning), want: true},

		{name: "io.EOF", err: io.EOF, want: true},
		{name: "io.ErrUnexpectedEOF", err: io.ErrUnexpectedEOF, want: true},

		{name: "ECONNRESET", err: syscall.ECONNRESET, want: true},
		{name: "ECONNREFUSED", err: syscall.ECONNREFUSED, want: true},
		{name: "EPIPE", err: syscall.EPIPE, want: true},
		{name: "wrapped ECONNREFUSED", err: &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, want: true},

		{name: "net.Error timeout", err: timeoutNetError{}, want: true},

		{name: "apiserver ServerTimeout", err: wrappedTimeout, want: true},
		{name: "apiserver TooManyRequests", err: wrappedTooMany, want: true},
		{name: "apiserver ServiceUnavailable", err: wrappedUnavail, want: true},
		{name: "apiserver InternalError", err: wrappedInternal, want: true},

		{name: "NotFound + reselect off", err: wrappedNotFound, allowReselect: false, want: false},
		{name: "NotFound + reselect on", err: wrappedNotFound, allowReselect: true, want: true},

		{name: "broken pipe substring", err: errors.New("write tcp: broken pipe"), want: true},
		{name: "use of closed network connection", err: errors.New("read: use of closed network connection"), want: true},
		{name: "no such host", err: errors.New("dial: no such host"), want: true},

		{name: "unrelated forbidden", err: errors.New("forbidden by policy"), want: false},
		{name: "user pod-running-timeout", err: errors.New("timed out waiting for pod-running"), want: false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := isRetriableError(c.err, c.allowReselect)
			if got != c.want {
				t.Fatalf("isRetriableError(%v, allowReselect=%v) = %v, want %v",
					c.err, c.allowReselect, got, c.want)
			}
		})
	}
}

type timeoutNetError struct{}

func (timeoutNetError) Error() string   { return "i/o timeout" }
func (timeoutNetError) Timeout() bool   { return true }
func (timeoutNetError) Temporary() bool { return false }

// --- validateRetryConfig ----------------------------------------------------

func TestValidateRetryConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  RetryConfig
		ok   bool
	}{
		{"defaults+enabled", RetryConfig{Enabled: true, InitialDelay: time.Second, MaxDelay: 10 * time.Second, MaxRetries: 0}, true},
		{"zero initial", RetryConfig{Enabled: true, InitialDelay: 0, MaxDelay: time.Second}, false},
		{"negative initial", RetryConfig{Enabled: true, InitialDelay: -time.Second, MaxDelay: time.Second}, false},
		{"zero max", RetryConfig{Enabled: true, InitialDelay: time.Second, MaxDelay: 0}, false},
		{"max < initial", RetryConfig{Enabled: true, InitialDelay: 10 * time.Second, MaxDelay: time.Second}, false},
		{"negative max-retries", RetryConfig{Enabled: true, InitialDelay: time.Second, MaxDelay: time.Second, MaxRetries: -1}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateRetryConfig(c.cfg)
			if (err == nil) != c.ok {
				t.Fatalf("validateRetryConfig(%+v) err=%v want ok=%v", c.cfg, err, c.ok)
			}
		})
	}
}

// --- applyPinnedLocalPorts --------------------------------------------------

func TestApplyPinnedLocalPorts(t *testing.T) {
	cases := []struct {
		name     string
		resolved []string
		pinned   []string
		want     []string
	}{
		{"no pinned", []string{":8080", "80:9090"}, nil, []string{":8080", "80:9090"}},
		{"pin first only", []string{":8080", ":9090"}, []string{"54321", ""}, []string{"54321:8080", ":9090"}},
		{"pin overrides explicit local", []string{"80:8080"}, []string{"54321"}, []string{"54321:8080"}},
		{"pinned shorter than resolved", []string{":80", ":443"}, []string{"54321"}, []string{"54321:80", ":443"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := applyPinnedLocalPorts(c.resolved, c.pinned)
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("got %v want %v", got, c.want)
			}
		})
	}
}

// --- runResilientPortForward ------------------------------------------------

// podObjects converts a typed pod slice into a runtime.Object slice for the
// fake clientset constructor.
func podObjects(pods []*corev1.Pod) []runtime.Object {
	out := make([]runtime.Object, len(pods))
	for i, p := range pods {
		out[i] = p
	}
	return out
}

// newResilientOpts builds a PortForwardOptions wired for retry-loop tests.
// pods are pre-seeded into a fake clientset; ports is the raw port arg list.
func newResilientOpts(t *testing.T, pf portForwarder, pods []*corev1.Pod, podName string, ports []string, retry RetryConfig, podSelector labels.Selector) *PortForwardOptions {
	t.Helper()
	clientset := kubefake.NewSimpleClientset(podObjects(pods)...)
	return &PortForwardOptions{
		Namespace:     "test",
		PodName:       podName,
		PodClient:     clientset.CoreV1(),
		RESTClient:    makeRESTClient(),
		Config:        &restConfigStub,
		Address:       []string{"localhost"},
		Ports:         ports,
		PortForwarder: pf,
		StopChannel:   make(chan struct{}, 1),
		ReadyChannel:  make(chan struct{}),
		RetryConfig:   retry,
		RawPorts:      append([]string(nil), ports...),
		PodSelector:   podSelector,
	}
}

func TestRunResilient_NonRetriableExitsImmediately(t *testing.T) {
	pf := &scriptedPortForwarder{errs: []error{errors.New("forbidden")}}
	opts := newResilientOpts(t, pf, []*corev1.Pod{makePod("foo", "http", 80)}, "foo",
		[]string{"80"}, RetryConfig{Enabled: true, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond}, nil)

	err := opts.runResilientPortForward(context.Background())
	if err == nil || err.Error() != "forbidden" {
		t.Fatalf("expected forbidden error, got %v", err)
	}
	if got := pf.callCount(); got != 1 {
		t.Fatalf("expected 1 ForwardPorts call, got %d", got)
	}
}

func TestRunResilient_RespectsMaxRetries(t *testing.T) {
	pf := &scriptedPortForwarder{errs: []error{portforward.ErrLostConnectionToPod}}
	opts := newResilientOpts(t, pf, []*corev1.Pod{makePod("foo", "http", 80)}, "foo",
		[]string{"80"}, RetryConfig{Enabled: true, InitialDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond, MaxRetries: 3}, nil)

	err := opts.runResilientPortForward(context.Background())
	if err == nil || !errors.Is(err, portforward.ErrLostConnectionToPod) {
		t.Fatalf("expected wrapped ErrLostConnectionToPod, got %v", err)
	}
	// 1 initial attempt + 3 retries = 4 ForwardPorts calls.
	if got := pf.callCount(); got != 4 {
		t.Fatalf("expected 4 ForwardPorts calls, got %d", got)
	}
}

func TestRunResilient_ContextCancelStopsLoopWithinBound(t *testing.T) {
	// Forwarder hangs on stop channel; cancelling ctx must propagate via the
	// internal stop and unblock the call.
	pf := &scriptedPortForwarder{
		errs:    []error{nil},
		holdFor: 5 * time.Second,
	}
	opts := newResilientOpts(t, pf, []*corev1.Pod{makePod("foo", "http", 80)}, "foo",
		[]string{"80"}, RetryConfig{Enabled: true, InitialDelay: 10 * time.Millisecond, MaxDelay: 10 * time.Millisecond}, nil)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- opts.runResilientPortForward(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runResilientPortForward did not return within 2s after cancel")
	}
}

func TestRunResilient_PodSwitchRefreshesNamedPortMapping(t *testing.T) {
	// Service in front of pods labelled app=x. Pod A is gone (rollout
	// already completed). Pod B exposes http=9090. The retry loop must
	// classify pods.Get("pod-a") as IsNotFound + allowReselect → retriable,
	// re-select pod-b, and re-resolve named ports against pod-b. The
	// observed port arg on the resulting ForwardPorts call must be 80:9090.
	podB := makePod("pod-b", "http", 9090)
	podB.Labels = map[string]string{"app": "x"}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "test"},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "x"},
			Ports: []corev1.ServicePort{{
				Port:       80,
				TargetPort: intstr.FromString("http"),
			}},
		},
	}

	pf := &scriptedPortForwarder{errs: []error{nil}}

	clientset := kubefake.NewSimpleClientset(podB, svc)
	selector, _ := labels.Parse("app=x")

	opts := &PortForwardOptions{
		Namespace:      "test",
		PodName:        "pod-a", // stale; will fail with NotFound on first Get
		PodClient:      clientset.CoreV1(),
		RESTClient:     makeRESTClient(),
		Config:         &restConfigStub,
		Address:        []string{"localhost"},
		Ports:          []string{"80:8080"}, // stale resolution from Complete()
		RawPorts:       []string{"80"},
		PortForwarder:  pf,
		StopChannel:    make(chan struct{}, 1),
		ReadyChannel:   make(chan struct{}),
		RetryConfig:    RetryConfig{Enabled: true, InitialDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond, MaxRetries: 2},
		OriginalObject: svc,
		PodSelector:    selector,
	}

	if err := opts.runResilientPortForward(context.Background()); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	captured := pf.capturedPortsCopy()
	if len(captured) != 1 {
		t.Fatalf("expected exactly 1 ForwardPorts call after reselect, got %d (%v)", len(captured), captured)
	}
	if got := captured[0]; len(got) != 1 || got[0] != "80:9090" {
		t.Fatalf("post-reselect ports: got %v want [80:9090]", got)
	}
}

func TestRunResilient_PinsLocalPortAcrossReconnects(t *testing.T) {
	// First attempt establishes ready and reports bound local port 54321.
	// Second attempt must reuse 54321 instead of empty local part.
	podA := makePod("pod-a", "http", 8080)
	podA.Labels = map[string]string{"app": "x"}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "test"},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "x"},
			Ports: []corev1.ServicePort{{
				Port:       80,
				TargetPort: intstr.FromString("http"),
			}},
		},
	}

	pf := &scriptedPortForwarder{
		errs:        []error{portforward.ErrLostConnectionToPod, nil},
		signalReady: true,
		bound:       []portforward.ForwardedPort{{Local: 54321, Remote: 8080}},
	}

	clientset := kubefake.NewSimpleClientset(podA, svc)
	sel, _ := labels.Parse("app=x")

	opts := &PortForwardOptions{
		Namespace:      "test",
		PodName:        "pod-a",
		PodClient:      clientset.CoreV1(),
		RESTClient:     makeRESTClient(),
		Config:         &restConfigStub,
		Address:        []string{"localhost"},
		Ports:          []string{":8080"},
		RawPorts:       []string{":80"},
		PortForwarder:  pf,
		StopChannel:    make(chan struct{}, 1),
		ReadyChannel:   make(chan struct{}),
		RetryConfig:    RetryConfig{Enabled: true, InitialDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond, MaxRetries: 1},
		OriginalObject: svc,
		PodSelector:    sel,
	}

	if err := opts.runResilientPortForward(context.Background()); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	captured := pf.capturedPortsCopy()
	if len(captured) < 2 {
		t.Fatalf("expected 2 attempts, got %d", len(captured))
	}
	// First attempt uses raw resolved ports (":8080"). Second must be pinned.
	if got := captured[1]; len(got) != 1 || got[0] != "54321:8080" {
		t.Fatalf("expected second attempt to pin local port; got %v want [54321:8080]", got)
	}
}

func TestRunResilient_StableConnectionResetGatedOnReady(t *testing.T) {
	// Forwarder fails twice: once after a "stable" period (ready + past
	// threshold), then again immediately. The second failure should still
	// see retryCount==1 (not 2), because the first stable run reset
	// counters. Reduce the threshold so the test is fast.
	prev := stableConnectionThreshold
	stableConnectionThreshold = 20 * time.Millisecond
	t.Cleanup(func() { stableConnectionThreshold = prev })

	pf := &readySignalingForwarder{
		readyAfter: 0,
		runFor:     30 * time.Millisecond, // > stableConnectionThreshold
		errSeq:     []error{portforward.ErrLostConnectionToPod, errors.New("non-retriable")},
	}
	opts := newResilientOpts(t, pf, []*corev1.Pod{makePod("foo", "http", 80)}, "foo",
		[]string{"80"}, RetryConfig{Enabled: true, InitialDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond, MaxRetries: 1}, nil)

	// MaxRetries=1: if reset doesn't happen we'd see "max retries (1) exceeded".
	// If reset does happen we get "non-retriable" at retryCount=1 (allowed) which
	// bubbles up directly.
	err := opts.runResilientPortForward(context.Background())
	if err == nil || err.Error() != "non-retriable" {
		t.Fatalf("expected backoff/retry to reset between stable attempts; got %v", err)
	}
	if got := pf.calls.Load(); got != 2 {
		t.Fatalf("expected 2 attempts (reset between them), got %d", got)
	}
}

// readySignalingForwarder closes ReadyChannel after readyAfter, then either
// blocks on Stop until runFor elapses or returns the next scripted error.
type readySignalingForwarder struct {
	readyAfter time.Duration
	runFor     time.Duration
	errSeq     []error
	calls      atomic.Int32
}

func (r *readySignalingForwarder) ForwardPorts(_ string, _ *url.URL, opts PortForwardOptions) error {
	idx := int(r.calls.Add(1)) - 1
	if r.readyAfter > 0 {
		time.Sleep(r.readyAfter)
	}
	if opts.ReadyChannel != nil {
		close(opts.ReadyChannel)
	}
	timer := time.NewTimer(r.runFor)
	defer timer.Stop()
	select {
	case <-opts.StopChannel:
	case <-timer.C:
	}
	if idx < len(r.errSeq) {
		return r.errSeq[idx]
	}
	return r.errSeq[len(r.errSeq)-1]
}

// --- reselectPod ------------------------------------------------------------

func TestReselectPod_RespectsContextCancel(t *testing.T) {
	clientset := kubefake.NewSimpleClientset()
	sel, _ := labels.Parse("app=x")
	opts := &PortForwardOptions{
		Namespace:   "test",
		PodClient:   clientset.CoreV1(),
		PodSelector: sel,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	start := time.Now()
	_, err := opts.reselectPod(ctx)
	elapsed := time.Since(start)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if elapsed > time.Second {
		t.Fatalf("reselectPod blocked %s; expected prompt return", elapsed)
	}
}
