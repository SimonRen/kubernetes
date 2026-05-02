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
	"math"
	"net"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/kubectl/pkg/polymorphichelpers"
	"k8s.io/kubectl/pkg/util/podutils"
)

// errPodNotRunning is returned by attemptSingleForward when the selected pod
// is not in the Running phase. It is treated as retriable and triggers
// pod re-selection when a label selector is available.
var errPodNotRunning = errors.New("pod is not running")

// reselectPodTimeout bounds a single pod-reselection call. We keep it short
// and let the retry-loop backoff drive any longer "no pods yet" wait.
const reselectPodTimeout = 5 * time.Second

// stableConnectionThreshold is how long an attempt must run AFTER the ready
// signal before backoff and retry counters reset. Wall-clock alone is not
// enough — a slow-failing dial would otherwise reset counters indefinitely.
// Declared as a var to allow targeted reduction in unit tests.
var stableConnectionThreshold = 30 * time.Second

// RetryConfig holds retry-related configuration for resilient port-forwarding.
type RetryConfig struct {
	// Enabled enables automatic reconnection when the connection is lost.
	Enabled bool
	// InitialDelay is the initial backoff before the first retry.
	InitialDelay time.Duration
	// MaxDelay caps the exponential backoff between retry attempts.
	MaxDelay time.Duration
	// MaxRetries caps the number of reconnect attempts after a failure.
	// Zero means unlimited.
	MaxRetries int
}

// DefaultRetryConfig returns the default retry configuration. Retry is
// disabled by default to preserve historic single-shot behavior.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		Enabled:      false,
		InitialDelay: 5 * time.Second,
		MaxDelay:     60 * time.Second,
		MaxRetries:   0,
	}
}

// validateRetryConfig is invoked from PortForwardOptions.Validate() when
// Enabled is true. It rejects configurations that would yield a tight retry
// loop or a useless backoff cap.
func validateRetryConfig(c RetryConfig) error {
	if c.InitialDelay <= 0 {
		return fmt.Errorf("--retry-delay must be greater than zero")
	}
	if c.MaxDelay <= 0 {
		return fmt.Errorf("--max-retry-delay must be greater than zero")
	}
	if c.MaxDelay < c.InitialDelay {
		return fmt.Errorf("--max-retry-delay (%s) must be greater than or equal to --retry-delay (%s)", c.MaxDelay, c.InitialDelay)
	}
	if c.MaxRetries < 0 {
		return fmt.Errorf("--max-retries must be greater than or equal to zero (0 = unlimited)")
	}
	return nil
}

// boundPortReporter is implemented by portForwarder implementations that can
// report the locally-bound ports after a successful Ready signal. The retry
// loop uses this to pin local ports across reconnects.
type boundPortReporter interface {
	GetBoundPorts() []portforward.ForwardedPort
}

// runResilientPortForward runs the resilient retry loop. It owns:
//   - signal handling (SIGINT/SIGTERM)
//   - mapping caller-supplied StopChannel/ReadyChannel onto per-attempt internals
//   - exponential backoff with jitter, gated by the ready signal
//   - pod re-selection for service/deployment targets
//   - per-attempt port re-resolution and pinned local ports
func (o *PortForwardOptions) runResilientPortForward(ctx context.Context) error {
	callerStop := o.StopChannel
	callerReady := o.ReadyChannel

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	go func() {
		select {
		case <-signals:
			cancel()
		case <-runCtx.Done():
		}
	}()

	// If the caller passed a StopChannel, treat its closure as a cancel.
	// Translate it onto runCtx instead of forwarding it to ForwardPorts —
	// each attempt uses its own internal stop channel.
	if callerStop != nil {
		go func() {
			select {
			case <-callerStop:
				cancel()
			case <-runCtx.Done():
			}
		}()
	}

	// Close the caller's ReadyChannel (if any) exactly once on the first
	// attempt that reaches ready.
	var readyOnce sync.Once
	signalCallerReady := func() {
		readyOnce.Do(func() {
			if callerReady != nil {
				close(callerReady)
			}
		})
	}

	// Pin local ports across reconnects. Index aligns with o.RawPorts.
	var pinMu sync.Mutex
	var pinnedLocal []string
	pinFromBound := func(bound []portforward.ForwardedPort) {
		if len(bound) == 0 {
			return
		}
		pinMu.Lock()
		defer pinMu.Unlock()
		if pinnedLocal == nil {
			pinnedLocal = make([]string, len(o.RawPorts))
		}
		for i, p := range bound {
			if i < len(pinnedLocal) && pinnedLocal[i] == "" {
				pinnedLocal[i] = strconv.Itoa(int(p.Local))
			}
		}
	}
	snapshotPinned := func() []string {
		pinMu.Lock()
		defer pinMu.Unlock()
		if pinnedLocal == nil {
			return nil
		}
		return append([]string(nil), pinnedLocal...)
	}

	newBackoff := func() wait.Backoff {
		return wait.Backoff{
			Duration: o.RetryConfig.InitialDelay,
			Factor:   2.0,
			Jitter:   0.1,
			Cap:      o.RetryConfig.MaxDelay,
			Steps:    math.MaxInt32,
		}
	}
	backoff := newBackoff()
	retryCount := 0

	for {
		if err := runCtx.Err(); err != nil {
			return err
		}

		iterStop := make(chan struct{})
		iterReady := make(chan struct{})

		iterCtx, iterCancel := context.WithCancel(runCtx)
		var stopOnce sync.Once
		go func() {
			<-iterCtx.Done()
			stopOnce.Do(func() { close(iterStop) })
		}()

		// Watch this attempt's ready signal: announce to the caller, pin
		// local ports, and remember readiness for the backoff-reset gate.
		var ready atomic.Bool
		readyDone := make(chan struct{})
		go func() {
			defer close(readyDone)
			select {
			case <-iterReady:
				ready.Store(true)
				signalCallerReady()
				if reporter, ok := o.PortForwarder.(boundPortReporter); ok {
					pinFromBound(reporter.GetBoundPorts())
				}
			case <-iterCtx.Done():
			}
		}()

		startTime := time.Now()
		err := o.attemptSingleForward(iterCtx, iterStop, iterReady, snapshotPinned())
		iterCancel()
		<-readyDone

		// If we ran past the threshold AFTER going ready, treat this as a
		// stable session — reset retry counters/backoff.
		if ready.Load() && time.Since(startTime) >= stableConnectionThreshold {
			backoff = newBackoff()
			retryCount = 0
		}

		// Caller cancelled (signal, parent ctx, or callerStop).
		if runCtx.Err() != nil {
			return runCtx.Err()
		}

		if err == nil {
			return nil
		}

		if !isRetriableError(err, o.PodSelector != nil) {
			return err
		}

		retryCount++
		if o.RetryConfig.MaxRetries > 0 && retryCount > o.RetryConfig.MaxRetries {
			return fmt.Errorf("max retries (%d) exceeded: %w", o.RetryConfig.MaxRetries, err)
		}

		delay := backoff.Step()
		fmt.Fprintf(os.Stderr, "Connection lost: %v. Reconnecting in %v (attempt %d)...\n",
			err, delay.Round(time.Second), retryCount)

		select {
		case <-runCtx.Done():
			return runCtx.Err()
		case <-time.After(delay):
		}

		if o.PodSelector != nil {
			newPod, reselectErr := o.reselectPod(runCtx)
			switch {
			case reselectErr != nil && errors.Is(reselectErr, context.Canceled):
				return runCtx.Err()
			case reselectErr != nil:
				fmt.Fprintf(os.Stderr, "Warning: failed to re-select pod: %v (will retry with current pod)\n", reselectErr)
			case newPod != "" && newPod != o.PodName:
				fmt.Fprintf(os.Stderr, "Switching to pod: %s\n", newPod)
				o.PodName = newPod
			}
		}
	}
}

// attemptSingleForward runs one port-forward attempt with internal channels
// (so the retry loop can recycle them safely without touching the caller's
// PortForwardOptions.StopChannel / ReadyChannel). It re-resolves named ports
// against the currently selected pod and applies pinned local ports.
func (o *PortForwardOptions) attemptSingleForward(
	ctx context.Context,
	stop, ready chan struct{},
	pinned []string,
) error {
	pod, err := o.PodClient.Pods(o.Namespace).Get(ctx, o.PodName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if pod.Status.Phase != corev1.PodRunning {
		return fmt.Errorf("%w (status: %s)", errPodNotRunning, pod.Status.Phase)
	}

	if err := o.resolvePortsForPod(pod, pinned); err != nil {
		return err
	}

	req := o.RESTClient.Post().
		Resource("pods").
		Namespace(o.Namespace).
		Name(pod.Name).
		SubResource("portforward")

	attempt := *o
	attempt.StopChannel = stop
	attempt.ReadyChannel = ready
	return o.PortForwarder.ForwardPorts("POST", req.URL(), attempt)
}

// resolvePortsForPod refreshes o.Ports from o.RawPorts against the supplied
// pod and applies pinned local ports. No-op if RawPorts is empty (single-shot
// callers leave it unset; Complete() populated o.Ports once).
func (o *PortForwardOptions) resolvePortsForPod(pod *corev1.Pod, pinned []string) error {
	if len(o.RawPorts) == 0 {
		return nil
	}
	var resolved []string
	var err error
	switch t := o.OriginalObject.(type) {
	case *corev1.Service:
		resolved, err = translateServicePortToTargetPort(o.RawPorts, *t, *pod)
	default:
		resolved, err = convertPodNamedPortToNumber(o.RawPorts, *pod)
	}
	if err != nil {
		return err
	}
	o.Ports = applyPinnedLocalPorts(resolved, pinned)
	return nil
}

// applyPinnedLocalPorts overwrites the local-port half of each resolved entry
// with the pinned local port if one exists at that index.
func applyPinnedLocalPorts(resolved, pinned []string) []string {
	if len(pinned) == 0 {
		return resolved
	}
	out := make([]string, len(resolved))
	for i, p := range resolved {
		if i < len(pinned) && pinned[i] != "" {
			_, remote := splitPort(p)
			out[i] = pinned[i] + ":" + remote
			continue
		}
		out[i] = p
	}
	return out
}

// reselectPod finds a Running pod matching o.PodSelector. The lookup runs in
// a goroutine so caller cancellation propagates without waiting for the
// upstream helper's hard timeout.
func (o *PortForwardOptions) reselectPod(ctx context.Context) (string, error) {
	if o.PodSelector == nil {
		return "", nil
	}

	type result struct {
		name string
		err  error
	}
	out := make(chan result, 1)
	go func() {
		sortBy := func(pods []*corev1.Pod) sort.Interface {
			return sort.Reverse(podutils.ActivePods(pods))
		}
		pod, _, err := polymorphichelpers.GetFirstPod(
			o.PodClient,
			o.Namespace,
			o.PodSelector.String(),
			reselectPodTimeout,
			sortBy,
		)
		if err != nil {
			out <- result{err: err}
			return
		}
		out <- result{name: pod.Name}
	}()

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case r := <-out:
		return r.name, r.err
	}
}

// isRetriableError reports whether err warrants another reconnect attempt.
// allowReselect lets selector-backed targets treat pod NotFound as retriable
// so the retry loop can re-select a replacement pod after rollouts.
func isRetriableError(err error, allowReselect bool) bool {
	if err == nil {
		return false
	}

	// Honor cancellation first — don't retry user/parent-initiated stops.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Sentinels we own.
	if errors.Is(err, portforward.ErrLostConnectionToPod) {
		return true
	}
	if errors.Is(err, errPodNotRunning) {
		return true
	}

	// Stream / I/O signals.
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	// Typed network errors.
	if errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ETIMEDOUT) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	// Apiserver-side transient errors.
	if apierrors.IsServerTimeout(err) ||
		apierrors.IsTooManyRequests(err) ||
		apierrors.IsInternalError(err) ||
		apierrors.IsServiceUnavailable(err) ||
		apierrors.IsTimeout(err) {
		return true
	}
	if allowReselect && apierrors.IsNotFound(err) {
		return true
	}

	// Last-resort substring fallback for SPDY/transport messages that don't
	// surface as typed errors. Kept narrow on purpose.
	errStr := strings.ToLower(err.Error())
	for _, p := range []string{
		"broken pipe",
		"use of closed network connection",
		"transport is closing",
		"no such host",
		"network is unreachable",
	} {
		if strings.Contains(errStr, p) {
			return true
		}
	}
	return false
}
