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
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/kubectl/pkg/polymorphichelpers"
	"k8s.io/kubectl/pkg/util/podutils"
)

// RetryConfig holds retry-related configuration for resilient port-forwarding
type RetryConfig struct {
	// Enabled enables automatic reconnection when the connection is lost
	Enabled bool
	// InitialDelay is the initial delay before retrying a failed connection
	InitialDelay time.Duration
	// MaxDelay is the maximum delay between retry attempts (exponential backoff cap)
	MaxDelay time.Duration
	// HealthCheckPeriod is the interval for connection health checks
	HealthCheckPeriod time.Duration
	// MaxRetries is the maximum number of retry attempts (0 = infinite)
	MaxRetries int
}

// DefaultRetryConfig returns the default retry configuration
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		Enabled:           false,
		InitialDelay:      5 * time.Second,
		MaxDelay:          60 * time.Second,
		HealthCheckPeriod: 10 * time.Second,
		MaxRetries:        0,
	}
}

// runResilientPortForward implements the retry loop for port-forwarding.
// It automatically reconnects when connections are lost and re-selects pods
// for service-based port-forwards when the original pod becomes unavailable.
func (o *PortForwardOptions) runResilientPortForward(ctx context.Context) error {
	// Setup single signal handler for entire retry session
	// Handle both SIGINT (Ctrl+C) and SIGTERM (container shutdown)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		select {
		case <-signals:
			cancel()
		case <-runCtx.Done():
		}
	}()

	backoff := wait.Backoff{
		Duration: o.RetryConfig.InitialDelay,
		Factor:   2.0,
		Jitter:   0.1,
		Cap:      o.RetryConfig.MaxDelay,
	}

	retryCount := 0
	for {
		select {
		case <-runCtx.Done():
			return runCtx.Err()
		default:
		}

		// Create fresh channels for each attempt
		// This is critical - reusing closed channels will cause panic
		o.StopChannel = make(chan struct{}, 1)
		o.ReadyChannel = make(chan struct{})

		// Per-iteration context to control the goroutine lifecycle
		iterCtx, iterCancel := context.WithCancel(runCtx)

		// Use sync.Once to prevent double-close panic on channel
		var closeOnce sync.Once
		stopChan := o.StopChannel // Capture in local var for goroutine

		// Wire context cancellation to stop channel
		// This goroutine exits when iterCtx is cancelled (either by runCtx or iterCancel)
		go func() {
			<-iterCtx.Done()
			closeOnce.Do(func() {
				close(stopChan)
			})
		}()

		startTime := time.Now()
		err := o.attemptSingleForward(iterCtx)

		// Cancel iteration context to clean up the goroutine
		iterCancel()

		if err == nil {
			return nil // Clean exit (shouldn't happen normally)
		}

		// Reset backoff and retry count if connection was stable for a reasonable period (> 30s)
		if time.Since(startTime) > 30*time.Second {
			backoff = wait.Backoff{
				Duration: o.RetryConfig.InitialDelay,
				Factor:   2.0,
				Jitter:   0.1,
				Cap:      o.RetryConfig.MaxDelay,
			}
			retryCount = 0 // Reset retry count after stable connection
		}

		if !isRetriableError(err) {
			return err
		}

		retryCount++
		if o.RetryConfig.MaxRetries > 0 && retryCount >= o.RetryConfig.MaxRetries {
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

		// Re-select pod for services/deployments
		if o.PodSelector != nil {
			newPod, reselectErr := o.reselectPod(runCtx)
			if reselectErr != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to re-select pod: %v (will retry with current pod)\n", reselectErr)
			} else if newPod != o.PodName {
				fmt.Fprintf(os.Stderr, "Switching to pod: %s\n", newPod)
				o.PodName = newPod
			}
		}
	}
}

// attemptSingleForward runs one port-forward session
func (o *PortForwardOptions) attemptSingleForward(ctx context.Context) error {
	pod, err := o.PodClient.Pods(o.Namespace).Get(ctx, o.PodName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	if pod.Status.Phase != corev1.PodRunning {
		return fmt.Errorf("pod is not running (status: %s)", pod.Status.Phase)
	}

	req := o.RESTClient.Post().
		Resource("pods").
		Namespace(o.Namespace).
		Name(pod.Name).
		SubResource("portforward")

	return o.PortForwarder.ForwardPorts("POST", req.URL(), *o)
}

// reselectPod finds a new running pod matching the selector
func (o *PortForwardOptions) reselectPod(ctx context.Context) (string, error) {
	if o.PodSelector == nil {
		return o.PodName, nil
	}

	sortBy := func(pods []*corev1.Pod) sort.Interface {
		return sort.Reverse(podutils.ActivePods(pods))
	}

	pod, _, err := polymorphichelpers.GetFirstPod(
		o.PodClient,
		o.Namespace,
		o.PodSelector.String(),
		30*time.Second,
		sortBy,
	)
	if err != nil {
		return "", err
	}
	return pod.Name, nil
}

// isRetriableError determines if an error should trigger a retry
func isRetriableError(err error) bool {
	if err == nil {
		return false
	}

	// Check for sentinel error (use errors.Is for wrapped errors)
	if errors.Is(err, portforward.ErrLostConnectionToPod) {
		return true
	}

	// Don't retry on context cancellation (user initiated)
	if errors.Is(err, context.Canceled) {
		return false
	}

	errStr := strings.ToLower(err.Error())
	patterns := []string{
		"connection refused",
		"connection reset",
		"eof",
		"broken pipe",
		"timeout",
		"pod is not running",
		"no such host",
		"network is unreachable",
		"i/o timeout",
		"connection timed out",
		"use of closed network connection",
		"transport is closing",
	}
	for _, p := range patterns {
		if strings.Contains(errStr, p) {
			return true
		}
	}
	return false
}
