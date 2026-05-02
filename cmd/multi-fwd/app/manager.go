/*
Copyright 2026 The Kubernetes Authors.

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

package app

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

const (
	backoffMin             = 5 * time.Second
	backoffMax             = 60 * time.Second
	backoffStableThreshold = 5 * time.Minute
)

// runner is the function the manager calls per restart cycle. Stored on the
// Manager so tests can inject a fake.
type runner func(ctx context.Context, fc ForwardConfig) error

// Manager supervises one goroutine per ForwardConfig. The inner goroutine
// runs kubectl's resilient retry loop, which already handles transient
// reconnects; this outer layer exists only to:
//   - recover from panics so one bad forward can't take down the daemon
//   - restart with backoff if RunPortForwardContext returns a terminal error
//     before ctx is canceled (e.g. MaxRetries exceeded, fatal API error)
type Manager struct {
	cfg *Config

	// run is the per-iteration runner. nil ⇒ use the production runForward.
	// Tests substitute a fake so they don't need a live cluster.
	run runner
}

// NewManager returns a manager bound to cfg.
func NewManager(cfg *Config) *Manager { return &Manager{cfg: cfg} }

// Run blocks until ctx is canceled, then waits for all forwards to drain.
// Returns the context's error.
func (m *Manager) Run(ctx context.Context) error {
	logger := klog.FromContext(ctx)

	var wg sync.WaitGroup
	for _, fc := range m.cfg.Forwards {
		fc := fc
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.superviseForward(ctx, fc)
		}()
	}

	logger.Info("multi-fwd running", "forwards", len(m.cfg.Forwards))
	<-ctx.Done()
	logger.Info("shutdown requested; waiting for forwards to drain")
	wg.Wait()
	logger.Info("multi-fwd stopped")
	return ctx.Err()
}

// superviseForward runs the per-forward runner in a loop with capped
// exponential backoff. Exits only when ctx is canceled. Resets backoff after
// a forward stayed up past backoffStableThreshold so a months-healthy forward
// isn't punished with the cap on its first restart.
func (m *Manager) superviseForward(ctx context.Context, fc ForwardConfig) {
	logger := klog.FromContext(ctx).WithValues("forward", fc.Name)
	backoff := backoffMin

	for {
		if ctx.Err() != nil {
			return
		}
		started := time.Now()
		err := m.runWithRecover(ctx, fc)
		ranFor := time.Since(started)
		if ctx.Err() != nil {
			return
		}

		// Apply this iteration's stable-reset before sleeping.
		backoff = backoffResetIfStable(backoff, ranFor)

		if err != nil {
			logger.Error(err, "forward exited with error; restarting", "ran_for", ranFor.String(), "backoff", backoff.String())
		} else {
			logger.Info("forward exited cleanly; restarting", "ran_for", ranFor.String(), "backoff", backoff.String())
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		// Grow toward the cap for next iteration.
		backoff = backoffGrow(backoff)
	}
}

// runWithRecover wraps the runner with a panic recover so a panic in one
// forward (or in the upstream library) doesn't kill the whole daemon.
func (m *Manager) runWithRecover(ctx context.Context, fc ForwardConfig) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in forward: %v", r)
		}
	}()
	r := m.run
	if r == nil {
		r = runForward
	}
	return r(ctx, fc)
}

// backoffResetIfStable returns backoffMin when the prior run lasted at least
// backoffStableThreshold; otherwise returns prev unchanged. Pure for tests.
func backoffResetIfStable(prev, ranFor time.Duration) time.Duration {
	if ranFor >= backoffStableThreshold {
		return backoffMin
	}
	return prev
}

// backoffGrow doubles toward backoffMax, with a floor at backoffMin for safety.
// Pure for tests.
func backoffGrow(prev time.Duration) time.Duration {
	next := prev * 2
	if next < backoffMin {
		return backoffMin
	}
	if next > backoffMax {
		return backoffMax
	}
	return next
}
