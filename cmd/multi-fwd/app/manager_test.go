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
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestBackoffResetIfStable(t *testing.T) {
	// A stable run resets to min regardless of how high prev got.
	if got := backoffResetIfStable(backoffMax, backoffStableThreshold+time.Second); got != backoffMin {
		t.Fatalf("stable run from max should reset to min; got %v", got)
	}
	// A short crash leaves prev unchanged.
	if got := backoffResetIfStable(20*time.Second, time.Second); got != 20*time.Second {
		t.Fatalf("short crash should not change prev; got %v", got)
	}
	// Exactly at threshold counts as stable.
	if got := backoffResetIfStable(40*time.Second, backoffStableThreshold); got != backoffMin {
		t.Fatalf("ranFor == threshold should reset; got %v", got)
	}
}

func TestBackoffGrow(t *testing.T) {
	if got := backoffGrow(backoffMin); got != 2*backoffMin {
		t.Fatalf("min*2 expected; got %v", got)
	}
	if got := backoffGrow(backoffMax); got != backoffMax {
		t.Fatalf("max should saturate; got %v", got)
	}
	if got := backoffGrow(40 * time.Second); got != backoffMax {
		t.Fatalf("80s clamps to max=%v; got %v", backoffMax, got)
	}
	if got := backoffGrow(0); got != backoffMin {
		t.Fatalf("zero floors at min=%v; got %v", backoffMin, got)
	}
}

// TestSuperviseForward_RespectCtxCancel verifies the supervisor exits when
// ctx cancels even if the runner is mid-call. It uses a runner that blocks
// on ctx.Done() so we don't depend on real cluster I/O.
func TestSuperviseForward_RespectCtxCancel(t *testing.T) {
	cfg := &Config{
		Forwards: []ForwardConfig{
			{Name: "blocker", Context: "c", Resource: "svc/x", Ports: []string{"5432"}},
		},
	}
	cfg.applyDefaults()

	var calls int32
	mgr := &Manager{
		cfg: cfg,
		run: func(ctx context.Context, fc ForwardConfig) error {
			atomic.AddInt32(&calls, 1)
			<-ctx.Done()
			return ctx.Err()
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = mgr.Run(ctx)
		close(done)
	}()

	// Give the goroutine a beat to enter the runner.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Manager.Run did not return after ctx cancel")
	}
	if atomic.LoadInt32(&calls) == 0 {
		t.Fatal("runner was never invoked")
	}
}

// TestSuperviseForward_PanicIsRecovered verifies a panic in the runner is
// caught and supervisor keeps looping. Drives the loop a few iterations,
// then cancels.
func TestSuperviseForward_PanicIsRecovered(t *testing.T) {
	cfg := &Config{
		Forwards: []ForwardConfig{
			{Name: "boomer", Context: "c", Resource: "svc/x", Ports: []string{"5432"}},
		},
	}
	cfg.applyDefaults()

	var calls int32
	mgr := &Manager{
		cfg: cfg,
		run: func(ctx context.Context, fc ForwardConfig) error {
			n := atomic.AddInt32(&calls, 1)
			if n == 1 {
				panic("synthetic panic")
			}
			// Subsequent runs return immediately with an error so the loop
			// races forward to the next backoff tick.
			return errors.New("done")
		},
	}

	// Run for up to 8s then cancel — should see >=2 calls (post-panic restart).
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	go func() { _ = mgr.Run(ctx) }()

	deadline := time.After(8 * time.Second)
	for {
		if atomic.LoadInt32(&calls) >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("only %d call(s); expected ≥2 (panic recovery)", atomic.LoadInt32(&calls))
		case <-time.After(50 * time.Millisecond):
		}
	}
}
