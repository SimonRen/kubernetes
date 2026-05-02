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
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/spf13/cobra"

	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/klog/v2"
	"k8s.io/kubectl/pkg/cmd/portforward"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
)

// completeRequestTimeout bounds individual API requests issued during
// PortForwardOptions.Complete (resource resolution, pod lookup, discovery).
// Without it, ConfigFlags defaults to "0" which means "no timeout" and a
// hung apiserver during Complete blocks the daemon's drain indefinitely.
// SPDY upgrade hijacks the connection, so this only affects pre-stream RTs.
const completeRequestTimeout = "30s"

// runForward runs a single port-forward to completion. It returns when ctx
// is canceled or when the underlying resilient retry loop in
// portforward.RunPortForwardContext bails out (max retries, terminal error).
//
// All retry / reconnect logic lives inside the upstream port-forward package;
// this function only sets up PortForwardOptions for one entry and bridges
// our context onto its non-context-aware Complete() call.
func runForward(ctx context.Context, fc ForwardConfig) error {
	logger := klog.FromContext(ctx).WithValues("forward", fc.Name)

	configFlags := genericclioptions.NewConfigFlags(false)
	if fc.Kubeconfig != "" {
		configFlags.KubeConfig = strPtr(fc.Kubeconfig)
	}
	configFlags.Context = strPtr(fc.Context)
	if fc.Namespace != "" {
		configFlags.Namespace = strPtr(fc.Namespace)
	}
	configFlags.Timeout = strPtr(completeRequestTimeout)

	factory := cmdutil.NewFactory(configFlags)

	// Synthetic command exists solely so Complete() can read the
	// --pod-running-timeout flag value via cmdutil.GetPodRunningTimeoutFlag.
	// FRAGILITY NOTE: if a future kubectl release adds a new
	// cmdutil.GetFlag*(cmd, "...") read inside Complete(), the helper calls
	// klog.Fatalf on the missing flag, which os.Exit(255)s past our
	// recover(). Track upstream Complete() changes when bumping kubernetes.
	syntheticCmd := &cobra.Command{Use: "multi-fwd-internal-" + fc.Name}
	cmdutil.AddPodRunningTimeoutFlag(syntheticCmd, fc.PodRunningTimeout.AsDuration())

	stdout := newLineWriter(fc.Name, "stdout")
	stderr := newLineWriter(fc.Name, "stderr")
	streams := genericiooptions.IOStreams{Out: stdout, ErrOut: stderr}

	opts := portforward.NewDefaultPortForwardOptions(streams)
	opts.Address = append([]string(nil), fc.Address...)

	if fc.Retry != nil && fc.Retry.IsEnabled() {
		opts.RetryConfig = portforward.RetryConfig{
			Enabled:      true,
			InitialDelay: fc.Retry.InitialDelay.AsDuration(),
			MaxDelay:     fc.Retry.MaxDelay.AsDuration(),
			MaxRetries:   fc.Retry.MaxRetries,
		}
	}

	args := append([]string{fc.Resource}, fc.Ports...)

	// Complete() does API discovery + resource builder calls that internally
	// use context.TODO. Wrap the call so ctx-cancel returns immediately even
	// if Complete is blocked on a slow apiserver. The Complete-goroutine then
	// drains in the background, bounded by ConfigFlags.Timeout.
	completeDone := make(chan error, 1)
	go func() { completeDone <- opts.Complete(factory, syntheticCmd, args) }()
	select {
	case err := <-completeDone:
		if err != nil {
			return fmt.Errorf("complete: %w", err)
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	if err := opts.Validate(); err != nil {
		return fmt.Errorf("validate: %w", err)
	}

	logger.Info("starting forward",
		"context", fc.Context,
		"namespace", opts.Namespace,
		"resource", fc.Resource,
		"ports", fc.Ports,
		"address", fc.Address,
		"retry", opts.RetryConfig.Enabled,
	)

	err := opts.RunPortForwardContext(ctx)

	// Flush any tail bytes the upstream code wrote without a newline.
	stdout.flush()
	stderr.flush()
	return err
}

func strPtr(s string) *string { return &s }

// lineWriter buffers writes by line and emits each line via klog tagged with
// the forward's name. Avoids interleaved garbage when many forwards write
// concurrently and turns kubectl's textual output into structured records.
type lineWriter struct {
	name   string
	stream string

	mu  sync.Mutex
	buf bytes.Buffer
}

func newLineWriter(name, stream string) *lineWriter {
	return &lineWriter{name: name, stream: stream}
}

// Write appends p to the internal buffer and emits any complete lines. A
// partial trailing line (no '\n') stays in the buffer for the next Write or
// flush. Uses bytes.IndexByte to peek before consuming so a stream that
// produces no newlines does not trigger O(N²) buffer copies.
func (w *lineWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf.Write(p)
	for {
		idx := bytes.IndexByte(w.buf.Bytes(), '\n')
		if idx < 0 {
			return len(p), nil
		}
		// Next consumes [0, idx+1) including the '\n'; the remainder stays
		// untouched in the buffer.
		line := w.buf.Next(idx + 1)
		w.emitLocked(strings.TrimRight(string(line), "\r\n"))
	}
}

// flush emits any unterminated trailing line. Call when the writer's producer
// is known to be done so a missing-newline last message isn't lost.
func (w *lineWriter) flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.buf.Len() == 0 {
		return
	}
	w.emitLocked(strings.TrimRight(w.buf.String(), "\r\n"))
	w.buf.Reset()
}

func (w *lineWriter) emitLocked(line string) {
	if line == "" {
		return
	}
	// stderr lines from kubectl carry the interesting stuff ("Connection lost",
	// reconnect attempts, errors) — log at default verbosity so they're visible
	// in journalctl. stdout lines are mostly "Forwarding from..." chatter,
	// demoted to V(2).
	if w.stream == "stderr" {
		klog.InfoS(line, "forward", w.name, "stream", w.stream)
	} else {
		klog.V(2).InfoS(line, "forward", w.name, "stream", w.stream)
	}
}
