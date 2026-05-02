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
	"flag"

	"github.com/spf13/cobra"

	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/klog/v2"
)

// NewCommand returns the root cobra command for multi-fwd.
func NewCommand() *cobra.Command {
	logsOpts := logs.NewOptions()

	root := &cobra.Command{
		Use:   "multi-fwd",
		Short: "Supervise many kubectl port-forward sessions from a YAML config",
		Long: `multi-fwd reads a YAML config and runs one resilient
kubectl port-forward session per entry, supervised in-process. It is
designed to run under user-mode systemd: 'multi-fwd service install'
writes a unit at ~/.config/systemd/user/multi-fwd.service and enables it.

Each forward uses kubectl's built-in retry / reconnect logic
(--retry et al.) — no subprocess, no shell-out: we link the
port-forward package directly and invoke it as a goroutine per entry.`,
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Apply --logging-format / --log-flush-frequency / etc. once flags
			// are parsed. ValidateAndApply rewires klog to the chosen format.
			return logsapi.ValidateAndApply(logsOpts, nil)
		},
	}

	logsapi.AddFlags(logsOpts, root.PersistentFlags())

	// Pull in klog's --v / --vmodule etc. so callers can crank verbosity.
	klogFS := flag.NewFlagSet("klog", flag.ContinueOnError)
	klog.InitFlags(klogFS)
	root.PersistentFlags().AddGoFlagSet(klogFS)

	root.AddCommand(newRunCommand())
	root.AddCommand(newValidateCommand())
	root.AddCommand(newServiceCommand())

	return root
}
