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
	"fmt"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"k8s.io/component-base/logs"
)

func newRunCommand() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run multi-fwd in the foreground (intended use: under user-mode systemd)",
		Long: `Reads the YAML config and supervises one goroutine per forward.
Blocks until SIGINT/SIGTERM, then drains all forwards and exits.

Logs go to stderr, structured via klog. Under systemd, journald captures
them: 'journalctl --user -u multi-fwd -f' to tail.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			logs.InitLogs()
			defer logs.FlushLogs()

			cfg, err := LoadConfig(configPath)
			if err != nil {
				return err
			}

			ctx, cancel := signal.NotifyContext(context.Background(),
				syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			mgr := NewManager(cfg)
			if err := mgr.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				return fmt.Errorf("manager: %w", err)
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", DefaultConfigPath(), "path to multi-fwd YAML config")
	return cmd
}
