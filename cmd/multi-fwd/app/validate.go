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
	"fmt"

	"github.com/spf13/cobra"
)

func newValidateCommand() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Parse and validate the YAML config without starting any forwards",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := LoadConfig(configPath)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "config OK: %d forwards\n", len(cfg.Forwards))
			for _, f := range cfg.Forwards {
				retry := "off"
				if f.Retry != nil && f.Retry.IsEnabled() {
					retry = fmt.Sprintf("on (initial=%s max=%s max_retries=%d)",
						f.Retry.InitialDelay.AsDuration(), f.Retry.MaxDelay.AsDuration(), f.Retry.MaxRetries)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "  - %s: ctx=%s ns=%s %s %v retry=%s\n",
					f.Name, f.Context, f.Namespace, f.Resource, f.Ports, retry)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", DefaultConfigPath(), "path to multi-fwd YAML config")
	return cmd
}
