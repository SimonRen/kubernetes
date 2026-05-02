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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"text/template"

	"github.com/spf13/cobra"
)

// systemdUnitTemplate is the user-mode unit. Type=simple + on-failure restart
// matches cloudflared's pattern; SIGTERM is what multi-fwd's signal.NotifyContext
// listens for. ExecStart paths and the rendered config path are wrapped with
// systemd's quoted-arg syntax so paths containing spaces don't tokenize.
//
// LimitNOFILE=65536 covers ~50 forwards comfortably (each forward has a few
// sockets + epoll fds; the upstream apiserver discovery client is also
// hungry). MemoryMax / CPUQuota are deliberately omitted — operators who
// need them should add a drop-in via 'systemctl --user edit multi-fwd.service'.
const systemdUnitTemplate = `[Unit]
Description=multi-fwd kubectl port-forward supervisor
Documentation=https://github.com/kubernetes/kubernetes/tree/master/cmd/multi-fwd
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart={{.QuotedBinaryPath}} run --config {{.QuotedConfigPath}} --logging-format={{.LoggingFormat}}
Restart=on-failure
RestartSec=5s
KillSignal=SIGTERM
TimeoutStopSec=20s
LimitNOFILE=65536
{{- range .Environment }}
Environment={{ . }}
{{- end }}

[Install]
WantedBy=default.target
`

// unitParams feeds the systemd unit template. Quoted* fields are systemd-quoted
// so paths with spaces survive ExecStart parsing.
type unitParams struct {
	QuotedBinaryPath string
	QuotedConfigPath string
	Environment      []string
	LoggingFormat    string
}

// systemdQuote wraps s in double quotes, escaping internal '"' and '\' per
// systemd's exec spec ('Quoting' section of systemd.exec(5)).
func systemdQuote(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return `"` + s + `"`
}

// DefaultConfigPath returns the user-scoped XDG path:
//   - $XDG_CONFIG_HOME/multi-fwd/config.yml, falling back to
//     $HOME/.config/multi-fwd/config.yml.
//
// If $HOME is unavailable, returns "/etc/multi-fwd/config.yml" as a
// last-resort default; the run command will surface a clear error if the
// file isn't there.
func DefaultConfigPath() string {
	if x := os.Getenv("XDG_CONFIG_HOME"); x != "" {
		return filepath.Join(x, "multi-fwd", "config.yml")
	}
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".config", "multi-fwd", "config.yml")
	}
	return "/etc/multi-fwd/config.yml"
}

// userUnitPath returns the user-mode systemd unit path for multi-fwd.
func userUnitPath() (string, error) {
	if x := os.Getenv("XDG_CONFIG_HOME"); x != "" {
		return filepath.Join(x, "systemd", "user", "multi-fwd.service"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("locate home dir: %w", err)
	}
	return filepath.Join(home, ".config", "systemd", "user", "multi-fwd.service"), nil
}

func newServiceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "service",
		Short: "Manage the multi-fwd user-mode systemd service",
	}
	cmd.AddCommand(newServiceInstallCommand())
	cmd.AddCommand(newServiceUninstallCommand())
	cmd.AddCommand(newServiceStatusCommand())
	return cmd
}

func newServiceInstallCommand() *cobra.Command {
	var (
		configPath    string
		binaryPath    string
		envFlags      []string
		loggingFormat string
		noStart       bool
	)

	cmd := &cobra.Command{
		Use:   "install",
		Short: "Install multi-fwd as a user-mode systemd service",
		Long: `Writes ~/.config/systemd/user/multi-fwd.service, runs
'systemctl --user daemon-reload', and (unless --no-start) enables and
starts the unit. Uses the running multi-fwd binary's absolute path for
ExecStart so the service is independent of $PATH at runtime.

This command always overwrites the unit file. To customize without
losing your changes on upgrade, use a drop-in:

    systemctl --user edit multi-fwd.service

That creates ~/.config/systemd/user/multi-fwd.service.d/override.conf,
which 'service install' never touches.

Tip: 'loginctl enable-linger $USER' once, so the service runs at boot
without a logged-in session.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if runtime.GOOS != "linux" {
				return fmt.Errorf("service install is only supported on linux (got %s); the run subcommand still works for development", runtime.GOOS)
			}

			absConfig, err := filepath.Abs(configPath)
			if err != nil {
				return fmt.Errorf("resolve --config: %w", err)
			}
			if _, err := os.Stat(absConfig); err != nil {
				return fmt.Errorf("config %s not found — create it before installing the service: %w", absConfig, err)
			}
			// Validate up-front to fail fast on a typo.
			if _, err := LoadConfig(absConfig); err != nil {
				return fmt.Errorf("config %s is invalid: %w", absConfig, err)
			}

			absBinary, err := resolveBinaryPath(binaryPath)
			if err != nil {
				return err
			}

			if loggingFormat != "json" && loggingFormat != "text" {
				return fmt.Errorf("--logging-format must be 'json' or 'text' (got %q)", loggingFormat)
			}
			for _, e := range envFlags {
				if !validEnvEntry(e) {
					return fmt.Errorf("--env entry %q is malformed; expected KEY=VALUE with no newlines", e)
				}
			}

			unitPath, err := userUnitPath()
			if err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(unitPath), 0o755); err != nil {
				return fmt.Errorf("mkdir %s: %w", filepath.Dir(unitPath), err)
			}

			rendered, err := renderUnit(unitParams{
				QuotedBinaryPath: systemdQuote(absBinary),
				QuotedConfigPath: systemdQuote(absConfig),
				Environment:      envFlags,
				LoggingFormat:    loggingFormat,
			})
			if err != nil {
				return err
			}
			if err := os.WriteFile(unitPath, []byte(rendered), 0o644); err != nil {
				return fmt.Errorf("write unit %s: %w", unitPath, err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "wrote %s\n", unitPath)

			if err := runSystemctl(cmd, "daemon-reload"); err != nil {
				return err
			}
			if noStart {
				if err := runSystemctl(cmd, "enable", "multi-fwd.service"); err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "enabled multi-fwd.service (not started — pass without --no-start to start now)\n")
				return nil
			}
			if err := runSystemctl(cmd, "enable", "--now", "multi-fwd.service"); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "enabled and started multi-fwd.service\n")
			fmt.Fprintf(cmd.OutOrStdout(), "tail logs:  journalctl --user -u multi-fwd -f\n")
			return nil
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", DefaultConfigPath(), "path to multi-fwd YAML config (must exist)")
	cmd.Flags().StringVar(&binaryPath, "binary", "", "absolute path to the multi-fwd binary (default: the running binary)")
	cmd.Flags().StringSliceVar(&envFlags, "env", nil, "extra Environment= entries for the unit (KEY=VALUE), repeatable")
	cmd.Flags().StringVar(&loggingFormat, "logging-format", "json", "logging format passed via --logging-format on ExecStart (json|text)")
	cmd.Flags().BoolVar(&noStart, "no-start", false, "do not enable+start the service after writing the unit")
	return cmd
}

func newServiceUninstallCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "uninstall",
		Short: "Stop, disable, and remove the multi-fwd user-mode systemd service",
		RunE: func(cmd *cobra.Command, args []string) error {
			if runtime.GOOS != "linux" {
				return fmt.Errorf("service uninstall is only supported on linux (got %s)", runtime.GOOS)
			}
			unitPath, err := userUnitPath()
			if err != nil {
				return err
			}
			// Best-effort sequence — any of these may legitimately fail if the
			// unit was already partially removed; surface only the unit-file
			// removal error.
			_ = runSystemctlBest(cmd, "disable", "--now", "multi-fwd.service")
			_ = runSystemctlBest(cmd, "stop", "multi-fwd.service")

			if err := os.Remove(unitPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove unit %s: %w", unitPath, err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "removed %s\n", unitPath)

			if err := runSystemctl(cmd, "daemon-reload"); err != nil {
				return err
			}
			return nil
		},
	}
	return cmd
}

func newServiceStatusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show 'systemctl --user status multi-fwd' output",
		RunE: func(cmd *cobra.Command, args []string) error {
			if runtime.GOOS != "linux" {
				return fmt.Errorf("service status is only supported on linux (got %s)", runtime.GOOS)
			}
			return runSystemctl(cmd, "status", "multi-fwd.service", "--no-pager")
		},
	}
	return cmd
}

// resolveBinaryPath returns the absolute path of the binary to put in
// ExecStart=. Caller-supplied path wins; otherwise os.Executable().
func resolveBinaryPath(override string) (string, error) {
	if override != "" {
		abs, err := filepath.Abs(override)
		if err != nil {
			return "", fmt.Errorf("resolve --binary: %w", err)
		}
		return abs, nil
	}
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("locate current binary (use --binary to override): %w", err)
	}
	resolved, err := filepath.EvalSymlinks(exe)
	if err != nil {
		return exe, nil // symlink eval is best-effort
	}
	return resolved, nil
}

// validEnvEntry rejects entries that would produce a malformed unit. We don't
// try to police values fully (systemd's quoting is permissive); we just block
// the obvious wreckers: empty key, missing '=', literal newlines.
func validEnvEntry(e string) bool {
	if e == "" || strings.ContainsAny(e, "\r\n") {
		return false
	}
	eq := strings.IndexByte(e, '=')
	if eq <= 0 {
		return false
	}
	key := e[:eq]
	if key == "" {
		return false
	}
	for _, r := range key {
		if !(r == '_' || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')) {
			return false
		}
	}
	return true
}

func renderUnit(p unitParams) (string, error) {
	t, err := template.New("multi-fwd.service").Parse(systemdUnitTemplate)
	if err != nil {
		return "", fmt.Errorf("parse unit template: %w", err)
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, p); err != nil {
		return "", fmt.Errorf("render unit: %w", err)
	}
	return buf.String(), nil
}

// runSystemctl runs 'systemctl --user <args...>' and surfaces its output
// via the cobra command's stdio. Returns a useful error on failure.
func runSystemctl(cmd *cobra.Command, args ...string) error {
	full := append([]string{"--user"}, args...)
	c := exec.Command("systemctl", full...)
	c.Stdout = cmd.OutOrStdout()
	c.Stderr = cmd.ErrOrStderr()
	if err := c.Run(); err != nil {
		return fmt.Errorf("systemctl %s: %w", strings.Join(full, " "), err)
	}
	return nil
}

// runSystemctlBest is runSystemctl that swallows the error.
// Used during uninstall where the unit may already be in a half-removed state.
func runSystemctlBest(cmd *cobra.Command, args ...string) error {
	full := append([]string{"--user"}, args...)
	c := exec.Command("systemctl", full...)
	c.Stdout = cmd.OutOrStdout()
	c.Stderr = cmd.ErrOrStderr()
	return c.Run()
}
