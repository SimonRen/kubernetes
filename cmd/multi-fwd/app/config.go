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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/yaml"
)

// Config is the top-level multi-fwd YAML schema.
type Config struct {
	// Defaults applied to every forward unless explicitly overridden.
	Defaults DefaultsConfig `json:"defaults,omitempty"`

	// Forwards is the list of port-forward sessions to supervise. Required, ≥1.
	Forwards []ForwardConfig `json:"forwards"`
}

// DefaultsConfig holds top-level defaults that backfill missing per-forward fields.
type DefaultsConfig struct {
	// Retry default applied to forwards that omit fields.
	Retry RetryConfig `json:"retry,omitempty"`

	// Address is the default listen address(es). Default: ["localhost"], which
	// matches kubectl's --address default and binds both 127.0.0.1 and ::1.
	Address []string `json:"address,omitempty"`

	// PodRunningTimeout caps the wait for the target pod to enter Running.
	// Default: 60s. Mirrors kubectl's --pod-running-timeout.
	PodRunningTimeout Duration `json:"pod_running_timeout,omitempty"`
}

// ForwardConfig describes a single port-forward session.
type ForwardConfig struct {
	// Name is a stable identifier; appears in logs. Required, must be unique.
	Name string `json:"name"`

	// Context is the kubeconfig context to use. REQUIRED — multi-fwd has no
	// implicit current-context fallback so the running daemon's behavior
	// stays explicit and stable across kubeconfig context switches.
	Context string `json:"context"`

	// Kubeconfig is the kubeconfig file path. Optional; when empty, normal
	// client-go discovery applies ($KUBECONFIG, ~/.kube/config). Relative
	// paths resolve against the config file's directory; '~/...' expands
	// against $HOME (the special form '~' alone expands to $HOME). Other
	// '~user' forms are rejected — we don't reimplement shell semantics.
	Kubeconfig string `json:"kubeconfig,omitempty"`

	// Namespace overrides the context's namespace. Optional.
	Namespace string `json:"namespace,omitempty"`

	// Resource is the resource ref, e.g. "svc/postgres", "deployment/api",
	// "pod/foo". Required.
	Resource string `json:"resource"`

	// Ports lists [LOCAL_PORT:]REMOTE_PORT specs. Required, ≥1.
	Ports []string `json:"ports"`

	// Address overrides Defaults.Address.
	Address []string `json:"address,omitempty"`

	// PodRunningTimeout overrides Defaults.PodRunningTimeout.
	PodRunningTimeout Duration `json:"pod_running_timeout,omitempty"`

	// Retry overrides Defaults.Retry. Pointer so we can distinguish "unset
	// (inherit)" from "explicitly set". Missing sub-fields backfill from
	// Defaults.Retry during applyDefaults.
	Retry *RetryConfig `json:"retry,omitempty"`
}

// RetryConfig mirrors kubectl's port-forward RetryConfig. Enabled is a *bool
// so we can distinguish "user did not set it" (nil → inherit/default-on) from
// "user wrote enabled: false" (explicit off).
type RetryConfig struct {
	Enabled      *bool    `json:"enabled,omitempty"`
	InitialDelay Duration `json:"initial_delay,omitempty"`
	MaxDelay     Duration `json:"max_delay,omitempty"`
	MaxRetries   int      `json:"max_retries,omitempty"`
}

// IsEnabled reports whether retry is on. Nil means unset; treat as off here —
// callers should resolve inheritance via applyDefaults before checking.
func (r RetryConfig) IsEnabled() bool { return r.Enabled != nil && *r.Enabled }

// Duration wraps time.Duration with YAML/JSON support for "5s", "1m30s", etc.
// sigs.k8s.io/yaml routes through encoding/json, so JSON unmarshalling is enough.
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "null" || s == "" {
		*d = 0
		return nil
	}
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}
	if s == "" || s == "0" {
		*d = 0
		return nil
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", time.Duration(d).String())), nil
}

// AsDuration returns the underlying time.Duration.
func (d Duration) AsDuration() time.Duration { return time.Duration(d) }

// LoadConfig reads, parses, applies defaults, validates, and resolves paths
// in a multi-fwd YAML config.
func LoadConfig(path string) (*Config, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve config path: %w", err)
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", abs, err)
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", abs, err)
	}
	c.applyDefaults()
	if err := c.resolvePaths(filepath.Dir(abs)); err != nil {
		return nil, fmt.Errorf("config %s: %w", abs, err)
	}
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("config %s: %w", abs, err)
	}
	return &c, nil
}

func boolPtr(b bool) *bool { return &b }

// applyDefaults backfills missing per-forward fields from Defaults. Retry is
// treated as on-by-default at both the Defaults and per-forward levels — a
// daemon that quietly drops every forward's retry the moment a YAML key is
// forgotten is a footgun. Explicit `enabled: false` is honored.
func (c *Config) applyDefaults() {
	if c.Defaults.PodRunningTimeout == 0 {
		c.Defaults.PodRunningTimeout = Duration(60 * time.Second)
	}
	if len(c.Defaults.Address) == 0 {
		// Match kubectl's --address default (binds both v4 and v6 loopback).
		c.Defaults.Address = []string{"localhost"}
	}
	if c.Defaults.Retry.Enabled == nil {
		c.Defaults.Retry.Enabled = boolPtr(true)
	}
	if c.Defaults.Retry.InitialDelay == 0 {
		c.Defaults.Retry.InitialDelay = Duration(5 * time.Second)
	}
	if c.Defaults.Retry.MaxDelay == 0 {
		c.Defaults.Retry.MaxDelay = Duration(60 * time.Second)
	}

	for i := range c.Forwards {
		f := &c.Forwards[i]
		if len(f.Address) == 0 {
			f.Address = append([]string(nil), c.Defaults.Address...)
		}
		if f.PodRunningTimeout == 0 {
			f.PodRunningTimeout = c.Defaults.PodRunningTimeout
		}
		if f.Retry == nil {
			r := c.Defaults.Retry
			r.Enabled = boolPtr(*c.Defaults.Retry.Enabled) // copy to a fresh ptr
			f.Retry = &r
			continue
		}
		if f.Retry.Enabled == nil {
			f.Retry.Enabled = boolPtr(*c.Defaults.Retry.Enabled)
		}
		if f.Retry.InitialDelay == 0 {
			f.Retry.InitialDelay = c.Defaults.Retry.InitialDelay
		}
		if f.Retry.MaxDelay == 0 {
			f.Retry.MaxDelay = c.Defaults.Retry.MaxDelay
		}
	}
}

// resolvePaths makes per-forward Kubeconfig paths absolute. Empty values are
// left empty (client-go discovery applies). Only '~/...' (current user) and
// the bare '~' are accepted; '~user' / '~/' / mid-string '~' are rejected to
// avoid silent misroutes.
func (c *Config) resolvePaths(configDir string) error {
	for i := range c.Forwards {
		kc := c.Forwards[i].Kubeconfig
		if kc == "" || filepath.IsAbs(kc) {
			continue
		}
		if kc == "~" || strings.HasPrefix(kc, "~/") {
			home, err := os.UserHomeDir()
			if err != nil {
				return fmt.Errorf("expand ~ in forward %q kubeconfig: %w", c.Forwards[i].Name, err)
			}
			if kc == "~" {
				kc = home
			} else {
				kc = filepath.Join(home, strings.TrimPrefix(kc, "~/"))
			}
		} else if strings.HasPrefix(kc, "~") {
			return fmt.Errorf("forwards[%d] (%s): kubeconfig %q uses unsupported '~user' form; use absolute path instead", i, c.Forwards[i].Name, kc)
		} else {
			kc = filepath.Join(configDir, kc)
		}
		c.Forwards[i].Kubeconfig = kc
	}
	return nil
}

// Validate checks invariants. Must be called after applyDefaults.
func (c *Config) Validate() error {
	if len(c.Forwards) == 0 {
		return fmt.Errorf("no forwards configured (need at least one entry under 'forwards')")
	}
	seen := make(map[string]int, len(c.Forwards))

	// portKey: address × local-port. Tracks which forward first claimed a
	// (addr, local) pair so we can name both sides of a collision.
	type portKey struct {
		addr  string
		local int
	}
	type portClaim struct {
		forwardIdx int
		name       string
	}
	claimed := map[portKey]portClaim{}

	for i, f := range c.Forwards {
		if f.Name == "" {
			return fmt.Errorf("forwards[%d]: name is required", i)
		}
		if prev, dup := seen[f.Name]; dup {
			return fmt.Errorf("forwards[%d] (%s): duplicate name (also at forwards[%d])", i, f.Name, prev)
		}
		seen[f.Name] = i

		if f.Context == "" {
			return fmt.Errorf("forwards[%d] (%s): context is required (multi-fwd has no current-context fallback)", i, f.Name)
		}
		if f.Resource == "" {
			return fmt.Errorf("forwards[%d] (%s): resource is required (e.g. svc/postgres, deployment/api, pod/foo)", i, f.Name)
		}
		if !strings.Contains(f.Resource, "/") && !looksLikePodName(f.Resource) {
			return fmt.Errorf("forwards[%d] (%s): resource %q must be in TYPE/NAME form", i, f.Name, f.Resource)
		}
		if len(f.Ports) == 0 {
			return fmt.Errorf("forwards[%d] (%s): at least one port is required", i, f.Name)
		}

		if f.Retry != nil && f.Retry.IsEnabled() {
			r := f.Retry
			if r.InitialDelay <= 0 {
				return fmt.Errorf("forwards[%d] (%s): retry.initial_delay must be > 0", i, f.Name)
			}
			if r.MaxDelay <= 0 {
				return fmt.Errorf("forwards[%d] (%s): retry.max_delay must be > 0", i, f.Name)
			}
			if r.MaxDelay < r.InitialDelay {
				return fmt.Errorf("forwards[%d] (%s): retry.max_delay (%s) must be >= retry.initial_delay (%s)",
					i, f.Name, r.MaxDelay.AsDuration(), r.InitialDelay.AsDuration())
			}
			if r.MaxRetries < 0 {
				return fmt.Errorf("forwards[%d] (%s): retry.max_retries must be >= 0 (0 = unlimited)", i, f.Name)
			}
		}

		// Cross-forward local-port collision check. Static; named/random ports
		// (e.g. ":5432" or "8888:https") are skipped — kubectl resolves those
		// at runtime, where binding errors will surface naturally.
		for _, spec := range f.Ports {
			locals, ok := localPortsFromSpec(spec)
			if !ok {
				continue
			}
			for _, lp := range locals {
				for _, addr := range f.Address {
					k := portKey{addr: normalizeAddress(addr), local: lp}
					if existing, dup := claimed[k]; dup {
						return fmt.Errorf("forwards[%d] (%s): local port %s:%d already claimed by forwards[%d] (%s)",
							i, f.Name, addr, lp, existing.forwardIdx, existing.name)
					}
					claimed[k] = portClaim{forwardIdx: i, name: f.Name}
				}
			}
		}
	}
	return nil
}

// localPortsFromSpec returns the static local port(s) implied by a kubectl
// port-forward spec. Returns ok=false if the local port is dynamic (random)
// or non-numeric (e.g. unsupported in the LOCAL slot anyway, but be safe).
//
// Spec forms:
//
//	"LOCAL:REMOTE" → local = LOCAL (must be int)
//	":REMOTE"      → ok=false (random local)
//	"PORT"         → local = PORT (must be int)
func localPortsFromSpec(spec string) ([]int, bool) {
	parts := strings.SplitN(spec, ":", 2)
	var localStr string
	switch len(parts) {
	case 1:
		localStr = parts[0]
	case 2:
		localStr = parts[0]
	}
	if localStr == "" {
		return nil, false
	}
	n, err := strconv.Atoi(localStr)
	if err != nil {
		return nil, false
	}
	return []int{n}, true
}

// normalizeAddress folds aliases that bind to the same loopback so collision
// detection catches "127.0.0.1" vs "localhost" overlap. Conservative: any
// non-loopback string is returned as-is.
func normalizeAddress(addr string) string {
	switch addr {
	case "localhost", "127.0.0.1", "::1":
		return "<loopback>"
	}
	return addr
}

// looksLikePodName accepts bare names ("mypod") as resource refs since kubectl
// also defaults TYPE to "pod" when omitted.
func looksLikePodName(s string) bool {
	for _, r := range s {
		if r == '/' || r == ' ' || r == ':' {
			return false
		}
	}
	return s != ""
}
