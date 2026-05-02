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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func mkForward(name, ctx, res string, ports ...string) ForwardConfig {
	return ForwardConfig{Name: name, Context: ctx, Resource: res, Ports: ports}
}

func TestApplyDefaults_RetryInferredOn_WhenNoDefaultsBlock(t *testing.T) {
	c := &Config{Forwards: []ForwardConfig{mkForward("x", "ctx", "svc/p", "5432")}}
	c.applyDefaults()
	if !c.Forwards[0].Retry.IsEnabled() {
		t.Fatal("expected retry inferred ON when defaults block is absent")
	}
}

func TestApplyDefaults_ExplicitFalseAtDefaults_Honored(t *testing.T) {
	f := false
	c := &Config{
		Defaults: DefaultsConfig{Retry: RetryConfig{Enabled: &f}},
		Forwards: []ForwardConfig{mkForward("x", "c", "svc/p", "5432")},
	}
	c.applyDefaults()
	if c.Forwards[0].Retry.IsEnabled() {
		t.Fatal("explicit defaults.retry.enabled:false must override the on-by-default inference")
	}
}

func TestApplyDefaults_PerForwardOverridesDefaults(t *testing.T) {
	f := false
	c := &Config{
		Defaults: DefaultsConfig{Retry: RetryConfig{Enabled: boolPtr(true)}},
		Forwards: []ForwardConfig{
			{Name: "x", Context: "c", Resource: "svc/p", Ports: []string{"5432"},
				Retry: &RetryConfig{Enabled: &f}},
		},
	}
	c.applyDefaults()
	if c.Forwards[0].Retry.IsEnabled() {
		t.Fatal("per-forward retry.enabled:false must win over defaults true")
	}
}

func TestApplyDefaults_BackfillsTimingFromDefaults(t *testing.T) {
	c := &Config{Forwards: []ForwardConfig{mkForward("x", "c", "svc/p", "5432")}}
	c.applyDefaults()
	r := c.Forwards[0].Retry
	if r.InitialDelay.AsDuration() == 0 || r.MaxDelay.AsDuration() == 0 {
		t.Fatalf("retry timing not backfilled: %+v", r)
	}
}

func TestApplyDefaults_DefaultAddressMatchesKubectl(t *testing.T) {
	c := &Config{Forwards: []ForwardConfig{mkForward("x", "c", "svc/p", "5432")}}
	c.applyDefaults()
	if got := c.Forwards[0].Address; len(got) != 1 || got[0] != "localhost" {
		t.Fatalf("default address = %v, want [localhost]", got)
	}
}

func TestResolvePaths_TildeUserRejected(t *testing.T) {
	c := &Config{
		Forwards: []ForwardConfig{
			{Name: "x", Context: "c", Resource: "svc/p", Ports: []string{"5432"},
				Kubeconfig: "~ubuntu/.kube/config"},
		},
	}
	err := c.resolvePaths(t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "~user") {
		t.Fatalf("expected ~user rejection error; got %v", err)
	}
}

func TestResolvePaths_TildeSlashExpanded(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		t.Skip("no $HOME")
	}
	c := &Config{
		Forwards: []ForwardConfig{
			{Name: "x", Context: "c", Resource: "svc/p", Ports: []string{"5432"},
				Kubeconfig: "~/.kube/config"},
		},
	}
	if err := c.resolvePaths(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(home, ".kube/config")
	if got := c.Forwards[0].Kubeconfig; got != want {
		t.Fatalf("kubeconfig = %q, want %q", got, want)
	}
}

func TestResolvePaths_BareTildeExpandsToHome(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		t.Skip("no $HOME")
	}
	c := &Config{
		Forwards: []ForwardConfig{
			{Name: "x", Context: "c", Resource: "svc/p", Ports: []string{"5432"},
				Kubeconfig: "~"},
		},
	}
	if err := c.resolvePaths(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if got := c.Forwards[0].Kubeconfig; got != home {
		t.Fatalf("bare ~ should expand to %q, got %q", home, got)
	}
}

func TestResolvePaths_RelativeAgainstConfigDir(t *testing.T) {
	dir := t.TempDir()
	c := &Config{
		Forwards: []ForwardConfig{
			{Name: "x", Context: "c", Resource: "svc/p", Ports: []string{"5432"},
				Kubeconfig: "kc.yaml"},
		},
	}
	if err := c.resolvePaths(dir); err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(dir, "kc.yaml")
	if got := c.Forwards[0].Kubeconfig; got != want {
		t.Fatalf("kubeconfig = %q, want %q", got, want)
	}
}

func TestResolvePaths_AbsoluteUntouched(t *testing.T) {
	abs := "/etc/kube/config"
	c := &Config{
		Forwards: []ForwardConfig{
			{Name: "x", Context: "c", Resource: "svc/p", Ports: []string{"5432"},
				Kubeconfig: abs},
		},
	}
	if err := c.resolvePaths(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if c.Forwards[0].Kubeconfig != abs {
		t.Fatalf("absolute path was rewritten to %q", c.Forwards[0].Kubeconfig)
	}
}

func TestResolvePaths_EmptyKubeconfigUntouched(t *testing.T) {
	c := &Config{Forwards: []ForwardConfig{mkForward("x", "c", "svc/p", "5432")}}
	if err := c.resolvePaths(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if c.Forwards[0].Kubeconfig != "" {
		t.Fatalf("empty kubeconfig got populated: %q", c.Forwards[0].Kubeconfig)
	}
}

func TestValidate_PortCollision_SameAddress(t *testing.T) {
	c := &Config{
		Forwards: []ForwardConfig{
			{Name: "a", Context: "c", Resource: "svc/a", Ports: []string{"5432:5432"}, Address: []string{"127.0.0.1"}},
			{Name: "b", Context: "c", Resource: "svc/b", Ports: []string{"5432:5432"}, Address: []string{"127.0.0.1"}},
		},
	}
	c.applyDefaults()
	err := c.Validate()
	if err == nil {
		t.Fatal("expected collision error")
	}
	if !strings.Contains(err.Error(), "already claimed by forwards[0]") {
		t.Fatalf("error doesn't name the original claimer: %v", err)
	}
}

func TestValidate_PortCollision_LoopbackAliases(t *testing.T) {
	c := &Config{
		Forwards: []ForwardConfig{
			{Name: "a", Context: "c", Resource: "svc/a", Ports: []string{"5432"}, Address: []string{"127.0.0.1"}},
			{Name: "b", Context: "c", Resource: "svc/b", Ports: []string{"5432"}, Address: []string{"localhost"}},
		},
	}
	c.applyDefaults()
	if err := c.Validate(); err == nil {
		t.Fatal("expected collision (localhost vs 127.0.0.1 are loopback aliases)")
	}
}

func TestValidate_PortCollision_DistinctAddressesOK(t *testing.T) {
	c := &Config{
		Forwards: []ForwardConfig{
			{Name: "a", Context: "c", Resource: "svc/a", Ports: []string{"5432"}, Address: []string{"10.0.0.1"}},
			{Name: "b", Context: "c", Resource: "svc/b", Ports: []string{"5432"}, Address: []string{"10.0.0.2"}},
		},
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		t.Fatalf("non-loopback distinct addresses must not collide: %v", err)
	}
}

func TestValidate_RandomLocalPortNoCollision(t *testing.T) {
	c := &Config{
		Forwards: []ForwardConfig{
			{Name: "a", Context: "c", Resource: "svc/a", Ports: []string{":5432"}},
			{Name: "b", Context: "c", Resource: "svc/b", Ports: []string{":5432"}},
		},
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		t.Fatalf("random local ports must not be flagged: %v", err)
	}
}

func TestValidate_DuplicateName(t *testing.T) {
	c := &Config{
		Forwards: []ForwardConfig{
			mkForward("x", "c", "svc/a", "5432"),
			mkForward("x", "c", "svc/b", "5433"),
		},
	}
	c.applyDefaults()
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "duplicate name") {
		t.Fatalf("expected duplicate-name error; got %v", err)
	}
}

func TestValidate_MissingContext(t *testing.T) {
	c := &Config{
		Forwards: []ForwardConfig{{Name: "x", Resource: "svc/a", Ports: []string{"5432"}}},
	}
	c.applyDefaults()
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "context is required") {
		t.Fatalf("expected context-required error; got %v", err)
	}
}

func TestValidate_RetryFloorsRespected(t *testing.T) {
	c := &Config{
		Forwards: []ForwardConfig{
			{Name: "x", Context: "c", Resource: "svc/a", Ports: []string{"5432"},
				Retry: &RetryConfig{Enabled: boolPtr(true), MaxDelay: Duration(1), InitialDelay: Duration(2)}},
		},
	}
	c.applyDefaults()
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "max_delay") {
		t.Fatalf("expected max_delay >= initial_delay error; got %v", err)
	}
}

func TestLocalPortsFromSpec(t *testing.T) {
	cases := []struct {
		spec  string
		local int
		ok    bool
	}{
		{"5432:5432", 5432, true},
		{"8888:5000", 8888, true},
		{"5432", 5432, true},
		{":5432", 0, false},
		{"8888:https", 8888, true},
		{"abc:5432", 0, false},
		{"", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.spec, func(t *testing.T) {
			got, ok := localPortsFromSpec(tc.spec)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if ok && got[0] != tc.local {
				t.Fatalf("local = %d, want %d", got[0], tc.local)
			}
		})
	}
}

func TestNormalizeAddress_LoopbackAliases(t *testing.T) {
	want := normalizeAddress("localhost")
	for _, alias := range []string{"127.0.0.1", "::1"} {
		if normalizeAddress(alias) != want {
			t.Fatalf("%s should normalize as a loopback alias", alias)
		}
	}
	if normalizeAddress("0.0.0.0") == want {
		t.Fatal("0.0.0.0 must NOT normalize as loopback")
	}
}

func TestLoadConfig_RoundTripsExample(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yml")
	yaml := `
defaults:
  retry: { enabled: true }
forwards:
  - name: pg
    context: prod
    resource: svc/postgres
    ports: ["5433:5432"]
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Forwards) != 1 {
		t.Fatalf("expected 1 forward, got %d", len(cfg.Forwards))
	}
	if !cfg.Forwards[0].Retry.IsEnabled() {
		t.Fatal("retry should be on")
	}
}
