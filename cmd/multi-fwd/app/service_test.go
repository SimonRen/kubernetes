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
	"strings"
	"testing"
)

func TestRenderUnit_ContainsCoreDirectives(t *testing.T) {
	out, err := renderUnit(unitParams{
		QuotedBinaryPath: systemdQuote("/usr/local/bin/multi-fwd"),
		QuotedConfigPath: systemdQuote("/etc/multi-fwd/config.yml"),
		LoggingFormat:    "json",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"[Unit]",
		"[Service]",
		"Type=simple",
		`ExecStart="/usr/local/bin/multi-fwd" run --config "/etc/multi-fwd/config.yml" --logging-format=json`,
		"Restart=on-failure",
		"KillSignal=SIGTERM",
		"TimeoutStopSec=20s",
		"LimitNOFILE=65536",
		"WantedBy=default.target",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("rendered unit missing %q\n--- full ---\n%s", want, out)
		}
	}
}

func TestRenderUnit_PathsWithSpaces(t *testing.T) {
	out, err := renderUnit(unitParams{
		QuotedBinaryPath: systemdQuote("/home/user/my bin/multi-fwd"),
		QuotedConfigPath: systemdQuote("/home/user/my configs/config.yml"),
		LoggingFormat:    "text",
	})
	if err != nil {
		t.Fatal(err)
	}
	wantLine := `ExecStart="/home/user/my bin/multi-fwd" run --config "/home/user/my configs/config.yml" --logging-format=text`
	if !strings.Contains(out, wantLine) {
		t.Fatalf("paths with spaces not properly quoted in ExecStart\nwant line: %s\n--- got ---\n%s", wantLine, out)
	}
}

func TestRenderUnit_EnvironmentEntries(t *testing.T) {
	out, err := renderUnit(unitParams{
		QuotedBinaryPath: systemdQuote("/bin/m"),
		QuotedConfigPath: systemdQuote("/c"),
		Environment:      []string{"FOO=bar", "AWS_PROFILE=prod"},
		LoggingFormat:    "json",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Environment=FOO=bar", "Environment=AWS_PROFILE=prod"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in rendered unit\n%s", want, out)
		}
	}
}

func TestRenderUnit_NoEnvironmentByDefault(t *testing.T) {
	out, err := renderUnit(unitParams{
		QuotedBinaryPath: systemdQuote("/bin/m"),
		QuotedConfigPath: systemdQuote("/c"),
		LoggingFormat:    "json",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, "Environment=") {
		t.Fatalf("no Environment= entry expected, got\n%s", out)
	}
}

func TestSystemdQuote_EscapesQuotesAndBackslashes(t *testing.T) {
	cases := []struct {
		in, out string
	}{
		{"/simple/path", `"/simple/path"`},
		{`/path with spaces/file`, `"/path with spaces/file"`},
		{`/has"quote/inside`, `"/has\"quote/inside"`},
		{`/has\back/slash`, `"/has\\back/slash"`},
		// Backslash-then-quote: ensure backslash is escaped first so the
		// produced string doesn't accidentally re-escape the closing quote.
		{`/weird\"path`, `"/weird\\\"path"`},
	}
	for _, tc := range cases {
		got := systemdQuote(tc.in)
		if got != tc.out {
			t.Fatalf("systemdQuote(%q) = %q, want %q", tc.in, got, tc.out)
		}
	}
}

func TestValidEnvEntry(t *testing.T) {
	cases := []struct {
		in string
		ok bool
	}{
		{"FOO=bar", true},
		{"FOO=bar=baz", true},
		{"_HIDDEN=x", true},
		{"FOO123=x", true},
		{"FOO BAR=x", false},   // space in key
		{"=value", false},      // empty key
		{"FOO", false},         // no '='
		{"", false},            // empty
		{"FOO=val\nue", false}, // embedded newline
		{"FOO=val\rue", false}, // embedded carriage return
		{"1FOO=x", true},       // digit-led keys are unusual but systemd-legal
	}
	for _, tc := range cases {
		if got := validEnvEntry(tc.in); got != tc.ok {
			t.Fatalf("validEnvEntry(%q) = %v, want %v", tc.in, got, tc.ok)
		}
	}
}
