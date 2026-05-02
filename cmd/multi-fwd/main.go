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

package main

import (
	"os"

	"k8s.io/component-base/cli"
	_ "k8s.io/component-base/logs/json/register"

	// Auth plugin imports — match kubectl so multi-fwd accepts the same
	// kubeconfigs (gcp, azure, oidc, exec, etc.).
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/kubernetes/cmd/multi-fwd/app"
)

func main() {
	command := app.NewCommand()
	code := cli.Run(command)
	os.Exit(code)
}
