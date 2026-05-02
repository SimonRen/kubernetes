# multi-fwd

Long-running daemon that supervises N concurrent `kubectl port-forward`
sessions, in-process, under user-mode `systemd`.

`multi-fwd` reads a YAML config, opens one port-forward per entry, and lets
the upstream resilient retry loop in
[`staging/src/k8s.io/kubectl/pkg/cmd/portforward/`](../../staging/src/k8s.io/kubectl/pkg/cmd/portforward)
handle reconnects, pod re-selection, and port pinning. An outer manager
adds panic recovery and capped exponential backoff (5s → 60s) for the rare
case the inner retry loop returns terminally. Because the daemon links the
port-forward code directly there is no subprocess and no log scraping;
context-based cancellation propagates through the whole stack and logs are
structured `klog` records tagged per forward.

## Subcommands

| Command                       | Purpose                                                  |
| ----------------------------- | -------------------------------------------------------- |
| `multi-fwd run`               | Foreground daemon. Intended use: under user-mode systemd.|
| `multi-fwd validate`          | Parse + check the YAML, no side effects.                 |
| `multi-fwd service install`   | Write user-mode unit, enable, start. Linux only.         |
| `multi-fwd service uninstall` | Disable, stop, remove the unit.                          |
| `multi-fwd service status`    | `systemctl --user status multi-fwd`.                     |

## Build

`cmd/multi-fwd` is registered in `KUBE_CLIENT_TARGETS`, so the standard
build matrix picks it up:

```sh
make multi-fwd
# binary at _output/local/bin/<os>/<arch>/multi-fwd
```

For cross-compile from a non-Linux host, skip `make` (it requires a
matching cgo toolchain) and use plain `go build`:

```sh
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /tmp/multi-fwd ./cmd/multi-fwd
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o /tmp/multi-fwd ./cmd/multi-fwd
```

The result is a statically-linked ELF (~69 MB). Auth plugin imports
(`k8s.io/client-go/plugin/pkg/client/auth/...`) are linked in just like in
`kubectl`, so any kubeconfig `kubectl` accepts works here.

## Install on a remote host

Copy the binary to `~/.local/bin/multi-fwd` on the box that should run the
forwards. Ubuntu's stock `~/.profile` adds `~/.local/bin` to `$PATH`
once the directory exists, so a fresh shell will pick it up.

```sh
ssh REMOTE 'mkdir -p ~/.local/bin ~/.config/multi-fwd'
scp /tmp/multi-fwd REMOTE:~/.local/bin/multi-fwd
ssh REMOTE 'chmod +x ~/.local/bin/multi-fwd'
```

To survive logout, enable linger once on the remote (one-time, needs
sudo):

```sh
sudo loginctl enable-linger $USER
```

Without linger the user-mode service stops when the last login session
ends.

## Configure

Default config path: `~/.config/multi-fwd/config.yml`
(or `$XDG_CONFIG_HOME/multi-fwd/config.yml`). Override per command with
`--config / -c`.

### Schema

```yaml
defaults:
  retry:
    enabled: true            # default true; set false to disable globally
    initial_delay: 5s
    max_delay: 60s
    max_retries: 0           # 0 = unlimited; recommended for a daemon
  address: [localhost]       # default; matches kubectl --address (binds v4+v6)
  pod_running_timeout: 60s

forwards:
  - name: <unique-id>        # required, used in logs
    context: <kubectx>       # REQUIRED, no current-context fallback
    kubeconfig: <path>       # optional; '~/...' and config-dir-relative OK
    namespace: <ns>          # optional; overrides context's default namespace
    resource: svc/postgres   # required; TYPE/NAME or bare pod name
    ports: ["5433:5432"]     # required, ≥1 entry; [LOCAL:]REMOTE
    address: [127.0.0.1]     # optional; overrides defaults.address
    pod_running_timeout: 60s # optional
    retry:                   # optional; merged over defaults.retry
      enabled: true
      initial_delay: 2s
      max_delay: 30s
      max_retries: 0
```

A complete annotated example sits at
[`cmd/multi-fwd/examples/config.yml`](examples/config.yml).

### Validation guarantees

`multi-fwd validate` rejects:

- empty `forwards:`
- duplicate `name:` across forwards
- missing `context:` (no implicit current-context fallback)
- missing `resource:` or non-`TYPE/NAME` form
- empty `ports:`
- retry where `max_delay < initial_delay` or `max_retries < 0`
- local-port collisions across forwards on the same address. Loopback
  aliases are collapsed: `localhost`, `127.0.0.1`, and `::1` count as the
  same address. Random local ports (`:REMOTE`) are skipped because they
  are chosen at runtime.
- `~user` kubeconfig paths (only `~`, `~/...`, and absolute /
  config-dir-relative paths are accepted).

## Run as a systemd user service

```sh
multi-fwd validate                      # sanity-check first
multi-fwd service install               # write unit, enable, start
multi-fwd service status
journalctl --user -u multi-fwd -f       # tail logs (Ctrl-C to stop tailing)
```

`service install` writes `~/.config/systemd/user/multi-fwd.service` with:

- `Type=simple`
- `ExecStart=` the absolute path of the running binary,
  `--logging-format=json`, and the resolved config path — all
  systemd-quoted so paths containing spaces survive
- `Restart=on-failure`, `RestartSec=5s`
- `KillSignal=SIGTERM`, `TimeoutStopSec=20s`
- `LimitNOFILE=65536` (covers ~50 forwards comfortably)
- `WantedBy=default.target` (user-mode equivalent of `multi-user.target`)

### Day-to-day commands

| Command                                         | What it does                                  |
| ----------------------------------------------- | --------------------------------------------- |
| `systemctl --user restart multi-fwd`            | re-read config after editing `config.yml`     |
| `systemctl --user stop multi-fwd`               | pause all forwards                            |
| `journalctl --user -u multi-fwd --since "10 min ago"` | recent logs                             |
| `multi-fwd service uninstall`                   | full teardown                                 |

There is no `SIGHUP` reload by design — config changes apply via
`systemctl --user restart`. systemd waits up to `TimeoutStopSec=20s` for
the daemon to drain.

### Operator overrides without losing them on upgrade

Use a drop-in. `multi-fwd service install` only ever rewrites the main
unit file — it never touches `*.service.d/override.conf`.

```sh
systemctl --user edit multi-fwd.service
# in the editor that opens, add e.g.:
#   [Service]
#   MemoryMax=512M
#   Environment=AWS_PROFILE=prod

systemctl --user daemon-reload && systemctl --user restart multi-fwd
```

### Upgrades

```sh
# on the build host:
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o /tmp/multi-fwd ./cmd/multi-fwd
scp /tmp/multi-fwd REMOTE:~/.local/bin/multi-fwd

# on the remote:
systemctl --user restart multi-fwd
```

`ExecStart` records an absolute path; as long as you overwrite that path,
restart picks up the new build.

## Reaching the forwards

`localhost` (binding both `127.0.0.1` and `::1`) is the default bind
address — the forward only accepts connections from the host running
multi-fwd. To reach a forward from another machine:

- SSH tunnel: `ssh -N -L 5433:127.0.0.1:5433 REMOTE` and connect to
  `127.0.0.1:5433` locally.
- Or set `address: ["0.0.0.0"]` on the forward to listen on the LAN
  interface. Open the firewall accordingly.

## Logs

Under systemd, the default `--logging-format=json` produces structured
klog records ingested by journald. Each line carries:

- `forward=<name>` — which entry produced the log
- `stream=stdout|stderr` — for upstream port-forward output
- standard klog fields: `level`, `ts`, `caller`, `msg`

The resilient retry loop's *Connection lost*, *Switching to pod*, and
*Warning: failed to re-select pod* messages all flow through this same
tagged path (`PortForwardOptions.ErrOut` routing).

To switch to text format, drop in:

```ini
[Service]
ExecStart=
ExecStart="/path/to/multi-fwd" run --config "/path/to/config.yml" --logging-format=text
```

(The empty `ExecStart=` line clears the inherited list before re-setting.)

## Troubleshooting

### Service stops immediately after install

```sh
journalctl --user -u multi-fwd -n 50 --no-pager
```

Most common causes: the daemon user can't read a kubeconfig the YAML
references, or the resource doesn't exist in the cluster yet. `validate`
catches schema problems but not "does this svc exist in this namespace"
problems — those surface at startup.

### Forward keeps reconnecting

That's the resilient retry loop doing its job — pods restart, the dialer
re-selects, ports get pinned across reconnects. Reconnect events are
tagged: `journalctl --user -u multi-fwd | grep -E 'Connection lost|Switching to pod'`.

If reconnects never stabilize, check the target pod is actually `Running`:

```sh
kubectl --context CTX -n NS get pods -l SELECTOR
```

### Service won't start at boot without me logged in

Linger isn't enabled. `sudo loginctl enable-linger $USER` fixes it
permanently for that user.

### "permission denied" on `~/.kube/config`

The daemon runs as the user who installed the service. That user needs
to be able to read every kubeconfig referenced from the YAML.

## Known limitations

- **One factory per goroutine.** Each forward gets its own
  `cmdutil.Factory` and REST client. Fine for a handful of forwards,
  potentially wasteful at large N on the same cluster (no shared TCP
  pool, concurrent auth-helper executions on token expiry).
- **`max_retries` is per restart cycle, not lifetime.** When the inner
  retry loop exhausts `max_retries`, the outer Manager waits out the
  backoff and starts a fresh `PortForwardOptions`, so the inner counter
  resets. Use `max_retries: 0` (the default — unlimited) unless you
  specifically want a per-cycle circuit breaker.
- **No `SIGHUP` reload.** Config edits apply via `systemctl --user
  restart multi-fwd`. Matches the cloudflared model.
- **Synthetic `*cobra.Command` for `PortForwardOptions.Complete()`.**
  Currently `Complete()` only reads `--pod-running-timeout` from the
  command, which we register. If a future kubectl release adds a
  `cmdutil.GetFlagString(cmd, "new-flag")` call inside `Complete()`,
  the helper calls `klog.Fatalf` on the missing flag, which
  `os.Exit(255)`s past our `recover()`. Audit `Complete()` when bumping
  the upstream kubernetes module.
