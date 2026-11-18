# Resources and power

The `resources` list names the hosts flowmanager knows about. A host with a `power` section
can also be powered. Terms are in [concepts.md](../concepts.md#hosts-and-power); behaviour is
in [architecture/power.md](../architecture/power.md).

## `resources[]`

| Key | Required | Meaning |
| --- | --- | --- |
| `id` | yes | Unique name that models, classes, sources and jobs refer to |
| `kind` | yes | `exclusive` (one model at a time; must have at least one model) or `shared` (no models; any number of holders) |
| `power` | no | How the host is powered. Without it the host is treated as always up and is never powered, and a lockout on it is ignored |

```yaml
resources:
  - id: blade-runner          # always up: no power section
    kind: shared
```

## The `power` section

Every key is optional except where `control: managed` requires it. Unknown keys are refused.

| Key | Default | Meaning and limits |
| --- | --- | --- |
| `control` | `manual` | `manual` or `managed`. `managed` requires both `wake_on_lan` and `shutdown`. See [power control](../concepts.md#hosts-and-power) |
| `wake_on_lan` | none | Power-on through the router: a mapping, below |
| `shutdown` | none | Power-off over SSH: a mapping, below |
| `power_on_timeout` | `10m` | How long the host may take to be observed up after a power-on |
| `shutdown_timeout` | `15m` | How long it may take to be observed down after a shutdown |
| `veto_timeout` | `30m` | How long a shutdown may wait for the [veto](../architecture/power.md#veto) to clear. Only meaningful for a host that has a veto reading |
| `min_off` | `0` | The least time the host stays off after a shutdown flowmanager started, before it powers it on by itself. `0` disables it. See [min_off](../architecture/power.md#min_off) |

The three timeouts are above `0` and at most `1d`; `min_off` is at most `1d`. Under
`control: managed` each timeout must also be above
[`policy.host_reading_stale_after`](README.md#policy), or it could expire before one fresh
reading. What an expiry does is in the [timeouts](../concepts.md#hosts-and-power) entry. The
timeouts, `min_off` and a `shutdown` or `wake_on_lan` section under `manual` are validated
but never acted on.

### `wake_on_lan`

| Key | Default | Format |
| --- | --- | --- |
| `router_url` | required | The OPNsense API base URL. Must start with `https://`, have a host, and carry no `user@`, query string or fragment. The API path `/api/wol/wol/set` is appended and a trailing `/` is dropped |
| `interface` | `lan` | The router interface the packet leaves by: an OPNsense interface name of letters, digits and `_` (`lan`, `opt1`) |
| `mac` | required | `aa:bb:cc:dd:ee:ff`, or with `-`; either case. All-zero and all-`ff` are refused. Written back as lowercase with `:` |

### `shutdown`

| Key | Default | Format |
| --- | --- | --- |
| `host` | required | Name or address to connect to; no whitespace |
| `port` | `22` | Above `0` |
| `user` | required | The account that owns the forced-command key; no whitespace |
| `host_key` | required | The host's key fingerprint as `ssh-keygen -lf` prints it: `SHA256:` and 43 base64 characters. A host presenting another key is refused. Certificates are refused |

No two resources may share a `wake_on_lan.mac`, or a `shutdown` `host` and `port` (host names
compare case-insensitively).

```yaml
  - id: basestar-nfs
    kind: shared
    power:
      control: manual
      wake_on_lan: { router_url: "https://router.example.internal", interface: lan, mac: "02:00:00:00:00:02" }
      shutdown: { host: basestar, port: 22, user: poweroff, host_key: "SHA256:BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB" }
      power_on_timeout: 5m
      shutdown_timeout: 10m
      veto_timeout: 30m
      # min_off: 10m
```

## What a powered resource needs

| Rule | Applies to | Detail |
| --- | --- | --- |
| A source declares `reachability` | Any resource with a `power` section, `manual` included | [requirements](../requirements.md#host-power); the sources are on [sources.md](sources.md#kinds-and-capabilities) |
| A `mounts` source with `nfs_servers` for the host | A `managed` shared resource | [requirements](../requirements.md#host-power); [`kubernetes_watch`](sources.md#kubernetes_watch) |
| Each model's `namespaces` are all in a `pods` source's `namespaces` | A `managed` exclusive resource, when any `pods` source exists | [requirements](../requirements.md#host-power); [models](workloads.md#models). With no `pods` source the host has no pods veto |
| `may_change_power` on a class needs a power section on at least one resource it requires | A [class](workloads.md#classes) | The load is refused otherwise |

## Host-side setup

`managed` control needs two things set up outside flowmanager, and two files mounted into the
pod ([install.md](../install.md#4-secrets)). Both fail without any other effect: the power
step fails, is counted and alerted, and is not retried.

### Power-on: the router

flowmanager sends one authenticated `POST <router_url>/api/wol/wol/set` with the interface and
MAC to the OPNsense `os-wol` plugin (`crates/sched-actuate/src/wol.rs`). The pod itself cannot
broadcast on your LAN.

1. Install the `os-wol` plugin on the router.
2. Create an OPNsense user whose only privilege is the Wake on LAN page, and an API key for it.
3. Store the key and secret as one line, `key:secret`, in the file given by
   `--opnsense-credentials-file`. If the router's certificate is from a private CA, give the
   CA bundle with `--opnsense-ca-file`; it is then the only CA trusted.
4. Enable Wake-on-LAN in the host's firmware and NIC.

Redirects are not followed and certificate verification is never off. An HTTP error other than
2xx, or a body whose `status` is not `ok`, fails the request.

### Power-off: a forced-command SSH user

flowmanager connects with the key from `--ssh-key-file`, checks the host's key against
`host_key`, authenticates as `user`, and sends an exec request for `poweroff`
(`crates/sched-actuate/src/ssh.rs`). The host side must make that request harmless to anything
but shutting down:

1. Create a user on the host, or reuse one, that is allowed to power the host off.
2. Generate an unencrypted key pair. Put the private key in the Secret that
   `--ssh-key-file` reads.
3. Add the public key to the user's `authorized_keys` with a forced command and restrictions,
   so the key can do nothing else, whatever command is sent:

   ```
   command="/sbin/poweroff",no-pty,no-port-forwarding,no-agent-forwarding,no-X11-forwarding ssh-ed25519 AAAA... flowmanager
   ```

   The command string is whatever powers your host off, run with the rights that user has
   (a `sudo` rule, for example); flowmanager ignores it.
4. Read the host's fingerprint with `ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub` and put
   it in `host_key`. A reinstalled host has a new key and needs the config updated.

Once the exec is accepted the host is going down: a dropped link or a missing exit status is
success, a non-zero exit status is a failure. Connecting, authenticating and the exec must
finish within 30 seconds.
