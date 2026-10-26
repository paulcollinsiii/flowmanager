# Kubernetes requirements

What the legacy flowmanager app (the root crate, `src/`: the `PowerObject` Restate
handlers) reads from the cluster, and what it needs permission to do. Nothing is
hand-listed in this repo: servers are discovered from labels. The conventions below
come from `homesvr/ops/scripts/power.py`, which this replaces.

> **For deploying the scheduler (`sched-service`) see `docs/kubernetes_setup.md`**: its image,
> flags, Restate registration, Istio policy, RBAC, secrets and ScaledObjects. This file covers
> the legacy power app and its conventions.

> **Retired in M3: KEDA group gating.** The legacy app used to gate workloads itself
> (`KedaGroup` / `KedaCatalog`: the `power.kaffi.internal/group` and `gpu-role` labels on
> ScaledObjects, and the `autoscaling.keda.sh/paused-replicas` annotation it set and
> removed). That code is gone. Workloads are now gated by the scheduler's KEDA external
> scaler (`sched-service`, `--keda-listen`): each ScaledObject has one `external` or
> `external-push` trigger pointing at it, with `model: <id>` or `class: <id>` in its
> metadata. See the README section *KEDA external scaler*. The sections below that
> belonged to the gating are kept, marked retired, so existing objects can be cleaned
> up; see *Migrating a gated ScaledObject*.

## Servers (ConfigMaps)

One ConfigMap per server, in any namespace, labelled `power.kaffi.internal/server=<name>`.
The label value is the server name and the key of its Restate `PowerObject`.

| Key | Required | Meaning |
|---|---|---|
| `HOST` | yes | Address probed and (for shutdown) connected to over SSH. |
| `PORT` | no | TCP port that answers when the server is up. Default 2049. |
| `MAC` | yes | MAC of the NIC that wakes the server. |
| `WOL_INTERFACE` | no | OPNsense interface the router sends the magic packet out of (`lan`, `opt1`, ...). Default `lan`. |
| `SHUTDOWN_USER` | to power off | SSH user with the forced-command key. |
| `HOST_KEY_FPR` | to power off | Pinned SSH host key, as `ssh-keygen -lf` prints it: `SHA256:...`. |

**Retired (M3):** `GROUPS`, `DRAIN` and `EXCLUSIVE_ROLES` were read only by the KEDA
group handlers. They are no longer read; a ConfigMap that still has them works, and
they can be deleted. Which model may run on the GPU is the scheduler's decision now,
from its own configuration (`config/example.yaml`), not from server ConfigMaps.

## ~~Workloads (KEDA ScaledObjects)~~ (retired in M3)

What follows described the retired gating, for reference while cleaning up.

| Label | Meaning |
|---|---|
| `power.kaffi.internal/group=<group>` | Puts the ScaledObject in a group. Required. |
| `power.kaffi.internal/gpu-role=<role>` | Optional role within the group. |

* The modes a group accepts are `off`, `auto` (alias `on`) and each role found on its
  ScaledObjects. `off`, `auto` and `on` may not be used as role names.
* A group needs at least one labelled ScaledObject to exist. A server whose `GROUPS`
  names a group with none is ignored.
* A group with no server (such as `idle`) is just a set of workloads to pause.
* flowmanager pauses by setting `autoscaling.keda.sh/paused-replicas`: `0` to pause,
  `1` to hold up, removed to hand control back to KEDA.
* If Flux owns that annotation (it is in git), flowmanager refuses to change it
  rather than fight Flux. To let flowmanager manage a ScaledObject, keep the
  annotation out of git and set `kustomize.toolkit.fluxcd.io/ssa: Merge` on it so
  Flux keeps annotations that other tools add.

### Migrating a gated ScaledObject

1. **Remove the `autoscaling.keda.sh/paused-replicas` annotation** (from git, and from
   the live object if the legacy app set it). While it is set, KEDA keeps the object
   paused at that count whatever any scaler says.
2. Remove the `power.kaffi.internal/group` and `power.kaffi.internal/gpu-role` labels;
   nothing reads them.
3. Replace the triggers with the single scheduler trigger described in the README
   (*KEDA external scaler*): `minReplicaCount: 0`, `maxReplicaCount: 1`, no `fallback`.

## ~~Nodes and pods (draining)~~ (retired in M3)

The drain wait belonged to the group handler's `off` mode and went with it, along with
the `--drain-timeout-secs` flag. `PowerObject/power_off` never drained: it shuts the
host down when asked, so the caller must make sure nothing still uses it. For reference,
the retired rules were:

* `DRAIN: gpu`: any non-terminal pod that is not owned by a DaemonSet and either
  requests `nvidia.com/gpu` or runs on a node labelled `kaffi.internal/gpu=nvidia`.
  The GPU node should also carry a taint so that other pods do not land on it.
* `DRAIN: nfs`: any non-terminal pod that mounts a PersistentVolume backed by NFS, or
  has an inline `nfs:` volume (any server, to be conservative).

## Permissions

Server discovery is cluster-wide, because the server ConfigMaps may live in any
namespace. Nothing is watched, created, patched or deleted. (Before M3 the gating also
needed `list` and `patch` on `keda.sh` `scaledobjects` and `list` on pods,
persistentvolumes and nodes; drop those rules.)

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: flowmanager
rules:
  # Servers: find the labelled ConfigMaps.
  - apiGroups: [""]
    resources: [configmaps]
    verbs: [list]
```

* `configmaps: list` is cluster-wide, so flowmanager can read every ConfigMap in the
  cluster. It filters by label on the server side, but RBAC cannot restrict by label.
  If that is too broad, move the server ConfigMaps into one namespace and use a
  Role there, then change `find_server` to a namespaced `Api`.
* `get` is not needed; everything is a `list` with a label selector.
* The scheduler's KEDA external scaler needs no Kubernetes permissions: KEDA calls it
  over gRPC, so only the network path from KEDA's operator to `--keda-listen` is needed.
* The SSH shutdown key is a mounted file, not read through the API, so the pod needs
  no access to Secrets.

## Pod configuration

* Mount the shutdown private key (a SOPS-encrypted Secret, decrypted by Flux) at
  `--shutdown-key-path`. Default: `/etc/flowmanager/shutdown_key`. It must be
  unencrypted (no passphrase), mode 0400.
* Egress to each server's SSH port (22) and probe `PORT`.
* Egress to the router's HTTPS port (443) at `--opnsense-url`
  (default `https://router.kaffi.internal`).
* Mount the OPNsense API credentials (a SOPS-encrypted Secret) at
  `--opnsense-credentials-path`. Default: `/etc/flowmanager/opnsense_credentials`. One
  line, `key:secret`. It is read on every wake, so a rotated Secret needs no restart.
* Mount the CA that signed the router's certificate (the `bladerunner-homelab-system`
  issuer's CA) as a PEM bundle at `--opnsense-ca-path`. Only that bundle is trusted.
  Without the flag the platform roots are used, and a minimal image has none.
  Verification is never skipped. `--opnsense-url` must be `https://`.

## Wake-on-LAN

A pod's UDP broadcast does not leave the pod network, so flowmanager does not send
the magic packet itself. It asks the OPNsense `os-wol` plugin to, with
`POST /api/wol/wol/set` and `{"wake": {"interface": "<WOL_INTERFACE>", "mac": "<MAC>"}}`.
No `hostNetwork` is needed. `BROADCAST` is no longer read.

On the router:

* Install `os-wol`.
* Create an API user whose only privilege is *Services: Wake on LAN*, and generate its
  key and secret. The secret is only downloadable once.
* The request shape and response are taken from forum posts, not verified against this
  router's version. Check it with one `curl` before relying on it. Any non-2xx answer,
  or a body whose `status` is not `ok`, is a terminal 502 with
  `event="wol_request_failed"`.

## On the hosts (outside this repo)

* The shutdown user's `authorized_keys` entry is forced-command:
  `restrict,command="<shutdown cmd>" ssh-ed25519 ...`.
  * Ubuntu: `sudo /usr/sbin/shutdown -h now`, with a sudoers `NOPASSWD` entry for it.
  * FreeBSD: `/sbin/shutdown -p now` (`-p`, since `-h` only halts), with the user in
    the `operator` group.
