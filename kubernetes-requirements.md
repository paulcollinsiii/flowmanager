# Kubernetes requirements

What flowmanager reads from the cluster, and what it needs permission to do.
Nothing is hand-listed in this repo: servers and groups are discovered from labels.
The conventions below come from `homesvr/ops/scripts/power.py`, which this replaces.

## Servers (ConfigMaps)

One ConfigMap per server, in any namespace, labelled `power.kaffi.internal/server=<name>`.
The label value is the server name and the key of its Restate `PowerObject`.

| Key | Required | Meaning |
|---|---|---|
| `HOST` | yes | Address probed and (for shutdown) connected to over SSH. |
| `PORT` | no | TCP port that answers when the server is up. Default 2049. |
| `MAC` | yes | MAC of the NIC that wakes the server. |
| `WOL_INTERFACE` | no | OPNsense interface the router sends the magic packet out of (`lan`, `opt1`, ...). Default `lan`. |
| `GROUPS` | for KEDA | Comma-separated KEDA groups this server serves. |
| `DRAIN` | to power off a group | `nfs` or `gpu`: how to find pods still holding the server. |
| `SHUTDOWN_USER` | to power off | SSH user with the forced-command key. |
| `HOST_KEY_FPR` | to power off | Pinned SSH host key, as `ssh-keygen -lf` prints it: `SHA256:...`. |
| `EXCLUSIVE_ROLES` | no | `true` or `false` (default). See below. |

A group may be served by at most one server. A server may serve several groups, but
turning one group off is refused while another group on the same server still has a
workload that is not paused.

### EXCLUSIVE_ROLES

Roles are not declared anywhere; they are the distinct values of the
`power.kaffi.internal/gpu-role` label on a group's ScaledObjects. Whether two roles
can run together is a fact about the hardware, so the server says so:

* `EXCLUSIVE_ROLES: "true"`: the roles share one resource and cannot coexist (the
  single GPU on cuda-central: OCR, the LLM and speech recognition). A role mode holds
  that role's ScaledObjects at one replica and pauses every other role.
* `EXCLUSIVE_ROLES: "false"` or absent: a role mode still holds its own role at one
  replica, but other roles are left to KEDA.

Either way, a ScaledObject with no role is not touched by a role mode. It follows
the server: it runs whenever the server is on and KEDA wants it.

Set it on cuda-central. Leave it off basestar, which has no roles.

## Workloads (KEDA ScaledObjects)

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

## Nodes and pods (draining)

Before a server is powered off, flowmanager waits (bounded by `--drain-timeout-secs`)
for the pods that hold it to exit. It does not cordon, evict or delete anything.

* `DRAIN: gpu`: any non-terminal pod that is not owned by a DaemonSet and either
  requests `nvidia.com/gpu` or runs on a node labelled `kaffi.internal/gpu=nvidia`.
  The GPU node should also carry a taint so that other pods do not land on it.
* `DRAIN: nfs`: any non-terminal pod that mounts a PersistentVolume backed by NFS, or
  has an inline `nfs:` volume (any server, to be conservative).

Pods not owned by a gated ScaledObject must be stopped by hand; the drain timeout
lists them.

## Permissions

All discovery is cluster-wide, because servers, ScaledObjects and pods live in many
namespaces. Nothing is watched, created or deleted.

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
  # Workloads: find gates, then pause/hold/unpause them by annotation.
  - apiGroups: [keda.sh]
    resources: [scaledobjects]
    verbs: [list, patch]
  # Draining: find the pods and volumes that hold a server.
  - apiGroups: [""]
    resources: [pods, persistentvolumes, nodes]
    verbs: [list]
```

* `configmaps: list` is cluster-wide, so flowmanager can read every ConfigMap in the
  cluster. It filters by label on the server side, but RBAC cannot restrict by label.
  If that is too broad, move the server ConfigMaps into one namespace and use a
  Role there, then change `list_servers` to a namespaced `Api`.
* `get` is not needed; everything is a `list` with a label selector.
* `patch` on ScaledObjects uses a merge patch with field manager `flowmanager`, and
  only touches `metadata.annotations["autoscaling.keda.sh/paused-replicas"]`.
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
