# Requirements

Whether flowmanager fits your environment. Work down the tables before installing
([install.md](install.md)). Terms are in [concepts.md](concepts.md). A row marked *optional*
names the feature it enables; a feature you do not use needs none of its rows.

## Cluster

| Requirement | Why | Without it |
| --- | --- | --- |
| Kubernetes (required) | The observers read nodes, pods, PersistentVolumes and Jobs through the API; the workloads run there. Built against the 1.36 API types; other versions are untested | No observers, nothing to scale |
| KEDA (required) | flowmanager is a KEDA external scaler: it serves the allowed set and KEDA scales each Deployment 0 or 1. The scaler protocol is vendored unmodified from KEDA v2.21.0 (`crates/sched-keda/proto/NOTICE`); other versions are untested | Nothing consumes the allowed set, so no workload is gated |
| A Restate server reachable from the pod and the pod reachable from it (required) | Durable state, leases and the tick run as Restate virtual objects (`restate-sdk` 0.12 in `Cargo.toml`). The server calls the pod on 9080; the pod calls the server's ingress (`--restate-ingress`) | The process starts and serves a scaler that answers `Unavailable`; no tick runs, so nothing is decided |
| Exactly one replica (required) | The allowed set the scaler serves is held in the memory of the process that ticked, and Restate sends each call to whichever pod answers the Service | With two pods one scaler is never ready or serves a stale set |

## GPU exclusivity

A hard deployment requirement, not a setting: flowmanager never reads pod state to decide, so
Kubernetes must keep two GPU pods from running at once
([invariant 2](architecture.md#safety-invariants)).

| Requirement | Why | Without it |
| --- | --- | --- |
| Every GPU workload requests `nvidia.com/gpu: 1` (required) | The scheduler leaves a new pod Pending until the old one releases the GPU | A new model's pod can start beside the old one |
| The GPU node exposes exactly one GPU: no time-slicing, MPS or MIG (required) | With more than one allocatable GPU two requests both fit | Two models share the GPU or fight over its memory |
| The GPU node is tainted, and only GPU workloads tolerate it (required) | Keeps other pods from taking the GPU | Other pods can land on the GPU node and compete with the gated ones |

## Workloads

| Requirement | Why | Without it |
| --- | --- | --- |
| Each gated workload scales 0 to 1 through a ScaledObject whose only trigger is flowmanager (required) | KEDA activates on any trigger, so a second trigger could start a workload that is not allowed. The ScaledObject names exactly one `model:` or `class:` id | The workload is not gated, or KEDA starts it against the decision. Template: [install.md](install.md#keda) |
| Each demand class's queue is readable by an observer kind that supplies `demand`: `http_json` or `prometheus` (required for a demand class) | The queue decides which model is chosen. `kubernetes_watch` supplies pods, reachability, mounts and jobs, not demand | A class with no demand source is a gate or plain class and never influences which model is chosen |
| An app that must be running for its queue to be read is not gated by a model ScaledObject (required where it applies) | A queue read through the app's own API fails while the app is at 0. A failed read is unknown demand, which holds the model, not zero | Demand reads as unknown while the app is down. Leave the app always on, read the queue from something that stays up (Prometheus, a database-backed API), or make it a [switched class](concepts.md#workloads) |
| Model and class ids of lowercase letters, digits and `-`, at most 50 characters (required) | They name the scaler's metrics (`flowmanager-model-<id>`) | The config is rejected |

## Host power

Optional. Enables `power.control: managed`; a host left at `manual` needs none of this, and
flowmanager never powers it ([concepts](concepts.md#hosts-and-power)).

| Requirement | Why | Without it |
| --- | --- | --- |
| An OPNsense router with the `os-wol` plugin and an API user whose only privilege is Wake on LAN | A pod's broadcast does not leave the pod network, so the router sends the magic packet (`POST /api/wol/wol/set`). The URL must be https and is verified; trust a private CA with `--opnsense-ca-file` | No power-on. The power step fails, is counted and alerted, and is not retried |
| Wake-on-LAN enabled on the host (BIOS or UEFI and NIC), with the MAC and the router interface in the config | The magic packet only wakes a host that listens for it | The host does not start; flowmanager stops after `power_on_timeout` |
| A forced-command SSH user on each managed host, with the private key mounted and the host's SHA256 key fingerprint pinned in the config | Power-off is an SSH exec; the key's `authorized_keys` entry runs the host's shutdown command whatever is sent, so the key can do nothing else. flowmanager has its own SSH client, and a host presenting another key is refused before anything is sent | No power-off. Fails and is not retried; after `shutdown_timeout` flowmanager stops |
| Reachability for each managed host: a Kubernetes node (`kubernetes_watch`), a TCP port to connect to (`tcp_probe`), or a Prometheus query | A power step is confirmed only by observing the host up or down | The host never reads up or down, so a power-on times out and nothing is allowed on it |
| For the NFS veto on a shared host: a `mounts` source naming the host's NFS server, and its pods' NFS mounts visible to the API (directly, or through a PVC and PV) | The host is not powered off while a pod mounts it. A managed shared host without a `mounts` source is rejected at config load | A mount the API cannot show reads as no mount. Pods in a namespace the source does not list are missed, so leave `namespaces` unset |
| For the pods veto on an exclusive host: a `pods` source listing every namespace of the host's models | Power-off waits for zero pods of every model | No applicable reading means no veto: the host is powered off without checking its pods |

## Optional integrations

| Requirement | Why | Without it |
| --- | --- | --- |
| Istio with mTLS and an AuthorizationPolicy | Ports 9080 (Restate), 9095 (KEDA) and 9097 (webhooks, `mesh` mode) authenticate no caller; the mesh must | Anyone who can reach 9080 can call every handler, and the endpoint checks no Restate signature. Treat as required unless another control restricts those ports |
| Home Assistant, able to call HTTP with a bearer token | The toggle listener (9096): switches for leases and for switched apps. Enabled by `--toggle-listen` | No toggles; leases and switches go through the Restate ingress |
| Services that can send a `POST` on a state change | The webhook listener (9097) polls a source at once instead of at its `resync`. Enabled by `--webhook-listen` | Sources are polled at their `resync` only |
| A `CronJob` template for each scheduled job | A scheduled job creates a Job from it | No scheduled jobs |
