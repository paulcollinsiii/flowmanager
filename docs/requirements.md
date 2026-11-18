# Requirements

Whether flowmanager fits your environment. Work down the tables before installing
([install.md](install.md)). Terms are in [concepts.md](concepts.md). A row is *required*, or
*optional* with the feature it enables; a feature you do not use needs none of its rows.

## Cluster

| Requirement | Why | Without it |
| --- | --- | --- |
| Kubernetes (required) | The observers read nodes, pods, PersistentVolumes and Jobs through the API; the workloads run there. Built against the 1.36 API types; other versions are untested | No observers, nothing to scale |
| KEDA (required) | flowmanager is a KEDA external scaler: it serves the allowed set and KEDA scales each Deployment. The scaler protocol is vendored unmodified from KEDA v2.21.0 (`crates/sched-keda/proto/NOTICE`); other versions are untested | Nothing consumes the allowed set, so no workload is gated |
| A Restate server, reachable from the pod and able to reach the pod (required) | Durable state, leases and the tick run as Restate virtual objects. The server calls the pod's endpoint; the pod calls the server's ingress (`--restate-ingress`). Tested with Restate server 1.8.1 and `restate-sdk` 0.12 | The process starts and its scaler answers `Unavailable`; no tick runs, so nothing is decided |
| Exactly one replica (required) | The allowed set the scaler serves is held in the memory of the process that ticked, and Restate sends each call to whichever pod answers the Service | With two pods one scaler is never ready or serves a stale set |
| Access control on the Restate endpoint and the KEDA scaler (required): Istio mTLS with an `AuthorizationPolicy`, or a `NetworkPolicy` naming the Restate server and the KEDA operator | Neither port authenticates a caller, and the endpoint checks no Restate signature | Anyone who can reach the endpoint can call every handler, including the ones the ingress hides. Policy: [install.md](install.md#9-istio-and-network-policy) |

## GPU exclusivity

A hard deployment requirement, not a setting. flowmanager decides from observations, not pod
state (the veto and recovery readiness are the only exceptions), so Kubernetes must keep two
GPU pods from running at once ([invariant 2](architecture.md#safety-invariants)).

| Requirement | Why | Without it |
| --- | --- | --- |
| Every GPU workload requests `nvidia.com/gpu: 1` (required) | A new pod stays Pending until the old one releases the GPU | A new model's pod can start beside the old one |
| The GPU node exposes exactly one GPU: no time-slicing, MPS or MIG (required) | With more than one allocatable GPU two requests both fit | Two models share the GPU or fight over its memory |
| The GPU node is tainted, and only GPU workloads tolerate it (required) | Keeps other pods from taking the GPU | Other pods can land on the GPU node and compete with the gated ones |

## Workloads

| Requirement | Why | Without it |
| --- | --- | --- |
| Each gated workload scales 0 to 1 through a ScaledObject whose only trigger is flowmanager (required) | The ScaledObject names one `model:` or `class:` id. Template and rules: [install.md](install.md#8-keda) | The workload is not gated, or KEDA starts it against the decision |
| Each demand class's queue is readable by an observer kind that supplies `demand`: `http_json` or `prometheus` (required for a demand class) | The queue decides which model is chosen. `kubernetes_watch` supplies pods, reachability, mounts and jobs, not demand | A class with no demand source is a gate or plain class and never influences which model is chosen |
| An app whose queue is read through its own API is not scaled to 0 by a model ScaledObject (required where it applies) | A read fails while the app is down, and a failed read is unknown demand, which holds the model rather than reading zero | Demand reads as unknown while the app is down. Leave the app always on, read the queue from something that stays up (Prometheus, a separate API), or make it a [switched class](concepts.md#workloads) |
| Model and class ids of lowercase letters, digits and `-`, starting and ending with a letter or digit, at most 50 characters (required) | They name the scaler's metrics (`flowmanager-model-<id>`) | The config is rejected |

## Host power

Optional: a `power` section on a resource, and with `control: managed` the rows marked
*managed*. A host with `control: manual` is never powered by flowmanager
([concepts](concepts.md#hosts-and-power)).

| Requirement | Why | Without it |
| --- | --- | --- |
| A source that declares `reachability` (required for any resource with a `power` section, `manual` included): a Kubernetes node (`kubernetes_watch`), a TCP port to connect to (`tcp_probe`), a Prometheus query or an `http_json` read | Host state, and so every decision about the host, comes from this reading; a power step is confirmed only by observing the host up or down | The config is rejected (`validate.rs`, "has a power section but no source declares the reachability capability") |
| *Managed:* an OPNsense router with the `os-wol` plugin and an API user whose only privilege is Wake on LAN | A pod's broadcast does not leave the pod network, so the router sends the magic packet. The URL must be https and is verified | No power-on. The step fails, is counted and alerted, and is not retried |
| *Managed:* Wake-on-LAN enabled on the host (firmware and NIC), with its MAC and the router interface in the config | The magic packet only wakes a host that listens for it | The host does not start; flowmanager stops after `power_on_timeout` |
| *Managed:* a forced-command SSH user on each host, with the private key mounted and the host's SHA256 key fingerprint pinned in the config | Power-off is an SSH exec. The key's `authorized_keys` entry runs the host's shutdown command whatever is sent, so the key can do nothing else. flowmanager has its own SSH client and refuses a host presenting another key before sending anything | No power-off; after `shutdown_timeout` flowmanager stops, and does not retry |
| *Managed, shared host:* a `mounts` source with `nfs_servers` for it, and the pods' NFS mounts visible to the API (directly, or through a PVC and PV) | The host is not powered off while a pod mounts it | The config is rejected. A mount the API cannot show reads as no mount, so leave the source's `namespaces` unset to see every namespace |
| *Managed, exclusive host with a `pods` source:* every namespace of every model of the host listed in a `pods` source's `namespaces` | The veto waits for zero pods of every model, and a pod it cannot see would read as unknown | The config is rejected. With no `pods` source at all the host has no pods veto |

The veto readings apply by condition, not by host kind
([veto](architecture/power.md#veto)).

## Other integrations

| Requirement | Why | Without it |
| --- | --- | --- |
| Home Assistant, able to call HTTP with a bearer token (optional) | The toggle listener: switches for leases and for switched apps. Enabled by `--toggle-listen` | No toggles; leases and switches go through the Restate ingress |
| Services that can send a `POST` on a state change (optional) | The webhook listener polls a source at once instead of at its `resync`. Enabled by `--webhook-listen` | Sources are polled at their `resync` only |
| A `CronJob` template for each scheduled job (optional) | A scheduled job creates a Job from it | No scheduled jobs |
