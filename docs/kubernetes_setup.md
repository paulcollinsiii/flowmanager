# Deploying flowmanager on Kubernetes

This file is written for an agent (or a person) writing the Kubernetes manifests for
`sched-service`, the flowmanager scheduler, in a separate repository (Flux, Istio mTLS with
AuthorizationPolicy, KEDA v2.21, a Restate server in the cluster). It is meant to be
self-contained. Every flag, environment variable, port, handler, metric and config key
below was checked against the code of this repository (the last section lists what could
not be checked and why). Values that belong to the reference deployment (service names,
namespaces, ports of other applications) come from `config/example.yaml` and are examples:
replace them with the cluster's real ones.

Companion documents: `README.md` (how the scheduler decides, handlers, metrics, blind
spots), `config/example.yaml` (the reference configuration), `planning/architecture.md`
(design), `kubernetes-requirements.md` (the retired legacy power app and how to migrate
its ScaledObjects).

Contents: 1 image and workload, 2 Restate, 3 Istio, 4 RBAC, 5 secrets and environment,
6 KEDA, 7 GPU exclusivity, 8 reachability and power, 9 operations, 10 what was not
verified.

## 1. Image and workload

### Build

The binary is `sched-service` (package `sched-service`, `crates/sched-service/src/main.rs`).
It is a Rust workspace and needs these at **build** time:

- a Rust toolchain with edition 2024 support (a current stable `rust` image);
- **`protoc`**: `crates/sched-keda/build.rs` compiles the vendored KEDA external scaler
  proto (`crates/sched-keda/proto/externalscaler.proto`) with `tonic-prost-build`. The
  build fails without `protoc` on `PATH` (or `PROTOC` set). `mise.toml` lists `protoc` for
  local builds; a build image must install it (Debian bookworm: `protobuf-compiler`);
- a C compiler (the TLS stack builds `aws-lc-sys`; the `rust` image has one).

Nothing is needed at runtime except `ca-certificates` (the HTTP and Kubernetes clients use
the system root store through `rustls-platform-verifier`) and a glibc matching the build.
Build only the service: `cargo build --release --locked -p sched-service`. Do not build
the workspace root package `flowmanager` (the retired legacy app, not part of the image).

This Dockerfile was built and the resulting image was run (`--help`, start-up with the
example config) while writing this document:

```dockerfile
FROM rust:1-bookworm AS build
RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p sched-service

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/sched-service /usr/local/bin/sched-service
USER 65532:65532
EXPOSE 9080 9090 9095
ENTRYPOINT ["/usr/local/bin/sched-service"]
```

(Use `COPY Cargo.toml Cargo.lock crates ...` layering or cargo-chef if build time matters;
`--locked` requires `Cargo.lock` in the context.)

### Flags and environment

Every flag has an environment variable; a flag wins over its variable. These are all of
them (`sched-service --help`):

| Flag | Environment variable | Default | Meaning |
| --- | --- | --- | --- |
| `--config <PATH>` | `SCHED_CONFIG` | required | Scheduler configuration (YAML). A mounted ConfigMap; re-read on change. |
| `--listen <ADDR>` | `SCHED_LISTEN` | `0.0.0.0:9080` | The **Restate service endpoint** (the `Scheduler` and `Recovery` virtual objects), HTTP/2 without TLS (h2c). |
| `--metrics-listen <ADDR>` | `SCHED_METRICS_LISTEN` | `0.0.0.0:9090` | Prometheus: `GET /metrics`; also `GET /healthz` (answers `ok`). |
| `--keda-listen <ADDR>` | `SCHED_KEDA_LISTEN` | `0.0.0.0:9095` | The **KEDA external scaler** (gRPC, plaintext). |
| `--restate-ingress <URL>` | `SCHED_RESTATE_INGRESS` | `http://localhost:8080` | The Restate **ingress** URL. The process calls it itself: `POST {url}/Scheduler/main/tick/send` when observations or leases change, and `POST {url}/Scheduler/main/arm_backstop/send` at start-up (retried every 5 s until Restate accepts) and whenever its backstop watchdog finds the chain overdue (not run for more than three intervals plus 10 s). **Must be set in the cluster** (the default is localhost). |
| `--backstop <DURATION>` | `SCHED_BACKSTOP` | `30s` | Interval of the periodic backstop tick. Greater than zero. |
| `--reload-interval <DURATION>` | `SCHED_RELOAD_INTERVAL` | `5s` | How often the config file is re-read. Greater than zero. |
| `--trigger-debounce <DURATION>` | `SCHED_TRIGGER_DEBOUNCE` | `250ms` | Merges bursts of changes into one requested tick. |
| `--log-level <LEVEL>` | `SCHED_LOG` | `info` | `error`, `warn`, `info`, `debug` or `trace`. |

Durations accept plain milliseconds or a suffix `ms`, `s`, `m`, `h`, `d`. There is no
`RUST_LOG`. Besides these, the process reads `KUBERNETES_SERVICE_HOST`/`..._PORT` and the
service account token through the Kubernetes client (set by Kubernetes in a pod) and the
secret variables named by the configuration (section 5). It writes logs to stdout.

Ports: **9080** Restate service endpoint, **9090** metrics and `/healthz`, **9095** KEDA
scaler. All three bind at start; a port already in use stops the process.

### The configuration file (ConfigMap)

Put the scheduler configuration (see `config/example.yaml`) in a ConfigMap and mount the
**directory** (not `subPath`: Kubernetes does not update `subPath` mounts, so reloads would
never arrive). The file is re-read every `--reload-interval` (5 s); the text is compared
and only a changed text is validated and applied. Behaviour, from `config_source.rs`:

- the file is validated in full before it replaces the active config; an invalid or
  unreadable file is logged once per distinct problem at `error` and the **last good
  config stays active** (`sched_config_reloads_total{outcome="rejected"|"unreadable"}`);
- an invalid file **at start-up** stops the process (no earlier config exists), so a bad
  ConfigMap breaks a restart, not a running pod;
- each tick uses one config generation (`sched_config_generation`);
- model and class ids must be lowercase letters, digits and `-`, starting and ending with
  a letter or digit, at most 50 characters. This is a deliberately conservative rule that keeps
  ids usable in resource-style names, not a Kubernetes or KEDA metric-name requirement
  (Kubernetes only validates an external metric name as a path segment; KEDA's 63 character
  limit applies to the ScaledObject name and the HPA name `keda-hpa-<name>`).

### Replicas: exactly 1

Run **one replica** and use `strategy: type: Recreate` (not a rolling update). The reasons
are in the code: the published allowed set lives in the memory of the process whose tick
computed it (a `tokio::sync::watch` channel), the KEDA scaler serves that memory, the
observation cache is per process, and `last_decision` is per process. Restate routes each
invocation to whichever pod answers on the Service, so with two pods one would tick and the
other's scaler would never be ready or would serve a stale set. During a rolling update the
same thing happens for a while. With `Recreate` there is a short gap with no scaler: KEDA
keeps replica counts while the scaler errors (section 6), so the gap is safe.

Persistent state lives in Restate, not in the pod (the sequencer state, leases, the
recovery attempts), so the pod needs no volume. Losing Restate's own data loses leases and
the sequencer state (the scheduler then restarts from observation, see section 9).

### Workload template

```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: flowmanager, namespace: flowmanager }
spec:
  replicas: 1
  strategy: { type: Recreate }
  selector: { matchLabels: { app: flowmanager } }
  template:
    metadata: { labels: { app: flowmanager } }
    spec:
      serviceAccountName: flowmanager        # section 4
      containers:
        - name: flowmanager
          image: registry.example/flowmanager:TAG
          env:
            - { name: SCHED_CONFIG, value: /etc/flowmanager/config.yaml }
            - { name: SCHED_RESTATE_INGRESS, value: "http://restate.restate.svc.cluster.local:8080" } # the real Restate ingress Service
            - { name: SCHED_BACKSTOP, value: 30s }
            - name: PAPERLESS_API_TOKEN       # names come from `auth.env` in the config
              valueFrom: { secretKeyRef: { name: flowmanager-tokens, key: paperless } }
            - name: SPEAKR_API_TOKEN
              valueFrom: { secretKeyRef: { name: flowmanager-tokens, key: speakr } }
            - name: IMMICH_API_KEY
              valueFrom: { secretKeyRef: { name: flowmanager-tokens, key: immich } }
          ports:
            - { name: http2, containerPort: 9080 }   # Restate service endpoint (h2c)
            - { name: http-metrics, containerPort: 9090 }
            - { name: grpc-keda, containerPort: 9095 }
          volumeMounts:
            - { name: config, mountPath: /etc/flowmanager, readOnly: true }
          livenessProbe:  { httpGet: { path: /healthz, port: 9090 }, periodSeconds: 20 }
          readinessProbe: { httpGet: { path: /healthz, port: 9090 }, periodSeconds: 10 }
      volumes:
        - { name: config, configMap: { name: flowmanager-config } }
---
apiVersion: v1
kind: Service
metadata: { name: flowmanager, namespace: flowmanager }
spec:
  selector: { app: flowmanager }
  ports:
    - { name: http2, port: 9080, targetPort: 9080 }        # Restate server -> here
    - { name: http-metrics, port: 9090, targetPort: 9090 } # Prometheus
    - { name: grpc-keda, port: 9095, targetPort: 9095 }    # KEDA operator
```

Notes on the template:

- `/healthz` only proves that the process serves its metrics listener; it does not say
  that Restate has registered the deployment or that a tick has completed. Use the
  metrics for that (section 9). The Restate endpoint and the scaler have no probe.
- Name the ports so Istio picks the right protocol: the Restate endpoint speaks HTTP/2
  prior knowledge (the SDK serves only HTTP/2, no HTTP/1.1), the scaler is gRPC.
  `http2` / `grpc-...` port names or `appProtocol` do that (not checked against this
  cluster's Istio version).
- Keep GPU workloads' `terminationGracePeriodSeconds` short: when a model is withdrawn,
  the next model's pod stays Pending until the old pod has terminated and released the
  GPU (section 7), so a long grace period delays every switch.

## 2. Restate

The service endpoint is `http://flowmanager.<namespace>.svc.cluster.local:9080`
(`--listen`). Register it with the Restate admin API (default admin port 9070, ingress
8080 are Restate server defaults, not facts of this repository):

```sh
restate deployments register http://flowmanager.flowmanager.svc.cluster.local:9080
# or: curl -X POST http://restate-admin:9070/deployments \
#       -H 'content-type: application/json' \
#       -d '{"uri": "http://flowmanager.flowmanager.svc.cluster.local:9080"}'
```

Registration runs a discovery request: the URL must be reachable **from the Restate
server**. Registering an endpoint that is already registered changes nothing, because
Restate cannot tell that the code behind the URL changed. A new image that adds or changes
a handler therefore needs `restate deployments register ... --force` (replaces in place,
meant for development) or a new URL (`--breaking` if a service's type changed); if the
Restate operator's `RestateDeployment` is used, it handles versioned deployments itself
(not checked here). Registration was checked live with `restate` CLI 1.8.1 and a
`restate-server` 1.8.1: it registers two services.

### Virtual objects and handlers

Two virtual objects, both keyed. `Scheduler` has exactly one instance, key **`main`**.
`Recovery` has one instance per workload class, key = the **class id** (for the example
config, `speakr-transcription` is the one class with a recovery action). All calls are
`POST {ingress}/{Object}/{key}/{handler}`; add `/send` for a one-way call. Bodies are JSON
(`content-type: application/json`); handlers marked "no body" take none.

**Ingress-private** (registered so that the ingress refuses them with
`400 {"code":400,"message":"the invoked service is not public","source":"ingress"}`; only
other Restate handlers can call them; checked live):

| Handler | Why private |
| --- | --- |
| `Scheduler/main/observe` | Pushed observations. A forged push (a host "up", queue demand) could steer decisions. The in-process observers do not use it. |
| `Recovery/{class}/run` | Runs the recovery action. Only the `tick` sends it. |

**Public** (callable through the Restate ingress by anything that is allowed to reach it;
the handlers have **no authentication of their own**, so Istio policy, section 3, is the
only control). Request and response shapes were exercised live:

| Handler | Body | What it can do | Who should be allowed |
| --- | --- | --- | --- |
| `Scheduler/main/tick` | none | Runs one decision now. Harmless (the core is deterministic, a tick is cheap). Answers `{"config_generation","at","kinds","error"}`. | flowmanager itself (it calls `.../tick/send`); an operator for debugging. |
| `Scheduler/main/arm_backstop` | none | Starts a new backstop chain and supersedes any earlier one (bumps an epoch in the object's state). Answers `{"epoch": N}`. | flowmanager itself (`.../arm_backstop/send`). |
| `Scheduler/main/backstop` | `{"epoch": N}` | One link of the backstop chain; the object sends it to itself with a delay. Called by hand with the **current** epoch it would run a tick and schedule itself again, which would fork the chain (read from the code, not exercised); with an old epoch it just ends. | Nobody. Block it at the ingress. |
| `Scheduler/main/last_decision` | none | Read only. The latest tick report this process published (inputs with their ages, the reason, the kinds, the sequencer state, the leases it saw); `null` right after a restart until the first tick. Does not wait behind a tick. | operators, monitoring. |
| `Scheduler/main/leases` | none | Read only. The leases held (a JSON array). | operators, monitoring, Open WebUI. |
| `Scheduler/main/acquire_model_lease` | `{"model": "<id>"}` | **Pins a model on its GPU**: batch work is cut short (its classes leave the allowed set at once, in-flight work gets `drain_timeout`, or a class's `kill_timeout`, then is stopped) and the model stays allowed however much other work waits, until the lease is released. Asking again for the leased model returns the existing lease; another model of the same resource is `409`; an unknown model is `400`. | an operator (a person); later Open WebUI's service account. |
| `Scheduler/main/release_lease` | `{"id": "manual-model-<n>"}` | Releases a lease and returns it; unknown id `404`. Whoever can release can end somebody's pin. | an operator; the same identity that acquired it. |
| `Scheduler/main/discard_state` | `{"key": "scheduler_state"\|"lease_book", "force": bool}` | **Destructive**: deletes a persisted document so the next tick starts it afresh. Without `force` only a document that cannot be read (`409` otherwise). `lease_book` drops every lease; `scheduler_state` drops the sequencer state. Returns what was stored. | operators only. |
| `Recovery/{class}/attempts` | none | Read only. The stored attempts document (text, `null` if none). | operators, monitoring. |
| `Recovery/{class}/discard_attempts` | `{"force": bool}` | Deletes the re-queue attempts of a class (with `force`, a readable document too, which gives every failed recording a fresh set of attempts). | operators only. |

Anything not listed here (`/restate/...` introspection, the admin API on 9070) is Restate's
own; protect the admin API separately.

### Using the lease handlers

A manual model lease is how a person (and, later, Open WebUI) gets the LLM loaded and kept
loaded: "pin the model `llm` on its GPU until I release it". Rules (`README.md`, *Manual
model leases*; the error texts below were produced by a live run): the model must exist in
the config; at most one manual model lease per exclusive resource; lease ids are
`manual-model-<n>`; a lease whose model later disappears from the config is kept but
ignored. A lease also asks for its host to be up; under `power.control: manual` that means
the lease **waits** until a person powers the host on. Nothing releases a lease by time:
**the release is manual**. Open WebUI will later hold such a lease and a person releases it.

```sh
INGRESS=http://restate.restate.svc.cluster.local:8080   # the Restate ingress

# Pin the LLM:
curl -sS -X POST $INGRESS/Scheduler/main/acquire_model_lease \
  -H 'content-type: application/json' -d '{"model": "llm"}'
# {"lease":{"id":"manual-model-1","resource":"cuda-central","kind":{"type":"manual_model","model":"llm"},"granted_at":1793027881869},"created":true}

# Another model of the same GPU while one is pinned:
#   HTTP 409 {"code":409,"message":"cannot lease \"mineru\": cuda-central already has manual model lease \"manual-model-1\" for \"llm\"; release it first", ...}
# An unknown model:
#   HTTP 400 {"code":400,"message":"unknown model \"nope\": the active config has no such model (configured: mineru, llm, whisperx, immich-ml)", ...}

# List, then release:
curl -sS -X POST $INGRESS/Scheduler/main/leases
curl -sS -X POST $INGRESS/Scheduler/main/release_lease \
  -H 'content-type: application/json' -d '{"id": "manual-model-1"}'

# What the scheduler decided and why (the tick kinds include `pinned` while a lease holds):
curl -sS -X POST $INGRESS/Scheduler/main/last_decision

# Ask for a decision right now:
curl -sS -X POST $INGRESS/Scheduler/main/tick
```

Acquiring and releasing also ask for a tick, so the effect shows within seconds (the
published set changes; KEDA follows at its next poll, section 6).

## 3. Istio AuthorizationPolicy

mTLS authenticates the peer, an AuthorizationPolicy decides what it may do. The
flowmanager has no authentication of its own, and **the service endpoint on port 9080 does
not verify Restate's request signature** (`Endpoint::builder()` in `main.rs` sets no
identity key). The "ingress-private" protection of `observe` and `Recovery.run` exists
only at the Restate ingress, so anything that can open a connection to flowmanager's
port 9080 can call those handlers directly. The policy on 9080 below is therefore
essential, not optional.

Principals are `cluster.local/ns/<namespace>/sa/<service account>`. The service accounts
named here are guesses of common defaults; use the real ones.

| From | To | Port / path | Protocol | Why |
| --- | --- | --- | --- | --- |
| Restate server | flowmanager | 9080 (all paths) | HTTP/2 (h2c inside the mesh) | Restate invokes every handler of both objects here. **Only** the Restate server's principal. |
| KEDA operator (`keda-operator`, namespace `keda`) | flowmanager | 9095 | gRPC | The external scaler: `IsActive`, `StreamIsActive`, `GetMetricSpec`, `GetMetrics`. KEDA's metrics adapter does not call the scaler directly; the operator does. Only the operator. |
| Prometheus | flowmanager | 9090, `GET /metrics` | HTTP | Scrape. `/healthz` is for kubelet probes. |
| flowmanager | Restate ingress | 8080 (Restate default), `POST /Scheduler/main/tick/send` and `POST /Scheduler/main/arm_backstop/send` | HTTP | The only ingress paths the process itself calls. |
| operators (a person through whatever path is trusted) | Restate ingress | 8080, `POST /Scheduler/main/{leases,last_decision,tick,acquire_model_lease,release_lease,discard_state}`, `POST /Recovery/*/{attempts,discard_attempts}` | HTTP | Section 2's table. Keep the destructive ones (`discard_*`) to operators only. |
| Open WebUI (later) | Restate ingress | 8080, `POST /Scheduler/main/{acquire_model_lease,release_lease,leases}` | HTTP | Holds a manual model lease. Nothing else. |
| nobody | Restate ingress | `/Scheduler/main/backstop` | | Not for callers. |

Egress from the flowmanager pod (the services named below are the examples in
`config/example.yaml`; take the real ones from the config's source URLs):

| To | Port | Used by |
| --- | --- | --- |
| speakr (`speakr.media.svc`) | 8899 | `speakr-pending`, `speakr-summaries` polls (`GET /api/v1/stats`, `GET /api/v1/recordings?...`) and the recovery's `POST /api/v1/recordings/{id}/transcribe` |
| paperless-ngx (`paperless-ngx.docs.svc`) | 8000 | `paperless-ocr-pending`, `paperless-post-ocr` (`GET /api/documents/?...`) |
| immich-server (`immich-server.media.svc`) | 2283 | `immich-queues` (`GET /api/queues/{smartSearch,faceDetection,ocr}`) |
| Restate ingress | 8080 | `SCHED_RESTATE_INGRESS`, the two paths above |
| Kubernetes API (`kubernetes.default.svc`, 443) | 443 | the node (and optional pod) watches, section 4 |
| basestar (`HOST`, port 2049) | 2049 | the `tcp_probe` source, section 8: **outside the mesh** |

Things that need care:

- **The TCP probe and the sidecar.** With a sidecar, an outbound connection is redirected
  to Envoy and the application's `connect()` succeeds at once, before Envoy has tried
  the upstream. A connect probe would then read "up" whatever basestar does. Exclude the
  probe's destination from interception on the flowmanager pod, for example
  `traffic.sidecar.istio.io/excludeOutboundIPRanges: "<basestar ip>/32"` (or
  `excludeOutboundPorts: "2049"`), and use an IP address in the probe's `host`. Not
  checked against this cluster; **check it**: with basestar powered off, the probe must
  turn it down (`sched_resource_phase_age_seconds{resource="basestar-nfs",axis="power",phase="off"}`
  appears and the `last_decision` inputs show it down), not stay up. Egress to speakr, paperless and immich goes through the mesh normally.
- If the mesh's outbound policy is `REGISTRY_ONLY`, basestar and the Kubernetes API need
  `ServiceEntry`s.
- The Restate server must be able to reach flowmanager on 9080 for discovery as well as for
  invocations.
- flowmanager serves the scaler without TLS on purpose: the mesh provides mTLS, and the
  ScaledObject's trigger leaves `enableTLS` off.

## 4. RBAC for the Kubernetes watch observer

Only the `kubernetes_watch` source talks to the Kubernetes API (the KEDA scaler needs no
permissions: KEDA calls it). The client is built from the pod's environment
(`kube::Client::try_default`: in-cluster service account). From `kube_watch.rs`, the
observer does exactly this, with `list` followed by `watch` (kube's default watcher), and
nothing else (no `get`, no writes, no ConfigMaps, no Secrets):

- **nodes**: `Api::<Node>::all`: `list` and `watch` on `nodes`, **cluster-wide**, when the
  source declares `observe: [reachability]`. It reads each node's `Ready` condition; the
  node named by `params.nodes.<resource>` is that resource's host. An absent node reads as
  down.
- **pods**: `Api::<Pod>::namespaced` per entry of `params.namespaces`: `list` and `watch`
  on `pods` in **each** of those namespaces, only when the source declares `observe: [pods]`.
  Pod counts are kept for information only (`sched_observation_age_seconds{capability="pods"}`);
  no decision reads them. A config whose kube source observes only `[reachability]`
  (and needs no `namespaces`) validates and starts (checked), which removes the pod
  permissions entirely.

```yaml
apiVersion: v1
kind: ServiceAccount
metadata: { name: flowmanager, namespace: flowmanager }
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata: { name: flowmanager-nodes }
rules:
  - apiGroups: [""]
    resources: [nodes]
    verbs: [list, watch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata: { name: flowmanager-nodes }
roleRef: { apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: flowmanager-nodes }
subjects: [{ kind: ServiceAccount, name: flowmanager, namespace: flowmanager }]
---
# Only if the kube source observes pods. One Role + RoleBinding per namespace in
# params.namespaces (the example: ai, media, docs).
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: { name: flowmanager-pods, namespace: ai }
rules:
  - apiGroups: [""]
    resources: [pods]
    verbs: [list, watch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: { name: flowmanager-pods, namespace: ai }
roleRef: { apiGroup: rbac.authorization.k8s.io, kind: Role, name: flowmanager-pods }
subjects: [{ kind: ServiceAccount, name: flowmanager, namespace: flowmanager }]
```

A missing permission shows up as a source error (`sched_source_errors_total{source="cluster-pods"}`,
a warning in the log); the host's reachability then goes stale and nothing is allowed on it
(safe, but nothing runs). The retired legacy ClusterRole (`configmaps: list`, KEDA
`scaledobjects` patch, pods/PVs/nodes list) is not needed; drop it.

## 5. Secrets and environment

Secrets are never in the config file. A source's `auth` (and a recovery action's `auth`)
names where the token comes from: `env: NAME` (an environment variable) or `file: /path`
(a mounted file, whitespace trimmed, read on each request, so a rotated Secret needs no
restart). The configuration chooses the variable names; the example config uses these three,
and the code reads no other secret:

| Variable | Used by | Needs |
| --- | --- | --- |
| `PAPERLESS_API_TOKEN` | `paperless-ocr-pending`, `paperless-post-ocr` (header `Authorization: Token <token>`) | A paperless-ngx API token that can list documents (the polls use `page_size=1` and read only `count`). |
| `SPEAKR_API_TOKEN` | `speakr-pending`, `speakr-summaries`, and the `speakr-transcription` recovery (`Authorization: Bearer <token>`) | **One speakr API token with the `read` and `process` scopes**: `read` lists recordings and reads `/api/v1/stats`; `process` re-queues failed transcriptions (`POST /api/v1/recordings/{id}/transcribe`). The token's user must be allowed to edit the recordings, and `/stats` is scoped to that user (other users' recordings are not seen). |
| `IMMICH_API_KEY` | `immich-queues` (header `x-api-key: <key>`, no prefix) | An **admin** API key with the permission **`queue.read`**; immich v4.0 or later. The `GET /api/queues/*` API is marked alpha upstream (since immich v2.4.0), so a future immich may change its shape; the poll then fails and demand goes stale (it does not read as zero). |
| `PROMETHEUS_TOKEN` (optional) | only a `prometheus`-kind source with `auth` | Not used by the example config (it is a commented, spare source). |

A missing or empty variable is not a start-up error: the affected source reports an error
at each poll (`environment variable X is not set`), counted in
`sched_source_errors_total`, and its demand reads as unknown (busy, never idle) until it
is fixed. Check the three after deploying. The scheduler's own settings are the `SCHED_*`
variables of section 1. In the manifests, take each token from a `Secret` (SOPS-encrypted
in git, decrypted by Flux) with `secretKeyRef`, or mount it as a file and use `file:`.

## 6. KEDA

KEDA v2.21. The scaler implements `externalscaler.ExternalScaler`
(`crates/sched-keda/proto`, vendored from KEDA v2.21.0) on port 9095, plaintext gRPC.
It is a lookup over the set the latest tick published: a model or a class is *allowed* or
not. It runs on its own task, so a scaler call never waits on a tick.

### The trigger

Each ScaledObject has **one** `external` (polled) or `external-push` (streaming) trigger.
Its `metadata` names exactly one subject (checked in `sched-keda/src/lib.rs`):

| Key | Active while |
| --- | --- |
| `model: <id>` | that model is in the allowed set (its pods may run: loading, serving or winding down). For a model's own Deployment. |
| `class: <id>` | that workload class is in the allowed set (every requirement met). For a consumer gated on a host condition (a gate class). |

`scalerAddress` (required by KEDA, and `enableTLS`, `unsafeSsl`) is accepted and ignored by
the scaler. Anything else is refused with gRPC `InvalidArgument` naming the ScaledObject:
neither or both of `model` and `class`, another key, an empty value, or an id the **active**
config does not have (model ids and class ids are separate namespaces). `GetMetricSpec`
answers one metric named `flowmanager-model-<id>` or `flowmanager-class-<id>` with target 1;
`GetMetrics` answers 1 while allowed, else 0; `StreamMetricSpec` is `Unimplemented` (KEDA
falls back to pull).

### ScaledObject rules

- `minReplicaCount: 0`, `maxReplicaCount: 1`. Replicas are exactly 0 or 1.
- A **short `pollingInterval` and `cooldownPeriod`** (the examples: 5 s and 15 s):
  hysteresis (minimum dwell, idle grace) lives in the scheduler, and a long cooldown only
  delays a withdrawal the scheduler is waiting for.
- **No `fallback`.** With no fallback, when the scaler returns an error KEDA keeps the
  current replica count (verified for KEDA v2.21 by the code's authors, not by a run in
  this cluster). The scaler returns `Unavailable` until this process has published its
  first tick, so after every restart of flowmanager nothing is scaled up or down. A
  `fallback` would set a replica count the scheduler never decided and defeat that.
- **The scaler is the only trigger.** KEDA activates on any trigger, so a second trigger
  (cpu, a cron) could start a model that is not allowed.
- No `autoscaling.keda.sh/paused-replicas` annotation (below).

```yaml
# A model's own Deployment, polled.
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata: { name: mineru, namespace: ai }
spec:
  scaleTargetRef: { name: mineru }          # the Deployment
  minReplicaCount: 0
  maxReplicaCount: 1
  pollingInterval: 5
  cooldownPeriod: 15
  # no fallback, no other triggers
  triggers:
    - type: external
      metadata:
        scalerAddress: flowmanager.flowmanager.svc.cluster.local:9095
        model: mineru
---
# A consumer gated on a class, pushed: StreamIsActive tells KEDA as soon as the answer changes.
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata: { name: paperless-gpt, namespace: docs }
spec:
  scaleTargetRef: { name: paperless-gpt }
  minReplicaCount: 0
  maxReplicaCount: 1
  pollingInterval: 5
  cooldownPeriod: 15
  triggers:
    - type: external-push
      metadata:
        scalerAddress: flowmanager.flowmanager.svc.cluster.local:9095
        class: paperless-gpt
```

### Which workload follows what (matching `config/example.yaml`)

The mapping lives only in the ScaledObjects and the scheduler config, never in code.

| Workload | ScaledObject | Why |
| --- | --- | --- |
| mineru | `model: mineru` | The OCR model; runs only while it is the allowed model on cuda-central. |
| ollama (the LLM) | `model: llm` | Also what a manual model lease pins. |
| whisperx | `model: whisperx` | Transcription. |
| the GPU immich ML replica (`immich-machine-learning-gpu`) | `model: immich-ml` | An accelerator; a CPU replica on the always-on node always serves. |
| paperless-gpt | `class: paperless-gpt` | A **gate class**: no model, "cuda-central up and basestar-nfs up" (`requires` a host with no `model`). It runs only while the GPU host and the NFS host are up; GPU contention is not gated. |
| speakr (the app), paperless-ngx, immich-server | **no ScaledObject: always running** | The scheduler reads their queues through their APIs, so they must run for the queues to be readable. Gating NFS dependents is later work (M4). (`config/example.yaml` still defines NFS-only classes `paperless-ngx` and `immich-server`; do not scale on them for now.) |

The ids on the right are the ids in the scheduler config, not workload names; one
model's `workloads` list in the config is informational (pod counts).

### Failure behaviour you must know

- **Not ready**: before the first tick of this process, and once the process is stopping,
  every call answers `Unavailable`. Observed live: after a restart the first scaler call
  answered `Unavailable`, and within about two seconds the answers were real.
- **A removed id**: a ScaledObject whose `model`/`class` id is no longer in the config gets
  `InvalidArgument`, so KEDA keeps that ScaledObject's replica count (a removed model that
  was running keeps running) and reports the scaler error on the ScaledObject. **Delete the
  ScaledObject and the Deployment together with the model** (and remove the model from the
  config in the same change). The scheduler logs this once per distinct error at `warn`
  (repeats at `debug`), not once per KEDA poll.
- **An added id** is answered from the allowed set at once, inactive until a tick allows it.

### Migrating from the retired `paused-replicas` annotations

The legacy power app paused ScaledObjects with `autoscaling.keda.sh/paused-replicas`.
While that annotation is set KEDA keeps the object paused at that count **whatever any
scaler says**, so:

1. remove the `autoscaling.keda.sh/paused-replicas` annotation from git **and** from the
   live objects (the legacy app set it at run time);
2. remove the `power.kaffi.internal/group` and `power.kaffi.internal/gpu-role` labels
   (nothing reads them);
3. replace the triggers with the single scheduler trigger above.

`kubernetes-requirements.md` has the detail and the retired conventions (the `GROUPS`,
`DRAIN` and `EXCLUSIVE_ROLES` server keys are ignored now).

## 7. GPU exclusivity is Kubernetes' job

flowmanager publishes **at most one model per exclusive resource** and decides from queues,
leases and policy only. It never inspects pods to decide, and it does not see GPU
contention. Exactly one model runs on the GPU only because Kubernetes makes it so:

- **Every GPU workload requests `nvidia.com/gpu: 1`** in its resource limits (mineru, the
  LLM, whisperx, the GPU immich ML replica, anything else on the GPU node).
- The node exposes **one** GPU: the NVIDIA device plugin with no time-slicing, no MPS, no
  MIG.
- The GPU node is **tainted** and only GPU workloads carry the toleration, so nothing else
  lands there.

When flowmanager switches, the old model leaves the allowed set and the new one enters it
in the same tick; KEDA scales the old Deployment to 0 and the new one to 1, and the new pod
stays Pending until the old pod has terminated and released the GPU.

**What breaks if a GPU workload lacks the limit**: its pod is not accounted against the
GPU, so Kubernetes will schedule it onto the GPU node (if it tolerates the taint) while
another model's pod holds the GPU. Two models then run at once on one GPU (memory
exhaustion, crashes, corrupted jobs) and **nothing in flowmanager notices**: it reports
one model allowed. The same applies to a node that exposes more than one GPU or
time-sliced GPUs. Treat the limit as a hard requirement and check it for every GPU
Deployment (`README.md`, *Known blind spots*).

## 8. Reachability and power

Which host is up decides what may be allowed: no model is allowed on a host observed off,
and allowing one needs a fresh "up" reading (`policy.safety_max_age`, 30 s in the example).

- **cuda-central** comes from the Kubernetes node watch (`kubernetes_watch`, `observe:
  [reachability]`): `params.nodes: { cuda-central: <node name> }` maps the resource to the
  Kubernetes node; the host is up while the node's `Ready` condition is `True` and down
  when it is not ready or absent from the cluster. Kubernetes marks a node NotReady only
  after its own grace period (40 s by default, a kube-controller-manager setting, not this
  repository's). The flip needs `reachability_confirmations` (default 3) agreeing checks,
  one per `resync` (10 s in the example); the **first** reading after a process start is
  adopted at once.
- **basestar** (the NFS host) is not a Kubernetes node. It uses a `tcp_probe` source: one
  TCP connect per `resync` from the flowmanager pod to `host:port` (the example: `basestar`
  port **2049**, the NFS port; the connection is closed at once, nothing is sent). Connected
  is **up**; connection refused, no answer within `timeout` (default 2 s, between 100 ms
  and 60 s and not above `resync`), or "no route to host" (`EHOSTUNREACH`; Linux reports it
  after about 3 s, or sooner, for a local-network address that does not answer ARP, which
  is what a powered-off basestar looks like) is **down**. Anything else (the name does not
  resolve, "network unreachable", an unusable address) is a failed read: the reading goes
  stale and decisions that need NFS wait; it is never read as "down". Up/down flips after
  3 agreeing readings (30 s with a 10 s resync).
  - Use an **IP address or a fully qualified name** the pod can resolve. The name is
    resolved by the pod through cluster DNS, not on the node. If cluster DNS does not
    resolve the bare name `basestar`, every read fails and basestar stays stale.
  - It only shows that something accepts connections on that port, not that the NFS
    export works. Behind an Istio sidecar the probe is meaningless unless the destination is
    excluded from interception (section 3).
- **`power.control: manual`** (the default, and what the example config says for both
  hosts): a person powers the hosts on and off. flowmanager never emits a power step and
  follows the observed reachability: work that needs a host that is off waits
  (`wait ... the host is not up`), including a manual model lease. `managed` (flowmanager
  powers hosts) is later work; do not configure it.

## 9. Operations

### Metrics

Scrape `:9090/metrics` (`/healthz` answers `ok`). The complete set, checked against a live
scrape and the registrations in `metrics.rs` (labelled counters appear after their first
event):

- `sched_tick_decisions_total{kind}`: ticks by decision kind (`allow_model`, `drain_model`,
  `pause_intake`, `power_on`, `power_off`, `kill_in_flight`, `run_recovery`, `wait`, `idle`,
  `error`, `pinned`, `demand_unknown`, `demand_unsupported`, `demand_unpolled`).
- `sched_persisted_state_errors_total{key}`: a tick or lease request could not read a
  persisted document (`scheduler_state`, `lease_book`).
- `sched_resource_phase_age_seconds{resource,axis,phase}`: age of the current power phase
  (`axis="power"`, phase `off|powering_on|on|shutting_down`) and mode phase
  (`axis="mode"`, phase `idle|serving|winding_down`).
- `sched_observation_age_seconds{source,capability}`: age of the newest observation
  (infinite if never reported).
- `sched_source_has_observer{source}`, `sched_source_errors_total{source}`,
  `sched_rejected_reports_total{reason}`.
- `sched_recovery_runs_total{class,outcome}` (`ran`, `skipped_interval`,
  `skipped_not_allowed`, `not_wired`, `list_failed`, `state_error`) and
  `sched_recovery_requeues_total{class,outcome}` (`attempted`, `queued`, `failed`,
  `skipped_cap`, `skipped_not_allowed`, `deferred`).
- `sched_tick_duration_seconds` (histogram), `sched_last_tick_timestamp_seconds`,
  `sched_tick_trigger_failures_total` (flowmanager could not ask the Restate ingress for a
  tick), `sched_config_generation`, `sched_config_reloads_total{outcome}`
  (`applied`, `unchanged`, `rejected`, `unreadable`).

### Alert suggestions

```yaml
- alert: FlowmanagerNoTicks            # Restate registration, ingress or the process
  expr: time() - sched_last_tick_timestamp_seconds > 180   # the backstop ticks every 30 s
  for: 5m
- alert: FlowmanagerCannotReachRestate
  expr: increase(sched_tick_trigger_failures_total[10m]) > 0
- alert: FlowmanagerPersistedStateError  # nothing is published while it lasts
  expr: increase(sched_persisted_state_errors_total[10m]) > 0
- alert: FlowmanagerStuckPhase           # a wind-down past drain_timeout; a busy non-preemptible class waits for ever by design
  expr: sched_resource_phase_age_seconds{phase=~"winding_down|powering_on|shutting_down"} > 900
- alert: FlowmanagerSourceBlind          # reachability never reported, or silent
  expr: sched_observation_age_seconds{capability="reachability"} > 120
  for: 5m
- alert: FlowmanagerSourceErrors
  expr: increase(sched_source_errors_total[15m]) > 5
- alert: FlowmanagerConfigRejected       # the last good config is still active
  expr: increase(sched_config_reloads_total{outcome=~"rejected|unreadable"}[15m]) > 0
- alert: FlowmanagerRecoveryFailing
  expr: increase(sched_recovery_requeues_total{outcome="failed"}[30m]) > 0
        or increase(sched_recovery_runs_total{outcome=~"list_failed|state_error"}[30m]) > 0
- alert: FlowmanagerRecoveryCapReached   # failed recordings that used all their attempts; re-queue by hand
  expr: increase(sched_recovery_requeues_total{outcome="skipped_cap"}[30m]) > 0
```

Thresholds are suggestions to tune; they are not tested against a live cluster. A tick of
kind `error` (`sched_tick_decisions_total{kind="error"}` increasing) is worth an alert as
well.

### Reading and changing state

- `POST .../Scheduler/main/last_decision`: the latest tick (inputs and their ages, the
  reason in words, the kinds, `not_executed`). Start here for "why is nothing running".
- `restate state get Scheduler main` shows the object's stored keys (`scheduler_state`,
  `lease_book`, `backstop_epoch`); `restate state get Recovery <class>` the attempts. Verified
  with `restate` CLI 1.8.1. A single key can be edited with `restate state patch`
  (JSON Patch) or cleared with `restate state clear Scheduler/main` (clears all of it; the
  backstop is then re-armed by restarting the pod).
- `discard_state` (and `Recovery/{class}/discard_attempts`) delete a stored document that
  cannot be read; a readable one only with `"force": true`. They return what was stored:
  keep a copy.

```sh
curl -sS -X POST $INGRESS/Scheduler/main/discard_state \
  -H 'content-type: application/json' -d '{"key": "scheduler_state"}'
# HTTP 409 {"code":409,"message":"scheduler_state reads fine; pass \"force\": true to discard it anyway", ...}
curl -sS -X POST $INGRESS/Scheduler/main/discard_state \
  -H 'content-type: application/json' -d '{"key": "scheduler_state", "force": true}'
```

Discarding `scheduler_state` restarts the sequencer from observation: no model is allowed
until the planner picks one again (KEDA scales a running pod of a model it does not pick
down), dwell and idle clocks start over, and the memory of which sources have reported is
dropped. Discarding `lease_book` drops every lease (an LLM pin vanishes).

### Rollback

**Roll the binary back together with the config.** The build before queue-only switching
rejects the current config (a gate class: "exclusive; a model is required"; and a config
without a pods source), so deploy the previous image with the previous ConfigMap. The
persisted state itself is readable by both builds (version 1). A document written by a
newer build that this one cannot decode makes ticks report an error and publish nothing
(the scaler keeps what it had); the fix is the build that wrote it, or `discard_state`.
After a rollback that changes handlers, re-register the deployment (section 2).

### The first tick after a restart

After a restart the observation cache is empty, so the scheduler continues from the
persisted state (a serving model stays allowed and keeps its dwell clock; a wind-down goes
on) and **holds every safety-critical step** (allowing a model, powering off) until the
observers report fresh reachability again, seconds later. Until the first tick completes
the scaler answers `Unavailable` and KEDA keeps replica counts (no fallback). Persisted
"on" keeps the serving model and every gate class (such as `paperless-gpt`) allowed for those
seconds even if a host went down meanwhile; a "down" reading closes it at its tick. The
backstop re-arms itself within seconds (`arm_backstop` is retried every 5 s until Restate
accepts).

## 10. Checked, and not checked

Checked against this repository's code (greps, reading, tests) and, where marked "live", by
running the debug binary with `restate-server` and the `restate` CLI 1.8.1 on a
workstation and the image built from the Dockerfile above:

- flags, environment variables, defaults and ports (`--help` of the built image);
- the three listeners, `/metrics`, `/healthz`, the metric names (a live scrape);
- registration, the handler list and which are ingress-private, request and response
  shapes and the `400`/`404`/`409` answers of every public handler (live);
- the scaler's metadata rules and answers (tests, and `scaler_probe` against the live
  process, including `Unavailable` just after a start);
- the RBAC (the watch calls in `kube_watch.rs`; a reachability-only config starts);
- the paths the process calls on the Restate ingress (`runtime.rs`);
- the env/file secret handling and which variables the example config names.

**Not checked (no cluster, no Istio, no live upstream services here)** and to be verified
when deploying:

- Istio behaviour: port protocol selection for the h2c and gRPC ports, the policies in
  section 3, `REGISTRY_ONLY`, and above all the TCP probe through a sidecar;
- KEDA v2.21 behaviour on scaler errors without `fallback` (stated by the scaler's
  design and by KEDA's documentation, never run against a real KEDA), the `external-push`
  stream, polling and cooldown timing, and a ScaledObject whose scaler returns
  `InvalidArgument`;
- the Restate operator's `RestateDeployment`, and Restate server default ports (8080,
  9070) used above;
- RBAC against a real API server (the observer has only run against a fake API in tests);
- the pod-label mapping `app.kubernetes.io/name` for pod counts (information only);
- speakr (scopes, user edit permission, `/stats` scoping), paperless-ngx and immich
  (`queue.read`, v4.0, the alpha queue API) as documented upstream; the speakr response
  shapes were checked against its source, not a live instance;
- that cluster DNS resolves basestar's name; that a fully qualified name or an IP is used;
- NotReady timing of the node (a Kubernetes setting);
- the backstop-forking claim for a hand-called `backstop` (read from the code);
- the Restate admin `curl` form of the registration (the CLI form was run);
- how KEDA's components talk (the operator calls the scaler; the metrics adapter calls the
  operator).
