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
 && apt-get install -y --no-install-recommends ca-certificates tzdata \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/sched-service /usr/local/bin/sched-service
USER 65532:65532
EXPOSE 9080 9090 9095 9096 9097
ENTRYPOINT ["/usr/local/bin/sched-service"]
```

`tzdata` is required, not optional, once any job is configured: jobs are scheduled in a time
zone (`UTC` by default) and the zone is looked up in the system database, which the Rust
time library does not bundle on Linux. Verified by hiding `/usr/share/zoneinfo`: the config
is then rejected at load with "failed to find timezone UTC" (even for `UTC`). The Dockerfile
above installs `tzdata`; an image built another way must include it too.

The repository root has a `.dockerignore` (`target/`, `.git/`, scratch and worktree
directories); keep it when building from the repository, or `COPY . .` sends the whole
`target/` directory into the build context.

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
| `--toggle-listen <ADDR>` | `SCHED_TOGGLE_LISTEN` | off (suggested `0.0.0.0:9096`) | The **toggle listener** for Home Assistant (plain HTTP, bearer token); off when absent; needs `--toggle-token-file`. See the toggle listener subsection of section 2. |
| `--toggle-token-file <PATH>` | `SCHED_TOGGLE_TOKEN_FILE` | none | The bearer token the toggle listener requires (a mounted Secret, one line), read on every request. Needs `--toggle-listen`; start-up fails if the file is missing or empty. |
| `--webhook-listen <ADDR>` | `SCHED_WEBHOOK_LISTEN` | off (suggested `0.0.0.0:9097`) | The **webhook listener** ("poll now", plain HTTP); off when absent; needs `--webhook-auth`. See the webhook listener subsection of section 2. |
| `--webhook-auth <mesh\|token>` | `SCHED_WEBHOOK_AUTH` | none, **no default** | `mesh`: this process authenticates no one, Istio must. `token`: bearer token from `--webhook-token-file`. Needs `--webhook-listen`. |
| `--webhook-token-file <PATH>` | `SCHED_WEBHOOK_TOKEN_FILE` | none | The bearer token for `--webhook-auth token` (a mounted Secret, one line), read on every request; start-up fails if missing or empty. Refused with `mesh`. |
| `--webhook-min-interval <DURATION>` | `SCHED_WEBHOOK_MIN_INTERVAL` | `10s` | The least time between two polls of a source that webhooks nudge. Greater than zero. |
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
scaler, **9096** (suggested) the toggle listener and **9097** (suggested) the webhook listener when enabled. Each binds at start; a port already in use stops the process.

### The configuration file (ConfigMap)

Put the scheduler configuration (see `config/example.yaml`) in a ConfigMap and mount the
**directory** (not `subPath`: Kubernetes does not update `subPath` mounts, so reloads would
never arrive). The file is re-read every `--reload-interval` (5 s). Text identical to the active
config is skipped (outcome `unchanged`); any other text is validated, and a persistently
invalid file is validated again at every interval, so
`sched_config_reloads_total{outcome="rejected"}` grows each interval (the log line is
written once per distinct problem). Behaviour, from `config_source.rs`:

- the file is validated in full before it replaces the active config; an invalid or
  unreadable file is logged once per distinct problem at `error` and the **last good
  config stays active** (`sched_config_reloads_total{outcome="rejected"|"unreadable"}`, one count per interval while the file stays bad);
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
| `Scheduler/main/acquire_host_lease` | `{"resource": "<id>"}` | **Holds a shared host up** (the NFS server): a `managed` host that is off is powered on, and while the lease is held everything gated on the host is allowed even with no work queued. Asking again returns the lease; an exclusive resource, one without a power section or an unknown one is `400`. Ids `manual-host-<n>`. | an operator. |
| `Scheduler/main/release_host_lease` | `{"resource": "<id>"}` | Releases the manual host lease of a resource; none held is `404`. The host is then shut down once nothing else holds it, `idle_grace` has passed and no pod mounts it. | an operator. |
| `Scheduler/main/reclaim` | `{"resource": "<id>"}` | **Takes an exclusive host back despite pending work**: intake stops, in-flight work gets `reclaim_timeout` and is then cut off, a `managed` host is shut down and not woken until `unreclaim`. Only for an exclusive resource with a power section (`400` otherwise). Ids `reclaim-<n>`. | an operator. |
| `Scheduler/main/unreclaim` | `{"resource": "<id>"}` | Releases the reclaim; none held is `404`. | an operator. |
| `Scheduler/main/resume` | `{"resource": "<id>"}` | After a host was fixed by hand: clears a failed power transition, or a shutdown stopped by mounting pods, for that one resource, which then follows observation (found running means up and not managed). Powers nothing; other resources and the leases are untouched. Not stopped: `409`. | an operator. |
| `Scheduler/main/jobs` | none | Read only. Every configured scheduled job with its state: next time, the run in progress (phase, attempt, Job name, retry time), the last outcome and the last success, and a Job past its deadline that may still be active. | operators, monitoring. |
| `Scheduler/main/trigger_job` | `{"id": "<job id>"}` | **Starts a run of a scheduled job now** (it waits for the job's needs, then the Job is created). Refused with `409` while the previous instance is pending or running, never queued; unknown id `404`. | an operator. |
| `Scheduler/main/discard_state` | `{"key": "scheduler_state"\|"lease_book"\|"job_book", "force": bool}` | **Destructive**: deletes a persisted document so the next tick starts it afresh. Without `force` only a document that cannot be read (`409` otherwise). `lease_book` drops every lease; `scheduler_state` drops the sequencer state; `job_book` forgets every run, lingering mark and next time (a Job still running is then unknown to the next run, which can start beside it). Returns what was stored. | operators only. |
| `Recovery/{class}/attempts` | none | Read only. The stored attempts document (text, `null` if none). | operators, monitoring. |
| `Recovery/{class}/discard_attempts` | `{"force": bool}` | Deletes the re-queue attempts of a class (with `force`, a readable document too, which gives every failed recording a fresh set of attempts). | operators only. |

Anything not listed here (`/restate/...` introspection, the admin API on 9070) is Restate's
own; protect the admin API separately.

### Using the lease handlers

A manual model lease is how a person (and Home Assistant, through the toggle listener below) gets the LLM loaded and kept
loaded: "pin the model `llm` on its GPU until I release it". Rules (`README.md`, *Manual
model leases*; the error texts below were produced by a live run): the model must exist in
the config; at most one manual model lease per exclusive resource; lease ids are
`manual-model-<n>`; a lease whose model later disappears from the config is kept but
ignored. A lease also asks for its host to be up; under `power.control: manual` that means
the lease **waits** until a person powers the host on. Nothing releases a lease by time:
**the release is manual**. Home Assistant can hold such a lease through the toggle listener below (Open WebUI itself is not integrated), and a person releases it.

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

The same shape serves the other lease handlers (`README.md`, *Host leases, reclaim and
resume*):

```sh
# Bring basestar up and keep it up (the consumers' gates stay open), then let it go:
curl -sS -X POST $INGRESS/Scheduler/main/acquire_host_lease \
  -H 'content-type: application/json' -d '{"resource": "basestar-nfs"}'
curl -sS -X POST $INGRESS/Scheduler/main/release_host_lease \
  -H 'content-type: application/json' -d '{"resource": "basestar-nfs"}'
# Take the GPU host back / give it up again:
curl -sS -X POST $INGRESS/Scheduler/main/reclaim \
  -H 'content-type: application/json' -d '{"resource": "cuda-central"}'
curl -sS -X POST $INGRESS/Scheduler/main/unreclaim \
  -H 'content-type: application/json' -d '{"resource": "cuda-central"}'
# After fixing a host flowmanager stopped at (the answer says what was cleared):
curl -sS -X POST $INGRESS/Scheduler/main/resume \
  -H 'content-type: application/json' -d '{"resource": "basestar-nfs"}'
```

A manual model lease on a GPU host that is off wakes it too (under `managed`), so there is no
separate GPU lease. After a release, a managed NFS host is shut down only when no pod mounts
it (section 8).

### The toggle listener (Home Assistant)

An optional HTTP listener lets Home Assistant's `rest` switch hold and release manual
leases by name, with no lease ids and no Restate calls on its side. It is **off unless
`--toggle-listen` is given** (the same convention as the optional secret files: an absent
flag means the feature is off) and it **needs `--toggle-token-file`**; each flag refuses to
parse without the other, and the process stops at start-up if the token file is missing or
empty, or the port is taken. Suggested address: `0.0.0.0:9096`, like the other listeners.
There is deliberately no default address: loopback would be unreachable for the client (a
pod, or a `hostNetwork` pod arriving from the node address), and an all-interfaces default
would open an unauthenticated-looking port on every install. Open WebUI itself is **not**
integrated; Home Assistant is the only client this listener is built and documented for.

The token is one line in a mounted Secret, read **on every request** (a rotated Secret takes
effect without a restart). Every request, including an unknown path, needs
`Authorization: Bearer <token>`; the comparison takes the same time wherever it differs, a
wrong or missing token is `401` with `WWW-Authenticate: Bearer`, an empty or unreadable file
is `503` (never a match), and the token is never logged.

```yaml
apiVersion: v1
kind: Secret
metadata: { name: flowmanager-toggle, namespace: flowmanager }
stringData:
  token: "<a long random string>"       # e.g. openssl rand -hex 32; SOPS-encrypt it in git
# in the Deployment: args --toggle-listen=0.0.0.0:9096 --toggle-token-file=/etc/toggle/token
#   volumeMounts: { name: toggle, mountPath: /etc/toggle, readOnly: true }
#   volumes:      { name: toggle, secret: { secretName: flowmanager-toggle } }
```

**Plain HTTP, on purpose.** Home Assistant's `rest` platform cannot trust a private CA (only
`verify_ssl: false`), so this phase adds no TLS to the listener: the bearer token plus the
network around the port (below) are the controls. The token travels in clear inside the
cluster; do not expose the port outside it.

**What it does.** Nothing is decided in the listener. Each request is turned into the
Restate handler a person would call (`acquire_model_lease`, `release_lease`,
`acquire_host_lease`, `release_host_lease`, `reclaim`, `unreclaim`, `resume`, `leases`) and
sent through the Restate ingress (`--restate-ingress`, the URL the process already uses), so
the object's lock, journal and persisted state are used exactly as for any other caller; the
listener keeps no lease state. Reads come from the lease list the same way and from the last
tick's report in the process (so a status is absent until this process has ticked).

| Request | Meaning |
| --- | --- |
| `GET /v1/leases` | Every model, powered resource and reclaimable resource with its state, and a top-level `needs_attention`. |
| `GET /v1/models/{model}` | A manual model lease (on = the lease is held, **not** that the model is loaded). |
| `POST /v1/models/{model}/on`, `/off` | Take or release it. Idempotent: on when held returns the same state; off when not held is a success. |
| `GET /v1/resources/{resource}`, `POST .../on`, `.../off` | The manual host lease of a shared host with a power section (the NFS server): powers a managed host on and keeps it up. |
| `POST /v1/resources/{resource}/resume` | Clears a failed power phase (or a shutdown stopped by mounting pods) after a person fixed the host. `409` if the resource is not stopped. |
| `GET /v1/reclaims/{resource}`, `POST .../on`, `.../off` | `reclaim` / `unreclaim` of an exclusive resource with a power section ("this GPU is mine"): intake stops, work is cut off after the reclaim timeout, the host is shut down if its power is managed, and it is not woken until released. |

`POST /v1/{models,resources,reclaims}/{name}` with a body of `ON` or `OFF` (also
`true`/`false`, or JSON `{"state": "on"}` / `{"held": true}`) does the same as `/on` and
`/off`: that is the shape Home Assistant's REST switch sends to a single URL. Names are the
config's ids (percent-encoded if needed). A body over 1 KiB is refused.

A state answer, for example `GET /v1/models/llm`:

```json
{"kind":"model","id":"llm","resource":"cuda-central","held":true,"state":"on",
 "held_since_ms":1793027881869,"held_for_seconds":120,
 "status":"waiting_for_host","detail":"cuda-central is powering_on"}
```

`held` (boolean) and `state` (`"on"` / `"off"`) are always there. `status` and `detail` are
derived from the last tick's report and are **left out when they cannot be said**:

- model: `active` (in the allowed set), `waiting_for_host` (its host is not up),
  `host_failed`, `draining` (another model of the resource is winding down, its in-flight work
  awaited), `reclaimed` (a reclaim is held on its resource: no model is allowed there until it is
  released, whatever the host is doing), `pending` (the last tick predates the lease, or another
  model is still allowed and the report gives no more);
- host lease (`GET /v1/resources/..`): `power` (the phase as the sequencer holds it, with `since`
  and, when failed, `during`), `wait` (a list: `min_off` with the seconds left, or
  `shutdown_held_by_mounts` with `stopped`), `needs_attention`;
- reclaim: `host_failed`, `powered_off`, `shutting_down`, `draining`, `left_for_person` (the host
  is up and its power is `control: manual`, so flowmanager never shuts it down: power it off
  yourself), `pending`, the `power`
  phase, `tick_reason` (the tick's reason in words) and `needs_attention`.

`needs_attention` is true when a person has to act: a resource is in a failed power phase, or
its shutdown was stopped by mounting pods (`resume` it after fixing the host). It is the same
condition as the metrics `sched_resource_power_failed == 1` and
`sched_resource_mounts_veto{state="stopped"} == 1`; the JSON field is derived from the same state,
there is no separate metric for it (alert on those two, see the alert list).

Errors are JSON `{"error": "<message>"}`. The handler's own message and status are relayed for
`400` (a resource that cannot be leased: an exclusive resource for a host lease, a shared one or
one without a power section for a reclaim) and `409` (another model already leased on the
resource; `resume` of a resource that is not stopped). `404` is the listener's own answer for a
name the config does not have (the message lists what is configured) or a path it does not
serve. A `404` from Restate is never relayed: Restate gives it for a service it does not know
(the `Scheduler` deployment is not registered), so it is a `502`, like any other unexpected
answer. `405` carries an `Allow` header, `401` a `WWW-Authenticate`, `503` means Restate or the
token file is unreachable and `504` that Restate did not answer in 15 seconds. An off request is a
success only when the lease listing shows the lease gone (or never there); a release that fails is
checked against a fresh listing and is otherwise the error. An exclusive resource's
`GET /v1/resources/..` answers `lease_supported: false`.

**Observability.** Every on/off/resume request, and every refused request of any kind (a
wrong or missing token, an unknown path, a wrong method, an oversize body), logs one structured
line `event="toggle"` with `kind` (`model`, `host`, `reclaim`, `list`, `unmatched`), `name`,
`action` (`get`, `on`, `off`, `resume`, `unknown`), `outcome`, `status` and `suppressed`; never the
token or a body. `name` is what the caller put in the path, shortened to 64 characters and logged
escaped (a newline in a path cannot start a log line of its own). At most 30 refused requests are
logged per 10 seconds; the next line says in `suppressed` how many were left out (every request is
still counted in the metric, and a successful change is always logged). The acquire and release
themselves log as
before. Metrics: `sched_toggle_requests_total{endpoint,outcome}` (counted per endpoint
label, never per name), `sched_manual_lease_age_seconds{kind,target}` for each held manual
model lease, host lease and reclaim (`kind` is `model`, `host` or `reclaim`), and the existing
`sched_resource_power_failed` / `sched_resource_mounts_veto` for needs-attention.

**Limits.** The token is checked before a request's body is read (an oversize or non-UTF-8 body
of an unauthenticated caller is a `401`, of an authenticated one a `400`; a body is limited to 1024
bytes and read only by the `POST /v1/<kind>/{name}` form). At most 64 connections are served at
once; a client has 5 seconds to send its request headers, so idle connections do not hold the slots,
and no connection lives longer than 30 seconds.

**Network.** Expose the port with a named Service port and let only Home Assistant reach it:

```yaml
# Service ports: add
    - { name: http-toggle, port: 9096, targetPort: 9096 }   # Home Assistant
# Deployment ports: add
            - { name: http-toggle, containerPort: 9096 }
---
# Home Assistant runs with hostNetwork: its packets arrive from the NODE address, not a pod
# address, so a podSelector/namespaceSelector rule will not match it. Allow the node network
# (the CIDR of the nodes' LAN; how a CNI treats host-network sources differs, e.g. Cilium
# classifies them as the "host" / "remote-node" entities: check yours). Not tested here.
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: { name: flowmanager-toggle, namespace: flowmanager }
spec:
  podSelector: { matchLabels: { app: flowmanager } }
  policyTypes: [Ingress]
  ingress:
    - from: [ { ipBlock: { cidr: 192.168.2.0/24 } } ]        # the node LAN
      ports: [ { port: 9096, protocol: TCP } ]
    # keep the rules for 9080/9090/9095 that you already have: a policy that selects the pod
    # denies everything else
```

If Istio runs this workload under `PeerAuthentication` `STRICT` mTLS, a caller without a
sidecar is refused, and Home Assistant has none (its namespace has no injection; it is
`pod-security: privileged`). Exempt the toggle port only:

```yaml
apiVersion: security.istio.io/v1
kind: PeerAuthentication
metadata: { name: flowmanager, namespace: flowmanager }
spec:
  selector: { matchLabels: { app: flowmanager } }
  mtls: { mode: STRICT }
  portLevelMtls:
    "9096": { mode: PERMISSIVE }
```

Keep one `PeerAuthentication` per workload: Istio applies a single workload-selector policy to a
workload (when several select it, the oldest wins), so a second resource for another port would
have its port setting silently ignored. If the webhook listener (section below) is also enabled,
add its port to this same resource instead. (Not tested.)

and give 9096 an `AuthorizationPolicy` rule with no `from.principals` (a plain-text caller has
none); the bearer token and the NetworkPolicy are then what protect the port. (Not tested: the
PeerAuthentication and AuthorizationPolicy behaviour for a caller without a sidecar is described
from how Istio documents them, not from a run on this cluster.) The 9080 port
stays as section 3 describes.

**Home Assistant.** Home Assistant resolves cluster DNS names (`hostNetwork: true` with
`dnsPolicy: ClusterFirstWithHostNet`; this and the plain-HTTP peer are taken from its
manifests, not tested here), and it already talks to an in-cluster service over plain
HTTP the same way. Keep the whole header value `Bearer <token>` in an environment variable (or
`!secret`), because a YAML header cannot concatenate:

```yaml
# configuration.yaml
switch:
  - platform: rest                  # one model: on = a manual lease is held
    name: Flowmanager LLM lease
    unique_id: flowmanager_lease_llm
    resource: http://flowmanager.flowmanager.svc.cluster.local:9096/v1/models/llm
    state_resource: http://flowmanager.flowmanager.svc.cluster.local:9096/v1/models/llm
    method: post
    body_on: "ON"
    body_off: "OFF"
    is_on_template: "{{ value_json.held }}"
    headers:
      Authorization: !env_var FLOWMANAGER_TOGGLE_AUTH "Bearer unset"   # the value is "Bearer <token>"
    timeout: 20
  - platform: rest                  # one host: the NFS server kept up
    name: Flowmanager NFS host lease
    unique_id: flowmanager_lease_basestar
    resource: http://flowmanager.flowmanager.svc.cluster.local:9096/v1/resources/basestar-nfs
    state_resource: http://flowmanager.flowmanager.svc.cluster.local:9096/v1/resources/basestar-nfs
    method: post
    body_on: "ON"
    body_off: "OFF"
    is_on_template: "{{ value_json.held }}"
    headers:
      Authorization: !env_var FLOWMANAGER_TOGGLE_AUTH "Bearer unset"
    timeout: 20
  - platform: rest                  # the GPU host reclaimed: "this GPU is mine"
    name: Flowmanager GPU reclaim
    unique_id: flowmanager_reclaim_cuda_central
    resource: http://flowmanager.flowmanager.svc.cluster.local:9096/v1/reclaims/cuda-central
    state_resource: http://flowmanager.flowmanager.svc.cluster.local:9096/v1/reclaims/cuda-central
    method: post
    body_on: "ON"
    body_off: "OFF"
    is_on_template: "{{ value_json.held }}"
    headers:
      Authorization: !env_var FLOWMANAGER_TOGGLE_AUTH "Bearer unset"
    timeout: 20

sensor:
  - platform: rest                  # the richer status of the model lease
    name: Flowmanager LLM status
    resource: http://flowmanager.flowmanager.svc.cluster.local:9096/v1/models/llm
    value_template: "{{ value_json.status | default(value_json.state) }}"
    headers:
      Authorization: !env_var FLOWMANAGER_TOGGLE_AUTH "Bearer unset"

binary_sensor:
  - platform: rest                  # a host flowmanager stopped at, waiting for a person
    name: Flowmanager needs attention
    resource: http://flowmanager.flowmanager.svc.cluster.local:9096/v1/leases
    value_template: "{{ value_json.needs_attention }}"
    headers:
      Authorization: !env_var FLOWMANAGER_TOGGLE_AUTH "Bearer unset"

rest_command:
  flowmanager_resume_basestar:      # after fixing the host by hand
    url: http://flowmanager.flowmanager.svc.cluster.local:9096/v1/resources/basestar-nfs/resume
    method: post
    headers:
      Authorization: !env_var FLOWMANAGER_TOGGLE_AUTH "Bearer unset"
```

(The names `flowmanager` / `flowmanager` are the Service and namespace of section 1; the
`rest` switch fields are from Home Assistant's documentation and were not run against a live
Home Assistant here.) A switch that is "on" for hours is the risk this design accepts instead
of an automatic expiry; the age metric lets Prometheus say so:

```yaml
- alert: FlowmanagerLeaseHeldTooLong      # a forgotten manual lease; tune the hours
  expr: sched_manual_lease_age_seconds > 6 * 3600
  for: 10m
  annotations:
    summary: "{{ $labels.kind }} lease on {{ $labels.target }} held for over 6 hours"
```

### The webhook listener ("poll now")

An optional listener makes a source poll now when something tells it to. It is **off unless
`--webhook-listen` is given**, it is **separate from the toggle listener** (its own port, suggested
`0.0.0.0:9097`) because the two need different policies, and **`--webhook-auth` has no default**:
the process refuses to start with `--webhook-listen` and no `--webhook-auth`, with
`--webhook-auth` and no `--webhook-listen`, with `token` and no `--webhook-token-file`, and with
`mesh` and a token file (a token that is never checked would only look like protection). What a
request means, the answers, the rate limit and the metrics are in the README (*Webhook listener*);
the interval is `--webhook-min-interval`, default **10 s**: a source starts a poll at least that
long after its previous poll started, however many requests arrive, and the source's `resync`
still polls as before. 10 s was chosen so that a burst of paperless events becomes one poll
within seconds while a flood cannot make flowmanager hammer paperless or speakr; lengthen it with
the flag if those services prefer. A source takes part by declaring
`webhook: { path: /hooks/paperless }` in its `http_json` params (the example config does for
four sources); nothing else about the source changes. Nothing sends webhooks today.

Ports and Service:

```yaml
# Service ports: add
    - { name: http-webhook, port: 9097, targetPort: 9097 }
# Deployment ports: add
            - { name: http-webhook, containerPort: 9097 }
```

**`mesh` mode.** flowmanager authenticates no one; Istio does. Flags: `--webhook-listen
0.0.0.0:9097 --webhook-auth mesh`. A caller must be a meshed workload (it has a sidecar), and
the port must be `STRICT` so that a plain-text caller is refused rather than let through, and
an `AuthorizationPolicy` must name who may call. Example, with the paperless-ngx and speakr
service accounts as placeholders (use the real ones):

```yaml
# Not tested: written from Istio's documented API, not run on this cluster.
apiVersion: security.istio.io/v1
kind: PeerAuthentication
metadata: { name: flowmanager, namespace: flowmanager }   # the one PeerAuthentication of this workload
spec:
  selector: { matchLabels: { app: flowmanager } }
  mtls: { mode: STRICT }                 # the workload default; shown for clarity
  portLevelMtls:
    "9096": { mode: PERMISSIVE }         # the toggle port, only if the toggle listener is enabled (Home Assistant has no sidecar)
    "9097": { mode: STRICT }             # the webhook port is never PERMISSIVE in `mesh` mode
---
apiVersion: security.istio.io/v1
kind: AuthorizationPolicy
metadata: { name: flowmanager-webhook, namespace: flowmanager }
spec:
  selector: { matchLabels: { app: flowmanager } }
  action: ALLOW
  rules:
    - from:
        - source:
            principals:
              - cluster.local/ns/docs/sa/paperless-ngx     # placeholder
              - cluster.local/ns/media/sa/speakr           # placeholder
      to:
        - operation: { ports: ["9097"], methods: ["POST"] }
```

Two cautions that follow from Istio's model rather than from a run here: an ALLOW policy applies
to the whole workload, so as soon as one ALLOW policy selects the pod, any port without an
ALLOW rule is denied (merge these rules with section 3's policies for 9080, 9090, 9095 and 9096,
or the existing ones stop applying as intended); and the toggle port's `PERMISSIVE` exemption
(above) must not be extended to 9097. If the sender is not in the mesh, use `token` mode instead.

**`token` mode**, for a sender outside the mesh. Flags: `--webhook-listen 0.0.0.0:9097
--webhook-auth token --webhook-token-file /var/run/secrets/webhook/token`. The token is one line
of a Secret, read on every request (a rotated Secret takes effect without a restart):

```yaml
# Not tested.
apiVersion: v1
kind: Secret
metadata: { name: flowmanager-webhook-token, namespace: flowmanager }
type: Opaque
stringData:
  token: "<a long random string>"
# Deployment: a volume from the Secret, mounted read-only at /var/run/secrets/webhook
#   volumes:      [ { name: webhook-token, secret: { secretName: flowmanager-webhook-token } } ]
#   volumeMounts: [ { name: webhook-token, mountPath: /var/run/secrets/webhook, readOnly: true } ]
```

A sender outside the mesh also needs a way in: a `NetworkPolicy` that allows its address to
port 9097 (modelled on the toggle one above), and, because it has no sidecar, a `PERMISSIVE`
`portLevelMtls` entry for `"9097"` under STRICT mTLS (in the workload's one `PeerAuthentication`,
not a second resource) plus an `AuthorizationPolicy` rule on 9097
without `from.principals`; the bearer token is then the only authentication on the port. Not
tested.

**Pointing paperless-ngx and speakr at it.**

- paperless-ngx. Verified from paperless-ngx's documentation (`docs/usage.md` and
  `docs/configuration.md` on its `dev` branch, read on Oct 27, 2026; not checked against the
  deployed 3.2.1): a workflow can have a *Webhook* action that sends a `POST` to a URL, with a
  body as text or key-value pairs in JSON or form encoding, and **request headers as key-value
  pairs**; *Document Updated* is a trigger type. So a workflow with a Document Added and/or
  Document Updated trigger and a Webhook action to
  `http://flowmanager.flowmanager.svc.cluster.local:9097/hooks/paperless` fits (the body can
  be anything, it is ignored), and in `token` mode the header `Authorization: Bearer <token>` can be
  configured in the action. paperless-ngx can restrict where webhooks go:
  `PAPERLESS_WEBHOOKS_ALLOWED_PORTS` (default: all ports allowed) and
  `PAPERLESS_WEBHOOKS_ALLOW_INTERNAL_REQUESTS` (default true; a cluster-internal URL is refused
  if set to false). Not verified: whether a paperless-ngx sidecar originates the call with the
  principal you put in the `AuthorizationPolicy`, whether paperless-gpt's tag edits through the
  API fire an Updated trigger, and that a tag-filter trigger fires when a tag is removed (the
  earlier planning note says it does not fire when a document *leaves* a state, hence the
  `resync`).
- speakr. **Unverified:** the architecture notes say speakr has webhooks for created,
  transcription and summary events, but speakr's documentation could not be fetched while this
  was written, so what it can send (events, URL configuration, headers, whether it can send an
  `Authorization` header at all) is unknown here. If it cannot send a header, use `mesh` mode
  with a meshed speakr, or a token only if a header can be set. The example config already
  declares `/hooks/speakr` for its two sources.

**Checks to run when deploying** (none were run against a cluster): `curl -X POST
http://<pod>:9097/hooks/paperless` from an allowed caller returns `202 {"sources":2,"nudged":2}`
and, from a caller the policy does not allow, is refused by the mesh (not by flowmanager); in
`token` mode a request without the header is `401`; `sched_webhook_requests_total` counts them
and `sched_observation_age_seconds{source="paperless-ocr-pending"}` drops
within the minimum interval of an accepted request.

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
| A client calling the ingress directly (Open WebUI is not integrated) | Restate ingress | 8080, `POST /Scheduler/main/{acquire_model_lease,release_lease,leases}` | HTTP | Holds a manual model lease. Nothing else. |
| Home Assistant (`hostNetwork`, no sidecar) | flowmanager | 9096 (when `--toggle-listen` is set) | HTTP, bearer token | The toggle listener (see the toggle listener subsection of section 2): needs a PERMISSIVE port exemption under STRICT mTLS and a NetworkPolicy rule for the node network. flowmanager then calls the Restate ingress for the same handlers. |
| A webhook sender (paperless-ngx, speakr; none sends today) | flowmanager | 9097, `POST` (when `--webhook-listen` is set) | HTTP | The webhook listener: in `mesh` mode STRICT mTLS and an AuthorizationPolicy on the sender's principal, in `token` mode a bearer token. A different policy from 9096. See the webhook listener subsection of section 2. |
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
  the upstream. A connect probe would then read "up" whatever basestar does. (Istio ambient
  mode, with ztunnel instead of sidecars, may behave differently; not verified.) Exclude the
  probe's destination from interception on the flowmanager pod, for example
  `traffic.sidecar.istio.io/excludeOutboundIPRanges: "<basestar ip>/32"` (or
  `excludeOutboundPorts: "2049"`), and use an IP address in the probe's `host`. Not
  checked against this cluster; **check it** with metrics that discriminate (verified live
  against a local listener, a closed port and an unresolvable name): for the probe source
  (`basestar-probe`) `sched_observation_age_seconds{source="basestar-probe",capability="reachability"}`
  must stay small (a few seconds, one reading per `resync`) and
  `sched_source_errors_total{source="basestar-probe"}` must not grow; an age of `inf` or a
  growing error count means the reads fail (for example the name does not resolve), which
  looks like a stale host, not a down one. Then
  `sched_resource_phase_age_seconds{resource="basestar-nfs",axis="power",phase="on"}` must
  exist while basestar is up and `phase="off"` (not `on`) within about three resyncs of
  powering it off. The `phase="off"` series alone proves nothing: it exists from the first
  tick, before any reading. Egress to speakr, paperless and immich goes through the mesh normally.
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
nothing else (no ConfigMaps, no Secrets), with one exception, the scheduled jobs below, which is the only place it writes:

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
- **scheduled jobs** (a `kubernetes_watch` source that declares `observe: [jobs]` and
  `job_namespaces`, the example's `scheduled-jobs`; and the jobs themselves): the one place
  flowmanager *writes* to Kubernetes. In each namespace of a job's `cronjob.namespace`
  (and the source's `job_namespaces`, which must include them) it **gets** the CronJob named
  by the job (its `spec.jobTemplate` is the template), **creates** Jobs, and **lists** Jobs by
  the label `app.kubernetes.io/managed-by=flowmanager` once per `resync` (no `watch`). It
  never deletes, patches or updates anything: a Job it made stays for the CronJob's or your
  own cleanup. If the template sets `ttlSecondsAfterFinished`, make it long: a finished Job
  that Kubernetes deletes before flowmanager has seen it finish reads as deleted by hand
  (`job_deleted`: the run fails, no retry, an alert) although it succeeded. flowmanager sees a
  finish after at most `resync` (10 s in the example) plus the 20 s read timeout plus a
  backstop interval (30 s), and not at all while it is down, so use at least an hour (and
  longer than any outage you want to ride out), or no TTL. A TTL shorter than that is not
  detected; the only sign is that failed event with reason `job_deleted` for a Job that really
  completed.
  Creating a Job owned by a
  CronJob also needs no `cronjobs/finalizers` permission here, because flowmanager does not
  set `blockOwnerDeletion`.
- **pods, volumes and claims for the NFS mount check**: only when a source declares
  `observe: [mounts]` (the example's `basestar-mounts`, a `kubernetes_watch` source of its
  own). It makes plain `list` calls once per `resync` (no `watch`): `persistentvolumes`
  (**cluster-scoped**), `persistentvolumeclaims` and `pods`. With no `params.namespaces` the
  claims and pods are listed in **every namespace** (cluster-wide `list`), which is what a
  safety check wants, since a pod in a namespace nobody listed would be missed; with
  `namespaces` they are listed in each of those (a Role per namespace). It reads pod
  phases and volumes, claims' `spec.volumeName` and volumes' `spec.nfs.server` or an NFS CSI
  volume's `server` attribute; which servers count comes from `params.nfs_servers` in the
  config. This is the one place a pod reading decides anything, and only to hold a
  shutdown back: a missing permission is a failed read, the count is then unknown, and an
  unknown count counts as mounted (the NFS host is never powered off while it lasts).

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
---
# Only if a source observes mounts (the NFS shutdown check). PersistentVolumes are
# cluster-scoped; claims and pods are listed in every namespace when the source has no
# params.namespaces (as in the example), otherwise use a Role + RoleBinding per namespace as for pods.
# `list` only: the mount check does not watch.
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata: { name: flowmanager-mounts }
rules:
  - apiGroups: [""]
    resources: [pods, persistentvolumeclaims, persistentvolumes]
    verbs: [list]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata: { name: flowmanager-mounts }
roleRef: { apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: flowmanager-mounts }
subjects: [{ kind: ServiceAccount, name: flowmanager, namespace: flowmanager }]
---
# Only if jobs are configured. One Role + RoleBinding per namespace that holds a job's
# CronJob (the example: backups). Least privilege, exactly the calls the code makes: get the
# template CronJob, create Jobs, list Jobs (no watch, no get of a single Job).
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: { name: flowmanager-jobs, namespace: backups }
rules:
  - apiGroups: [batch]
    resources: [cronjobs]
    verbs: [get]
    # resourceNames: [postgres-backup, actualbudget-backup]   # narrower still
  - apiGroups: [batch]
    resources: [jobs]
    verbs: [create, list]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: { name: flowmanager-jobs, namespace: backups }
roleRef: { apiGroup: rbac.authorization.k8s.io, kind: Role, name: flowmanager-jobs }
subjects: [{ kind: ServiceAccount, name: flowmanager, namespace: flowmanager }]
```

A missing permission shows up as a source error (`sched_source_errors_total{source="cluster-pods"}`,
a warning in the log); the host's reachability then goes stale and nothing is allowed on it
(safe, but nothing runs). The retired legacy ClusterRole (`configmaps: list`, KEDA
`scaledobjects` patch, pods/PVs/nodes list) is not needed; drop it.
A missing permission for the mount check shows up the same way, as errors of the
`basestar-mounts` source: the NFS host is then never powered off (the step is
`wait ... mounts_unknown`, and after `mounts_timeout` flowmanager alerts and stops).

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
| `IMMICH_API_KEY` | `immich-queues` (header `x-api-key: <key>`, no prefix) | An **admin** API key with the permission **`queue.read`**; an immich that serves `/api/queues` (upstream spec: v2.4.0 and later, not v2.3.1; alpha, may change; the deployment is expected to run v4.0 or later, though no release numbered 4.0 could be found). The API is marked alpha upstream, so a future immich may change its shape; the poll then fails and demand goes stale (it does not read as zero). |
| `PROMETHEUS_TOKEN` (optional) | only a `prometheus`-kind source with `auth` | Not used by the example config (it is a commented, spare source). |

A missing or empty variable is not a start-up error: the affected source reports an error
at each poll (`environment variable X is not set`), counted in
`sched_source_errors_total`, and its demand reads as unknown (busy, never idle) until it
is fixed. Check the three after deploying. The scheduler's own settings are the `SCHED_*`
variables of section 1. In the manifests, take each token from a `Secret` (SOPS-encrypted
in git, decrypted by Flux) with `secretKeyRef`, or mount it as a file and use `file:`.

The toggle listener's bearer token (`--toggle-token-file`) is a fourth secret, not named by
the configuration: a mounted file read on every request (the Secret in the toggle listener
subsection of section 2). It is never logged and never sent anywhere.

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

- **Any scaler error resets KEDA's view of the scaler.** Per KEDA's scale handler
  (`scale_handler.go`, upstream; not run here), an error from the scaler, including the
  `Unavailable` window after a flowmanager restart, makes KEDA clear its cached scaler for
  that ScaledObject and, for `external-push`, restart the stream. Streams re-open with
  backoff, and the replica count is held meanwhile, so a flowmanager restart does not scale
  anything; it can delay a change that is due until the stream is back.
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
and allowing one needs a fresh "up" reading (`policy.host_reading_stale_after`, 30 s in the example).

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
  powers hosts) needs the `wake_on_lan` and `shutdown` sections of the resource and the
  secret files below; see "Power actions" in the README.
- **Before a managed host is shut down** (`control: managed`; the example leaves both hosts
  `manual`): an NFS host is powered off only when no pod mounts it. The `basestar-mounts`
  source (section 4 has its RBAC) names the NFS server as the pods and PersistentVolumes
  write it (`params.nfs_servers: {basestar-nfs: [basestar]}`; add the address too if volumes
  use it). A managed shared host without such a source is refused at load. While pods mount
  it, or the reading is unknown (a failed list, a missing permission, a reading older than
  `host_reading_stale_after`; unknown counts as mounted), the shutdown waits
  (`wait ... pods_mounted` / `mounts_unknown`) for at most the resource's `mounts_timeout`
  (default 30 m); then flowmanager alerts (`event="shutdown_mounts_timeout"`,
  `sched_mounts_timeouts_total`, `sched_resource_mounts_veto{state="stopped"} == 1`) and
  stops: the host stays up and is not shut down by flowmanager again until a person calls
  `resume`, it is seen down, or a new manual lease takes it over. A GPU host's shutdown
  follows the bounded wind-down of its model (`drain_timeout`, `reclaim_timeout`); see "Power
  actions" in the README.
- **A minimum off time for a managed host** (`resources[].power.min_off`, default 0:
  disabled; at most 1 d): after flowmanager has shut a host down and seen it down, it does not
  power the host on again by itself (a workload, a scheduled job's hold) until `min_off` has
  passed; a manual lease does not wait. Meant for a host such as the NFS server, where
  flapping power is worse than a delayed job. While it waits the step is
  `wait ... min_off`, the tick's `reason` says `waiting for min_off, N s left`, and
  `sched_resource_min_off_remaining_seconds{resource}` counts down. A scheduled job's start
  window keeps running during the wait, so keep `min_off` well inside the job's
  `start_window` (or accept that a `skip` run is missed). Rolling the binary back needs the
  config rolled back too (an older build rejects the key). See "Power actions" in the README.

## 9. Operations

### Metrics

Scrape `:9090/metrics` (`/healthz` answers `ok`). The complete set, checked against a live
scrape and the registrations in `metrics.rs` (labelled counters appear after their first
event):

- `sched_tick_decisions_total{kind}`: ticks by decision kind (`allow_model`, `drain_model`,
  `pause_intake`, `power_on`, `power_off`, `kill_in_flight`, `run_recovery`, `wait`, `idle`,
  `error`, `pinned`, `shutdown_stopped`, `power_failed`, `demand_unknown`, `demand_unsupported`, `demand_unpolled`).
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
- `sched_resource_mounts_veto{resource,state}` (`waiting`, `stopped`; only resources a
  `mounts` source names) and `sched_mounts_timeouts_total{resource}`: a shutdown held back by
  mounting pods, and the waits that timed out. `sched_resource_min_off_remaining_seconds{resource}`:
  seconds left of the `min_off` wait before a host that flowmanager shut down may be powered on
  again by itself (0 outside it). The power metrics (`sched_resource_power_failed`,
  `sched_power_timeouts_total`, `sched_power_requests_total`) are in the README.
- `sched_manual_lease_age_seconds{kind,target}`: seconds since each manual model lease (`kind="model"`), manual host lease (`host`) and reclaim (`reclaim`) was granted; alert on a lease held too long. `sched_toggle_requests_total{endpoint,outcome}`: requests to the toggle listener. `sched_webhook_requests_total{outcome}`: requests to the webhook listener; `sched_webhook_nudges_total`: sources nudged to poll now.
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
- alert: FlowmanagerShutdownStopped      # the mounts-stopped case alone; FlowmanagerNeedsAttention below covers it with the failed power phase
  expr: sched_resource_mounts_veto{state="stopped"} == 1
- alert: FlowmanagerJobProblem           # a scheduled job failed for good, was missed, overlapped, started late or could not be created
  expr: sum by (scheduled_job, event) (increase(sched_job_events_total{event=~"failed|missed|skipped_overlap|start_late|create_failed"}[30m])) > 0
- alert: FlowmanagerBackupStale          # no success for a week and a day (a job that never succeeded has no series: it counts as 0, so this fires at once on a new install; silence it for the first week); tune per job
  expr: time() - (sched_job_last_success_timestamp_seconds{scheduled_job="backup"} or on() vector(0)) > 8 * 86400
- alert: FlowmanagerNeedsAttention       # a host in a failed power phase, or a shutdown stopped by mounting pods: resume it after fixing it (the toggle listener's needs_attention is these two)
  expr: sched_resource_power_failed == 1 or sched_resource_mounts_veto{state="stopped"} == 1
- alert: FlowmanagerLeaseHeldTooLong     # a forgotten manual lease or reclaim
  expr: sched_manual_lease_age_seconds > 6 * 3600
  for: 10m
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
  backstop chain then ends, and the process's watchdog re-arms it by itself: it checks once
  per backstop interval and re-arms when no backstop has run for more than three intervals
  plus 10 s, so within about 100 to 130 s with the default 30 s; `backstop_overdue` in
  `runtime.rs`, read from the code, not re-run).
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

A build without scheduled jobs rejects the `jobs:` section of the config (roll the config back
with it), ignores the `job_book` key and never releases `hold-job-*` leases already in the
lease book: a run in progress at the moment of the rollback leaves its NFS hold in place and
the host powered. After rolling back, `POST /Scheduler/main/leases` and release any
`hold-job-*` with `release_lease` (or roll back between runs). Jobs already created keep
running under Kubernetes. The README's rollback section has the details.

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

- the webhook listener's manifests (PeerAuthentication, AuthorizationPolicy, NetworkPolicy, Secret), what paperless-ngx and speakr send and whether they can set an Authorization header (paperless-ngx: from its `dev` documentation only; speakr: not read), and the webhook listener against a live Istio;
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
  (`queue.read`, the immich version, the alpha queue API) as documented upstream; the speakr response
  shapes were checked against its source, not a live instance;
- that cluster DNS resolves basestar's name; that a fully qualified name or an IP is used;
- NotReady timing of the node (a Kubernetes setting);
- the backstop-forking claim for a hand-called `backstop` (read from the code);
- the Restate admin `curl` form of the registration (the CLI form was run);
- how KEDA's components talk (the operator calls the scaler; the metrics adapter calls the
  operator).
