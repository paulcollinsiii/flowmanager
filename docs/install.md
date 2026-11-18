# Install

Deploy flowmanager into a cluster that meets [requirements.md](requirements.md). Examples use
the namespace `flowmanager` and the ids of `config/example.yaml`. Terms are in
[concepts.md](concepts.md). Parts that no real deployment has exercised are marked
*untested*.

## 1. Build the image

`docker build -t registry.example.internal/flowmanager:1.0.0 .`, then push it. The
`Dockerfile` compiles the release binary `flowmanager` in the build stage (it installs
`protoc` for the KEDA proto) and copies it into `gcr.io/distroless/cc-debian12:nonroot`: no
shell, UID 65532, CA certificates and tzdata included.

## 2. Configuration

The config file describes your cluster: resources, models, classes and sources. Write it from
`config/example.yaml`. It is read from `--config` at start, and an invalid file stops the
start. After that it is re-read every `--reload-interval`: a valid new file applies at the
next tick, and an invalid or unreadable one changes nothing while the last good one keeps
running.

Mount it from a ConfigMap with **no name-suffix hash** (kustomize: `disableNameSuffixHash:
true`), and mount the directory, not a `subPath` (a `subPath` never sees updates). A hashed
name rolls the pod on every edit, and a bad config would then fail the new pod's start
instead of being rejected live.

## 3. Flags and environment

Every flag has an environment variable. Durations take plain milliseconds or `ms`, `s`, `m`,
`h`, `d`.

| Flag | Env | Default | When needed |
| --- | --- | --- | --- |
| `--config` | `SCHED_CONFIG` | none | Always |
| `--listen` | `SCHED_LISTEN` | `0.0.0.0:9080` | Restate endpoint (HTTP/2, no TLS) |
| `--metrics-listen` | `SCHED_METRICS_LISTEN` | `0.0.0.0:9090` | `/metrics` and `/healthz` |
| `--keda-listen` | `SCHED_KEDA_LISTEN` | `0.0.0.0:9095` | KEDA scaler (plaintext gRPC) |
| `--restate-ingress` | `SCHED_RESTATE_INGRESS` | `http://localhost:8080` | Always in a cluster: the `localhost` default only suits a process beside the ingress. The process calls it to request ticks and arm the backstop |
| `--toggle-listen` | `SCHED_TOGGLE_LISTEN` | off | Home Assistant toggles; requires `--toggle-token-file`. Suggested `0.0.0.0:9096` |
| `--toggle-token-file` | `SCHED_TOGGLE_TOKEN_FILE` | none | With `--toggle-listen`, and only with it |
| `--webhook-listen` | `SCHED_WEBHOOK_LISTEN` | off | Webhooks (*untested*); requires `--webhook-auth`. Suggested `0.0.0.0:9097` |
| `--webhook-auth` | `SCHED_WEBHOOK_AUTH` | none, no default | With `--webhook-listen` (*untested*): `mesh` (this process authenticates no one) or `token` |
| `--webhook-token-file` | `SCHED_WEBHOOK_TOKEN_FILE` | none | With `--webhook-auth token` (*untested*); refused with `mesh` |
| `--webhook-min-interval` | `SCHED_WEBHOOK_MIN_INTERVAL` | `10s` | Least time between polls of a webhook-nudged source (*untested*); above zero |
| `--backstop` | `SCHED_BACKSTOP` | `30s` | Periodic tick that covers a missed event; above zero |
| `--reload-interval` | `SCHED_RELOAD_INTERVAL` | `5s` | How often the config file is checked; above zero |
| `--trigger-debounce` | `SCHED_TRIGGER_DEBOUNCE` | `250ms` | Merges bursts of changes into one tick |
| `--opnsense-credentials-file` | `SCHED_OPNSENSE_CREDENTIALS_FILE` | none | A managed resource with `wake_on_lan` |
| `--opnsense-ca-file` | `SCHED_OPNSENSE_CA_FILE` | platform roots | A router certificate from a private CA |
| `--ssh-key-file` | `SCHED_SSH_KEY_FILE` | none | A managed resource with `shutdown` |
| `--log-level` | `SCHED_LOG` | `info` | `error`, `warn`, `info`, `debug` or `trace` |

At start the process refuses to run if the config has managed hosts and the matching file flag
is missing. A config reloaded later with a managed host and no file starts fine; the power
request then fails, is counted and logged, and is not retried. A listener whose token file is
missing or empty also refuses to start.

## 4. Secrets

Mount each as a file from a Kubernetes Secret. Every file is re-read on every call, so a
rotated Secret needs no restart.

| Secret | Read by | Needed when |
| --- | --- | --- |
| Router credentials, one line `key:secret` | `--opnsense-credentials-file` | A managed host has `wake_on_lan` |
| Router CA, a PEM bundle: the only CA trusted for the router, whose URL must be https (verification is never off) | `--opnsense-ca-file` | The router's certificate is from a private CA |
| SSH shutdown private key, unencrypted | `--ssh-key-file` | A managed host has `shutdown` |
| Toggle bearer token, one line | `--toggle-token-file` | `--toggle-listen` is set |
| Webhook bearer token, one line (*untested*) | `--webhook-token-file` | `--webhook-auth token` |
| A source's API token | `auth: { header, prefix, file: PATH }` in an `http_json` source or a recovery action; `env: NAME` and a `prometheus` source's `auth` are *untested* | The service needs a token. A `file` is re-read on every poll; an `env` variable is fixed when the pod starts, so rotating it needs a restart |

A missing source token is not a start error: the source reports an error at each poll and its
demand reads as unknown. Keep a Secret volume's default file mode: the pod runs as 65532 and
sets no `fsGroup`, so `0400` would leave the files readable by root alone.

## 5. Workload

One replica with the `Recreate` strategy, not a rolling update
([why](requirements.md#cluster)). Restate holds the state, so the pod needs no volume. The gap
while the pod is replaced is safe ([KEDA scaler](concepts.md#decisions-and-durability)).

| Object | Notes |
| --- | --- |
| Namespace `flowmanager` | With Istio, `istio-injection: enabled`; Pod Security `restricted` |
| ServiceAccount `flowmanager` | The subject of every binding in [section 6](#6-rbac) |
| Service `flowmanager` | The ports of the table below, each named for its protocol (the Restate SDK serves HTTP/2 only) |

```yaml
spec:                                   # Deployment
  replicas: 1
  strategy: { type: Recreate }
  template:
    spec:
      serviceAccountName: flowmanager
      securityContext: { runAsNonRoot: true, runAsUser: 65532, seccompProfile: { type: RuntimeDefault } }
      volumes:                          # each volume is mounted below and read by the flag or env named
        - { name: config,     configMap: { name: flowmanager-config } }
        - { name: opnsense,   secret: { secretName: flowmanager-opnsense } }     # credentials
        - { name: router-ca,  configMap: { name: flowmanager-router-ca } }       # ca.crt
        - { name: ssh-key,    secret: { secretName: flowmanager-shutdown-key } } # id_ed25519
        - { name: toggle,     secret: { secretName: flowmanager-toggle } }       # token
      containers:
        - name: flowmanager
          image: registry.example.internal/flowmanager:1.0.0
          env:
            - { name: SCHED_CONFIG,                     value: /etc/flowmanager/config.yaml }
            - { name: SCHED_RESTATE_INGRESS,            value: "http://restate.restate.svc.cluster.local:8080" }
            - { name: SCHED_OPNSENSE_CREDENTIALS_FILE,  value: /run/flowmanager/opnsense/credentials }
            - { name: SCHED_OPNSENSE_CA_FILE,           value: /run/flowmanager/router-ca/ca.crt }
            - { name: SCHED_SSH_KEY_FILE,               value: /run/flowmanager/ssh-key/id_ed25519 }
            - { name: NO_COLOR,                         value: "1" }   # else the log carries ANSI colour codes
            - { name: SCHED_TOGGLE_LISTEN,              value: "0.0.0.0:9096" }
            - { name: SCHED_TOGGLE_TOKEN_FILE,          value: /run/flowmanager/toggle/token }
          ports: [{ name: http2, containerPort: 9080 }, { name: http-metrics, containerPort: 9090 }, { name: grpc-keda, containerPort: 9095 }, { name: http-toggle, containerPort: 9096 }]
          livenessProbe:  { httpGet: { path: /healthz, port: 9090 } }   # and readinessProbe
          volumeMounts:
            - { name: config,    mountPath: /etc/flowmanager, readOnly: true }
            - { name: opnsense,  mountPath: /run/flowmanager/opnsense, readOnly: true }
            - { name: router-ca, mountPath: /run/flowmanager/router-ca, readOnly: true }
            - { name: ssh-key,   mountPath: /run/flowmanager/ssh-key, readOnly: true }
            - { name: toggle,    mountPath: /run/flowmanager/toggle, readOnly: true }
          securityContext: { allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: { drop: ["ALL"] } }
```

Drop the volumes, mounts and env of a feature you do not use. `/healthz` shows only that the metrics listener answers.

| Port | Service port name | Serves | Enabled by |
| --- | --- | --- | --- |
| 9080 | `http2` | Restate service endpoint | Always |
| 9090 | `http-metrics` | `/metrics`, `/healthz` | Always |
| 9095 | `grpc-keda` | KEDA external scaler | Always |
| 9096 | `http-toggle` | Toggle listener | `--toggle-listen` |
| 9097 | `http-webhook` | Webhook listener (*untested*) | `--webhook-listen` |

## 6. RBAC

Bind these to the ServiceAccount (a Role or ClusterRole plus its binding each). A missing
permission makes that reading failed or unknown, never zero, so the effect is to hold, not to
act ([staleness](concepts.md#demand-and-observation)).

| Kind | Resources and verbs | For |
| --- | --- | --- |
| ClusterRole | `nodes`: `list`, `watch` | `reachability` from a node (`kubernetes_watch`) |
| Role in each namespace of the source's `namespaces` | `pods`: `list`, `watch` | `pods`, the pods veto and recovery |
| ClusterRole, or a Role per namespace if the source sets `namespaces` | `pods`, `persistentvolumeclaims`: `list` | `mounts`, the NFS veto. Unset `namespaces` reads every namespace |
| ClusterRole | `persistentvolumes`: `list` | `mounts`: always cluster-wide, in addition to the row above |
| Role in each of the source's `job_namespaces` | `jobs`: `list` | `jobs`, the state of Jobs flowmanager created |
| Role in each scheduled job's CronJob namespace | `cronjobs`: `get` (restrict with `resourceNames`); `jobs`: `create` | Starting a scheduled job from its CronJob template |

flowmanager never deletes or patches anything. Only the pods source watches; mounts only list.

## 7. Restate

Register the deployment with the Restate admin API; `restate deployments register` needs the
admin address (the CLI default is `http://localhost:9070`, so port-forward the server's admin
port or point the CLI at it). The URL given must be reachable from the Restate server, which
makes a discovery request to it.

```sh
restate deployments register http://flowmanager.flowmanager.svc.cluster.local:9080
```

It lists three virtual objects: `Scheduler` (one instance, key `main`), `Recovery` (key: a
class id) and `Power` (key: a resource id). Registering an unchanged URL does nothing; an image
that adds or changes a handler needs `--force` (or a new URL). Until the deployment is
registered no tick runs.

The process itself calls the ingress (`--restate-ingress`) at `POST /Scheduler/main/tick/send`
and `.../arm_backstop/send`. The same ingress reaches the handlers for leases, drains and
state; `Scheduler/main/observe`, `Recovery/<class>/run` and `Power/<resource>/run` are
ingress-private.

## 8. KEDA

One ScaledObject per gated Deployment. What it does with the allowed set is in
[concepts](concepts.md#choosing-and-serving) and
[KEDA scaler](concepts.md#decisions-and-durability).

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata: { name: mineru, namespace: ai }
spec:
  scaleTargetRef: { name: mineru }          # the Deployment
  minReplicaCount: 0
  maxReplicaCount: 1
  pollingInterval: 5
  cooldownPeriod: 15
  triggers:
    - type: external                        # or external-push
      metadata:
        scalerAddress: flowmanager.flowmanager.svc.cluster.local:9095
        model: mineru                       # or `class: <id>`, never both
```

| Setting | Why |
| --- | --- |
| Exactly one of `model: <id>` or `class: <id>` | `model` is for a model's own Deployment; `class` for a consumer gated on a host (a [gate class](concepts.md#workloads)). Neither, both, another key or an empty value is refused with `InvalidArgument` |
| `external` (polled) or `external-push` | `external-push` streams each change to KEDA at once |
| `minReplicaCount: 0`, `maxReplicaCount: 1` | The scaler answers 1 while allowed and 0 otherwise |
| Short `pollingInterval` and `cooldownPeriod` | Hysteresis is flowmanager's (`min_dwell`, `idle_grace`); a long cooldown only postpones a withdrawal it is waiting for |
| The scaler is the only trigger | KEDA activates on any trigger, so a cpu or cron trigger could start a model that is not allowed |
| No `fallback` | Without one KEDA keeps the count while the scaler is unavailable ([KEDA scaler](concepts.md#decisions-and-durability)); a `fallback` would set a count the scheduler never decided |
| No `autoscaling.keda.sh/paused-replicas` annotation | While set, KEDA holds the count whatever the scaler says |

`scalerAddress` is required by KEDA; it, `enableTLS` and `unsafeSsl` are otherwise ignored: the
scaler is plaintext gRPC and the mesh adds mTLS. For an app that must stay running to be
observed, see [requirements](requirements.md#workloads).

An id the active config does not have is refused with `InvalidArgument`, so KEDA keeps that
ScaledObject's count and a removed model that was running keeps running: delete the
ScaledObject and the Deployment in the same change that removes the model from the config
(flowmanager logs each distinct error once at `warn`). An id added to the config is answered
at once, inactive until a tick allows it.

## 9. Istio and network policy

Strongly recommended, for the reason and the alternatives in
[requirements](requirements.md#cluster). The policy below is for a namespace under STRICT mTLS
(`PeerAuthentication` mode `STRICT`). Ports are those of the table in
[section 5](#5-workload).

| Port | Who may call | Auth | Mesh and network |
| --- | --- | --- | --- |
| 9080 | The Restate server only | Principal of its ServiceAccount | `AuthorizationPolicy` on the port. It also covers the ingress-private handlers, which only the ingress hides |
| 9090 | Prometheus, kubelet | None | Istio rewrites probes to the sidecar agent and, with metrics merging, serves `/metrics` on 15020 outside mTLS; the policy covers neither. Scraping 9090 through the mesh needs your own rule |
| 9095 | The KEDA operator only (not the metrics adapter) | Principal of the operator's ServiceAccount | `AuthorizationPolicy` on the port. The operator must be meshed |
| 9096 | Home Assistant | Bearer token | Usually outside the mesh: the port is `PERMISSIVE`, the rule names no source, and a `NetworkPolicy` admits only the Home Assistant host's node addresses |
| 9097 | `mesh`: named senders; `token`: anyone with the token | `mesh`: principal; `token`: bearer | *Untested.* `mesh`: STRICT plus a rule per sender principal. `token`: `PERMISSIVE` for senders outside the mesh, plus a `NetworkPolicy` |

```yaml
apiVersion: security.istio.io/v1
kind: AuthorizationPolicy
metadata: { name: flowmanager, namespace: flowmanager }
spec:
  selector: { matchLabels: { app: flowmanager } }
  action: ALLOW
  rules:
    - from: [{ source: { principals: ["cluster.local/ns/restate/sa/restate"] } }]
      to:   [{ operation: { ports: ["9080"] } }]
    - from: [{ source: { principals: ["cluster.local/ns/keda/sa/keda-operator"] } }]
      to:   [{ operation: { ports: ["9095"] } }]
    - to:   [{ operation: { ports: ["9096"] } }]      # token-protected
```

A workload-selector `PeerAuthentication` with `mtls: { mode: STRICT }` and
`portLevelMtls: { "9096": { mode: PERMISSIVE } }` admits a client with no sidecar; Istio
applies one such object per workload, so put every port exemption in it.

A `NetworkPolicy` with `policyTypes: [Ingress]` mirrors the table: Restate's pods to 9080, the
operator's pods to 9095, and an `ipBlock` per Home Assistant node address to 9096 (a
hostNetwork client's packets carry its node address). It must also admit what the table's
9090 row leaves to the sidecar, or it blocks both: your Prometheus agent's pods to 15020 (the
merged metrics endpoint), and kubelet probe traffic from the node network. Some CNIs pass a
node's own traffic to its pods whatever the policy (the tested cluster's does); on others add
an `ipBlock` for the node addresses.

Outbound from the pod:

| To | Through the sidecar | Why |
| --- | --- | --- |
| Restate ingress (8080) | Yes | Its own policy must allow flowmanager's principal on `POST /Scheduler/main/tick/send` and `arm_backstop/send`, and on the handlers the toggle listener calls |
| The Kubernetes API, and the services of `http_json` and `prometheus` sources | Yes | Normal mesh traffic |
| The router (443), each managed host (22), a `tcp_probe` target | No: `traffic.sidecar.istio.io/excludeOutboundIPRanges` | A sidecar accepts every connect before trying the upstream, so a probe would always read up; the router's certificate is verified directly, which Envoy would only pass through |

If the router's name resolves to several interfaces, pin the LAN address with `hostAliases`:
the exclusion works on an address and the certificate names one host. If the mesh's outbound
policy is `REGISTRY_ONLY`, the Kubernetes API and the hosts other sources reach need
`ServiceEntry`s (*untested*).

## 10. Check it works

1. **Process.** The log shows `serving /metrics`, `serving the KEDA external scaler (gRPC)`
   and `serving the Restate endpoint` (and the toggle or webhook listener if enabled). A
   refusal to start names the flag.
2. **A tick.** `kubectl -n flowmanager port-forward deploy/flowmanager 9090:9090`, then
   `curl -s localhost:9090/metrics | grep -E 'sched_(last_tick_timestamp_seconds|tick_trigger_failures_total)'`.
   `sched_last_tick_timestamp_seconds` advances about every `--backstop`;
   `sched_tick_trigger_failures_total` stays flat (the process reaches the ingress).
3. **The decision.** Each tick logs the message `tick` with the fields `generation`,
   `decision`, `reason`, `allowed_models`, `allowed_classes`, `leases`, `unknown_demand` and
   `drains`; `queue` lines carry each class's `ready`, `in_flight` and `failed`. A failed tick
   logs `tick failed; the previous state and allowed set stay`.
4. **The scaler.** `kubectl get scaledobject -A` shows each `READY True`, and `ACTIVE`
   follows the allowed set; `kubectl describe` shows no scaler errors (`Unavailable` for a few
   seconds after a restart is normal). `--log-level debug` logs each `scaler answer`.

Day-to-day use is in [operations.md](operations.md); the Home Assistant and webhook listeners
are in [home-assistant.md](home-assistant.md).
