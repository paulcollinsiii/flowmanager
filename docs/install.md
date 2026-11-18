# Install

Deploy flowmanager into a cluster that meets [requirements.md](requirements.md). Examples use
the namespace `flowmanager` and the ids of `config/example.yaml`. Terms are in
[concepts.md](concepts.md).

## 1. Build the image

```sh
docker build -t registry.example.internal/flowmanager:1.0.0 .
docker push registry.example.internal/flowmanager:1.0.0
```

The `Dockerfile` compiles the release binary `flowmanager` in the image build (it needs
`protoc` for the KEDA proto, which the build stage installs) and copies it into
`gcr.io/distroless/cc-debian12:nonroot`: no shell, UID 65532, CA certificates and tzdata
included. It runs under the `restricted` Pod Security level with a read-only root filesystem
and no capabilities.

## 2. Configuration

The config file is the cluster's description: resources, models, classes and sources
(configuration pages to come; `config/example.yaml` is a complete example). It is read from
`--config` at start, and an invalid file stops the start.
After that the file is re-read every `--reload-interval`; a valid new one applies at the next
tick, and an invalid or unreadable one changes nothing while the last good one keeps running.

Mount it from a ConfigMap whose name has **no hash suffix**, and mount the directory, not a
`subPath` (a `subPath` never sees ConfigMap updates). A hashed name rolls the pod on every
edit, and a bad config would then fail the new pod's start instead of being rejected live.

```yaml
# kustomization.yaml
configMapGenerator:
  - name: flowmanager-config
    namespace: flowmanager
    files: [config.yaml]
    options:
      disableNameSuffixHash: true
```

## 3. Flags and environment

Every flag has an environment variable. Durations take plain milliseconds or `ms`, `s`, `m`,
`h`, `d`.

| Flag | Env | Default | When needed |
| --- | --- | --- | --- |
| `--config` | `SCHED_CONFIG` | none | Always |
| `--listen` | `SCHED_LISTEN` | `0.0.0.0:9080` | Restate endpoint (HTTP/2, no TLS); change only with the Service |
| `--metrics-listen` | `SCHED_METRICS_LISTEN` | `0.0.0.0:9090` | `/metrics` and `/healthz` |
| `--keda-listen` | `SCHED_KEDA_LISTEN` | `0.0.0.0:9095` | KEDA scaler (plaintext gRPC) |
| `--restate-ingress` | `SCHED_RESTATE_INGRESS` | `http://localhost:8080` | Always in a cluster: the process calls the ingress to request ticks and arm the backstop |
| `--toggle-listen` | `SCHED_TOGGLE_LISTEN` | off | Home Assistant toggles. Requires `--toggle-token-file`. Suggested `0.0.0.0:9096` |
| `--toggle-token-file` | `SCHED_TOGGLE_TOKEN_FILE` | none | With `--toggle-listen`, and only with it |
| `--webhook-listen` | `SCHED_WEBHOOK_LISTEN` | off | Webhooks. Requires `--webhook-auth`. Suggested `0.0.0.0:9097` |
| `--webhook-auth` | `SCHED_WEBHOOK_AUTH` | none, no default | With `--webhook-listen`: `mesh` (this process authenticates no one) or `token` |
| `--webhook-token-file` | `SCHED_WEBHOOK_TOKEN_FILE` | none | With `--webhook-auth token`; refused with `mesh` |
| `--webhook-min-interval` | `SCHED_WEBHOOK_MIN_INTERVAL` | `10s` | Least time between polls of a nudged source; above zero |
| `--backstop` | `SCHED_BACKSTOP` | `30s` | Periodic tick that covers a missed event; above zero |
| `--reload-interval` | `SCHED_RELOAD_INTERVAL` | `5s` | How often the config file is checked; above zero |
| `--trigger-debounce` | `SCHED_TRIGGER_DEBOUNCE` | `250ms` | Merges bursts of changes into one tick |
| `--opnsense-credentials-file` | `SCHED_OPNSENSE_CREDENTIALS_FILE` | none | A resource with `control: managed` and `wake_on_lan` |
| `--opnsense-ca-file` | `SCHED_OPNSENSE_CA_FILE` | platform roots | A router certificate a public CA did not sign |
| `--ssh-key-file` | `SCHED_SSH_KEY_FILE` | none | A resource with `control: managed` and `shutdown` |
| `--log-level` | `SCHED_LOG` | `info` | `error`, `warn`, `info`, `debug` or `trace` |

At start the process refuses to run if the config has managed hosts and the matching file flag
is missing. A config reloaded later with a managed host and no file starts fine; the power
request then fails, is counted and logged, and is not retried. A listener with a missing or
empty token file also refuses to start.

## 4. Secrets

Mount each as a file from a Kubernetes Secret. Every file is re-read on every call, so a
rotated Secret needs no restart (the kubelet refreshes the volume).

| Secret | Flag or config key that reads it | Needed when |
| --- | --- | --- |
| Router credentials, one line `key:secret` | `--opnsense-credentials-file` | A managed host has `wake_on_lan` |
| Router CA, a PEM bundle; the only CA trusted for the router (verification is never off) | `--opnsense-ca-file` | The router's certificate is from a private CA |
| SSH shutdown private key, unencrypted | `--ssh-key-file` | A managed host has `shutdown` |
| Toggle bearer token, one line | `--toggle-token-file` | `--toggle-listen` is set |
| Webhook bearer token, one line | `--webhook-token-file` | `--webhook-auth token` |
| A source's API token | `auth: { header, prefix, file: PATH }` or `env: NAME` in an `http_json` or `prometheus` source or a recovery action | The service needs a token. `file` is re-read on every poll; an `env` variable is fixed when the pod starts, so rotating it needs a restart |

A missing source token is not a start error: the source reports an error at each poll and its
demand reads as unknown. Keep a Secret volume's default file mode: the pod runs as 65532
and sets no `fsGroup`, so a mode of `0400` would leave the files readable by root alone.

## 5. Workload

One replica, and `Recreate`, not a rolling update: two pods would each hold their own allowed
set, and Restate sends calls to whichever answers. Restate holds the state, so the pod needs
no volume. The gap while the pod is replaced is safe: KEDA keeps replica counts while the
scaler errors ([KEDA](#8-keda)).

```yaml
apiVersion: v1
kind: ServiceAccount
metadata: { name: flowmanager, namespace: flowmanager }
---
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
      serviceAccountName: flowmanager
      securityContext: { runAsNonRoot: true, runAsUser: 65532, seccompProfile: { type: RuntimeDefault } }
      volumes:
        - { name: config, configMap: { name: flowmanager-config } }
        # one Secret volume per secret of section 4, for example:
        - { name: toggle-token, secret: { secretName: flowmanager-toggle } }
      containers:
        - name: flowmanager
          image: registry.example.internal/flowmanager:1.0.0
          env:
            - { name: SCHED_CONFIG, value: /etc/flowmanager/config.yaml }
            - { name: SCHED_RESTATE_INGRESS, value: "http://restate.restate.svc.cluster.local:8080" }
          ports:
            - { name: http2, containerPort: 9080 }       # named: the Restate SDK serves HTTP/2 only
            - { name: http-metrics, containerPort: 9090 }
            - { name: grpc-keda, containerPort: 9095 }
          livenessProbe: { httpGet: { path: /healthz, port: 9090 } }
          readinessProbe: { httpGet: { path: /healthz, port: 9090 } }
          volumeMounts:
            - { name: config, mountPath: /etc/flowmanager, readOnly: true }
          securityContext: { allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: { drop: ["ALL"] } }
```

`/healthz` shows only that the metrics listener answers, not that Restate has registered the
deployment or that ticks arrive ([check it](#10-check-it-works)).

The Service exposes the ports the deployment uses; name them so Istio picks the protocol.

| Port | Name | Serves | Callers |
| --- | --- | --- | --- |
| 9080 | `http2` | Restate service endpoint | The Restate server |
| 9090 | `http-metrics` | `/metrics`, `/healthz` | Prometheus, kubelet |
| 9095 | `grpc-keda` | KEDA external scaler | The KEDA operator |
| 9096 | `http-toggle` | Toggle listener (optional) | Home Assistant |
| 9097 | `http-webhook` | Webhook listener (optional) | Services that send webhooks |

## 6. RBAC

Bind these to the ServiceAccount. Each is needed only for the capability named; a missing
permission makes that reading unknown or failed, never zero, so the effect is to hold, not to
act ([staleness](concepts.md#demand-and-observation)).

| Kind | Resources and verbs | For |
| --- | --- | --- |
| ClusterRole | `nodes`: `list`, `watch` | `reachability` from a node (`kubernetes_watch`) |
| Role in each namespace of the source's `namespaces` | `pods`: `list`, `watch` | `pods`, the pods veto and recovery |
| ClusterRole, or a Role per namespace if `namespaces` is set | `pods`, `persistentvolumeclaims`: `list` | `mounts`, the NFS veto. Unset `namespaces` reads every namespace |
| ClusterRole | `persistentvolumes`: `list` | `mounts`: always cluster-wide, needed in addition to the row above |
| Role in each of the source's `job_namespaces` | `jobs`: `list` | `jobs`, the state of Jobs flowmanager created |
| Role in each scheduled job's CronJob namespace | `cronjobs`: `get` (restrict with `resourceNames`); `jobs`: `create` | Starting a scheduled job from its CronJob template |

flowmanager never deletes, patches or watches Jobs, and never writes anything else to
Kubernetes. The mounts source lists; only the pods source watches, which is why the pods row
needs `watch` and the mounts row does not.

## 7. Restate

Register the deployment with the Restate admin API. The URL must be reachable from the
Restate server, which makes a discovery request to it:

```sh
restate deployments register http://flowmanager.flowmanager.svc.cluster.local:9080
```

It lists three virtual objects, each keyed: `Scheduler` (one instance, key `main`), `Recovery`
(key: a class id) and `Power` (key: a resource id). Registering an unchanged URL does nothing;
an image that adds or changes a handler needs `--force` (or a new URL).

Until it is registered no tick runs. `--restate-ingress` is the ingress the process calls
(`POST /Scheduler/main/tick/send`, `.../arm_backstop/send`); the default `localhost` is wrong
in a cluster. The same ingress reaches every handler for leases, drains and state (the
Operations page, to come); `Scheduler/main/observe` and `Recovery/<class>/run` are
ingress-private.

## 8. KEDA

One ScaledObject per gated Deployment:

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
  triggers:                                 # exactly one trigger, and no `fallback`
    - type: external                        # or external-push
      metadata:
        scalerAddress: flowmanager.flowmanager.svc.cluster.local:9095
        model: mineru                       # or `class: <id>`, never both
```

| Setting | Why |
| --- | --- |
| `model: <id>` or `class: <id>`, exactly one | `model`: active while that model is allowed (its pods may load, serve or wind down). `class`: active while that class is allowed, for a consumer gated on a host (a [gate class](concepts.md#workloads)). Anything else is refused with `InvalidArgument` |
| `external` or `external-push` | `external-push` streams each change to KEDA at once; `external` is polled |
| `minReplicaCount: 0`, `maxReplicaCount: 1` | The scaler answers 1 while allowed and 0 otherwise, so replicas are 0 or 1 |
| Short `pollingInterval` and `cooldownPeriod` | The delay before withdrawal is flowmanager's (`min_dwell`, `idle_grace`); a long cooldown only postpones a withdrawal it is waiting for |
| No `fallback` | With none, KEDA keeps the current replica count when the scaler errors, so a restart of flowmanager scales nothing. A `fallback` would set a count the scheduler never decided |
| No other trigger | KEDA activates on any trigger, so a cpu or cron trigger could start a model that is not allowed |
| No `autoscaling.keda.sh/paused-replicas` annotation | While set, KEDA holds the count whatever the scaler says |

`scalerAddress`, `enableTLS` and `unsafeSsl` are accepted and ignored: the scaler is
plaintext gRPC and the mesh adds mTLS. An app whose queue flowmanager reads through its own
API has no ScaledObject; see [requirements](requirements.md#workloads).

Failure behaviour:

- **Not ready.** Before this process publishes its first tick, and while it stops, every call
  answers `Unavailable` and KEDA leaves replica counts alone. After a restart the first tick is
  seconds away; before the deployment is registered it never comes.
- **An unknown id.** A `model` or `class` the active config does not have answers
  `InvalidArgument`: KEDA keeps that ScaledObject's count, so a removed model that was running
  keeps running, and the error shows on the ScaledObject. Delete the ScaledObject and the
  Deployment together with the model, in the same change as the config. flowmanager logs each
  distinct error once at `warn`.
- **An added id.** Answered at once from the allowed set; inactive until a tick allows it.

## 9. Istio and network policy

Optional, but the Restate endpoint checks no request signature and the scaler and webhook
`mesh` mode authenticate no one: if you do not use a mesh, restrict these ports another way.
Everything below is the policy of a cluster running STRICT mTLS (the namespace
has `PeerAuthentication` mode `STRICT`).

| Port | Who may call | Auth | Mesh and network |
| --- | --- | --- | --- |
| 9080 | The Restate server only | Principal of its ServiceAccount | `AuthorizationPolicy` on the port. It also guards the ingress-private handlers, which only the ingress hides |
| 9090 | Prometheus, kubelet | None | Istio rewrites probes to the sidecar agent, and with metrics merging serves `/metrics` on 15020 outside mTLS; this policy covers neither. Scraping 9090 through the mesh needs your own rule |
| 9095 | The KEDA operator only (not the metrics adapter) | Principal of the operator's ServiceAccount | `AuthorizationPolicy` on the port. The operator must be meshed; the scaler is plaintext |
| 9096 | Home Assistant | Bearer token | Usually outside the mesh, so the port is `PERMISSIVE`, the rule names no source, and a `NetworkPolicy` admits only the node addresses of the Home Assistant host |
| 9097 | `mesh`: named senders. `token`: anyone with the token | `mesh`: principal. `token`: bearer | `mesh`: STRICT plus a rule naming each sender's principal. `token`: `PERMISSIVE` for senders outside the mesh, plus a `NetworkPolicy`. Derived from the code, not run in a cluster |

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
    - to:   [{ operation: { ports: ["9096"] } }]      # token-protected; see the table
---
apiVersion: security.istio.io/v1
kind: PeerAuthentication
metadata: { name: flowmanager, namespace: flowmanager }
spec:
  selector: { matchLabels: { app: flowmanager } }
  mtls: { mode: STRICT }
  portLevelMtls: { "9096": { mode: PERMISSIVE } }     # a client with no sidecar
```

A `NetworkPolicy` with `policyTypes: [Ingress]` mirrors the table: Restate's pods to 9080,
the operator's pods to 9095, and for 9096 an `ipBlock` for each Home Assistant node address
(a hostNetwork client's packets carry its node address). Istio applies one
workload-selector `PeerAuthentication` per workload, so put every port exemption in it.

Outbound from the pod:

| To | Through the sidecar | Why |
| --- | --- | --- |
| Restate ingress (8080) | Yes | Allow flowmanager's principal on `POST /Scheduler/main/tick/send` and `arm_backstop/send`, and on the handlers the toggle listener calls |
| Kubernetes API, the services of `http_json` and `prometheus` sources | Yes | Normal mesh traffic |
| The router (443), each managed host (22), a `tcp_probe` target | No: `traffic.sidecar.istio.io/excludeOutboundIPRanges` | A sidecar accepts every connect before trying the upstream, so a probe would always read up; the router's certificate is verified directly, which Envoy would only pass through; SSH pins the host key |

If the router's name resolves to several interfaces, pin the LAN address with `hostAliases`:
the certificate names one host and the sidecar exclusion works on an address.

## 10. Check it works

1. **Process.** `kubectl -n flowmanager logs deploy/flowmanager` shows `serving /metrics`,
   `serving the KEDA external scaler (gRPC)` and `serving the Restate endpoint` (and the
   toggle or webhook listener lines if enabled). A refusal to start names the flag.
2. **A tick.** Port-forward 9090 and read the metrics:
   ```sh
   kubectl -n flowmanager port-forward deploy/flowmanager 9090:9090 &
   curl -s localhost:9090/metrics | grep -E 'sched_(last_tick_timestamp_seconds|config_generation|tick_decisions_total|tick_trigger_failures_total)'
   ```
   `sched_last_tick_timestamp_seconds` advances about every `--backstop`;
   `sched_tick_trigger_failures_total` stays flat (the process reaches the ingress);
   `sched_config_generation` is the config's generation.
3. **The decision.** Each tick logs a line with the message `tick` and the fields
   `generation`, `decision`, `reason`, `allowed_models`, `allowed_classes`, `leases`,
   `unknown_demand` and `drains`; `queue` lines carry each class's `ready`, `in_flight` and
   `failed`. A failed tick logs `tick failed; the previous state and allowed set stay`.
4. **The scaler.** `kubectl get scaledobject -A` shows each ScaledObject `READY True`;
   `ACTIVE` follows the allowed set. `kubectl describe` shows no scaler errors (`Unavailable`
   for a few seconds after a restart is normal). Set `--log-level debug` to see each
   `scaler answer`.
