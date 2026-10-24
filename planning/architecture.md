# GPU Scheduler: Architecture Design

Oct 23, 2026 · @Paul Collins

## Problem and goals

A single Rust service, flowmanager (the scheduler), decides which model runs on the GPU node and which shared resources must be up, so every job queue drains in a predictable order without services contending for the GPU.

Several services need a GPU that only fits one model at a time, and some also need an NFS share that is not always powered. Today each service scales on its own demand, nothing arbitrates between them, and the backup job powers NFS down without knowing who else is using it.

Goals:

- **One decision authority.** Only the scheduler decides which model is loaded and which shared resources are held. Everything else either reports state or carries out a decision.
- **Switch-cost-aware ordering.** Pick the next model by first-in-first-out across ready work, then drain that queue before switching, within configurable bounds.
- **Safe transitions.** Never run two models at once, never power down a resource in use, never act on stale safety-critical data.
- **Extensible by configuration.** Adding a service that speaks a supported protocol requires a config change, not a code change.
- **Event-driven where reasonable.** React to events for latency and use slow periodic resyncs for correctness.
- **Durable.** Decisions, leases, and in-progress transitions survive restarts of the scheduler.

## Scope and non-goals

The scheduler arbitrates two resources and observes the services that use them; it does not change how those services work internally.

**In scope**

- Arbitrating which single model runs on cuda-central.
- Leases on the basestar NFS share, including manual leases from power requests, with power-down only when no lease is held. Every NFS-dependent service is gated on it, including services that never use the GPU.
- Power actuation for cuda-central and basestar, triggered only by manual requests or by workloads explicitly allowed to change power (the backup job, in v1).
- Observing job demand from each service through configurable adapters.
- Gating service pods through a KEDA external scaler.
- Running the weekly backup as a scheduler-managed workflow.
- Interactive LLM sessions (Open WebUI, editor code assist) as time-limited leases, and manual requests: release, cuda-central reclaim, and manual LLM leases that cut batch work short.

**Non-goals**

- Modifying paperless-gpt, speakr, immich, or other services' internals. The scheduler observes and gates them from outside.
- Replacing the Kubernetes scheduler or general cluster-wide queueing.
- Sharing one GPU between several models at once, or fractional GPU allocation.
- Demand-driven power on or off in v1. The design supports it as a later policy change.
- More than one GPU node.
- A user interface beyond logs, metrics, and a minimal API for manual requests.

## Environment and workloads

The cluster has two nodes plus an NFS host, and the workload classes below compete for them, alongside the NFS-only service classes described after the table.

| Host | Role | Power |
| --- | --- | --- |
| blade-runner | Always-on Kubernetes node; runs the flowmanager pod, the Restate operator, and KEDA | Always on |
| cuda-central | GPU Kubernetes node; fits one model at a time | Wake-on-LAN; not always on |
| basestar | NFS host for documents and backups | Powered on demand |

| Workload class | Service | Requires | Demand signal today |
| --- | --- | --- | --- |
| paperless OCR | paperless-gpt (mineru) | GPU: mineru, NFS | Documents carrying the OCR-pending tag in paperless-ngx |
| paperless tagging | paperless-gpt (LLM) | GPU: LLM, NFS | Documents carrying the post-OCR tag in paperless-ngx |
| speakr transcription | speakr (whisperX) | GPU: whisperX, NFS | speakr API v1 (pending recordings, plus failed ones counted as pending) and webhooks |
| speakr summarization | speakr (LLM) | GPU: LLM, NFS | speakr API v1 (pending recordings, stats) and webhooks |
| immich ML | immich machine-learning (GPU replica only; a CPU replica on blade-runner always serves) | GPU: immich ML, NFS | immich job queues (Valkey); API preferred |
| backup | Postgres dump cronjob | NFS | Weekly schedule |
| interactive LLM | Open WebUI, editor code assist | GPU: LLM | Session requests (not yet built) |

OCR must finish before LLM tagging for the same document, and transcription before summarization for the same recording. The backup does not interact with the GPU.

Services that never touch the GPU, such as paperless-ngx itself and the immich server, also depend on NFS. They are modeled as NFS-only workload classes, so turning NFS off scales them to zero and turning it on lets them run, with or without the GPU.

## Core concepts

The scheduler reasons only about generic resources, models, and workload classes; it never knows a service by name.

- **Resource.** Something workloads need. Two kinds:
  - *Exclusive-mode*: holds at most one mode at a time. cuda-central is exclusive, and its modes are models.
  - *Shared*: any number of holders, tracked by a reference count. The basestar NFS share is shared.
- **Model.** A mode of an exclusive resource, implemented by one or more scaled workloads, with an estimated load time used as its switch cost.
- **Workload class.** A unit of demand: its resource requirements, priority, preemptibility, whether it may change power state, and where its demand is observed.
- **Lease.** A durable record that a holder is using a shared resource or has an interactive claim on a model. Interactive leases expire unless renewed, and while held they pin their model: batch work cannot evict it. The client renews the lease while the user is active, so a session never drops mid-use. An LLM loaded only for batch work has no such pin and can be evicted when higher-priority work arrives. A manual power-on of a host is a lease held by the user until they release it. Releasing it powers the host down only after every other hold ends: other leases on basestar, such as a running backup, and queued or in-flight GPU work on cuda-central. A manual lease on the LLM is the interactive counterpart of a reclaim: it cuts batch work short (intake stops; in-flight jobs finish or are stopped after a timeout) and keeps the LLM loaded until released.
- **Reclaim.** The forceful alternative to releasing, for cuda-central only: the user takes the host back despite pending work. Intake stops at once, in-flight jobs finish or are stopped after a configured timeout, pending jobs stay queued, and the host is not woken or scheduled until the user returns it. A reclaim ends with a full shutdown of the host, since it is rebooted into another operating system; while reclaimed, the node's absence from the cluster is expected rather than an error.
- **Precedence vs. priority.** Precedence (OCR before tagging for one document) is a property of jobs and is handled by readiness: a job is not counted until its upstream step is done. Priority decides which ready model to load next. The two are never mixed.
- **Exhaustive service, bounded.** Once a model is loaded, it keeps serving while it has ready work, subject to a maximum dwell time or an age threshold on other queues to prevent starvation.
- **Hysteresis.** A minimum dwell after loading and an idle grace period before unloading or releasing, to prevent flapping.
- **Preemptibility.** Per workload class: whether intake can be paused, whether in-flight work can be killed and safely retried, or neither. A class may also declare a recovery action to run after a kill, such as re-queueing failed jobs through the service's API. The scheduler only switches early for classes that declare it safe.

## Architecture overview

&#91;embedded content: scheduler architecture · observe, decide, act\]

Observers feed a cache; the planner and sequencer turn that snapshot into one next step per resource; KEDA and Restate workflows carry the step out, and the effects are observed again on the next pass.

## Components and responsibilities

Each component has one job, and only the planner and sequencer make decisions.

| Component | Owns | Never does |
| --- | --- | --- |
| Observers | Translating each external source into normalized observations with a timestamp and fidelity | Decide anything; write to services |
| Observation cache | Latest observation per source, its age, and a dirty flag | Interpret observations |
| Planner | Intent: which model the GPU should serve and which shared resources should be held, based on demand, leases, and policy | I/O; knowledge of pods, drain timing, or transitional states |
| Sequencer | Per-resource state machines and the next safe step toward the intent | I/O; scheduling policy |
| Lease manager | Durable leases: NFS reference counts, manual host and LLM leases, and interactive TTL leases | Grant leases on its own; the planner and sequencer decide |
| KEDA external scaler | Exposing which workload classes are currently allowed (every requirement met, GPU or not) to KEDA, which converges replicas | Any logic beyond a lookup |
| Restate workflows | Imperative, multi-step actions: power on and off, post-kill recovery actions such as re-queueing failed jobs, the backup procedure | Choose when to run; report success as truth without observation |
| Config loader | Loading, validating, and versioning configuration; keeping the last good config | Partial application of invalid config |

Actuators come in two kinds. KEDA is level-triggered: the scheduler publishes desired state and KEDA converges. Restate workflows are edge-triggered: the sequencer invokes a procedure and later confirms its effect through observation.

## Crate layout

The workspace splits into three pure crates that hold the decision logic and four I/O crates around them.

| Crate | Purpose | I/O |
| --- | --- | --- |
| `sched-model` | Shared types: snapshot, observation, intent, step, lease, resource, model, workload class, config schema | No |
| `sched-planner` | Policy strategies that turn a snapshot and leases into an intent | No |
| `sched-sequencer` | Resource state machines and safety rules that turn an intent and current state into the next step | No |
| `sched-observe` | Adapter kinds (Kubernetes watch, HTTP/JSON, SQL, Prometheus, webhook receiver) and their registry | Yes |
| `sched-actuate` | Restate workflow definitions: power, recovery actions, backup | Yes |
| `sched-keda` | KEDA external scaler server | Yes |
| `sched-service` | The flowmanager binary: Restate service wiring (the scheduler virtual object, invoked directly by Restate), the KEDA scaler endpoint, config loading, startup | Yes |

Rules for the boundary:

- Pure crates depend only on `sched-model`, never on an I/O crate or async runtime.
- Pure crates are deterministic: the same snapshot and config always produce the same result, so they can be tested by replaying recorded snapshots.
- Service-specific knowledge lives in configuration, not in any crate.

## Control loop and event model

Observers push into a cache on their own schedules, and a short decision step, the tick, runs on every relevant event plus a slow periodic resync.

**Observation**

- Observers run outside the decision step and push results into the scheduler. A slow source never holds up decisions or lease requests.
- Kubernetes state (pods per model, node readiness) comes from watch streams, not polling.
- Other sources use webhooks where the service offers them, treated as notify-then-fetch: a webhook marks the source dirty, and the observer re-reads the source's current state.
- Every source also has a slow periodic resync, so a missed event cannot leave the scheduler stuck.
- Each observation carries its age. Safety-critical checks, such as "previous model has zero pods", require fresh data; scheduling inputs such as queue depth tolerate some staleness.

**Decision**

- The tick reads the cached snapshot, runs the planner, then the sequencer, and emits at most the next step per resource. It holds the scheduler's state lock only briefly.
- Ticks are triggered by new observations, lease changes, and workflow completions, and by a periodic backstop.
- Running the tick more often than needed is harmless because the core is deterministic.

**Example: switching from OCR to transcription**

1. The planner's intent changes to whisperX. The sequencer sees mineru serving with no in-flight work, sets the allowed set to empty, and marks mineru as draining. KEDA scales mineru to zero.
2. A pod event shows mineru at zero pods. The sequencer allows whisperX and marks it as loading. KEDA scales it up.
3. A readiness event shows whisperX ready. The sequencer marks it as serving.

No component performs the switch; it emerges from one decision source and two converging actuators. KEDA's polling and cooldown settings for these workloads must be short, with hysteresis owned by the sequencer instead.

## Configuration and extensibility

Kinds live in code and instances live in configuration, so adding a service that speaks a supported protocol never requires a new build.

| Concern | Lives in | Changes when |
| --- | --- | --- |
| Policy logic | Code (`sched-planner`) | Scheduling behavior changes |
| Policy parameters (dwell, grace, age thresholds) | Config | Tuning |
| Resource kinds (exclusive, shared) | Code | Almost never |
| Resources, models, workload classes | Config | Hardware, model, or service added |
| Adapter kinds (HTTP/JSON, SQL, Prometheus, webhook, Kubernetes) | Code (`sched-observe`) | A new protocol is needed |
| Adapter instances (endpoint, query, field extraction) | Config | A service is added |
| Power actuator kinds | Code (`sched-actuate`) | New power hardware |

**Adding a service** requires its Deployment, a ScaledObject pointing at the scheduler's scaler with its workload class in the metadata, one workload class entry, and one source entry. Neither the planner nor the sequencer changes.

**Code extension points** are narrow traits selected by a `kind` or `strategy` name in config: queue sources, policy strategies, and power actuators. A new policy strategy can run alongside the current one against recorded snapshots before it is switched on.

**Delivery and safety**

- Configuration starts as a watched ConfigMap; per-service custom resources are a later option if registering services through their own manifests becomes valuable.
- The whole config is validated before it is applied: every reference resolves, and policy parameters are consistent. Invalid config is rejected and the last good config stays active.
- Each tick is pinned to one config generation.
- A workload class removed while its model is loaded is drained normally rather than stopped abruptly.

## Safety invariants

These hold regardless of policy or configuration, and every milestone's tests should check them.

1. At most one model is allowed on cuda-central at any time.
2. A new model is allowed only after the previous model is observed at zero pods.
3. A host is never powered down while any lease on it is held, including a manual one, nor before its idle grace period ends. Releasing a manual lease never powers a host down by itself; the host powers down only once no hold remains. The one exception is a reclaim of cuda-central, which overrides pending GPU work but still stops in-flight jobs only through their declared preemptibility or after the reclaim timeout.
4. In v1, power changes come only from manual requests or from workload classes that are explicitly allowed to change power.
5. Safety-critical steps require observations newer than a configured bound; otherwise the sequencer holds.
6. The planner and sequencer are deterministic and have no side effects.
7. Only the scheduler makes decisions; actuators carry them out and never decide.
8. Work is switched away from early only for workload classes that declare it safe, or when the user forces it with a reclaim or a manual LLM lease; forced switches still give in-flight jobs a timeout to finish. An interactive lease pins its model against batch eviction.
9. Invalid configuration never replaces the last good configuration.

## Open questions

- [x] Interactive preemption: does an interactive session wait for the current batch job to finish, or may it cut a batch short? Answered: a manual LLM lease may cut batch work short, like a reclaim; an ordinary interactive request waits for the current batch job.
- [x] Kill safety (answered): immich leaves interrupted work pending and easy to find; speakr marks it failed, and it can be re-queued by hand or through speakr's API. Automatic re-queueing is a future extension.
- [x] immich ML: should the scheduler pause immich's ML-dependent queues whenever immich ML is not the loaded model, to avoid failed jobs? Answered: no. A CPU replica on blade-runner always serves immich ML, and the scheduler only scales the GPU-hosted replica on cuda-central, which Istio load-balances by least open requests. Because work proceeds without it, the GPU replica is an accelerator and should rank below workloads that would otherwise wait.
- [x] paperless-ngx: does the deployed version support workflow webhooks on tag changes? Likely yes on 3.2.1: workflows offer a Document Updated trigger with tag filters and a Webhook action. A has-tag filter fires when a document enters a state but not when it leaves, so pair it with notify-then-fetch and the periodic resync. Decision: proceed on the assumption that paperless-gpt's API tag edits fire the trigger.
- [x] speakr: does its API expose enough per-job status and timing, or is a narrow read-only database view needed? Answered: the API is enough. Pending recordings with their creation times give depth and oldest age, the PROCESSING and SUMMARIZING statuses separate the two stages, and webhooks for created, transcription and summary events make it event-driven. Risk (verified against speakr's current source): its worker is a thread inside the app that calls the transcription endpoint directly. A connection error is re-queued immediately, with no backoff, up to JOB\_MAX\_RETRIES (default 3); after that the recording is marked FAILED and kept, with its audio, for reprocessing. Nothing is deleted. Scaling whisperX to zero while speakr has queued work therefore turns those jobs into failures within seconds, which then need re-queueing. Decision: no gateway. Transcriptions that failed with a connection error count as pending demand for whisperX, so they pull the model in like any queued job. Re-queueing them through speakr's API once whisperX is serving comes later, in milestone M7. **Correction found during M2 (Oct 24, 2026):** the failure text is stored in the recording's `transcription` field as `ERROR_JSON:{...}`, not in `error_message`, and the v1 list API exposes only `has_transcription`, so these failures cannot be counted from the list. M2 leaves them unobserved; M7 must add detection (see milestones.md, Plan changes and follow-ups). Queued work is counted from `/api/v1/stats` (`queue.jobs_queued`, `recordings.pending`), because the list API has no filter for the `QUEUED` status.
- [x] Model unload signal: is "zero pods" a sufficient signal that GPU memory is free, or is a node-level check also needed? Answered: zero pods is enough.
- [x] Power-off detection: what signal confirms a host is actually off, given that node status lags? Answered in part: power-off runs as a forced command on a dedicated SSH shutdown key, and the session connecting then closing confirms the host accepted the shutdown. That moves the sequencer to shutting down, not off. Off is confirmed when the host's Linux services (SSH, kubelet) stay unreachable for a few consecutive checks, with a timeout that raises an alert. Ping alone is not enough, since cuda-central may be running Windows after a reclaim.
- [x] Configuration delivery: stay on a ConfigMap, or move to per-service custom resources later? Decision: ConfigMap, with no custom resources for now. flowmanager validates each new ConfigMap in full before applying it and keeps the last good config on failure.
