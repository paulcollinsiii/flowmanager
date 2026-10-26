# GPU Scheduler: Architecture Design

Oct 23, 2026 · @Paul Collins

## Problem and goals

A single Rust service, flowmanager (the scheduler), decides which model runs on the GPU node and which shared resources must be up, so every job queue drains in a predictable order without services contending for the GPU.

Several services need a GPU that only fits one model at a time, and some also need an NFS share that is not always powered. Today each service scales on its own demand, nothing arbitrates between them, and the backup job powers NFS down without knowing who else is using it.

Goals:

- **One decision authority.** Only the scheduler decides which model is loaded and which shared resources are held. Everything else either reports state or carries out a decision.
- **Switch-cost-aware ordering.** Pick the next model by first-in-first-out across ready work, then drain that queue before switching, within configurable bounds.
- **Safe transitions.** Never publish two models of one GPU at once (Kubernetes enforces that two never run: see invariant 2), never power down a resource in use, never act on stale safety-critical data.
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
- Running scheduled jobs, starting with the weekly backup, as scheduler-managed Kubernetes Jobs (M4, rescoped: see `milestones.md`, *Scope reset during M3*).
- Manual requests: release, cuda-central reclaim, and manual LLM leases that cut batch work short. Open WebUI will hold such a lease and a person releases it (M6, reduced); time-limited interactive leases with renewal are no longer planned.

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
| speakr transcription | speakr (whisperX) | GPU: whisperX, NFS | speakr API v1 (pending recordings; failed ones counted in a separate `failed` bucket and re-queued by the class's recovery) and webhooks |
| speakr summarization | speakr (LLM) | GPU: LLM, NFS | speakr API v1 (pending recordings, stats) and webhooks |
| immich ML | immich machine-learning (GPU replica only; a CPU replica on blade-runner always serves) | GPU: immich ML, NFS | immich job queues (Valkey); API preferred |
| backup | Postgres dump cronjob | NFS | Weekly schedule |
| interactive LLM | Open WebUI, editor code assist | GPU: LLM | Session requests (not yet built) |
| paperless-gpt (gate) | paperless-gpt itself | cuda-central up, NFS up (no model) | None: a gate class on host readiness |

OCR must finish before LLM tagging for the same document, and transcription before summarization for the same recording. The backup does not interact with the GPU.

Services that never touch the GPU, such as paperless-ngx itself and the immich server, also depend on NFS. They are modeled as NFS-only workload classes, so turning NFS off scales them to zero and turning it on lets them run, with or without the GPU.

## Core concepts

The scheduler reasons only about generic resources, models, and workload classes; it never knows a service by name.

- **Resource.** Something workloads need. Two kinds:
  - *Exclusive-mode*: holds at most one mode at a time. cuda-central is exclusive, and its modes are models.
  - *Shared*: any number of holders, tracked by a reference count. The basestar NFS share is shared.
- **Model.** A mode of an exclusive resource, implemented by one or more scaled workloads, with an estimated load time used as its switch cost.
- **Workload class.** A unit of demand: its resource requirements, priority, preemptibility, whether it may change power state, and where its demand is observed.
- **Lease.** A durable record that a holder is using a shared resource or has an interactive claim on a model. Interactive leases expire unless renewed, and while held they pin their model: batch work cannot evict it. The client renews the lease while the user is active, so a session never drops mid-use. (The lease type exists in the core, but nothing issues it: the plan was reduced to the manual model lease, which has no expiry and is released by a person; see `milestones.md`.) An LLM loaded only for batch work has no such pin and can be evicted when higher-priority work arrives. A manual power-on of a host is a lease held by the user until they release it. Releasing it powers the host down only after every other hold ends: other leases on basestar, such as a running backup, and queued or in-flight GPU work on cuda-central. A manual lease on the LLM is the interactive counterpart of a reclaim: it cuts batch work short (intake stops; in-flight jobs finish or are stopped after a timeout) and keeps the LLM loaded until released.
- **Reclaim.** The forceful alternative to releasing, for cuda-central only: the user takes the host back despite pending work. Intake stops at once, in-flight jobs finish or are stopped after a configured timeout, pending jobs stay queued, and the host is not woken or scheduled until the user returns it. A reclaim ends with a full shutdown of the host, since it is rebooted into another operating system; while reclaimed, the node's absence from the cluster is expected rather than an error.
- **Gate class.** A workload class whose requirements are hosts only: on an exclusive resource it names no model, which means "that host is up". It has no demand source and never influences which model is chosen. It gates a consumer on host readiness, for example paperless-gpt, which may run only while cuda-central is up and paperless-ngx (its documents on basestar-nfs) is reachable. Consumers are not gated for GPU contention at all: that is Kubernetes' job (invariant 2).
- **Precedence vs. priority.** Precedence (OCR before tagging for one document) is a property of jobs and is handled by readiness: a job is not counted until its upstream step is done. Priority decides which ready model to load next. The two are never mixed.
- **Exhaustive service, bounded.** Once a model is loaded, it keeps serving while it has ready work, subject to a maximum dwell time or an age threshold on other queues to prevent starvation.
- **Hysteresis.** A minimum dwell after a model is allowed (counted from the tick it is allowed, so it includes load time) and an idle grace period before unloading or releasing, to prevent flapping.
- **Preemptibility.** Per workload class: whether intake can be paused, whether in-flight work can be killed and safely retried, or neither. A class may also declare a recovery action, such as re-queueing failed jobs through the service's API. Demand has three buckets: ready, in-flight and *failed*; failed items wait to be re-queued, never age into starvation priority, make their model wanted but never hold the model that is serving, and trigger the class's recovery while its model is allowed (it needs the model up, so it does not run after a kill). The action runs in its own Restate object so it cannot delay a tick, and its per-item attempt cap is durable. The scheduler only switches early for classes that declare it safe.

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
| Lease manager | Durable leases. Built: manual model leases. Planned (M4): NFS reference counts and manual host leases (a manual NFS lease brings up the host and its consumers). Interactive TTL leases are no longer planned | Grant leases on its own; the planner and sequencer decide |
| KEDA external scaler | Exposing which models and which workload classes are currently allowed (a model in the allowed set; a class with every requirement met, GPU or not) to KEDA, which converges replicas | Any logic beyond a lookup |
| Restate workflows | Imperative, multi-step actions: power on and off, recovery actions such as re-queueing failed jobs (a `Recovery` virtual object per class, beside the `Scheduler`), the backup procedure | Choose when to run; report success as truth without observation |
| Config loader | Loading, validating, and versioning configuration; keeping the last good config | Partial application of invalid config |

Actuators come in two kinds. KEDA is level-triggered: the scheduler publishes desired state and KEDA converges. Restate workflows are edge-triggered: the sequencer invokes a procedure and later confirms its effect through observation.

## Crate layout

The workspace splits into three pure crates that hold the decision logic and four I/O crates around them.

| Crate | Purpose | I/O |
| --- | --- | --- |
| `sched-model` | Shared types: snapshot, observation, intent, step, lease, resource, model, workload class, config schema | No |
| `sched-planner` | Policy strategies that turn a snapshot and leases into an intent | No |
| `sched-sequencer` | Resource state machines and safety rules that turn an intent and current state into the next step | No |
| `sched-observe` | Adapter kinds and their registry. Built: Kubernetes watch, HTTP/JSON, Prometheus, TCP probe (`tcp_probe`). Planned, no observer yet: SQL, webhook receiver, and the `schedule` and `session` kinds | Yes |
| `sched-actuate` | Actuator I/O: recovery actions (`requeue_failed`: list, re-queue request, attempt bookkeeping), later power and backup. The Restate object that sequences and journals an action (`Recovery`) lives in `sched-service`, which owns the Restate SDK | Yes |
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
- Kubernetes state (node readiness, and pods per model for information only) comes from watch streams, not polling.
- Reachability of a host that is not a Kubernetes node (basestar) comes from a TCP connect probe (`tcp_probe`, as the user's `power.py` checks it): one connect per resync to a host and port, connected is up, refused, no answer or "no route to host" is down, anything else is a failed read. The `prometheus` kind remains available for hosts a Prometheus probe covers.
- Other sources use webhooks where the service offers them, treated as notify-then-fetch: a webhook marks the source dirty, and the observer re-reads the source's current state.
- Every source also has a slow periodic resync, so a missed event cannot leave the scheduler stuck.
- Each observation carries its age. Safety-critical checks, such as "the host is up" before a model is allowed or a host powered off, require fresh data; scheduling inputs such as queue depth tolerate some staleness. No decision reads pod state.

**Decision**

- The tick reads the cached snapshot, runs the planner, then the sequencer, and emits at most the next step per resource. It holds the scheduler's state lock only briefly.
- Ticks are triggered by new observations, lease changes, and workflow completions, and by a periodic backstop.
- Running the tick more often than needed is harmless because the core is deterministic.

**Example: switching from OCR to transcription**

1. The planner's intent changes to whisperX (mineru has served its minimum dwell, counted from the tick it was allowed, and is idle past its grace, or another queue is starved). The sequencer withdraws mineru's classes from the allowed set (intake paused, as far as anything is gated on them) and waits for the in-flight OCR work its queue reports to finish, but only for `drain_timeout` (or the class's `kill_timeout`; `reclaim_timeout` on a reclaim) from the start of the wind-down, forced or not. Nothing gates a consumer on its queue, so a queue fed without end would otherwise hold the GPU for ever; the cut-off is the withdrawal in step 2 (KEDA scales the workload to 0). Only a class with preemptibility `none` and work blocks without a timeout. End to end, a model allowed at `t0` is replaced by about `t0 + max_dwell + drain_timeout` (plus a tick) when another model has runnable ready work and no `none` class of the serving model is busy. That bound holds only while the planner's intent stays away from the serving model: if it flickers back (the waiting queue reading `ready = 0` for one tick every few minutes, or the serving class's demand going stale for one tick every few minutes) the wind-down is dropped and its clock restarts, which can postpone the switch indefinitely (a one-tick blip every 9 minutes never switched in a 6 hour simulation). A class's `kill_timeout` shorter than another blocking class's timeout does not end the wind-down early: each class is cut off at its own timeout but the model is withdrawn only when none of its classes still blocks, so the bound is the maximum over the blocking classes' timeouts. A reclaim that starts and ends mid wind-down restarts the clock twice.
2. With no work in flight (or at the timeout), in one tick and on a fresh "up" reading of cuda-central, the sequencer replaces mineru by whisperX in the allowed set: mineru leaves and whisperX enters together, and whisperX is serving from that tick. KEDA scales mineru to zero and whisperX to one.
3. Kubernetes keeps the whisperX pod pending until mineru's pod has terminated and released the GPU (both request `nvidia.com/gpu: 1`, the node exposes one GPU, and the GPU node is tainted so nothing else lands there). The scheduler does not watch for this: pod counts are information only.

No component performs the switch; it emerges from one decision source, two converging actuators and Kubernetes' resource accounting. KEDA's polling and cooldown settings for these workloads must be short, with hysteresis owned by the sequencer instead.

## Configuration and extensibility

Kinds live in code and instances live in configuration, so adding a service that speaks a supported protocol never requires a new build.

| Concern | Lives in | Changes when |
| --- | --- | --- |
| Policy logic | Code (`sched-planner`) | Scheduling behavior changes |
| Policy parameters (dwell, grace, age thresholds) | Config | Tuning |
| Resource kinds (exclusive, shared) | Code | Almost never |
| Resources, models, workload classes | Config | Hardware, model, or service added |
| Adapter kinds (HTTP/JSON, Prometheus, TCP probe, Kubernetes watch built; SQL, webhook planned) | Code (`sched-observe`) | A new protocol is needed |
| Adapter instances (endpoint, query, field extraction) | Config | A service is added |
| Power actuator kinds | Code (`sched-actuate`) | New power hardware |

**Adding a service** requires its Deployment, a ScaledObject pointing at the scheduler's scaler with its model or its workload class in the metadata (`model: <id>` for a model's own workload, `class: <id>` for a consumer gated on a class), one workload class entry, and one source entry. Neither the planner nor the sequencer changes.

**Code extension points** are narrow traits selected by a `kind` or `strategy` name in config: queue sources, policy strategies, and power actuators. A new policy strategy can run alongside the current one against recorded snapshots before it is switched on.

**Delivery and safety**

- Configuration starts as a watched ConfigMap; per-service custom resources are a later option if registering services through their own manifests becomes valuable.
- The whole config is validated before it is applied: every reference resolves, and policy parameters are consistent. Invalid config is rejected and the last good config stays active.
- Each tick is pinned to one config generation.
- A workload class removed while its model is loaded is drained normally rather than stopped abruptly. A model removed while it is allowed leaves the allowed set at the next tick (its pods then scale down; nothing waits for them).

## Safety invariants

These hold regardless of policy or configuration, and every milestone's tests should check them.

1. At most one model is allowed on cuda-central at any time.
2. GPU exclusivity is enforced by Kubernetes, not by the scheduler: every GPU workload requests `nvidia.com/gpu: 1`, the device plugin exposes exactly one GPU on cuda-central (no time-slicing or MIG), and the GPU node is tainted so that only those workloads run there. This is a deployment requirement. The scheduler decides from queues, leases and policy only, never from pod state: when it switches, the old model leaves the allowed set and the new one enters it in the same decision, and the new pod stays pending until the old one has released the GPU. A model is allowed only if it is configured, and only on a host that is up (never on a host observed off); a model removed from the configuration leaves the allowed set at the next tick.
3. A host is never powered down while any lease on it is held, including a manual one, nor before its idle grace period ends. Releasing a manual lease never powers a host down by itself; the host powers down only once no hold remains. The one exception is a reclaim of cuda-central, which overrides pending GPU work but still stops in-flight jobs only through their declared preemptibility or after the reclaim timeout.
4. In v1, power changes come only from manual requests or from workload classes that are explicitly allowed to change power.
5. Safety-critical steps (allowing a model, powering a host off) require a reachability observation of the host newer than a configured bound (`safety_max_age`); otherwise the sequencer holds, and a switch waits as a whole (the old model stays) rather than leaving the GPU empty. Pod observations are not safety-critical: no decision reads them.
6. The planner and sequencer are deterministic and have no side effects.
7. Only the scheduler makes decisions; actuators carry them out and never decide.
8. Work is switched away from early only for workload classes that declare it safe, or when the user forces it with a reclaim or a manual LLM lease; every switch, forced or not, gives in-flight jobs only a timeout (`drain_timeout`, a class's `kill_timeout`, or `reclaim_timeout`) to finish, except that a busy `none` class holds an unforced switch without one (and the bound assumes a stable intent; see the example in *Control loop and event model*). An interactive lease pins its model against batch eviction.
9. Invalid configuration never replaces the last good configuration.

## Open questions

- [x] Interactive preemption: does an interactive session wait for the current batch job to finish, or may it cut a batch short? Answered: a manual LLM lease may cut batch work short, like a reclaim; an ordinary interactive request waits for the current batch job.
- [x] Kill safety (answered): immich leaves interrupted work pending and easy to find; speakr marks it failed, and it can be re-queued by hand or through speakr's API. Automatic re-queueing of speakr's failed transcriptions is done (M3, P5; see milestones.md, *Decided during M3*).
- [x] immich ML: should the scheduler pause immich's ML-dependent queues whenever immich ML is not the loaded model, to avoid failed jobs? Answered: no. A CPU replica on blade-runner always serves immich ML, and the scheduler only scales the GPU-hosted replica on cuda-central, which Istio load-balances by least open requests. Because work proceeds without it, the GPU replica is an accelerator and should rank below workloads that would otherwise wait.
- [x] paperless-ngx: does the deployed version support workflow webhooks on tag changes? Likely yes on 3.2.1: workflows offer a Document Updated trigger with tag filters and a Webhook action. A has-tag filter fires when a document enters a state but not when it leaves, so pair it with notify-then-fetch and the periodic resync. Decision: proceed on the assumption that paperless-gpt's API tag edits fire the trigger.
- [x] speakr: does its API expose enough per-job status and timing, or is a narrow read-only database view needed? Answered: the API is enough. Pending recordings with their creation times give depth and oldest age, the PROCESSING and SUMMARIZING statuses separate the two stages, and webhooks for created, transcription and summary events make it event-driven. Risk (verified against speakr's current source): its worker is a thread inside the app that calls the transcription endpoint directly. A connection error is re-queued immediately, with no backoff, up to JOB\_MAX\_RETRIES (default 3); after that the recording is marked FAILED and kept, with its audio, for reprocessing. Nothing is deleted. Scaling whisperX to zero while speakr has queued work therefore turns those jobs into failures within seconds, which then need re-queueing. Decision: no gateway. Transcriptions that failed with a connection error count as pending demand for whisperX, so they pull the model in like any queued job. Re-queueing them through speakr's API once whisperX is serving comes later, in milestone M7. **Done in M3 (P5):** see below and milestones.md. **Correction found during M2 (Oct 24, 2026):** the failure text is stored in the recording's `transcription` field as `ERROR_JSON:{...}`, not in `error_message`, and the v1 list API exposes only `has_transcription`, so these failures cannot be counted from the list. M2 leaves them unobserved; M7 must add detection (see milestones.md, Plan changes and follow-ups). **Superseded in M3 (Oct 24, 2026):** they are counted from the list after all: `status=failed` recordings with `has_summary: false` (a failed summary's error is stored in `summary`, every other failure's in `transcription`) updated within a 15m `updated_since` window count as whisperX demand, and automatic re-queueing, capped at 2 per recording, moved from M7 into M3 (see milestones.md, *Decided during M3*). Queued work is counted from `/api/v1/stats` (`queue.jobs_queued`, `recordings.pending`), because the list API has no filter for the `QUEUED` status.
- [x] Model unload signal: is "zero pods" a sufficient signal that GPU memory is free, or is a node-level check also needed? Answered: zero pods is enough. **Superseded in M3 (Oct 24, 2026):** the scheduler no longer waits for any unload signal. Kubernetes enforces GPU exclusivity (invariant 2): the next model's pod cannot start until the previous one has released the GPU.
- [x] Power-off detection: what signal confirms a host is actually off, given that node status lags? Answered in part: power-off runs as a forced command on a dedicated SSH shutdown key, and the session connecting then closing confirms the host accepted the shutdown. That moves the sequencer to shutting down, not off. Off is confirmed when the host's Linux services (SSH, kubelet) stay unreachable for a few consecutive checks, with a timeout that raises an alert. Ping alone is not enough, since cuda-central may be running Windows after a reclaim.
- [x] Configuration delivery: stay on a ConfigMap, or move to per-service custom resources later? Decision: ConfigMap, with no custom resources for now. flowmanager validates each new ConfigMap in full before applying it and keeps the last good config on failure.
