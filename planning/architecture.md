# GPU Scheduler: Architecture Design

Oct 23, 2026 · @Paul Collins

> **Host power is described in [`power-model.md`](power-model.md), which is authoritative** for ownership (who started a host decides), the shutdown stages (withdraw, veto, power off), the veto, lockout, adoption and `min_off`. This document keeps only a short summary of those and points there. The code does not match `power-model.md` yet: until refactor phases 2 to 5 land (see `milestones.md`, *Power-model refactor*), it behaves as `power-model.md` section 1 describes (for example the current names are *reclaim* for lockout and *drain* for a requested shutdown, and only the NFS mount veto exists). Where this document marks something *target*, the code does not enforce it yet.

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
- Manual requests: on, release, off, off-now, lockout (called reclaim in the code until phase 4, and valid today for cuda-central only), and manual LLM leases that cut batch work short. Open WebUI will hold such a lease and a person releases it (M6, reduced); time-limited interactive leases with renewal are no longer planned.

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
| backup (a scheduled job, not a class) | Postgres dump CronJob | NFS (a hold) | Cron schedule in the `jobs` config |
| interactive LLM | Open WebUI, editor code assist | GPU: LLM | None planned: Open WebUI will hold a manual model lease (the class and its session source stay in the example config until removed) |
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
- **Lease.** A durable record that a holder is using a shared resource or has claimed a model. In M3 only the **manual model lease** exists: it pins a model on its resource, cuts batch work short and is released by a person (it has no expiry). Interactive TTL leases (renewal, expiry) and session sources are no longer planned; the core still has the lease type but nothing issues it (see `milestones.md`, *Scope reset during M3*). An LLM loaded only for batch work has no pin and can be evicted when higher-priority work arrives. A manual power-on of a host is a lease held by the user until they release it, and it always keeps the host up on top of any other hold. Releasing it does not power the host down by itself: the host then goes down like any other, once nothing holds it and `idle_grace` is over (other leases on basestar, such as a running backup, and queued or in-flight GPU work on cuda-central, still hold it). A person who wants the host down asks for `off` or `off-now`, a different action (see *Host power*). A host found running that flowmanager did not start is adopted: the service mints a manual host lease for it, so the user holds it until released or switched off (`power-model.md` 2.1b, D3, D6). A manual lease on the LLM is the interactive counterpart of a lockout: it cuts batch work short (intake stops; in-flight jobs finish or are stopped after a timeout) and keeps the LLM loaded until released.
- **Host power** (summary; `power-model.md` is authoritative).
  - *Ownership: who started a host decides.* A host flowmanager powers on is flowmanager's, whatever wanted it (a job's hold, a workload, a manual lease), and it goes down when nothing holds it and `idle_grace` is over. A host something else started is adopted and held by the user through a lease. A manual lease always keeps a host up.
  - *Shutdown stages (target model; the code has no stages today).* Withdrawing (the host's consumers leave the allowed set, bounded by a timeout), Vetoing (a fresh reading must show nothing uses the host, unknown counts as in use, bounded; on expiry flowmanager stops and alerts), then power off. Today the same ground is covered by separate mechanisms: the shared host drain (`drain_step`), the exclusive mode wind-down and the NFS mount veto. *Target:* any hold arriving cancels an *idle* shutdown, and a *requested* one only a new manual lease does. *Today:* an idle drain on a shared host ends when anything holds it; the other cases follow `power-model.md` section 1.
  - *Actions.* *Target:* the same six on any managed host: `on`, `release`, `off` (requested shutdown, graceful), `off-now` (requested, immediate), `resume`, `lockout`. *Today:* `on`, `release` and `resume` (`POST /v1/resources/{resource}/resume`, `crates/sched-service/src/resume.rs`) exist for hosts with a power section; `off` is a graceful drain on a shared host and a plain release on an exclusive host; `off-now` is an immediate drain on a shared host and refused on an exclusive host; lockout is the reclaim lease, exclusive hosts only. A plain release never shuts a host down by itself; a drain does.
  - *Veto on both host kinds (target).* Today only the NFS mount veto exists; a GPU pod veto on cuda-central (D2) comes in phase 3.
- **Lockout** (target name; called **reclaim** in the code until phase 4). The forceful alternative to releasing: the user tells flowmanager to keep away from a host until they clear it, despite pending work. Intake stops at once, in-flight jobs finish or are stopped after a configured timeout (`reclaim_timeout` today, `lockout_timeout` later), pending jobs stay queued, the host is shut down, and nothing wakes, adopts or schedules it until the user clears the lockout. While locked out the host may be up running something else, and flowmanager neither adopts nor powers it off. The node's absence from the cluster is expected rather than an error. Today it applies to an exclusive resource with a power section (cuda-central) only; the target is any managed host, including basestar, where it deliberately blocks the backup job.
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
| Lease manager | Durable leases. Built: manual model leases, NFS reference counts (a running job's `SharedHold`), manual host leases (including the adopted lease the service mints for a host found running; target: a typed `origin: adopted`) and the reclaim lease (target: lockout). Interactive TTL leases are no longer planned | Grant leases on its own; the planner and sequencer decide |
| KEDA external scaler | Exposing which models and which workload classes are currently allowed (a model in the allowed set; a class with every requirement met, GPU or not) to KEDA, which converges replicas | Any logic beyond a lookup |
| Restate workflows | Imperative, multi-step actions: power on and the final power-off command (the shutdown stages before it, withdraw and veto, are sequencer state, not workflow steps; see `power-model.md`), recovery actions such as re-queueing failed jobs (a `Recovery` virtual object per class, beside the `Scheduler`), the backup procedure | Choose when to run; report success as truth without observation |
| Config loader | Loading, validating, and versioning configuration; keeping the last good config | Partial application of invalid config |

Actuators come in two kinds. KEDA is level-triggered: the scheduler publishes desired state and KEDA converges. Restate workflows are edge-triggered: the sequencer invokes a procedure and later confirms its effect through observation.

## Crate layout

The workspace splits into three pure crates that hold the decision logic and four I/O crates around them.

| Crate | Purpose | I/O |
| --- | --- | --- |
| `sched-model` | Shared types: snapshot, observation, intent, step, lease, resource, model, workload class, config schema | No |
| `sched-planner` | Policy strategies that turn a snapshot and leases into an intent | No |
| `sched-sequencer` | Resource state machines and safety rules that turn an intent and current state into the next step | No |
| `sched-observe` | Adapter kinds and their registry. Built: Kubernetes watch, HTTP/JSON, Prometheus, TCP probe (`tcp_probe`). Planned, no observer yet: SQL and the `session` kind. The webhook receiver is built (M5), not an observer: it nudges `http_json` sources to poll now | Yes |
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

1. The planner's intent changes to whisperX (mineru has served its minimum dwell, counted from the tick it was allowed, and is idle past its grace, or another queue is starved). The sequencer withdraws mineru's classes from the allowed set (intake paused, as far as anything is gated on them) and waits for the in-flight OCR work its queue reports to finish, but only for `drain_timeout` (or the class's `kill_timeout`; `reclaim_timeout` under a lockout, today's reclaim) from the start of the wind-down, forced or not. Nothing gates a consumer on its queue, so a queue fed without end would otherwise hold the GPU for ever; the cut-off is the withdrawal in step 2 (KEDA scales the workload to 0). Only a class with preemptibility `none` and work blocks without a timeout. End to end, a model allowed at `t0` is replaced by about `t0 + max_dwell + drain_timeout` (plus a tick) when another model has runnable ready work and no `none` class of the serving model is busy. That bound holds only while the planner's intent stays away from the serving model: if it flickers back (the waiting queue reading `ready = 0` for one tick every few minutes, or the serving class's demand going stale for one tick every few minutes) the wind-down is dropped and its clock restarts, which can postpone the switch indefinitely (a one-tick blip every 9 minutes never switched in a 6 hour simulation). A class's `kill_timeout` shorter than another blocking class's timeout does not end the wind-down early: each class is cut off at its own timeout but the model is withdrawn only when none of its classes still blocks, so the bound is the maximum over the blocking classes' timeouts. A lockout that starts and ends mid wind-down restarts the clock twice.
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
| Adapter kinds (HTTP/JSON, Prometheus, TCP probe, Kubernetes watch built; SQL planned; the webhook receiver built as a nudge, M5) | Code (`sched-observe`) | A new protocol is needed |
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

These hold regardless of policy or configuration, and every milestone's tests should check them. This is the one canonical list; the numbers are contiguous. Host power invariants follow the model in [`power-model.md`](power-model.md). Each item says whether the code enforces it today: **enforced** means `crates/sched-service/src/invariants.rs` checks it (or the property tests, where stated); **target** means it belongs to the `power-model.md` target model and the code does not enforce it yet (phases 2 to 5). Until phase 2 renumbers the code, the *Mapping* table below translates the old numbers.

**Models and the GPU**

1. At most one model is allowed on an exclusive resource (cuda-central) at any time. A switch withdraws the old model and allows the new one in the same decision, and a withdrawal step applies only to a model that is allowed. *Enforced.*
2. GPU exclusivity is enforced by Kubernetes, not by the scheduler: every GPU workload requests `nvidia.com/gpu: 1`, the device plugin exposes exactly one GPU on cuda-central (no time-slicing or MIG), and the GPU node is tainted so that only those workloads run there. This is a deployment requirement. The scheduler decides from queues, leases and policy only, never from pod state, so the new model's pod stays pending until the old one has released the GPU. The only pod readings that hold anything back are the shutdown veto (13), which only ever vetoes, and recovery (17), which waits for a ready pod. *Deployment requirement; that decisions do not depend on pod observations is checked by the property tests.*
3. A model is allowed only if it is configured on that resource and only on a host that is up (never one observed off), and never on a host under lockout except while winding down. A model that was not allowed before needs an allow step in the same decision. A model removed from the configuration leaves the allowed set at the next tick. A workload class is allowed only while every requirement holds: its host is up and not under lockout, its model (if it names one) is allowed and not winding down, and its intake is not paused. *Enforced (lockout today as reclaim, exclusive hosts with a power section only).*
4. A resource gets at most one acting step per tick, except a switch (the withdrawal of the old model together with the allowance of the new one). *Enforced.*
5. Safety-critical steps (allowing a model on a powered resource, powering a host off) require a reachability observation of the host newer than a configured bound (`host_reading_stale_after`) showing it up; otherwise the sequencer holds, and a switch waits as a whole (the old model stays) rather than leaving the GPU empty. Pod observations are not safety-critical: no decision reads them for this. *Enforced.*

**Host power: starting a host**

6. Power changes come only from the intent's `power_up` (a manual lease, a scheduled job's hold, or a workload class that may change power with work), and a host under `power.control: manual` never gets a power step: its phase follows observation. *Enforced.*
7. Ownership: flowmanager powers a host off only if it owns it. A host flowmanager powered on is its own, whatever wanted it; a host something else started is adopted and held by the user (15), not powered off until released or switched off; a host under lockout that is up is not powered off or adopted. *Enforced today as "only a host with the `managed` flag, or one under reclaim" (checked by the old invariant 4); the target wording replaces the flag with the ownership rule (phase 2).*
8. After a shutdown flowmanager started and saw confirmed (the host observed down), an automatic power-on (a workload, a job's hold) waits for `power.min_off` (default 0: no wait); a manual lease does not wait. A host found down, or gone down by itself, is not penalised. The wait only delays a fresh decision; it is never a retry. *Enforced.*
9. A power transition (powering on, shutting down) is bounded by `power_on_timeout` / `shutdown_timeout`. On expiry the resource enters the failed phase: nothing is allowed on it, no power step is emitted for it again, and it leaves that phase only when a fresh reading shows the state the transition was after (or its control is manual). Other resources are unaffected. *Enforced.*

**Host power: taking a host down**

10. A host is never powered down while any lease on it is held (a manual lease, an adopted one, a job's hold), while any class that needs it has or may have work (unknown demand counts as work), or while a model is allowed on it; and not before `idle_grace` has ended. Releasing a lease never powers a host down by itself: the host then goes down like any other, once nothing holds it. The exceptions, both overriding queued and in-flight work but never a lease, are a requested shutdown (`off`, `off-now`; today a *drain*), which also waives the grace period, and a lockout (today a *reclaim*, 14), whose lease does not count as a hold. *Enforced (requested shutdown today only on shared hosts, lockout only on exclusive hosts with a power section; target: both on any managed host).*
11. Withdraw before power off, and bounded: before a host is shut down its consumers leave the allowed set (on an exclusive host the model's wind-down, on a shared host scaling its consumer classes down). Graceful waits for in-flight work up to the timeout (`drain_timeout`, a class's `kill_timeout`, `reclaim_timeout` under lockout); immediate does not wait. *Enforced for shared hosts (a drain must scale consumers down first) and for exclusive hosts (no model allowed at power-off); target: one shared Withdrawing stage (phase 3).*
12. Any hold arriving during Withdrawing or Vetoing cancels an *idle* shutdown. A *requested* shutdown is cancelled only by a new manual lease. *Enforced for the idle drain of a shared host (an idle drain never survives a hold); target: both host kinds.*
13. Veto: a host is powered off only on a fresh reading showing nothing still uses it. A missing, stale or failed reading is unknown and counts as in use: the shutdown waits, bounded by a timeout, and on expiry flowmanager stops (the host stays up, no retry) and alerts, until the host is seen down, a person sends `resume`, or a new manual request takes it over. Veto on a shared host (NFS): zero mounting pods, bounded by `power.mounts_timeout`. *Enforced for hosts whose NFS mounts are checked; target: the GPU pod veto on cuda-central, zero model pods (D2, phase 3).*
14. Lockout: while a host is locked out, nothing wakes, adopts or schedules it (no workload, job hold or manual lease), its consumers are withdrawn (in-flight work is cut off after the lockout timeout) and it is shut down; flowmanager neither adopts nor powers off a locked-out host that is up running something else. Clearing it returns the host to the normal model. *Today the reclaim lease, valid only on an exclusive resource with a power section (ignored and reported as a wait elsewhere); checked: no model or class allowed on it, no power-on. Target: any managed host (D1, phase 4); "nothing is adopted under lockout" is not checked yet.*
15. Adoption: a host found running that flowmanager did not power on gets a manual host lease at once (typed `origin: adopted` in the target, D3; a magic id prefix today), so the user holds it until released or switched off. A host flowmanager itself woke is never adopted. *Not checked by `invariants.rs`; adoption is applied in `tick.rs` from `adopt.rs`. Target: in the sequencer, even while a job holds the host (D6, phase 4).*

**Work and switching**

16. Work is switched away from early only for workload classes that declare it safe, or when the user forces it (a lockout or a manual LLM lease). Every switch, forced or not, gives in-flight jobs only a timeout (`drain_timeout`, a class's `kill_timeout`, or `reclaim_timeout`) to finish, except that a busy `none` class holds an unforced switch without one (and the bound assumes a stable intent; see the example in *Control loop and event model*). In-flight work is stopped only on a forced switch, class by class, each when its own timeout has run out; in-flight work is what the queues report, not pods. A manual model lease pins its model against batch eviction. A model is not withdrawn before `min_dwell` has passed since it was allowed, unless the switch is forced (a lockout or a manual model lease). A class's work is stopped only if it is in flight and only in the class's own running wind-down for the current cause. *Enforced.*
17. A recovery action runs only for a class in the allowed set (its model serving, not winding down) that declares one, and only for failed items a fresh reading reports. *Enforced.*

**Design**

18. The planner and sequencer are deterministic and have no side effects: the same snapshot and config always produce the same result. Each tick is pinned to one config generation (a mismatched snapshot is an error). *Enforced by the type of the tick (`TickError::GenerationMismatch`) and the replay and property tests.*
19. Only the scheduler makes decisions; actuators carry them out and never decide. *Structural.*
20. Invalid configuration never replaces the last good configuration. *Enforced by the config loader.*

### Mapping from the old numbers

Phase 2 renumbers `invariants.rs` and the module doc comment of `crates/sched-sequencer/src/lib.rs` with this table. "Old checker" is the number in the messages of `crates/sched-service/src/invariants.rs` (there is no 6, 7 or 10; eleven checks carry no number and each has a row below, listed by their message text); "old rule" is the numbered *Safety rules* list in the doc comment of `crates/sched-sequencer/src/lib.rs`; "old design" is this document's list before 2026-10-30.

| Old checker (`invariants.rs`) | New | Notes |
| --- | --- | --- |
| 1 | 1 | |
| 2 | 1, 3 | "allowed X while old stays allowed" (the switch rule) is 1; not configured, host observed down or not on, allowed on reclaimed (lockout), no allow step are 3 |
| (no number) "allowed class ... which is not configured" | 3 | |
| (no number) "allowed class ... while R is not up" | 3 | |
| (no number) "allowed class ... on reclaimed R" | 3 | Lockout, see also 14 |
| (no number) "allowed class ... while M is not allowed" | 3 | |
| (no number) "allowed class ... while M is being wound down" | 3 | |
| (no number) "allowed class ... whose intake is paused" | 3 | |
| (no number) "more than one acting step for R" | 4 | |
| (no number) "powered on reclaimed R" | 14 | |
| (no number) "stopped C though nothing of it is in flight" | 16 | |
| (no number) "withdrew M on R, which was not allowed" | 1 | |
| (no number) "drained M before min_dwell" | 16 | |
| 3 | 10, 11 | Holds, work, model allowed and idle grace are 10; "powered off on a graceful drain before its consumers were scaled down" is 11 |
| 4 | 6 | The message "powered off unmanaged host" is 7 |
| 4b | 8 | `min_off` |
| 5 | 5 | |
| 8 | 16 | |
| 9 | 17 | |
| 11 | 9 | |
| 12 | 13 | Mount veto |
| 13 | 11 | Shared consumers scaled down before power-off |
| 14 | 12 | Idle drain ends on any hold |

| Old rule (`sched-sequencer/src/lib.rs`) | New | Notes |
| --- | --- | --- |
| 1 | 1 | |
| 2 | 2, 3 | Pod-state clause 2, allowed-set clause 3 |
| 3 | 10, 11 | Holds, work and grace 10; drain withdrawal 11; the reclaim override is 14 |
| 4 | 6, 7, 14 | Power-up sources and manual control 6; "took responsibility" 7; the reclaim-lease sentence 14 |
| 5 | 5 | |
| 6 | 9 | |
| 7 | 13 | |
| 8 | 16 | |
| 9 | 8 | `min_off` |

| Old design invariant (this document, before 2026-10-30) | New | Notes |
| --- | --- | --- |
| 1 | 1 | |
| 2 | 2, 3 | |
| 3 | 10 | The sentence "Releasing a manual lease never powers a host down by itself" stays true; requested `off` and `off-now` are the stated exception, as is lockout (14) |
| 4 | 6 | |
| 5 | 5 | |
| 6 | 18 | |
| 7 | 19 | |
| 8 | 16 | |
| 9 | 20 | |

New invariants with no old number: 7 (ownership, as a rule; the check today is the `managed` flag under old 4), 12 in its target form, 14 in its target form, 15 (adoption), 18's generation clause.

## Open questions

- [x] Interactive preemption: does an interactive session wait for the current batch job to finish, or may it cut a batch short? Answered: a manual LLM lease may cut batch work short, like a reclaim; there is no ordinary interactive request path any more (interactive leases are no longer planned).
- [x] Kill safety (answered): immich leaves interrupted work pending and easy to find; speakr marks it failed, and it can be re-queued by hand or through speakr's API. Automatic re-queueing of speakr's failed transcriptions is done (M3, P5; see milestones.md, *Decided during M3*).
- [x] immich ML: should the scheduler pause immich's ML-dependent queues whenever immich ML is not the loaded model, to avoid failed jobs? Answered: no. A CPU replica on blade-runner always serves immich ML, and the scheduler only scales the GPU-hosted replica on cuda-central, which Istio load-balances by least open requests. Because work proceeds without it, the GPU replica is an accelerator and should rank below workloads that would otherwise wait.
- [x] paperless-ngx: does the deployed version support workflow webhooks on tag changes? Likely yes on 3.2.1: workflows offer a Document Updated trigger with tag filters and a Webhook action. A has-tag filter fires when a document enters a state but not when it leaves, so pair it with notify-then-fetch and the periodic resync. Decision: proceed on the assumption that paperless-gpt's API tag edits fire the trigger.
- [x] speakr: does its API expose enough per-job status and timing, or is a narrow read-only database view needed? Answered: the API is enough. Pending recordings with their creation times give depth and oldest age, the PROCESSING and SUMMARIZING statuses separate the two stages, and webhooks for created, transcription and summary events make it event-driven. Risk (verified against speakr's current source): its worker is a thread inside the app that calls the transcription endpoint directly. A connection error is re-queued immediately, with no backoff, up to JOB\_MAX\_RETRIES (default 3); after that the recording is marked FAILED and kept, with its audio, for reprocessing. Nothing is deleted. Scaling whisperX to zero while speakr has queued work therefore turns those jobs into failures within seconds, which then need re-queueing. Decision: no gateway. Transcriptions that failed with a connection error count as pending demand for whisperX, so they pull the model in like any queued job. Re-queueing them through speakr's API once whisperX is serving comes later, in milestone M7. **Done in M3 (P5):** see below and milestones.md. **Correction found during M2 (Oct 24, 2026):** the failure text is stored in the recording's `transcription` field as `ERROR_JSON:{...}`, not in `error_message`, and the v1 list API exposes only `has_transcription`, so these failures cannot be counted from the list. M2 leaves them unobserved; M7 must add detection (see milestones.md, Plan changes and follow-ups). **Superseded in M3 (Oct 24, 2026):** they are counted from the list after all: `status=failed` recordings with `has_summary: false` (a failed summary's error is stored in `summary`, every other failure's in `transcription`) updated within a 15m `updated_since` window count as whisperX demand, and automatic re-queueing, capped at 2 per recording, moved from M7 into M3 (see milestones.md, *Decided during M3*). Queued work is counted from `/api/v1/stats` (`queue.jobs_queued`, `recordings.pending`), because the list API has no filter for the `QUEUED` status.
- [x] Model unload signal: is "zero pods" a sufficient signal that GPU memory is free, or is a node-level check also needed? Answered: zero pods is enough. **Superseded in M3 (Oct 24, 2026):** the scheduler no longer waits for any unload signal. Kubernetes enforces GPU exclusivity (invariant 2): the next model's pod cannot start until the previous one has released the GPU.
- [x] Power-off detection: what signal confirms a host is actually off, given that node status lags? Answered in part: power-off runs as a forced command on a dedicated SSH shutdown key, and the session connecting then closing confirms the host accepted the shutdown. That moves the sequencer to shutting down, not off. Off is confirmed when the host's Linux services (SSH, kubelet) stay unreachable for a few consecutive checks, with a timeout that raises an alert. Ping alone is not enough, since cuda-central may be running Windows after a reclaim.
- [x] Configuration delivery: stay on a ConfigMap, or move to per-service custom resources later? Decision: ConfigMap, with no custom resources for now. flowmanager validates each new ConfigMap in full before applying it and keeps the last good config on failure.
