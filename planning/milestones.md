# GPU Scheduler: Milestones

Oct 23, 2026 · @Paul Collins

## How to use this plan

Eight milestones take the scheduler from types and tests to a fully event-driven system, and each one leaves something working and verifiable. This plan accompanies the architecture design doc, which defines the components, crates, and safety invariants referenced here.

Sequencing principles:

- **Pure core first.** Decision logic is built and tested against recorded and synthetic snapshots before any I/O exists.
- **Shadow before control.** The scheduler observes real demand and logs what it would do before it is allowed to act.
- **One new risk per milestone.** Each milestone introduces one new kind of external interaction, so failures are easy to place.
- **Polling before events.** Each source works with periodic resync first; webhooks are layered on afterward without changing the core.
- **Safety invariants are tested in every milestone** that touches the sequencer or an actuator.

Each milestone below states its goal, what is in and out of scope, and the exit criteria that mark it done. Detailed task planning is left to the implementer.

## Roadmap

&#91;embedded content: milestone dependencies · 8 milestones\]

M0 through M4 run in sequence. M5 can start any time after M2 because it only changes observation, and M6 and M7 can proceed in either order once M4 is done.

## Milestone details

### M0: Foundations

**Goal:** a workspace whose shared types and configuration can describe the whole system, with a test harness ready for the core.

- **In scope:** crate skeletons per the design doc; shared types for resources, models, workload classes, observations, snapshots, intents, steps, and leases; the configuration schema with loading and validation; a fixture format for recorded and synthetic snapshots.
- **Out of scope:** any decision logic, I/O, Restate, or Kubernetes interaction.
- **Exit criteria:** a config describing every current workload loads and validates; invalid configs (dangling references, inconsistent policy values) are rejected with clear errors; snapshot fixtures can be loaded in tests.

### M1: Decision core

**Goal:** a deterministic planner and sequencer that make correct, safe decisions on paper.

- **In scope:** the first-in-first-out, exhaustive-with-bounds policy strategy; minimum dwell, maximum dwell, and idle grace; readiness-based precedence; resource state machines for the GPU node and the NFS share, including manual leases, release, reclaim, and manual LLM leases; every safety invariant enforced in the sequencer; table-driven and replay tests.
- **Out of scope:** observers, actuators, persistence, and preemption of in-flight work.
- **Exit criteria:** scenario tests cover interleaved OCR and transcription arrivals, OCR followed by LLM tagging, starvation bounds, flapping prevention, and stale-observation holds; every safety invariant has at least one test that would fail if it were violated.

### M2: Shadow mode

**Goal:** the scheduler runs in the cluster, observes real demand, and logs what it would do, without acting.

- **In scope:** the Restate-hosted scheduler with its tick and periodic backstop; the observation cache with ages; a Kubernetes watch observer for model pods and node readiness; a generic HTTP/JSON adapter configured for paperless-ngx and speakr's v1 API, counting speakr's queued and in-flight work as demand (see *Plan changes* below: failed-with-connection-error transcriptions are NOT observable in M2); logs and metrics of each decision and its reason.
- **Out of scope:** the KEDA scaler, power, leases, and webhooks.
- **Exit criteria:** over at least a week of real use, the logged decisions match what a person would have chosen, and any disagreements are resolved by policy or config changes rather than code in the core.

### M3: GPU control

**Goal:** the scheduler, not each service, decides which model runs on cuda-central.

- **In scope:** the KEDA external scaler serving the allowed set; ScaledObjects for paperless and speakr workloads pointed at it; short KEDA polling and cooldown so hysteresis lives in the scheduler; manual power only.
- **Out of scope:** NFS, backup, interactive sessions, and immich.
- **Exit criteria:** with cuda-central powered on, mixed paperless and speakr workloads drain in the expected order; no two models ever run at once (the scheduler publishes at most one, and Kubernetes enforces it: see *Decided during M3*, scope reset); speakr transcriptions that fail while whisperX is unloaded show up as demand and bring it in; a scheduler restart mid-switch resumes safely.

### M4: Leases, NFS, and backup

**Goal:** shared resources are reference-counted, and the backup becomes a client of the scheduler instead of owning power.

- **In scope:** the lease manager; NFS gating for every NFS-dependent service, including NFS-only ones; leases for backup and manual power-on; power actuator workflows for basestar and cuda-central, with power-off confirmed by the SSH shutdown signal and then unreachability checks; a minimal API for manual power-on, release, and cuda-central reclaim; the weekly backup rewritten as a workflow that requests a lease, waits for it, dumps, and releases.
- **Out of scope:** demand-driven power and interactive leases.
- **Exit criteria:** basestar never powers down while a lease is held; a backup overlapping paperless work completes without disrupting it; manual power requests work for both hosts; a reclaim shuts cuda-central down despite pending work, which stays queued until it returns.

### M5: Event-driven observation

**Goal:** decisions react to events within seconds, with polling kept only as a slow safety net.

- **In scope:** a generic webhook receiver using notify-then-fetch; paperless-ngx workflow webhooks (including paperless-gpt's tag edits) and speakr webhooks; resync intervals lengthened for sources with webhooks; metrics for event-to-decision latency.
- **Out of scope:** changes to the planner or sequencer.
- **Exit criteria:** a new paperless document leads to a decision within seconds; dropping a webhook still leads to a correct decision at the next resync.

### M6: Interactive sessions

**Goal:** a person can claim the LLM for interactive use from Open WebUI or an editor without manually juggling models.

- **In scope:** interactive leases with renewal and expiry; a request path for clients to acquire and renew them; manual LLM leases that cut batch work short, as a reclaim does; priority over batch work at job boundaries for ordinary interactive requests; optionally a model-routing proxy in front of the LLM.
- **Out of scope:** demand-driven power for interactive requests.
- **Exit criteria:** a manual LLM lease preempts batch work within its timeout; other interactive requests are served after at most the current batch job; an idle session releases the LLM after its lease expires; batch draining resumes afterward.

### M7: immich and preemption capabilities

**Goal:** immich ML joins the schedule, and classes that can be paused or safely retried can be switched away from early.

- **In scope:** an immich demand source; scaling the GPU-hosted immich ML replica as a low-priority accelerator alongside the always-on CPU replica; per-class preemptibility declarations and recovery actions, including re-queueing failed speakr transcriptions through its API once whisperX is serving, **and the detection of those failures (not possible through the list API; see Plan changes)** (superseded: detection from the list and the capped automatic re-queue moved to M3; see Plan changes, *Decided during M3*); early switching for classes that declare it safe.
- **Out of scope:** new policy strategies.
- **Exit criteria:** the GPU immich ML replica runs only when chosen, and immich keeps working through the CPU replica otherwise; early switches happen only for classes that declare them safe, and no work is lost.

## Deferred beyond these milestones

These are supported by the design but intentionally left out of the plan above.

- **Demand-driven power:** switching the GPU node's power policy from manual to auto-wake, then auto-wake-and-sleep.
- **Per-service custom resources:** replacing the ConfigMap with resources that each service's manifests register themselves.
- **Cost-aware policy:** a predictive, cost-based strategy that weighs switch cost against waiting time, run side by side with the current policy on recorded snapshots before being enabled.
- **Lease-based admission:** services asking the scheduler for a lease before taking a job, instead of the scheduler inferring demand from outside.
- **SQL adapter:** a generic SQL queue source, no longer needed now that speakr's API covers its demand.

## Plan changes and follow-ups

Decisions made during implementation that change or extend the milestones above. Each item names the milestone that owns it. Do not drop items from this list until the owning milestone's exit criteria are checked against them.

### Decided during M2 (Oct 23-24, 2026)

- **Speakr failed transcriptions are invisible in M2 (owner: M7, user decision).** Verified against speakr's source: when a transcription job exhausts its retries (`JOB_MAX_RETRIES`, default 3, retried immediately), `job_queue.py` sets `status=FAILED` and stores the error in `recording.transcription` as `ERROR_JSON:{...}`, not in `error_message`. The v1 list API returns only `has_transcription`, so the original M2 scope item "count speakr transcriptions that failed with a connection error as pending demand" cannot be met from the list. M2 therefore sees speakr transcription demand only while jobs are PENDING, queued, or processing, which in a whisperX outage is only seconds. M7 must pick a detection method: per-recording detail fetch (`GET /api/v1/recordings/{id}`, match the ERROR_JSON error type, cache per recording id to stay inside the 120/min read limit), counting every FAILED recording in a window, or the SQL adapter against speakr's database (this makes the "SQL adapter no longer needed" note under *Deferred* wrong for this case). The `requeue_failed` recovery in M7 needs the same detection. **Superseded in M3 (Oct 24, 2026):** detection is done from the list after all (`status=failed`, `has_summary: false`, `updated_since` window of 15m), and automatic re-queueing, capped at 2 per recording, moved into M3; see *Decided during M3*.
- **Speakr queue counts come from `/api/v1/stats` (owner: M2, done).** The list API has no filter for `QUEUED`, which is the state a recording is in while it waits for transcription. Ready = `recordings.pending` + `queue.jobs_queued`; in-flight = `queue.jobs_processing`. These count all job types (summary, merge, stitch) for the token's user only, so transcription demand is a conservative overcount. M7 or later may tighten this if it distorts decisions.
- **Shadow output is stateless (owner: M2/M3).** Ticks start from a fresh sequencer state, so dwell, idle grace, drain, pause-intake and power-off never appear in M2 logs. Compare logs to human choices with that in mind. M3 is the first milestone where hysteresis can be judged.
- **Restate ingress is trusted-network only (owner: M3, user decision).** The `observe` handler accepts any valid-looking push for a configured source, so anyone who can reach the ingress could forge, for example, `pods: 0`. The cluster uses Istio mTLS, but mTLS authenticates and does not authorize. Before M3 (when a false zero can load a second model), add an Istio `AuthorizationPolicy` allowing only the flowmanager service account to call the Restate ingress, or move observer pushes in-process. Record the policy in the deploy manifests.
- **Kube dependency features (owner: done).** `kube` now uses `default-features = false` with `client`, `runtime`, `rustls-tls`, `aws-lc-rs` so only one rustls crypto provider is present. `futures` (0.3, std only) was approved for watch streams. `unsafe_code` is forbidden workspace-wide.

### Decided during M3 (Oct 24, 2026)

- **Speakr failed-transcription detection, and its automatic re-queue, pulled forward from M7 into M3 (owner: M3, user decision).** Detected from the list only, no per-recording fetch: `GET /api/v1/recordings?status=failed&per_page=100` with `updated_since=<now - 15m>`, counting items with `has_summary: false` (speakr stores a failed summary's error in `summary`, every other failure's in `transcription`) as ready `speakr-transcription` demand, as a second request of `speakr-pending`. The window is 15m, below `age_threshold` (30m), so a failure never reaches starvation priority. A failure counts as whisperX demand until it is re-queued or 15m pass; a later edit of a FAILED recording (tags, title, inbox, highlight, share) bumps `updated_at` and restarts its window; permanent failures count until the window passes. Failed transcriptions are re-queued automatically in a later M3 phase, at most twice per recording; after that the recording stays FAILED and ages out of the window. Missed: a reprocess-transcription failure on a recording that already has a summary (accepted). speakr load: 3 requests per minute against the 120/min read limit. Adapter: `where.equals` now compares JSON booleans and numbers as well as text (quote a scalar for text), and an item whose field is of another JSON type fails the poll instead of matching nothing.
- **Prometheus observer pulled forward (owner: M3).** The `prometheus` kind now has an observer supplying reachability: one instant query per resync, exactly one series, `1` up and `0` down; HTTP or Prometheus errors, a non-vector result, no series, several series or any other value are source errors, never "down". Params: `url`, `query`, `resource`, `timeout`, optional `auth`, `reachability_confirmations` (debounce, default 3, as for the kube watch). `basestar-probe` uses it, so basestar reachability no longer depends on pushes. The blackbox probe of basestar is not set up yet, so the source fails every read until it is; its query must match exactly one series (pin the blackbox module/job label). `-0` and `-1` are refused, `1.0`/`0.0` accepted.
- **immich reads the queue API (owner: M3; immich-ml scheduling stays M7).** `GET /api/jobs` is deprecated since immich v2.4.0; `immich-queues` reads `GET /api/queues/{smartSearch,faceDetection,ocr}` (alpha since v2.4.0; the deployment runs v4.0 or later; admin key with `queue.read`, sent as `x-api-key` from `IMMICH_API_KEY`), `statistics.waiting` + `statistics.delayed` as ready and `statistics.active` as in-flight, summed. A paused queue counts as zero (user decision): from v4.0 a paused queue reports its jobs under `waiting`, so each request carries the new generic `when: { field: /isPaused, equals: false }` response condition (a response that fails it contributes zero; a missing or non-boolean field fails the poll).
- **Stateful, durable core (owner: M3, done in P1).** The `Scheduler` object keeps the sequencer state, which sources have ever reported, and the manual model leases in its Restate state; each tick decides inside a journaled step and publishes the allowed set in process (a `watch` channel of `Option<PublishedAllowed>` for the KEDA scaler) only after that step returns. `observe` is ingress-private, which also settles the trusted-ingress item of M2 for forged pushes. Manual model leases have an ingress API (acquire, release, list).
- **Power control is per resource, `manual` by default (owner: M3; `managed` is M4, user decision).** `power.control: manual | managed`. Under `manual` the scheduler never emits `PowerOn`/`PowerOff`, never takes responsibility for a host's power, and the power phase follows observed reachability; a manual model lease on a host that is off waits (`HostNotUp`) for a person. Persisted transitional phases from managed control converge to the observed one. Motivation: with no power actuator, a released manual LLM lease led to an unexecuted `PowerOff` and the GPU stayed "shutting down" with nothing allowed. M4 switches hosts to `managed` together with the power actuators; the managed machine and its tests are kept for it.
- **Demand that can never be observed counts as no demand (owner: M3, user decision).** A class whose source has no observer in this build (its kind has none, or an observable kind is configured without params) and that has never reported (per the persisted reported-sources set) is not busy, for the planner and the sequencer, host holds included. `observe` is ingress-private, so such demand can never arrive; as unknown, the example's `interactive-llm` (`llm-sessions`, a session source) kept the LLM loaded forever. A source that has reported before and goes silent stays unknown (busy). Passed to the pure core as data (`Snapshot::unobservable`). Revisit when the schedule and session observers exist (M4/M6): their classes then have real demand.
- **Scope reset: queue-only switching; Kubernetes enforces GPU exclusivity; gate classes on host readiness (owner: M3, done in P6, user decision).**
  - *Kubernetes enforces GPU exclusivity.* Every GPU workload requests `nvidia.com/gpu: 1`, the device plugin exposes one GPU on cuda-central (no time-slicing), and the GPU node is tainted. This is now a deployment requirement (architecture invariant 2). flowmanager gates no decision on pod state: it decides from queues, leases and policy only, and publishes at most one model per exclusive resource. A switch withdraws the old model and allows the new one in the same tick; the new pod stays pending in Kubernetes until the old one has released the GPU. Pod counts remain observations (and metrics) for information only; a config no longer needs a pods source.
  - *State machine.* The mode of an exclusive resource is idle or serving (possibly winding down); `loading` and `draining` are gone, and with them the wait reasons `loading`, `draining` and `other_model_present`. A model is serving from the tick it is allowed, so `min_dwell` counts from that tick and includes load time. Wind-down keeps the queue-based rules (in-flight counts, `min_dwell`, `max_dwell`, `age_threshold`, forced manual leases and reclaims with their timeouts, per-class `kill_timeout`); they decide when to switch, and the wait for in-flight work is bounded by a timeout whether forced or not (see *Bounded wind-down* below). Allowing a model (alone or in a switch) needs a fresh "up" reading of its host (`stale_observation` now means reachability only); withdrawing needs nothing. `RunRecovery` runs in the tick the model is withdrawn. Power-off no longer looks at pods (it happens only with no model allowed).
  - *Persisted state.* `scheduler_state` keeps version 1: `loading` written by the previous build reads as serving since the same instant, `draining` as idle (any pending recovery then runs). Everything this build writes the previous build can read, so neither direction is an error tick.
  - *Removed models.* A model removed from the config while allowed leaves the allowed set at the next tick after reload, with no dwell, grace or wind-down; persisted state naming an unconfigured model does not block the resource (this replaces the stall described in the next item).
  - *Gate classes.* A requirement on an exclusive resource without a model is valid and means "that host is up". Such a class must have no demand source (refused at load otherwise) and never influences model choice. The example config adds `paperless-gpt` (cuda-central up and basestar-nfs up). Consumers (paperless-gpt, speakr, immich) are not gated for GPU contention: switching a model away mid-job is safe for all of them (paperless-gpt reprocesses an unfinished document, speakr marks the job failed and it is re-queued, immich falls back to its CPU replica).
  - *Reporting.* `PauseIntake` only removes a demand class from the allowed set, which no consumer scales on, so it is listed in `not_executed` (nothing stops the consumer taking new work, so in-flight counts may stay up). `KillInFlight` is listed too, with a note that it has no actuator: the work ends because the model is withdrawn.
  - *Bounded wind-down (P6 fix round).* Review found that an unforced wind-down had no timeout and, with `PauseIntake` stopping nobody, a continuously fed queue held the GPU for ever (reproduced in a 48h simulation and live: whisperx with a job always processing; real case: immich-ml's `statistics.active` during a library import). Decided: every wind-down has a timeout, counted from its start: the class's `kill_timeout`, else `reclaim_timeout` (reclaim) or `drain_timeout`. A cause change restarts the clock only when it flips between reclaim and not-reclaim; a manual lease coming or going keeps it. Preemptibility `none` with busy work still blocks the switch (`not_preemptible`) without a timeout, the only unbounded case. End-to-end bound, checked against the planner (`fifo.rs`, `target_for`): intent leaves a busy serving model at the first tick with served time > `max_dwell` (or once another queue's oldest ready item is older than `age_threshold`; never before `min_dwell`), only if some other model has runnable ready work and no busy class is `none` or unknown; the switch completes at most the timeout later, so a model allowed at `t0` is replaced by `t0 + max_dwell + drain_timeout` plus a tick period (2h10m in the example). Example config: paperless-ocr and paperless-tagging are `kill` (paperless-gpt reprocesses unfinished documents); speakr-summarization stays `pause_intake` and an interrupted summary is not re-queued automatically (the planned speakr recovery covers failed transcriptions only). Rolling the binary back needs the config rolled back too (the old build refuses gate classes and a config without a pods source). A tick in which a manual model lease pins a model lists the kind `pinned` and never `idle`.
- **A model removed from the config while loaded (owner: later M3 phase, user decision; done in P6, see the scope reset above).** GPU exclusivity is enforced by Kubernetes itself (`nvidia.com/gpu: 1`, no time-slicing), so a later phase removes pod-count gating from the core. The planned "retain a removed model's mapping until its pods are at zero" change was cancelled. Until then (checked in P1 fix round 1 with a throwaway multi-tick run: whisperx serving, its model, class and sources removed, OCR waiting): the removed model stays in the published allowed set until the planner releases it after `idle_grace` (10m), then it is drained and the sequencer waits forever (`stale_observation`) on a pod count the cache no longer accepts, so the waiting OCR work is never scheduled. Recovery today: put the model back, or discard `scheduler_state` once its pods are gone.
- **Persisted state is versioned (owner: M3, done in P1).** `scheduler_state` and `lease_book` are stored as `{"version": N, "data": ...}`. A document that cannot be decoded or has another version (e.g. after a rollback) is not reset and does not fail the invocation (no retry loop blocking the exclusive handlers): the tick reports an error (report, error log, `sched_persisted_state_errors_total{key}`) and publishes nothing; the lease handlers keep working where they can. Recovery: the `discard_state` handler, or the Restate CLI (README). Bump a document's version whenever its shape changes incompatibly.
- **PowerOn/PowerOff timeouts (owner: M4).** Moot under `manual` (no power step is emitted). When M4 switches hosts to `managed`, PoweringOn and ShuttingDown need timeouts and alerts (the managed machine waits for an observation indefinitely); `sched_resource_phase_age_seconds` already exposes the age to alert on.

### Carried backlog (not yet owned by a finished milestone)

- **N-5:** a second ~~pods or~~ reachability source that reported once and then died blocks allowing a model (and power-off) until refreshed or removed; add alerting/metrics. (M3) The pods half no longer applies since P6: pod readings block nothing.
- **N-6:** a flaky reachability reading can flip a found-running host On to Off; the observer-side debounce exists for the kube watch, but multi-source reachability debouncing is still open. (M4)
- **No timeouts** for ~~Loading,~~ PoweringOn, ShuttingDown; `sched_resource_phase_age_seconds` (M3 P1) exposes the age to alert on. (The Loading phase no longer exists since P6; a pod that stays pending is Kubernetes' to report.) A demand source that has reported once and then never again keeps a loaded model busy forever (one that never reported now counts as no demand). (M3/M4)
- **Coverage of ~~pods/~~reachability sources is per-config**, not per host; sources are not bound to subjects (needs a `subjects` field). (M3/M4) The pods half no longer applies since P6: no decision reads pods and a config needs no pods source.
- **Preemptibility is a single enum**; there is no per-class interactive marker and SharedHold holders are not linked to a class. (M6/M7)
- **tonic/prost** are needed for the M3 KEDA gRPC scaler: ask the user before adding them. (M3)
- **Schedule and session source kinds have no observer yet**; classes fed by them show `demand_unsupported` and, since M3 P1, count as no demand (see above). (Prometheus gained one in M3, see above.) (M4/M6)
- **"Reported once, then silent" memory** is persisted in Restate since M3 P1, so it survives restarts. (M3, done)
- **Metric rename:** `sched_decisions_total` is now `sched_tick_decisions_total`; update any dashboard.
- **Unverified against real systems:** the Kubernetes observer has only run against a fake API (TLS, RBAC, in-cluster config, long watches), the pod-to-model label mapping (`app.kubernetes.io/name`), and paperless-ngx response shapes. Speakr shapes were checked against its source, not a live instance. Check all of these in the first week of shadow running.
- **Pre-existing:** the legacy root `src/` crate still has clippy warnings and fmt diffs; the first M2 commit does not pass its own tests (fixed by the next commit; history intentionally not rewritten).
