# What is it
* Custom Rust based workflow manager that uses restate.dev to coordinate workflows

# Workflows
## Backups
* Ensure power is on for nfs
* trigger backup jobs across systems and wait for completion
  * postgres
  * actualbudget
* ensure successful backups have landed
* if nfs was powered on by flowmanager, then shutdown nfs

## GPU Coordinator
Depending on the service needed, will need to power on the GPU machine and have a cooldown period before powering off.

### Services
#### OCR
* Content extraction from scanned docs
* MinerU based with it's own models
* Once completed needs to feed into LLM for categorization and tagging

#### Transcription
* WhisperX based
* Requires specific model from hugging face
* Once completed needs to feed into LLM for summary generation and tagging
* Should also then feed links into Note tracking system
* Tasks and follow-ups should be extracted and created from here

#### General LLM
* ollama handles
* might support somethign like open-webui or openclaw (some problems with this)
* could have scheduled tasks that would need to run which does have some real risk associated with it

## Nightly Sanity
* Depending on queued jobs, if nothing is running power down the NFS and GPU boxes to save power
* Depending on system state transition to idle load (see power.py in homesvr)
# Scheduler service

`sched-service` observes the cluster and the queue sources, decides which model runs on the
GPU, and publishes the resulting **allowed set** (models and workload classes) for the KEDA
external scaler. It runs as the `Scheduler` Restate virtual object (key `main`); a tick is
snapshot, then planner, then sequencer, then publication and a log line with the decision,
its reason, the config generation and each input's age (per source and capability).

**GPU exclusivity is Kubernetes' job (deployment requirement).** Every GPU workload must
request `nvidia.com/gpu: 1`, the device plugin must expose exactly one GPU on cuda-central
(no time-slicing, no MIG), and the GPU node must be tainted so only those workloads land
there. The scheduler decides from queues, leases and policy only, never from pod state, and
publishes at most one model per exclusive resource. When it switches, the old model leaves
the allowed set and the new one enters it in the same tick; the new pod stays pending in
Kubernetes until the old one has released the GPU. Pod counts from the cluster watch are
kept as observations (`sched_observation_age_seconds{capability="pods"}`) for information
only. Host reachability still decides: no model is allowed on a host observed off, and
allowing one (alone or in a switch) needs a fresh "up" reading of its host.

How a model is chosen and kept:

- A model is **serving from the tick it is allowed**. `min_dwell` is counted from that tick,
  so it includes the time its pods take to load (and to wait for the previous model's pods).
- **When** to switch is decided from the queues and leases: a busy model yields only after
  `min_dwell`, only past `max_dwell` or when another queue is older than `age_threshold`,
  and only if none of its busy classes is unpreemptible; an idle one after `idle_grace`.
  Once the planner's intent has moved away, in-flight work (as its queue reports it) is
  awaited for at most `drain_timeout` (a class's own `kill_timeout` if set; `reclaim_timeout`
  on a reclaim), counted from the start of the wind-down, and then cut off. Forced or not,
  the bound is the same. Only the pod-based gating is gone.
- **The wait is bounded, except for a busy `none` class.** Nothing gates a consumer on its
  queue, so a queue that is fed without end shows in-flight work without end (immich-ml's
  `statistics.active` stays above zero throughout a large import; a whisperx job is always
  processing). Withdrawing the model is what stops that work (KEDA scales the workload to
  0), so no wind-down waits for it for ever. End to end, with `none` classes not busy and
  another model's queue holding runnable ready work: a model allowed at `t0` is no longer
  wanted by the planner at the first tick after `t0 + max_dwell` (earlier if that queue's
  oldest item is older than `age_threshold`, but never before `t0 + min_dwell`), and the
  switch is done at most `drain_timeout` (or the class's `kill_timeout`) after that; so the
  model is replaced by `t0 + max_dwell + drain_timeout` plus a tick period (2h10m with the
  example policy). An idle model is replaced once it has been idle for `idle_grace` and
  has served `min_dwell`, with no wait. The bound counts from the last wind-down that was not abandoned: if
  the planner's intent returns to the serving model the wind-down is dropped and a later
  one starts its clock again. The one unbounded wait is a class with preemptibility `none`
  that has work (an interactive session): the switch is not even started, with no timeout,
  and the wait reason is `not_preemptible`. (A demand source that has gone silent also
  holds a switch, as `stale_demand`, until it reports or is removed.)
- **A change of cause mid wind-down** (a manual lease or a reclaim arriving or ending)
  restarts the clock exactly when the cause flips between reclaim and not-reclaim, with the
  new cause's timeout counted from that tick (so a reclaim ending gives the work a fresh
  `drain_timeout`). A manual lease coming or going changes nothing: same timeout, same
  clock. Classes already cut off stay recorded.
- A **model removed from the config** while it is allowed leaves the allowed set at the
  next tick after the reload (no dwell, grace or wind-down). Persisted state naming a model
  the config no longer has does not block the resource.
- **Gate classes**: a requirement on an exclusive resource without a model means only
  "that host is up". A class with such a requirement must have no demand source and never
  influences which model is chosen. The example's `paperless-gpt` requires cuda-central
  and basestar-nfs up: paperless-gpt's deployment scales on it, so it runs only while both
  hosts are up. Consumers are not gated for GPU contention: switching a model away
  mid-job is safe for paperless-gpt (it reprocesses the document), speakr (the job is
  marked failed, and re-queued by the class's recovery) and immich (its CPU replica takes
  over).

What is carried out and what is not:

- **Allowing and withdrawing a model** take effect through the allowed set: KEDA converges
  replicas on it. The set is published in process on a `tokio::sync::watch` channel
  (`Shared::subscribe_allowed`, a `watch::Receiver<Option<PublishedAllowed>>`, `None` until
  the first tick of the process) and served to KEDA by the external scaler in the same
  binary (see *KEDA external scaler* below).
- **Power is manual.** Each powered resource has `power.control: manual | managed`
  (default `manual`; the example config says `manual` for both hosts). Under `manual` the
  scheduler never emits a power step and never takes responsibility for a host's power:
  its power phase is what is observed (a fresh "up" reads on, a fresh "down" off), and work
  that needs a host that is off waits (`wait ... the host is not up`) for a person to power
  it on, including a manual model lease. A powering-on or shutting-down phase left in the
  persisted state by managed control converges to the observed phase. `managed` (the
  scheduler powers hosts on and off) is for M4, with the power actuators.
- **Stopping in-flight work has no actuator** (nor do power steps under `managed`). The
  sequencer still takes these steps (its state moves on as if they happened), and each one
  is reported as not executed: listed in the tick's `not_executed`, marked
  `[NOT EXECUTED: ...]` in the reason, and logged as a warning. See Known blind spots.
- **Recovery actions are carried out**, by the class's `Recovery` object (see *Recovery
  actions* below); `RunRecovery` is no longer listed as not executed.
- **Pausing intake stops nobody.** A `pause_intake` step takes a demand class (say
  `paperless-ocr`) out of the allowed set when its model starts winding down, but no
  consumer scales on a demand class (consumers are gated on gate classes, by host
  readiness, if at all). So it is listed in `not_executed` as well, with its own note: the
  consumer keeps taking work and its queue's in-flight count may stay up. The wind-down does
  not wait for that: it ends at the timeout (see above).
- **Cutting off in-flight work is the withdrawal of the model.** `KillInFlight` has no
  actuator of its own; the work ends because the same tick withdraws its model and KEDA
  scales the workload to 0. The step is reported as not executed, with a note saying so.
  What re-runs the interrupted work is up to the consumer: paperless-gpt reprocesses
  unfinished documents (paperless-ocr, paperless-tagging are `kill` for that reason), immich
  leaves it pending, speakr marks a transcription failed (re-queued by its recovery), and
  an interrupted speakr summary is not re-queued automatically (speakr-summarization stays
  `pause_intake` to mark that, and has no recovery; the recovery covers failed
  transcriptions only).
- **Failed items are a third demand bucket.** Next to ready and in-flight, a source can
  count a request `counts_as: failed` (the example does, for speakr's failed
  transcriptions). Failed items are work waiting to be *re-queued*, not ready work:
  - they never age: they contribute no "oldest ready" time, so they cannot starve another
    model's queue or force a switch by `age_threshold`, however long they stay listed;
  - they make their class's model *wanted*, at the class's normal priority, so the model is
    brought in to re-queue them. A model wanted only for failed items ranks as if its work
    had arrived this instant: it loses ties on age to real work of its priority;
  - they never hold the model that is serving: for the serving model they count as nothing,
    so they neither keep it from going idle nor make it the pick when it is released. A
    model loaded only for failed items, whose items were re-queued (they then show as ready
    work through the source's other request, and as none failed), is released after the usual
    `min_dwell` and `idle_grace`, not longer. **Flapping while spent items stay listed.**
    If the items cannot be re-queued (the cap is used, or every request fails) and they stay
    listed, the model is released after `min_dwell` + `idle_grace` (about 11 minutes in the
    example), then the failed-only class wants it again at the next tick and it is allowed
    again: allowed about 11 minutes, one empty tick, allowed again, repeating about every
    12 minutes (seen in replay). It is bounded: it stops when the items leave the source's
    `since` window (15m after their last update), so at most about two cycles per failure in
    the example, and any edit of a failed recording starts the window again. To reduce it,
    shorten the failed request's `since.ago` (and the recovery's, which must match), or
    lower the class's `priority` so real work wins when it competes. Failed items do not
    power a host on and do not keep one on;
  - a window shorter than `idle_grace` plus load time means a model may come and go without
    ever serving a re-queue, which is harmless.
- **Demand that can never be observed counts as none.** A class whose demand source has no
  observer in this build (a `schedule` or `session` source, for example) or is an
  observable kind configured without `params`, and that has never reported, counts as
  having **no demand**: it does not keep a model loaded or a host held. Since `observe` is
  ingress-private, nothing outside Restate can push to such a source, so its demand could
  never arrive (before M3 P1 it read as unknown, and the example's `interactive-llm`, fed
  by the `llm-sessions` session source, kept the LLM loaded forever). Such classes are
  still listed apart in the tick (`unsupported_demand`, `unpolled_demand`) and its kinds
  (`demand_unsupported`, `demand_unpolled`). A source that has reported before (in any
  process; the set is persisted) and then goes silent stays unknown: busy, not idle.
- Every observer is read-only.

Run it locally against a Restate server:

```sh
restate-server &
PAPERLESS_API_TOKEN=... SPEAKR_API_TOKEN=... \
  cargo run -p sched-service -- --config config/example.yaml --backstop 30s
restate deployments register http://localhost:9080
```

- `--keda-listen` (or `SCHED_KEDA_LISTEN`, default `0.0.0.0:9095`) is the KEDA external
  scaler's gRPC address; see below.
- `--config` (or `SCHED_CONFIG`) is re-read every `--reload-interval`; an invalid file is
  logged and the last good config stays active. In the cluster this is a mounted ConfigMap.
- Tokens come from environment variables or files named in a source's `auth` (or a
  recovery action's `auth`), never from the config. `SPEAKR_API_TOKEN` is one speakr API
  token with the **read and process** scopes: `read` for the demand polls, `process` for the
  re-queue request (`POST /api/v1/recordings/{id}/transcribe`). The token's user must also
  be allowed to edit the recordings.
- Backstop: `arm_backstop` starts a self-scheduling `backstop` chain (delayed send to itself,
  every `--backstop`); the binary calls it through `--restate-ingress` until Restate accepts.
  Changed observations ask for an extra `tick` the same way, and so does every lease change.

## KEDA external scaler

The binary serves KEDA's external scaler protocol (`externalscaler.ExternalScaler`, the
proto vendored from KEDA v2.21.0 in `crates/sched-keda/proto`) on `--keda-listen` /
`SCHED_KEDA_LISTEN` (default `0.0.0.0:9095`), as plaintext gRPC: in the cluster Istio
provides mTLS, so the trigger leaves `enableTLS` off. It is a lookup over the allowed set
the latest tick published and nothing else; it runs on its own task, so a scaler call
never waits on a tick and a tick never waits on a scaler call.

**Metadata.** Each ScaledObject names exactly one subject in its trigger's `metadata`:

| Key | Active while |
| --- | --- |
| `model: <id>` | that model is in the allowed set (its pods may run: loading, serving or winding down). For a model's own Deployment. |
| `class: <id>` | that workload class is in the allowed set (every requirement met). For consumers gated on a class, e.g. a client that may run only while its host is ready. |

Which workload follows which model or class lives only in the ScaledObjects and the
scheduler config, never in code. KEDA passes its own trigger keys on too, so
`scalerAddress`, `enableTLS` and `unsafeSsl` are accepted and ignored. Anything else is
refused with gRPC `InvalidArgument` and a message naming the ScaledObject: no subject,
both, any other key, an empty value, or an id the **active** config does not have
(model and class ids are separate: a class id under `model` is unknown). Ids are checked
against the config as it is now, so a reload takes effect at once: an added id is
answered from the allowed set (inactive until a tick allows it); a removed id is refused,
even while it is still in the last published set.

**Answers.**

| RPC | Answer |
| --- | --- |
| `IsActive` | `true` while allowed. |
| `StreamIsActive` (`external-push`) | The current answer at once, then again each time this ScaledObject's answer changes (not when another's does, nor on a tick that leaves it as it was). The stream ends with the error if the answer turns into one (the id left the config, or the scheduler is stopping), and the server stops watching when KEDA goes away. |
| `GetMetricSpec` | One metric, `flowmanager-model-<id>` or `flowmanager-class-<id>`, target `1`. |
| `GetMetrics` | That metric: `1` while allowed, else `0` (both the deprecated integer and the float field). Another metric name is `InvalidArgument`. |
| `StreamMetricSpec` | `Unimplemented` (the spec never changes; KEDA falls back to `GetMetricSpec`). |

**Not ready.** Until this process has published its first tick (`None` on the channel),
every call answers `Unavailable`, never "inactive"; so does every call once the
scheduler has dropped its sender (the process is stopping), since that answer could no
longer change. After a restart the scaler is not ready again until the first tick (the
backstop, armed at startup, ticks within seconds), which answers from the decision made
on the persisted state. KEDA v2.21 leaves the replica count as it is when the scaler
errors and the ScaledObject has no `fallback`: it neither scales from zero nor to zero.
An "inactive" answer instead would scale everything down whenever the scheduler
restarts. For the same reason a ScaledObject whose metadata is broken, or whose id was
removed from the config, keeps its current replica count (a removed model that was
running keeps running) until it is fixed or deleted; KEDA reports the scaler error on
the ScaledObject.

**ScaledObjects.** Replicas are exactly 0 or 1, so `minReplicaCount: 0` and
`maxReplicaCount: 1`. Keep `pollingInterval` and `cooldownPeriod` short: hysteresis is
the scheduler's (dwell, idle grace), and a long cooldown only delays a drain the
sequencer is waiting for. Do **not** set `fallback` (on errors it would set a replica
count the scheduler never decided), and make the scaler the **only** trigger (KEDA
activates on any trigger, so another one could start a model that is not allowed).
Remove any `autoscaling.keda.sh/paused-replicas` annotation left by the retired legacy
gating (see `kubernetes-requirements.md`).

```yaml
# A model's Deployment, polled.
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: mineru
  namespace: paperless
spec:
  scaleTargetRef:
    name: mineru
  minReplicaCount: 0
  maxReplicaCount: 1
  pollingInterval: 5      # seconds
  cooldownPeriod: 15      # seconds from the last active answer to scaling to zero
  # No fallback: when the scaler errors (not ready), KEDA keeps the replica count.
  triggers:
    - type: external
      metadata:
        scalerAddress: flowmanager.flowmanager.svc.cluster.local:9095
        model: mineru
---
# A consumer gated on a class, pushed: StreamIsActive tells KEDA as soon as it changes.
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: paperless-gpt
  namespace: paperless
spec:
  scaleTargetRef:
    name: paperless-gpt
  minReplicaCount: 0
  maxReplicaCount: 1
  pollingInterval: 5
  cooldownPeriod: 15
  triggers:
    - type: external-push
      metadata:
        scalerAddress: flowmanager.flowmanager.svc.cluster.local:9095
        class: paperless-gpt-host   # a class id from the scheduler config
```

To query it by hand, `cargo run -p sched-keda --example scaler_probe -- <addr> model=mineru`
calls `IsActive`, `GetMetricSpec` and `GetMetrics` once (`--stream` follows
`StreamIsActive`); see `crates/sched-keda/examples/scaler_probe.rs`.

## How state persists

The object keeps its state in Restate, under these keys:

| Key | Holds |
| --- | --- |
| `scheduler_state` | Versioned document: the sequencer state (per resource the power phase: off, powering on, on, shutting down; and mode phase: idle, or serving with its wind-down; with when each began) and which configured sources have ever reported values. |
| `lease_book` | Versioned document: the leases held and the counter lease ids are drawn from. |
| `backstop_epoch` | The current backstop chain (a number). |

The `Recovery` virtual object (one instance per class id) keeps its own state:

| Key | Holds |
| --- | --- |
| `recovery_attempts` | Versioned document (`{"version":1,"data":{"last_run_at":..., "attempts":{"<id>":{"count":N,"last_at":...}}}}`): re-queue attempts per item id and when the action last ran. Read by `Recovery/<class>/attempts`; an unreadable one is discarded with `discard_attempts` (`{"force":true}` for a readable one, which gives every item fresh attempts). |

A tick loads `scheduler_state` and `lease_book`, decides inside a journaled step
(`ctx.run`, named `decide`), stores the new `scheduler_state`, and only then publishes the
allowed set in process. A retried or replayed invocation gets the journaled decision back
instead of deciding again, so what is published and what is stored agree; a replayed tick
is published and logged again (`replayed=true`) but not counted again in the metrics. A
tick whose core fails (no decision) keeps the previous state and the previous publication.
The full tick report is journaled once (as the step's result): `last_decision` returns the
latest report this process published, kept in memory, so it is `null` after a restart
until the first tick (the backstop ticks within seconds).

### Versions, and recovering the persisted state

Each document is stored as `{"version": N, "data": ...}`; this build reads and writes
version 1 of both. The queue-only change kept version 1 of `scheduler_state`: the
`loading` and `draining` mode phases the build before it wrote are still read (`loading`
as serving since the same instant, `draining` as idle), and nothing this build writes is
new to that build. A document that cannot be decoded, or that has another version (written
by a newer build, say, before a rollback), is not reset and does not fail the invocation
(Restate would retry it forever and block every exclusive handler, the lease handlers
included). Instead:

- the tick reports an error (kind `error`, the key and the problem in `error` and the
  reason), logs it at error level, counts it in `sched_persisted_state_errors_total{key}`,
  stores nothing and **publishes nothing**, so the scaler keeps what it had;
- the lease handlers keep working if only `scheduler_state` is unreadable; if `lease_book`
  is, they (and `leases`) answer `503` naming this section.

**Rolling the binary back needs the config rolled back too.** The build before queue-only
switching rejects the shipped config: a gate class (a requirement on an exclusive resource
without a model) fails validation with "exclusive; a model is required", and a config
without a pods source is refused as well. Deploy the previous binary with the previous
config (the persisted state itself is readable by both builds).

To recover, either deploy the build that wrote the document, or discard it. Discarding
`scheduler_state` restarts the sequencer from observation: no model is allowed until the
planner picks one again (any running pod of a model it does not pick is scaled down by
KEDA once the scaler publishes), dwell and idle clocks start over, and the memory of
which sources have ever reported is dropped too, so a source that went silent before reads
as never reported (no demand, for a source without an observer) instead of
`demand_unknown`. Discarding `lease_book` drops every lease. The handler returns what was
stored, so keep a copy. `discard_state` only discards a document that cannot be read; one
that reads fine is refused with `409` unless the request also says `"force": true`:

```sh
curl -X POST localhost:8080/Scheduler/main/discard_state \
  -H 'content-type: application/json' -d '{"key": "scheduler_state"}'
# A readable document needs "force": true:
curl -X POST localhost:8080/Scheduler/main/discard_state \
  -H 'content-type: application/json' -d '{"key": "scheduler_state", "force": true}'
```

With the Restate CLI (1.8): `restate state get Scheduler main` shows every key;
`restate state clear Scheduler/main` deletes all of the object's state (both documents and
the backstop epoch; re-arm the backstop by restarting the binary); one key alone is removed
with `restate state patch` and a JSON Patch `remove` operation (see `restate state patch
--help`).

When a later change alters a document's shape in a way this build cannot read, bump its
version (`persist::STATE_VERSION`, `persist::LEASES_VERSION`).

The observation cache is not persisted: it lives in the process. After a restart the
scheduler continues from the persisted state (a serving model stays allowed and keeps its
dwell clock, a wind-down goes on) and holds every safety-critical step (allowing a model,
powering off) until the observers have reported fresh reachability again. Which sources
had reported before is seeded from `scheduler_state`, so a source that went silent before
the restart still reads as `demand_unknown` (busy), not as never reported (no demand).

Gate classes follow the same rule. The persisted "on" power phase keeps the serving model
**and every gate class** (such as paperless-gpt) allowed for the seconds between the start
and the first fresh reachability reading: nothing re-checks the hosts before then, so a
host that went down meanwhile still has its gate open until the first reading says
otherwise (a reading of "down" closes it at that tick). The same holds for the serving
model's classes. Only allowing something new waits for a fresh "up" reading.

## Recovery actions

A class may declare `recovery: { action, source, params }`. The only action is
`requeue_failed`, which re-queues the class's failed items through the service's API. It is
carried out by the `Recovery` Restate object (key: the class id), not by the `Scheduler`.

**When.** The sequencer emits `RunRecovery` for a class that has a recovery, while the class
is in the published allowed set (its model serving and not winding down in the sequencer's
state, every host it needs on in that state, no reclaim) and the snapshot reports failed items
for it (fresh demand, `failed > 0`). That is the existing allowed-set rule, not a fresh
host check: a serving model stays allowed on a stale reachability reading until a reading
says the host is down, and recovery follows it. It is emitted on
every such tick; the action is idempotent and rate-limited by itself, so that is cheap.
It never runs for a class that is not allowed: the action re-checks the allowed set the tick
published before listing and again **before every request**, and stops when the class has
left it. After in-flight work is cut off the model is withdrawn, so an action that needs the
model is not run then (it runs when the model is back and items are still failed). Failed
items are counted by the `failed` demand bucket (see above); `speakr-summarization` has no
recovery: a failed summary is not re-queued.

**Where it runs, and why.** A `Scheduler` handler holds the object's exclusive lock, so HTTP
requests there would queue every tick and lease request behind a slow service. The `tick`
handler therefore only sends the class's `Recovery` object a one-way request, after
publishing the allowed set; the object has its own lock and state, so a hung or failing
action cannot delay a tick (checked live: ticks kept their cadence while the mock service
was stopped). Every request is bounded by the action's `timeout`; a run is at most one list
request plus `max_per_run` re-queue requests, with `min_interval` between runs (the example:
at most 6 requests a minute against speakr's 120).

**A run.** (1) a journaled gate: the class has an action, is allowed, `min_interval` has
passed (otherwise it stops there, one cheap step); (2) one journaled list request, filtered
by the configured `where` and `since`; (3) per item with attempts left, at most
`max_per_run`: re-check allowed, **record the attempt and make it durable, then** send the
re-queue request, each a journaled step, so a replay cannot send it again. A crash between
recording and sending wastes an attempt and cannot allow a third; a crash after the request
was sent but before its result was journaled makes Restate's retry send it once more for
the same recorded attempt (steps are at least once), the one case with more requests than
recorded attempts.

**The cap.** `max_attempts` per item id (default 2), counted in `recovery_attempts`. A
request counts as an attempt whether or not it succeeded (the attempt is recorded first).
An item that used its attempts is skipped (`skipped_cap`, counted per item and run) and stays
failed until it leaves the list's `since` window. Records are kept for `retention` (default
24h, at least `window`) after the latest attempt **whether or not the item is listed**, and
only then forgotten, which also bounds the stored document. So a recording that used both
attempts, left the list after 15m and is listed again by a later edit gets no fresh attempts.
The deliberate limit: a recording edited more than `retention` after its last attempt may get
a fresh set of two. Two classes with recoveries each keep their own book, so a recording id
shared by two classes is counted per class (nothing to fix; the ids are the service's).

**Params** (`recovery.params`; validated at config load, an unknown action is refused):

| Param | Meaning |
| --- | --- |
| `list.url`, `list.items`, `list.id` | The list request, the JSON pointer to the items array, the pointer (inside an item) to its id (a string or an integer made of letters, digits and `-_.`; anything else is skipped). |
| `list.where`, `list.since`, `list.max_items` | As for an `http_json` request. A full page (`max_items`) is normal here: up to `max_per_run` items are re-queued per run, it is warned about, and the rest follows on later runs. |
| `requeue.method`, `requeue.url` | `POST` (default), `PUT` or `PATCH`; the URL with `{id}` once. |
| `requeue.expect` | Conditions on the JSON answer (`{field, equals}`), all of which must hold for the request to count as queued. A non-2xx answer never does. |
| `auth` | As for a source: an env var or file, never the secret. |
| `max_attempts` / `window` / `retention` / `min_interval` / `max_per_run` / `timeout` | 2 / 15m / 24h / 60s / 5 / 10s by default. `window` is the list's `since` window, for reference; `retention` must be at least it. URLs must carry no userinfo (`user:pw@`); use `auth`. |

Failures (network, 4xx, 5xx, an answer that is not JSON or does not match `expect`) are
logged, counted and never panic; the item's attempt stays used and the next run retries it
while attempts are left. A list that fails sends nothing and leaves the attempts alone.
Metrics: `sched_recovery_runs_total{class,outcome}` (`ran`, `skipped_interval`,
`skipped_not_allowed`, `not_wired`, `list_failed`, `state_error`) and
`sched_recovery_requeues_total{class,outcome}` (`attempted`, `queued`, `failed`,
`skipped_cap`, `skipped_not_allowed`, `deferred`).

## Handlers

All are called as `POST {ingress}/Scheduler/main/{handler}`. The `Recovery` object's
handlers are `Recovery/{class}/run` (**ingress-private**, like `observe`: only the `tick`
can send it), `.../attempts` (the stored document) and `.../discard_attempts`. The last two
are public on the ingress like `Scheduler.discard_state` and the lease handlers: anyone who
can reach the Restate ingress can read the attempts or discard them (`discard_attempts`
with `"force": true` gives every item fresh attempts), so keep the ingress on a trusted
network (see *Trust boundary*). Metrics label only configured classes (an invented key is
counted under `unknown`).

| Handler | Ingress | What it does |
| --- | --- | --- |
| `tick` | yes | One decision; answers `{config_generation, at, kinds, error}` (the report is in `last_decision`). |
| `arm_backstop` | yes | Starts a new backstop chain (the binary calls it). |
| `backstop` | yes | One link of the chain; not meant to be called by hand. |
| `last_decision` | yes (shared) | The latest tick report this process published (`null` until its first tick), without waiting behind a tick. |
| `acquire_model_lease` | yes | Acquires a manual model lease. |
| `release_lease` | yes | Releases a lease by id. |
| `leases` | yes (shared) | Every lease held. |
| `discard_state` | yes | Recovery: discards an unreadable persisted document (see above). |
| `observe` | **no** (ingress-private) | Pushed observations, from other Restate handlers only. |

`observe` is registered as private: the ingress answers
`400 {"code":400,"message":"the invoked service is not public","source":"ingress"}`. The
in-process observers do not use it (they write to the cache directly), so no pusher outside
Restate can forge a reading (a host "up", or queue demand) through the ingress any more.

### Manual model leases

A manual model lease hard-pins a model on its resource until it is released: the planner
targets that model, batch work on the resource is cut short (its classes leave the
allowed set at once, in-flight work is given `drain_timeout`, or a class's own
`kill_timeout`, then stopped, and the leased model is allowed in that same tick), and the
model stays allowed however much other work waits. While it is pinned, ticks list the kind
`pinned`, not `idle`. Rules:

- The model must exist in the active config; the lease is on the model's resource.
- At most one manual model lease per exclusive resource. Asking again for the leased model
  returns the existing lease (`"created": false`); asking for another model of the same
  resource is refused with `409` until the first lease is released. An unknown model is
  refused with `400`, an unknown lease id on release with `404`.
- Lease ids are `manual-model-<n>`, from a counter in the object's state.
- A lease whose model or resource later disappears from the config is kept but ignored
  (listed in the tick's `ignored_leases` and logged).
- Other lease kinds (interactive, reclaim, manual host power, shared holds) have no API yet.
- A manual model lease also asks for its host to be up. Under `power.control: manual` this
  only means it waits until a person powers the host on; under `managed` (M4) the scheduler
  powers the host on and later takes responsibility for powering it down.

```sh
# Pin the LLM.
curl -X POST localhost:8080/Scheduler/main/acquire_model_lease \
  -H 'content-type: application/json' -d '{"model": "llm"}'
# {"lease":{"id":"manual-model-1","resource":"cuda-central","kind":{"type":"manual_model","model":"llm"},"granted_at":1792812767461},"created":true}

# List the leases.
curl -X POST localhost:8080/Scheduler/main/leases

# Release it.
curl -X POST localhost:8080/Scheduler/main/release_lease \
  -H 'content-type: application/json' -d '{"id": "manual-model-1"}'

# The latest decision, with the sequencer state and the leases it saw.
curl -X POST localhost:8080/Scheduler/main/last_decision
```

## Metrics

`/metrics` on `--metrics-listen`, the complete set (checked against a scrape and the
registrations in `metrics.rs`; labelled counters appear after their first event):

- `sched_tick_decisions_total{kind}`: ticks by decision kind (a replayed tick is not counted again).
- `sched_persisted_state_errors_total{key}`: ticks and lease requests that could not read a
  persisted document (`scheduler_state`, `lease_book`); alert on any increase.
- `sched_resource_phase_age_seconds{resource,axis,phase}`: how long each resource has been
  in its current phase at the last tick, one series per axis: `axis="power"` with `phase`
  `off`, `powering_on`, `on` or `shutting_down`, and `axis="mode"` with `phase` `idle`,
  `serving` (measured from the tick the model was allowed) or `winding_down` (a serving
  model whose intake is paused). `loading` and `draining` no longer exist (no phase waits on
  pods); update alerts that name them. When the phase changes, the old phase's series is
  removed. Ages come from the persisted state, so they survive restarts; `off` and `idle`
  have no recorded start and are measured from when this process first saw them. Alert on
  stuck transitions, for example
  `sched_resource_phase_age_seconds{phase=~"winding_down|powering_on|shutting_down"} > 900`.
  A pod that stays pending is not seen here: that is Kubernetes' to report.
- `sched_observation_age_seconds{source,capability}`: infinite if that capability never reported.
- `sched_source_has_observer{source}`: 1 if an observer runs for the source, else 0.
- `sched_source_errors_total{source}`: failed reads reported by a source.
- `sched_rejected_reports_total{reason}`: reports (`unknown_source`) or values
  (`unknown_subject`) dropped because the config has no place for them.
- `sched_recovery_runs_total{class,outcome}` and `sched_recovery_requeues_total{class,outcome}`:
  runs and items of a class's recovery action (see *Recovery actions* for the outcomes).
- `sched_tick_duration_seconds` (histogram), `sched_last_tick_timestamp_seconds`,
  `sched_tick_trigger_failures_total`.
- `sched_config_generation`, `sched_config_reloads_total{outcome}`.

Notes:

- **Trust boundary**: the Restate ingress (and the service port, which Restate calls) is
  assumed to be reachable only from a trusted network. The public handlers (`tick`, the
  lease handlers, ...) have no authentication of their own; do not expose the ingress
  beyond the cluster. `observe` is ingress-private, and bounded all the same: unknown
  sources and values with no place in the config are dropped and counted
  (`sched_rejected_reports_total`).
- **Decision kinds**: `sched_tick_decisions_total{kind}` counts ticks (once per distinct
  kind in a tick), not actions; ticks run on every change and on the backstop. Kinds
  include the step kinds (`allow_model`, `drain_model`, `pause_intake`, `power_on`,
  `power_off`, `kill_in_flight`, `run_recovery`, `wait`), `idle`, `error`, `pinned` (a
  manual model lease pins a model; such a tick is never `idle`, even with no step), and
  `demand_unknown` (something should be reporting the demand, an observer runs or the source
  has reported before, and it is silent or stale), `demand_unsupported` (the source kind has
  no observer in this build, e.g. schedule and session sources, and nothing has ever pushed
  to it) and `demand_unpolled` (an observable kind configured without `params`: nothing polls
  it and nothing has pushed to it). `sched_source_has_observer{source}` is 0 for the last two.
  `demand_unknown` means "not idle"; `demand_unsupported` and `demand_unpolled` count as no
  demand (see above) and can appear together with `idle`. A source that has reported once
  and then goes quiet is always `demand_unknown`, also after a restart. The counter was called
  `sched_decisions_total` in the first cut; dashboards on the old name need updating.
- **Bounding `http_json` by age**: a request may carry `since: { param, ago }`; each poll
  appends `param=<now - ago>` (ISO 8601 UTC, from the scheduler's clock). The example config
  uses it for speakr's failed recordings (a 15m window; see Known blind spots).
- **`http_json` conditions**: `where: { field, contains | equals }` keeps the items of a
  `length` array whose field matches; `when: { field, equals }` counts a whole response (its
  own and its `also` numbers) only if a field of it has that value, else it contributes zero.
  `equals` keeps the YAML scalar's type: `equals: false` / `true` is a JSON boolean and
  `equals: 3` a JSON number (compared numerically); quote it (`"true"`, `"3"`) to compare text.
  **Behaviour change (M3):** an item whose `where` field holds a non-null value of another
  JSON type, or a response whose `when` field is missing or of another type, now fails the
  whole source (its demand becomes unknown) instead of silently counting low. Before M3 a
  number after `equals:` was compared as text.
- A `429` response is recorded in the source error text with its `Retry-After`; the polling
  cadence is not changed.
- **Reachability sources for hosts outside the cluster**: `tcp_probe` (params `host`,
  `port`, `resource`, `timeout` default 2s within 100ms..60s and not above `resync`,
  `reachability_confirmations` default 3) connects to `host:port` each resync; connected is
  up, refused, timed out or "no route to host" is down, anything else is a source error. `basestar-probe` uses
  it. `prometheus` (params `url`, `query`, `resource`, `timeout`, optional `auth`,
  `reachability_confirmations`) stays available for hosts a Prometheus probe covers: one
  instant query per resync that must return exactly one series, `1` up and `0` down, every
  other answer a source error. No source in the example config uses it (no blackbox exporter
  probes basestar); the example shows it as a commented source. Both may declare only
  `reachability`.
- **Unknown is not idle**: if a class's demand source should be reporting (an observer runs
  for it, or it has reported before) and is silent or stale, the tick is `demand_unknown`
  (listed in `unknown_demand`), never `idle`.

## Known blind spots

Read these before trusting a "no demand" or "idle" line, or a phase.

- **Kill steps are not executed** (nor power steps, which only appear under
  `power.control: managed`). Recovery is: see *Recovery actions*. The sequencer records
  them as taken:
  - `KillInFlight` marks the class as stopped and stops waiting for it (at `drain_timeout`
    or the class's `kill_timeout` after the wind-down started, forced or not); the model is
    withdrawn (and the next one allowed) in the same tick. The in-flight work actually ends
    when KEDA scales the old model's pods down; the next model's pod starts once they are
    gone (Kubernetes).
  - `RunRecovery` (carried out) is emitted while the class is allowed and items are
    failed, not when the model is withdrawn. A recording that fails again after its
    attempts are used stays FAILED (re-queue it by hand) until it leaves the window. A
    crash between sending a re-queue request and journaling its result can repeat that one
    request.
  - `PauseIntake` stops no consumer (see above).
  - Under `managed` without an actuator (not a supported setup before M4): `PowerOn` moves
    the host to "powering on" until it is seen up; `PowerOff` moves it to "shutting down",
    and a host that stays up is never read back as on, so nothing is allowed on it until it
    is seen down. Under `manual` neither step is ever emitted.
- **Removing a model from the config cuts its work off at once.** A model removed while
  it is allowed leaves the allowed set at the next tick after the reload, with no dwell,
  grace or wind-down (its classes are gone with it, so there is nothing left to wait for),
  and the next model is allowed in the same tick. KEDA then scales the removed model's
  pods down mid-job; remove a model while it is idle if that matters.
- **The scheduler does not see GPU contention.** It relies on Kubernetes for exclusivity
  (see the top of this section). If a GPU workload is deployed without
  `nvidia.com/gpu: 1`, or the node exposes more than one GPU, two models can run at once
  and nothing here notices.
- **Demand that cannot be observed is treated as none.** A `schedule` or `session` source
  (backup, interactive LLM) that never reported counts as no demand, so the scheduler will
  not keep the LLM loaded for interactive use or NFS up for a backup it cannot see.
- **Failed speakr transcriptions are `failed` demand for 15 minutes.** `speakr-pending` lists
  FAILED recordings updated in the last 15m and counts those with `has_summary: false` in
  the `failed` bucket of `speakr-transcription` (speakr stores a failed summary's error in
  `summary`, every other failure's in `transcription`), so a whisperX outage brings whisperX
  in, to re-queue them. They are not ready work and never age: a failure cannot reach
  starvation priority whatever its age or the window (the earlier claim that the 15m window
  being under `age_threshold` guaranteed that is replaced by this rule). A failure counts until it is re-queued
  or 15m pass; any later edit of a FAILED recording (tags,
  title, inbox, highlight, share) bumps its `updated_at` and restarts the window; permanent
  failures (bad file, codec) count too until the window passes; a failure older than 15m is
  not seen at all. Automatic re-queueing is capped at 2 per recording (see *Recovery
  actions*), after which the recording stays FAILED and ages out. A reprocess-transcription failure on
  a recording that already has a summary is missed; a failed merge or stitch job counts as
  transcription. A full page (100) of failed recordings counts as 100 ("at least"): the
  poll does not fail, so a mass failure keeps demand fresh and recovery running. A full page
  for a `ready` or `in_flight` request still fails the poll rather than under-counting.
- **The failed list is written twice (source and recovery) and the two can drift.** If the
  source counts failures the recovery would not re-queue (a wider `where` or window), the
  model is brought in for nothing, and flaps as above. If the recovery's list is wider than
  the source's, it re-queues the extra recordings, but only while the source counts
  something. Keep the two identical in URL, filter and window. A duplicate re-queue request
  after a crash between sending and its journal entry stays possible (see *Recovery
  actions*).
- **Queued speakr summary, merge and stitch jobs count as transcription demand.** The speakr
  `/stats` queue counts jobs of every type, so this is a deliberate, conservative overcount.
- **speakr `/stats` is scoped to the token's user.** Recordings of other users are not seen.
- **basestar's reachability is one TCP connect, the check power.py makes.**
  `basestar-probe` (kind `tcp_probe`) connects to `basestar:2049` (NFS) once per resync and
  closes the connection at once without sending anything. One probe takes at most its 2s
  timeout, name lookup and connect together. Connected is up. The host not answering or
  answering no is down: refused, no answer within the timeout, or "no route to host"
  (`EHOSTUNREACH`: Linux reports it, after about 3s or sooner, for an address on the local
  network that does not answer ARP, which is how a powered-off basestar looks). A name that
  does not resolve (or whose lookup does not finish in time), "network unreachable" (no
  route at all), an unusable address or any other local error fails the read and the
  reading goes stale (decisions needing NFS wait) rather than reading "down". Up/down flips
  only after 3 agreeing readings; a failed read is not a reading. What it misses:
  - It shows that something accepts connections on port 2049, not that the NFS export
    works: a hung `nfsd` that still completes the TCP handshake, or a stale or full export,
    reads as up.
  - Another host taking basestar's address, or anything else listening on that port there,
    reads as up.
  - A firewall in the way that drops the connection, or rejects it with a TCP reset, an
    ICMP port-unreachable or an ICMP host- or admin-prohibited ("no route to host"), reads
    as down even with basestar running.
  - The name is resolved where the scheduler runs (its pod, through cluster DNS), not where
    power.py runs. If cluster DNS does not resolve the bare name `basestar`, every read
    fails and basestar's reachability stays stale; set `host` to an IP address or a name
    the pod can resolve. Not checked against the cluster.
  - If the name `basestar` stops resolving while the host is off (a name served only by the
    host itself or by DHCP leases, for example), the probe fails instead of reading down,
    and basestar's reachability goes stale rather than "down".
  - A slow name lookup leaves the connect less of the timeout, so a lookup that takes
    nearly all of it can make a running host read as down (no answer in the time left).
  - Several addresses for the name are probed at once: any connection is up; down needs
    every address to answer down; if none connects and any fails another way, the read
    fails. A name that also has an address the pod cannot route to at all (an IPv6
    address without IPv6 routing, for example) therefore never reads down; use an IP
    address or a name with only reachable addresses.
- **immich demand is the three ML queues only, through an alpha API.** `immich-queues` reads
  `GET /api/queues/{smartSearch,faceDetection,ocr}` (added in v2.4.0 and marked alpha there,
  `.alpha('v2.4.0')`; this deployment runs v4.0 or later) with an admin API key (`queue.read`,
  `IMMICH_API_KEY`). An immich older than v2.4.0 answers 404 and a changed response shape
  fails the poll. A paused queue (`isPaused: true`) counts as zero, ready and in-flight; from
  v4.0 a paused queue reports its jobs under `waiting`, so without that check they would count.
- **The Restate ingress is trusted-network only.** The public handlers have no authentication.
