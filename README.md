# What is it
* Custom Rust based workflow manager that uses restate.dev to coordinate workflows

# Workflows
## Backups
* Ensure power is on for nfs (the scheduled job `backup`, see *Scheduled jobs*)
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
Each tick also logs one `event="queue"` line per class that has a demand source, with
`class`, `ready`, `in_flight`, `failed`, `oldest_ready_age_ms` and `known` (false when the
reading is missing or stale; the counts are then zero, not measured). A replayed tick does
not log them again, so a graph does not count a tick twice.
To deploy it on Kubernetes (image, flags, Restate, Istio, RBAC, KEDA, secrets, operations),
see `docs/kubernetes_setup.md`.

**GPU exclusivity is Kubernetes' job (deployment requirement).** Every GPU workload must
request `nvidia.com/gpu: 1`, the device plugin must expose exactly one GPU on cuda-central
(no time-slicing, no MIG), and the GPU node must be tainted so only those workloads land
there. The scheduler decides from queues, leases and policy only, never from pod state (the
one exception is a veto: a managed NFS host is not powered off while a pod may mount it, see
*Power actions*), and
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
- **When the bound does not hold.** The bound above assumes the planner's intent stays
  away from the serving model for the whole wind-down. If the intent flickers back to it,
  the wind-down is dropped (the serving mode returns to "no wind-down") and a later one
  starts its clock again, so the switch can be postponed without limit. Two ways to flicker:
  the waiting queue reads `ready = 0` for one tick every few minutes, or the serving
  class's demand goes stale for one tick every few minutes. Reproduced in simulation (a
  one-tick blip every 9 minutes never switched in 6 hours, with a 10 minute
  `drain_timeout`; `crates/sched-service/tests/blips.rs` pins the idleness side of it). Two
  more limits of the same bound: a class's `kill_timeout` that is shorter than another
  blocking class's timeout does not end the wind-down early, because each class is cut off
  at its own timeout but the model is withdrawn only when no class of it is still blocking,
  so the effective bound is the **maximum** over the blocking classes' timeouts; and a
  reclaim that starts and ends in the middle of a wind-down restarts the clock twice (a
  10 minute timeout took 16 minutes in a test). Watch `sched_resource_phase_age_seconds`
  for `winding_down` (section *Metrics*) when a switch seems stuck.
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
  persisted state by managed control (including a failed one, see *Power actions*) converges to the observed phase. `managed` (the
  scheduler powers hosts on and off) is described under *Power actions* below. Under
  `managed`, a shared host found running is adopted: held by the user through a manual host
  lease until they switch it off (see *Host leases, reclaim and resume*).
- **Stopping in-flight work has no actuator.** The
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
  actuator of its own; the work ends because the model is withdrawn and KEDA scales the
  workload to 0. Each class is cut off at its own timeout, but the model is withdrawn only
  when no class of it is still blocking: in the same tick if the cut-off class was the last
  one, otherwise the model stays allowed and the other classes' work keeps running until
  the last of them is cut off or finishes. It also stays allowed if the switch itself is
  waiting (a stale host reading, say). The step is reported as not executed, with a note
  saying so, and the reason says which of the three cases it is (withdrawn this tick, other
  classes still blocking, or the switch waiting, naming the wait reason).
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
  observer in this build (a `session` source, for example) or is an
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

- `--toggle-listen` and `--toggle-token-file` (off unless given) start the Home Assistant toggle
  listener; see *Toggle listener*.
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
(model and class ids are separate: a class id under `model` is unknown). Model and class ids are validated at config load by a deliberately conservative rule that keeps them usable in resource-style names (the scaler builds metric names from them, but Kubernetes does not require this rule): lowercase letters, digits and `-`, starting and ending with a letter or digit, at most 50 characters. Ids are checked
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
        class: paperless-gpt   # a class id from the scheduler config (config/example.yaml)
```

To query it by hand, `cargo run -p sched-keda --example scaler_probe -- <addr> model=mineru`
calls `IsActive`, `GetMetricSpec` and `GetMetrics` once (`--stream` follows
`StreamIsActive`); see `crates/sched-keda/examples/scaler_probe.rs`.

## How state persists

The object keeps its state in Restate, under these keys:

| Key | Holds |
| --- | --- |
| `scheduler_state` | Versioned document: the sequencer state (per resource the power phase: off, powering on, on, shutting down, failed (a power transition timed out); and mode phase: idle, or serving with its wind-down; with when each began) and which configured sources have ever reported values. |
| `lease_book` | Versioned document: the leases held and the counter lease ids are drawn from. |
| `backstop_epoch` | The current backstop chain (a number). |
| `job_book` | Versioned document: per scheduled job the next scheduled time, the run in progress (phase, attempt), a Job that outran its deadline or whose creation failed and may still be active (the lingering mark, kept for a removed job too), the namespace its Job was made in, and the last outcome and success. Unreadable: only the job step is switched off (an error log `job_book_unreadable`, `sched_persisted_state_errors_total{key="job_book"}`); `discard_state` with `"key": "job_book"` clears it. |

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
config (the persisted state is readable by both builds, except while it holds what only
this build writes: a `failed` power phase, a `shutdown_confirmed_at` stamp (only for a resource with a `min_off`, see *A minimum off time*), or a `mounts_veto` entry, which is present
whenever a shutdown is waiting on pods that still mount NFS or has stopped. Until those
clear, or until `discard_state` is used, the older build cannot read the document).

**Rolling back across scheduled jobs.** A build without scheduled jobs (the one before the
`jobs:` key) rejects a config that has the `jobs:` section, so roll the config back with it
(remove `jobs:`). It ignores the `job_book` key and never reads it. It also knows nothing of
`hold-job-*` leases: any such lease already in the `lease_book` (a run in progress when you
rolled back) is never released by it, and keeps its resource (the NFS host) held and powered
until a person releases it: `release_lease` with the lease's id (see `leases`), or `discard_state`
of `lease_book` (which drops every lease). Check `leases` for `hold-job-*` entries after the
rollback; better, roll back when no run is in progress (the `jobs` handler shows `run`). A Job
that flowmanager already created is not touched either way: it runs to its end under
Kubernetes (the CronJob stays `suspend: true`). Rolling forward again finds `job_book` as it
was left, which may be stale (a run that ended meanwhile), so a run in it ends from the readings
as usual; discard it (`job_book`) if you want a clean start.

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
the backstop epoch; the process re-arms the backstop by itself once it has not run for more than three
backstop intervals plus 10 s); one key alone is removed
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
| `requeue.body` | Optional JSON body (for example `{}`), sent with `Content-Type: application/json`. Absent: no body and no content type. |
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

## Power actions

A resource with `power.control: managed` is powered on and off by the scheduler. The
sequencer emits `PowerOn` and `PowerOff` steps for it (never for a `manual` one), and the
tick carries each one out by sending the resource's `Power` object (key: the resource id)
a one-way request after storing the decision; Restate journals the send, so a retried or
replayed tick sends it once, and the sequencer emits one step per transition. The object
makes the call in a journaled step, so a replay never repeats it (a crash after the call
but before its result is journaled makes Restate run the step once more: both calls are
harmless to repeat).

- **Power on** is Wake-on-LAN sent by the router, because flowmanager runs in the cluster
  and cannot broadcast on the LAN: `POST /api/wol/wol/set` to the OPNsense `os-wol` API with
  basic auth. `https` only, verification never off, no redirect followed.
- **Power off** is SSH (`russh`) with a forced-command key: the host key is pinned by its
  SHA256 fingerprint (any other key is refused), `poweroff` is exec'd (the forced command
  ignores it), and the wait after the exec is bounded (the link may drop as the host goes
  down; that is success).
- **Config** (`resources[].power`, typed and checked at load; see `config/example.yaml`):
  `wake_on_lan: {router_url, interface (default lan), mac}` and
  `shutdown: {host, port (default 22), user, host_key}`; `router_url` must be a plain
  `https://` base URL, without credentials, a query string or a fragment (the API path is
  appended to it). `managed` requires both. Optional `power_on_timeout`, `mounts_timeout`, `min_off` and
  `shutdown_timeout` (see below). A MAC or
  an SSH target may not be shared by two resources. A reload that turns a host `managed`
  applies like any other change; one that leaves it incomplete is rejected and the previous
  config stays.
- **Secrets are not config.** Files mounted from Kubernetes Secrets and named on the
  command line (or in the environment), read on every call so rotation needs no restart:
  `--opnsense-credentials-file` (`SCHED_OPNSENSE_CREDENTIALS_FILE`, `key:secret` on one
  line), `--opnsense-ca-file` (`SCHED_OPNSENSE_CA_FILE`, optional PEM bundle, the only CA
  trusted when given) and `--ssh-key-file` (`SCHED_SSH_KEY_FILE`). The service refuses to
  start when the config has a managed host and the files its calls need are not given.
- **A failed power action is never retried automatically.** A host that does not come up or
  go down is beyond the scheduler's scope, and a retry risks flapping it. The failure is
  logged at error level (`event="power_request_failed"`), counted
  (`sched_power_requests_total{resource,action,outcome="failed"}`) and stored; the
  invocation still succeeds, so Restate does not retry it either. The next ticks also say
  so: the `TickReport` (`last_decision`) lists it in `power_request_failures` and the
  `reason` carries `power on of <resource> FAILED at <t>ms: <error>` for as long as the
  resource is in the transition that request belongs to (a request of an earlier cycle or of
  the other action is dropped, a later accepted request clears it). (This is in memory of the process that
  ran the request: after a restart only the log, the metric and `last_action` have it. A
  request that failed is never reported as success: the sequencer's `powering on` /
  `shutting down` phase means a request was decided, not that it was accepted.) The
  sequencer has recorded the step and follows the host's observed reachability from there,
  until the timeout below.
- **Power transitions time out: alert and stop.** A power-on that is not observed up within
  `power_on_timeout` (default 10m), or a shutdown not observed down within
  `shutdown_timeout` (default 15m), moves the resource to the **failed** power phase
  (`{phase: failed, since, during: powering_on|shutting_down}` in `scheduler_state`). Both
  are set per resource under `resources[].power`, must be above 0, above
  `policy.host_reading_stale_after` (checked under `managed` only) and at most 1d, and only matter under `managed` (a
  `manual` host has no such phase). In the failed phase flowmanager:
  - raises the alert once: an error log line with `event="power_timeout"`, a count in
    `sched_power_timeouts_total{resource,during}` and a `power_timeouts` entry in that
    tick's `TickReport`;
  - stays visibly failed: `sched_resource_power_failed{resource,during} == 1` (alert on
    it), `sched_resource_phase_age_seconds{axis="power",phase="failed"}`, every tick's
    `reason` ends with `<resource> is in the FAILED power state ...`, the report's `kinds`
    has `power_failed` and `power_failed` lists the resource, and the resource's step is
    `wait ... power_failed`;
  - allows nothing on that host (a model, or a class that requires it), exactly as while it
    is powering on or shutting down. Other resources keep scheduling: their state machines
    and the models and classes that need only them are unaffected by one resource's failure;
  - **never takes a power action for it again**, however long work stays queued or the
    host stays as it is. Waking or shutting it down is for a person.
  It leaves the failed phase only when a fresh reachability reading shows the state the
  transition was after (up after a power-on, down after a shutdown), which is also what
  happens when a person fixes or powers the host, or when the resource is switched to
  `control: manual` in the config (the phase then simply follows observation). No lease or
  reclaim clears it; a person can clear it with `resume` (see *Host leases, reclaim and
  resume*). A host that comes back down to `off` while work is queued is then
  treated like any off host (a manual lease or a power-changing class may wake it, as before).
  A host that comes back up out of a failed power-on is treated like a host found running,
  not as one flowmanager powered on: the recovery itself never powers it off. For a shared
  host under `control: managed` that means it is **adopted** (see *Host leases, reclaim and
  resume*): a manual host lease is minted, which takes responsibility for the host, and
  releasing it (plus the idle grace) powers the host off. Any other found-running host is
  powered off only by an explicit reclaim, or a manual lease that is held and then released.
  The timeouts are measured on the wall clock only, with no fresh reading required, so a
  scheduler restart or a long outage can raise the alert before a fresh reading could show
  the host in the state wanted (a known trade-off; the failed phase then clears on that
  reading, but the alert has fired).
- **A minimum off time (`min_off`), so a host does not flap.** Per resource under
  `resources[].power`, a duration, **default 0: disabled**, and then nothing here applies and
  nothing in the stored state changes. At most 1d; `0` is allowed; it only matters under
  `control: managed` (a `manual` host accepts and ignores it). With `min_off` set, after a
  shutdown that **flowmanager started** and saw **confirmed** (the phase reached `off` because a
  fresh reading showed the host down while it was `shutting_down`, or `failed` while shutting
  down), an *automatic* power-on waits until `min_off` has passed: a power-changing class with
  demand, the hold of a scheduled job, any other want that is not a manual lease. A **manual
  lease overrides it** (the person asked; they do not wait).
  - *What starts the clock.* The instant of the reading that confirmed the shutdown (the tick
    that moved the phase to `off`), kept as `shutdown_confirmed_at` on the resource in
    `scheduler_state`. A host found down at first sight, one that was up and went down by
    itself, a host never managed, and one under manual control have no stamp and are not
    delayed: flowmanager did not shut them down. The stamp ends when the phase leaves `off`.
    It is written only for a resource that has a `min_off`. Changing `min_off` takes effect
    on the next confirmed shutdown for a host with no stamp yet (a stamp already kept uses
    the new value; setting it back to 0 drops the stamp).
  - *Durable.* The stamp is part of the persisted sequencer state, so a scheduler restart
    does not shorten the wait. `STATE_VERSION` is unchanged: the key is optional and absent
    while unset. **Rolling back:** a build without `min_off` cannot read a state that has the
    key (like `failed` and `mounts_veto`), which is only while a host with a `min_off` is
    `off` after a shutdown of ours and the wait has not been left (the key goes away when the
    host is powered on or seen up). Until then, or until `discard_state`, the older build
    reports a persisted-state error. Roll the config back (remove `min_off`, which an older
    build rejects as an unknown key) with it.
  - *Clock going backwards.* A stamp in the future (the wall clock moved back) is made again
    at the current time on the next tick, so the wait is never longer than `min_off`.
  - *Never a retry.* The wait only delays a fresh decision: nothing is requested while it
    lasts, and when it is over the next tick decides from the then current intent, so a want
    that went away in the meantime powers nothing on. A power-on that is then requested is
    subject to everything else as before (timeouts, failed phase).
  - *Reported.* Step `wait <resource> min_off`, a `min_off_waits` entry in the tick's report
    (`resource`, `since`, `until`, `remaining`, all in ms), the `reason` says `<resource> is
    wanted but was shut down by flowmanager and seen down at <t>ms: waiting for min_off, <N>s
    left`, and the gauge `sched_resource_min_off_remaining_seconds{resource}` counts the
    seconds left (0 outside the window; series only for managed resources with a `min_off`).
  - *Scheduled jobs.* A job's hold is a want like any other, so a run waits for the host
    through the wait. Its start window keeps running meanwhile: under `skip` a run whose
    window ends inside the wait is dropped as `missed` and its hold released (the host is then
    not powered on); under `catch_up` it alerts `start_late` at the end of the window and
    starts, late and once, after the host is up. Choose `min_off` with the start window in
    mind. Checked by the end-to-end tests in `crates/sched-service/src/jobs/scenarios.rs`.
  - Invariant 4b (`crates/sched-service/src/invariants.rs`): no `power_on` within `min_off` of
    a confirmed shutdown without a manual lease. Replay scenarios `min-off-wait.yaml` and
    `min-off-manual-lease.yaml`.
- **A shutdown first checks that no pod mounts the host, and a GPU shutdown first winds
  its model down.** Neither is a retry or a new power action; both only hold a shutdown back.
  - *NFS host (the mount check).* A host named by a `mounts` source (`kubernetes_watch`
    with `observe: [mounts]` and `params.nfs_servers: {<resource>: [<server name or address>]}`,
    see `config/example.yaml` and `docs/kubernetes_setup.md` for the RBAC) is powered off
    only on a fresh reading that no pod mounts it. A pod mounts it when it has not finished
    and one of its volumes, directly or through a PersistentVolumeClaim, is an NFS volume
    (or an NFS CSI volume, by its `server` attribute) of one of the configured servers
    (compared ignoring case; list the name and the address if the cluster uses both). The
    mapping from a host to its servers is in the config, never in the code. The source lists
    PersistentVolumes, claims and pods once per `resync` (every namespace unless
    `params.namespaces` restricts them). **Unknown is never "no mounts"**: a failed or slow
    list, a reading older than `policy.host_reading_stale_after`, or a pod whose claim or
    volume the lists do not show makes the count unknown, and unknown counts as mounted. (One
    thing is not counted: a claim not bound to any volume yet, which cannot be mounted.) A
    `managed` shared host must be named by such a source, or the config is refused at load.
    While the shutdown is due (idle, no holds, `idle_grace` over) and pods mount the host
    (or the count is unknown), the step is `wait ... pods_mounted` (or `mounts_unknown`), the
    `reason` says so, the host stays up and what needs it stays allowed, and
    `sched_resource_mounts_veto{resource,state="waiting"}` is 1. The wait is bounded by
    `resources[].power.mounts_timeout` (default 30m, above `host_reading_stale_after`, at most
    1d). When it runs out flowmanager **alerts and stops**: `event="shutdown_mounts_timeout"`
    at error level once (a replayed tick logs `shutdown_mounts_timeout_replayed` at info),
    `sched_mounts_timeouts_total{resource}`, a `mounts_timeouts` entry in that tick's report,
    then `mounts_held` lists the resource with `stopped: true`, the kind
    `shutdown_stopped`, the step `wait ... shutdown_stopped` and
    `sched_resource_mounts_veto{state="stopped"} == 1` (alert on it). The host is **not** shut
    down, however long it stays like this and even after the pods are gone, and the
    `managed` flag is dropped (as after a failed power transition). It leaves the stopped
    state when the host is observed down (a person shut it down), when a person calls
    `resume` for it, or when a new manual lease takes the host over again (the next idle
    period then decides afresh). The wait counts from the moment the shutdown is due, not
    from when the host went idle, and is cleared at once if something holds the host again.
  - *GPU host (the wind-down).* No mount check applies unless a `mounts` source names it.
    Its shutdown is already preceded by the bounded wind-down of the model: a managed
    exclusive host is powered off only when its mode is idle, so a model that is allowed, or
    being wound down, blocks it. The wind-down gives in-flight work `drain_timeout` (a class's
    `kill_timeout`; `reclaim_timeout` for a reclaim), then stops it and withdraws the model in
    one tick, and the shutdown is decided on a later tick, after the host has also been idle for
    `idle_grace` (a reclaim overrides that grace). Checked by the sequencer tests
    `a_managed_gpu_host_is_never_powered_off_while_a_model_is_allowed_and_only_after_the_drain`
    and `a_reclaim_gives_in_flight_work_its_timeout_before_the_managed_gpu_host_is_shut_down`,
    and the `reclaim.yaml` scenario.
- `Power/{resource}/run` is ingress-private; `Power/{resource}/last_action` returns the
  last action (`{action, at, outcome: requested|failed, error}`, `null` if none).

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
| `acquire_host_lease` | yes | Acquires a manual lease on a shared host (`{"resource": "basestar-nfs"}`). |
| `release_host_lease` | yes | Releases the manual host lease of a resource (`{"resource": ...}`); with `"drain": "graceful"` or `"immediate"` it also takes the host down despite queued work (see *Host leases*). |
| `reclaim` | yes | Reclaims an exclusive resource with a power section (`{"resource": "cuda-central"}`). |
| `jobs` | yes (shared) | Every configured scheduled job with its state (see *Scheduled jobs*). |
| `trigger_job` | yes | Starts a run of a scheduled job now (`{"id": "backup"}`); `409` while the previous instance is pending or running, `404` for an unknown job. |
| `unreclaim` | yes | Releases the reclaim of a resource (`{"resource": ...}`). |
| `resume` | yes | Clears a failed power transition, or a shutdown stopped by mounting pods, for one resource (`{"resource": ...}`). |
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
- Other lease kinds: manual host leases and reclaims have handlers (see below); shared holds are taken only by scheduled jobs (see *Scheduled jobs*), with no handler; interactive
  leases with expiry are no longer planned (see `planning/milestones.md`, *Scope reset
  during M3*).
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

### Host leases, reclaim and resume

These four handlers and `resume` take `{"resource": "<id>"}`, are validated against the
active config inside a journaled step (a replay gets the same answer after a reload), answer
`400` for an unknown or unsuitable resource (the message lists what is configured), `404` for
a release with nothing to release and `409` for a `resume` with nothing to resume, and ask for
a tick. Like the other lease handlers they are public on the Restate ingress: keep it on a
trusted network.

- **Manual host lease** (`acquire_host_lease` / `release_host_lease`): a hold on a *shared*
  resource with a `power` section, typically the NFS host. It wakes the host when it is off
  (under `managed`; under `manual` it waits for a person to power it on), keeps it up until it
  is released, and, because the host is up, allows everything gated on it (the NFS-only
  consumers and the gate classes that require it) even with no work queued, so their queues
  become visible. Asking again returns the lease (`"created": false`); an exclusive resource
  is refused (lease its models instead). Ids are `manual-host-<n>`. Released, the host is
  shut down only when nothing else holds it (a shared hold, another lease, queued or
  in-flight work of a class that needs it), `idle_grace` has passed and the mount check above
  passes; two holders keep it up until both are gone.
- **A host found running is adopted** (a changed default: it used to be left alone for ever,
  so a host switched on by hand stayed up until a person shut it down). For a shared host with
  `power.control: managed`, when a fresh reading shows it up and flowmanager did not power it
  on, it counts as held by the user and the tick mints the same manual host lease for it
  (id `adopted-host-<resource>`, listed by `leases`). "Did not power it on" is read from the
  previous state: no state (first start, discarded state), `off` (a power button, a manual
  Wake-on-LAN, a host that went down and came back, adopted afresh each time), or a failed
  power-on that a reading has now cleared. A host flowmanager powered on itself (a backup
  job's hold, a lease acquired through the API) is `powering_on` and is not adopted, and a
  restart with the persisted state adopts nothing (the host is already `on`). The lease is
  minted before the planner looks, so the same tick sees it and takes responsibility for the
  host (`managed`); the Home Assistant switch reads `on` for it, and switching it off
  releases it like any manual host lease. An adopted lease is dropped when a fresh reading
  shows the host down (the user switched it off by hand), so it is never woken again by a
  lease nobody asked for. The tick report lists `adopted` and `adoptions_ended`, and the log
  has `event="host_adopted"` and `event="host_adoption_ended"`. Under `control: manual`
  nothing changes: the host is never adopted and never powered off. After a `resume` the
  host is found running again and is adopted at the next tick.
- **Drain on release** (`release_host_lease` with `"drain": "graceful"` or `"immediate"`;
  absent means the behaviour above). A person who switches a host off does not want queued
  work to hold it up. A drain releases the manual lease and records the request in the host's
  state (`drain: {mode, since, scaled_down}`, persisted, so it survives a restart): then (a)
  queued and in-flight work of the classes that need the host stops counting as a hold, (b)
  those classes leave the allowed set, so KEDA scales their consumers down and their pods stop
  mounting the host (`immediate`: at once; `graceful`: when none of them has work in flight,
  or `policy.drain_timeout` after the request, whichever is first; an unreadable in-flight
  count waits for the timeout), and (c) the host is shut down once no lease holds it and the
  mount check above passes. **A drain overrides no lease**: a scheduled job's hold or another
  lease keeps the host up, the step is `wait ... drain_held` and the shutdown follows when
  they are gone. Before the scale-down the step is `wait ... draining`. The mount check and
  `mounts_timeout` apply after the scale-down as for any shutdown; for a host with no `mounts`
  source the shutdown waits `idle_grace` after the scale-down, as nothing can show the pods
  are gone. The drain ends when the host is no longer up, and a **new manual host lease
  cancels it** (the consumers are allowed again). It is visible as `drains` in the tick
  report (and `last_decision`), as `drain` in the Home Assistant state of the host, and as
  `event="drain_requested"` / `event="drain"` log lines. Known limit: a class that may change
  power and still has queued work wakes the host again once it is off (after `min_off`),
  exactly as it would after any shutdown.
- **A manual model lease on a host that is off** already wakes a managed host (the planner
  asks for the power of any resource a manual lease names), and the model is allowed once the
  host is observed up; there is no separate GPU lease.
- **Reclaim** (`reclaim` / `unreclaim`): the existing `Reclaim` lease, for an exclusive
  resource with a power section only (`400` otherwise, as for any resource it cannot apply
  to). Until released, intake stops, in-flight work gets `reclaim_timeout` and is then cut
  off, the model is withdrawn, a `managed` host is shut down (a `manual` one is left for a
  person) and it is not woken. Ids are `reclaim-<n>`; asking again returns the lease.
- **`resume`**: for a person who has fixed a host by hand. It applies to a resource in the
  failed power phase, or whose shutdown was stopped by mounting pods, and forgets that
  resource's state so the sequencer starts it afresh from observation, as for a resource it
  has not seen: found running is up and *not managed* (flowmanager will not power it off on
  account of an earlier action; a shared host under `control: managed` is then adopted, see
  above), found down is off. It powers nothing on or off itself, does
  not touch any other resource's state or the leases, and is logged at warn level
  (`event="power_resumed"`). `409` if the resource is not stopped (including a host that is
  merely powering on or shutting down, or has no state yet).

```sh
curl -X POST localhost:8080/Scheduler/main/acquire_host_lease \
  -H 'content-type: application/json' -d '{"resource": "basestar-nfs"}'
curl -X POST localhost:8080/Scheduler/main/release_host_lease \
  -H 'content-type: application/json' -d '{"resource": "basestar-nfs"}'
# take the host down even though work is queued (or "immediate")
curl -X POST localhost:8080/Scheduler/main/release_host_lease \
  -H 'content-type: application/json' -d '{"resource": "basestar-nfs", "drain": "graceful"}'
curl -X POST localhost:8080/Scheduler/main/reclaim \
  -H 'content-type: application/json' -d '{"resource": "cuda-central"}'
curl -X POST localhost:8080/Scheduler/main/unreclaim \
  -H 'content-type: application/json' -d '{"resource": "cuda-central"}'
# After fixing a host that flowmanager stopped at (the answer says what was cleared).
curl -X POST localhost:8080/Scheduler/main/resume \
  -H 'content-type: application/json' -d '{"resource": "basestar-nfs"}'
# {"resource":"basestar-nfs","cleared":"shutdown_stopped_by_mounts"}
```

### Toggle listener (Home Assistant)

`--toggle-listen` (`SCHED_TOGGLE_LISTEN`; suggested `0.0.0.0:9096`) with `--toggle-token-file`
(`SCHED_TOGGLE_TOKEN_FILE`) starts a small authenticated HTTP listener so that Home Assistant's
`rest` switch can hold and release manual leases by name. It is **off when the flag is absent**
and the two flags need each other; the token is read from the file on every request, every
request needs `Authorization: Bearer <token>` (compared in constant time, `401` otherwise), and
the token is never logged. It is plain HTTP, on purpose (Home Assistant cannot trust a private
CA); the token and the network policy are the controls. Open WebUI itself is not integrated.

| Request | Meaning |
| --- | --- |
| `GET /v1/leases` | every model, powered resource and reclaimable resource with its state, and `needs_attention` |
| `GET /v1/models/{model}`, `POST .../on`, `.../off` | the manual model lease (on = held, not "loaded") |
| `GET /v1/resources/{resource}`, `POST .../on`, `.../off`, `.../off-now`, `POST .../resume` | the manual host lease (a host found running reads on, see *adopted*); `off` releases it with a graceful drain, `off-now` with an immediate one; `resume` clears a stopped power phase |
| `GET /v1/reclaims/{resource}`, `POST .../on`, `.../off` | `reclaim` / `unreclaim` of an exclusive resource with a power section |

`POST /v1/<kind>/{name}` with the body `ON` or `OFF` is the same as `/on` and `/off`. On and
off are idempotent (off when not held is a success), and nobody sees a lease id. Each request
goes to the matching `Scheduler` handler through the Restate ingress, so locking and
journaling are the handlers'; the handler's refusal comes back as `{"error": "<message>"}`
with its status (`400` unsuitable resource, `409` another model leased or nothing to resume,
`404` unknown name or path; a `404` from Restate is a `502`, never an answer). The token is checked
before a body is read. A state read has `held` and `state`, and, when the last tick's report
supports it, `status` / `detail` (a model: `active`, `waiting_for_host`, `draining`, `reclaimed`,
`host_failed`, `pending`; a reclaim also `left_for_person` on a `control: manual` host), `power`,
`wait` and `needs_attention` (the failed power phase or mounts-stopped shutdown also seen in
`sched_resource_power_failed` and `sched_resource_mounts_veto`); a field that cannot be derived is
left out. Every on/off/resume request and every refused request logs one `event="toggle"` line
(names escaped, refusals rate limited), is counted in
`sched_toggle_requests_total{endpoint,outcome}`, and a held lease shows in
`sched_manual_lease_age_seconds{kind,target}`. The flags, the token Secret, the NetworkPolicy and
`PeerAuthentication` notes for a `hostNetwork` client, the Home Assistant YAML and an alert
are in `docs/kubernetes_setup.md` (the toggle listener subsection of section 2).

### Webhook listener ("poll now")

`--webhook-listen` (`SCHED_WEBHOOK_LISTEN`; suggested `0.0.0.0:9097`) starts a second small HTTP
listener, separate from the toggle listener (its own port, so the two can have different
network and Istio policies). It is **off when the flag is absent**. A webhook here means one
thing: *poll the sources mapped to this path now*. The source declares the path in its params,
`webhook: { path: /hooks/paperless }`; several sources may share a path (the example config has
two for paperless and two for speakr), and one request nudges all of them. The body is never
interpreted: it is read up to 64 KiB and thrown away. The nudged source polls exactly as it
does on its `resync`, and the result enters the pipeline unchanged (the `Report` channel, the
cache, a tick if the value changed), so no report type or decision logic is involved and a lost
webhook costs only the wait until the next `resync`, which stays the safety net. Only `http_json`
sources take nudges; `tcp_probe`, `prometheus` and the Kubernetes observers are not nudged. (The
`webhook` source kind in the config model is unused and unchanged: it can supply nothing, no
observer starts for it, and nothing in this listener refers to it.)

Authentication is chosen at startup and **has no default**: `--webhook-listen` without
`--webhook-auth` refuses to start, as does `--webhook-auth` without `--webhook-listen`.

- `--webhook-auth mesh` (`SCHED_WEBHOOK_AUTH`): this process authenticates no one (it logs a
  warning at start). Istio must: `STRICT` mTLS on the port and an `AuthorizationPolicy` naming the
  caller principals. `--webhook-token-file` is refused with `mesh`.
- `--webhook-auth token` with `--webhook-token-file` (`SCHED_WEBHOOK_TOKEN_FILE`): every request
  needs `Authorization: Bearer <token>`; the token is read from the file on every request,
  compared in constant time and never logged, and start-up fails if the file is missing or
  empty. The token is checked **first**: before the path is looked up and before any of the body
  is read, so a caller without it gets `401` for every path and learns nothing.

| Request | Answer |
| --- | --- |
| `POST <declared path>` | `202 {"sources": n, "nudged": m}` |
| `POST <any other path>` | `404` (token mode: only after authentication) |
| another method on a declared path | `405`, `Allow: POST` |
| body over 65536 bytes | `413` (nothing is nudged) |
| `POST` on a declared path whose sources have no running observer yet | `503` |
| token mode, token missing or wrong | `401`; token file unreadable or empty `503` |

Nudges are coalesced and rate-limited **per source**, in the observer, not in the listener:
a source starts a poll at least `--webhook-min-interval` (`SCHED_WEBHOOK_MIN_INTERVAL`, default
`10s`) after its previous poll started, nudges that arrive while it waits are merged into that one
poll, and a nudge that arrives during a poll causes one more poll after the interval (the poll may
have read before the change). A flood of requests therefore costs at most one poll per source per
interval. A nudged poll restarts the source's `resync` clock. The nudge handle of a source lives
outside its observer task, so an observer restarted by a config reload keeps it; a source that
loses its `webhook:` setting or is removed from the config loses the handle.

Every request that reaches the handler is counted in `sched_webhook_requests_total{outcome}` (`accepted`, `unauthorized`,
`not_found`, `method_not_allowed`, `payload_too_large`, `bad_request`, `unavailable`; no
path or source label) and `sched_webhook_nudges_total` counts sources nudged. Every request logs one
`event="webhook"` line with `path` (escaped, at most 64 characters), `outcome`, `status`, `sources`,
`nudged` and `suppressed`; at most 30 lines are logged per 10 seconds (accepted ones too), the next
line says how many were left out. The flags, the Secret, the Service and NetworkPolicy, and the
`PeerAuthentication` / `AuthorizationPolicy` for `mesh` mode are in `docs/kubernetes_setup.md`
(the webhook listener subsection of section 2). Requests the HTTP layer rejects before the handler (a malformed request line, an oversized header block, a non-UTF-8 path) are neither counted nor logged. Nothing sends webhooks to flowmanager today.

## Scheduled jobs

A scheduled job starts a Kubernetes Job on a cron schedule after taking what it needs. The
definition stays where it is: a Kubernetes **CronJob kept `suspend: true`** is the template,
and flowmanager creates a Job from its `spec.jobTemplate` the way
`kubectl create job --from=cronjob/<name>` does (the template's labels, annotations and spec,
an owner reference to the CronJob, the annotation `cronjob.kubernetes.io/instantiate: manual`),
and marks it with the annotations `invoked-by: flowmanager` and `flowmanager/scheduled-at`
(RFC3339 UTC) and the labels `app.kubernetes.io/managed-by: flowmanager` and
`flowmanager/job: <id>`. It does not store or maintain the definition, never deletes a Job, and
runs no verification (the backup Jobs check their own preconditions in init containers). RBAC:
`docs/kubernetes_setup.md`, section 4. Config (`jobs:` in `config/example.yaml`, validated and
hot-reloaded like the rest; the `schedule` source kind is gone):

| Key | Meaning | Default |
| --- | --- | --- |
| `cron`, `timezone` | Five-field crontab and an IANA zone. A time in the hour DST skips does not run that day; one in the hour it repeats runs twice (use `UTC` for a strict once). Every zone, `UTC` included, is read from the host's tz database (`tzdata` in the image): jiff bundles none on Linux, so without `/usr/share/zoneinfo` (or `$TZDIR`) every config that has a job is rejected at load ("failed to find timezone"), as is an unknown zone. | `UTC` |
| `cronjob` | `{namespace, name}` of the template CronJob. | required |
| `needs` | A list of tagged needs; only `{type: nfs, resource: <shared resource>}` (a hold on it) today, so a model need can be added later without changing existing configs. | none |
| `retries`, `retry_backoff` | Retries after a failed Job (0: none); the wait doubles from `retry_backoff` up to 1h. Power is never retried: that is not about the Job. | 1, 5m |
| `missed_run` | `skip` or `catch_up` (below). | `skip` |
| `start_window` | How long after its time a run may still start. | 30m |
| `deadline` | How long a started run may take before it is alerted and failed. | 6h |

How a run goes (the state machine is pure, `sched_sequencer::jobs`, with a test per rule):

- **Needs.** From the moment a run exists it holds each need (a shared hold of holder
  `job:<id>` in the lease book; a managed host that is off is woken for it, a `manual` one waits
  for a person within the window). The Job is created only while every needed host is up in
  the sequencer's state; a host whose power is failed, or whose shutdown flowmanager gave up,
  does not count, and flowmanager does not try to repair power. The hold is released when the
  run ends however it ends, and the holds are *derived* from the runs on every tick, so one
  cannot leak: a stray `job:` hold with no run (a lost record, a removed job) is released by
  the next tick. After the Job completes, the host powers down only after all holds are gone,
  `idle_grace` and the mount check.
- **One instance.** A run that comes due (or is asked for with `trigger_job`) while the previous
  instance is waiting, running or in backoff, or while a Job past its deadline may still be
  active, is skipped, logged and alerted (`job_skipped_overlap`), never run beside itself or
  queued. Different jobs run in parallel.
- **Missed runs.** A run whose needs did not come up within `start_window` is *missed*, not
  failed: it uses no retries. `skip` drops it then (`job_missed`). `catch_up` alerts once
  (`job_start_late`), keeps waiting, and runs late (counted `caught_up`) if the needs come up
  before the next scheduled time; at that time it is dropped and the new run starts, so an
  outage never produces a burst. Downtime is the same: of all the times missed while
  flowmanager was down only the latest can run (late, under `catch_up`, or on time under
  `skip` if still inside its window); the older ones are dropped in one `missed` event. A
  freshly configured job, or one whose schedule changed, never runs for the past.
- **Failure.** A failed Job is retried after the backoff, keeping the hold. After the last
  retry the run is failed (`job_failed`, an alert) and the hold released; the next scheduled
  time starts afresh. A Job deleted by hand fails the run without a retry. A Job past its
  `deadline` is alerted and failed without deleting it (the job then stays blocked while the
  Job is still active).
- **Readings.** Job states come from listing Jobs by label every resync. A reading that is
  stale (older than `host_reading_stale_after`), failed or missing never means "finished". A Job
  absent from a reading proves it deleted only if the reading was received more than 20 s
  (`JOB_READ_SETTLE`, the observer's read timeout) after the creation call *returned* (the time
  the run is stamped with, not the time the tick decided to make the Job, which the call can
  trail by up to 15 s), because a read in flight when the Job was made lacks it. A Job that
  carries a `ttlSecondsAfterFinished` that expires before flowmanager has seen it finish is
  deleted too, and reads as `job_deleted` (failed, no retry, an alert) although it succeeded:
  keep the TTL well above `resync` plus the 20 s read plus a backstop interval, and above any
  outage of flowmanager you want to ride out (the setup guide suggests at least an hour), or
  leave it off. The machine does not tell the two apart.
- **Ambiguous creation.** A creation that returned an error may still have made the Job (the
  call can fail after the API created it). A run that then gives up (its start window ends)
  is `missed` and leaves the job's `lingering` mark, so the next run does not start beside it;
  the mark is freed by a reading received after the failed try that does not list the Job as
  active. The creation failure alert is sent once per attempt.
- **Overlap memory.** The `lingering` mark (a Job that outran its `deadline`, or may exist
  after a failed creation) blocks new runs of that job until a reading shows it is not active.
  A job removed from the config while its Job runs keeps a tombstone with that mark (its
  hold ends with its place in the config, as holds are derived from the config's jobs), so
  adding it back does not start a second instance; the mark is freed once the job is back and
  a reading shows the Job is not active. A tombstone of a job that had no live Job is dropped.
  The namespace the Job was made in is remembered: if `cronjob.namespace` is changed while
  it runs, readings (which now come from the new namespace) are ignored for that Job until the
  namespace is restored, so it is neither finished nor deleted by them; the run ends at its
  `deadline` and stays `lingering`. `discard_state` of `job_book` deliberately forgets
  everything (runs, marks, tombstones, `next_due`): a Job still running then is not known to
  the next run, which can start beside it, so use it only when no Job of flowmanager is active.
  `start_window` and `deadline` are not checked against the schedule's period: a `skip` run
  stops waiting at the next scheduled time anyway, and a `deadline` longer than the period
  means a due run is skipped (alerted) while the Job runs.
- **Durability.** Per-job state is the `job_book` document in the Scheduler object. The Job's
  name is `<id>-<scheduled epoch seconds>-a<attempt>` and creation runs in a journaled step in
  which `AlreadyExists` is success, so a replay or crash creates it once. A tick (every
  backstop interval, 30 s by default, and on every change) drives it rather than a Restate
  timer: the machine is a pure function of the clock and the readings, so there is nothing
  extra to keep alive. A time is noticed up to one interval late, and a host that just came up
  counts from the next tick.
- **Alerts** are log events `job_failed`, `job_missed`, `job_skipped_overlap`,
  `job_start_late`, `job_create_failed` (error level; a replayed tick logs `<event>_replayed`
  at info) and the counters in *Metrics*.

```sh
curl -X POST localhost:8080/Scheduler/main/jobs
curl -X POST localhost:8080/Scheduler/main/trigger_job \
  -H 'content-type: application/json' -d '{"id": "backup"}'
```

## Metrics

`/metrics` on `--metrics-listen`, the complete set (checked against a scrape and the
registrations in `metrics.rs`; labelled counters appear after their first event):

- `sched_tick_decisions_total{kind}`: ticks by decision kind (a replayed tick is not counted again).
- `sched_manual_lease_age_seconds{kind,target}`: seconds since each manual lease was granted, as of
  the last tick: `kind` `model` (target the model), `host` (the manual host lease) or `reclaim`
  (target the resource); alert on a lease held for hours. `sched_toggle_requests_total{endpoint,outcome}`:
  requests to the toggle listener (a fixed set of endpoint and outcome labels, never a name).
- `sched_webhook_requests_total{outcome}`: requests to the webhook listener (a fixed set of outcomes,
  never a path or source); `sched_webhook_nudges_total`: sources asked to poll now by accepted
  webhooks (merged and rate-limited by each observer, so polls can be fewer).
- `sched_persisted_state_errors_total{key}`: ticks and lease requests that could not read a
  persisted document (`scheduler_state`, `lease_book`); alert on any increase.
- `sched_resource_phase_age_seconds{resource,axis,phase}`: how long each resource has been
  in its current phase at the last tick, one series per axis: `axis="power"` with `phase`
  `off`, `powering_on`, `on`, `shutting_down` or `failed`, and `axis="mode"` with `phase` `idle`,
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
- `sched_resource_power_failed{resource,during}`: 1 while the resource is in the failed power
  phase (a power-on or shutdown timed out; alert on `== 1`), else 0. Only resources with a
  `power` section have the series.
- `sched_power_timeouts_total{resource,during}`: power transitions that timed out (alert on
  any increase; the resource then stays failed until a person acts).
- `sched_resource_min_off_remaining_seconds{resource}`: seconds left of the resource's `min_off`
  after a shutdown flowmanager started (an automatic power-on waits meanwhile, the step is
  `wait ... min_off`; a manual lease does not wait); 0 outside the window. Only managed
  resources with a `min_off` above zero have the series.
- `sched_resource_mounts_veto{resource,state}`: 1 while a shutdown of the resource is held back
  because pods mount its NFS server or the count is unknown (`waiting`, bounded by
  `mounts_timeout`), or because that wait timed out and flowmanager stopped (`stopped`; alert
  on `== 1`), else 0. Only resources named by a `mounts` source have the series.
- `sched_mounts_timeouts_total{resource}`: shutdown waits that timed out (alert on any
  increase; the host stays up until a person acts).
- `sched_job_events_total{scheduled_job,event}`: scheduled-job events by configured job: `started` (a Job
  was created), `succeeded`, `failed` (after its retries, past its deadline, or deleted;
  alert), `retried`, `missed` (alert), `skipped_overlap` (alert), `caught_up`, `start_late`
  (alert), `create_failed` (alert) and `triggered`. The label is `scheduled_job`, not `job`, which
  Prometheus uses for its own scrape label (a series named `job` would be exported as
  `exported_job`). `sched_job_running{scheduled_job}` is 1 while a run
  exists, `sched_job_last_success_timestamp_seconds{scheduled_job}` the last success (absent until one),
  `sched_job_next_due_timestamp_seconds{scheduled_job}` the next scheduled time.
- `sched_power_requests_total{resource,action,outcome}`: power actions by `on`/`off` and
  `requested` (accepted by the router or host), `failed` (never retried automatically; alert on
  any increase) or `not_wired` (the active config no longer has the host under managed control).
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
  `power_off`, `kill_in_flight`, `run_recovery`, `wait`), `idle`, `error`, `shutdown_stopped` (a
  shutdown stopped after pods kept mounting the host), `power_failed` (a
  resource is in the failed power phase), `pinned` (a
  manual model lease pins a model; such a tick is never `idle`, even with no step), and
  `demand_unknown` (something should be reporting the demand, an observer runs or the source
  has reported before, and it is silent or stale), `demand_unsupported` (the source kind has
  no observer in this build, e.g. session sources, and nothing has ever pushed
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
  `where.equals` (and `when.equals`) keeps the YAML scalar's type: `equals: false` / `true`
  is a JSON boolean and `equals: 3` a JSON number (compared numerically), as well as text
  for a string; quote a scalar (`"true"`, `"3"`) to compare it as text.
  **Behaviour change (M3):** an item whose `where` field holds a non-null value of another
  JSON type, or a response whose `when` field is missing or of another type, now fails the
  whole source (its demand becomes unknown) instead of silently counting low. Before M3 a
  number after `equals:` was compared as text.
- **Behaviour change (M5): `webhook: { path: ... }` in an `http_json` source's params** names the path of the webhook
  listener that makes this source poll now (see *Webhook listener*). It is validated at load
  (a path of letters, digits and `-._~/`, starting with `/`, no trailing `/`; unknown keys under
  `webhook` are refused). Without `--webhook-listen` it has no effect and the source is polled at
  its `resync` only.
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

- **Kill steps are not executed.** Power steps (only under `power.control: managed`) and
  recovery are carried out: see *Power actions* and *Recovery actions*. The sequencer
  records every one of them as taken:
  - `KillInFlight` marks the class as stopped and stops waiting for it (at `drain_timeout`
    or the class's `kill_timeout` after the wind-down started, forced or not). The model is
    withdrawn (and the next one allowed) in the same tick when no other class of it is
    still blocking; otherwise it stays allowed until the last blocking class is cut off or
    finishes. The in-flight work actually ends when KEDA scales the old model's pods down;
    the next model's pod starts once they are gone (Kubernetes).
  - `RunRecovery` (carried out) is emitted while the class is allowed and items are
    failed, not when the model is withdrawn. A recording that fails again after its
    attempts are used stays FAILED (re-queue it by hand) until it leaves the window. A
    crash between sending a re-queue request and journaling its result can repeat that one
    request.
  - `PauseIntake` stops no consumer (see above).
  - `PowerOn` and `PowerOff` (only under `managed`) are carried out (see *Power actions*).
    `PowerOn` moves the host to "powering on" until it is seen up; `PowerOff` moves it to
    "shutting down", and a host that stays up is never read back as on, so nothing is
    allowed on it until it is seen down. Neither phase waits for ever: at the
    `power_on_timeout` / `shutdown_timeout` the resource enters the failed phase, alerts and
    stops (no automatic retry). Under `manual` neither step is ever emitted.
- **Removing a model from the config cuts its work off at once.** A model removed while
  it is allowed leaves the allowed set at the next tick after the reload, with no dwell,
  grace or wind-down (its classes are gone with it, so there is nothing left to wait for),
  and the next model is allowed in the same tick. KEDA then scales the removed model's
  pods down mid-job; remove a model while it is idle if that matters.
- **The scheduler does not see GPU contention.** It relies on Kubernetes for exclusivity
  (see the top of this section). If a GPU workload is deployed without
  `nvidia.com/gpu: 1`, or the node exposes more than one GPU, two models can run at once
  and nothing here notices.
- **Demand that cannot be observed is treated as none.** A `session` source
  (interactive LLM) that never reported counts as no demand, so the scheduler will
  not keep the LLM loaded for an interactive session it cannot see (scheduled jobs such as the
  backup are not demand: they hold NFS through leases, see *Scheduled jobs*).
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
  transcription. A full page (100) of recordings in the failed request counts the ones on
  the page that pass the filter, as "at least N" (0 if none do): the poll does not fail, so a
  mass failure keeps demand fresh and recovery running. The list is one page sorted by
  `updated_at`, newest first, so a page entirely filled with failures the filter rejects
  (failed summaries, say) hides transcription failures beyond it: they are not counted until
  the page has room. A full page for a `ready` or `in_flight` request still fails the poll
  rather than under-counting.
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
  - Behind an Istio sidecar the application's connect is accepted by the sidecar before
    the upstream is tried, which would read "up" whatever basestar does: exclude basestar's
    address from outbound interception on the flowmanager pod (see
    `docs/kubernetes_setup.md`). Not checked against the cluster.
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
  `GET /api/queues/{smartSearch,faceDetection,ocr}` (the upstream API spec has it from v2.4.0, not in
  v2.3.1, marked alpha there with `.alpha('v2.4.0')`; the deployment is expected to run
  v4.0 or later, as the user stated, though no release numbered 4.0 could be found) with an
  admin API key (`queue.read`, `IMMICH_API_KEY`). An immich that does not serve
  `/api/queues` (older than v2.4.0) answers 404 and a changed response shape
  fails the poll. A paused queue (`isPaused: true`) counts as zero, ready and in-flight; from
  the versions this was written against a paused queue reports its jobs under `waiting`, so without that check they would count.
  What that means when an operator pauses or unpauses a queue (the `when` condition skips
  the whole response, so every number of that request reads zero):
  - **Pausing** takes the queue's waiting jobs out of the demand at the next poll (the
    source polls every 120s), and so does any job still running: active jobs read as not in
    flight while the queue is paused (whether immich lets them finish is not checked here).
    A wind-down does not wait for them, and if all three queues are paused the class reads
    idle: the model is released after `min_dwell` and `idle_grace` even while a job
    finishes. Withdrawing the GPU replica mid-job is safe (the CPU replica serves; the job
    stays in immich's queue).
  - **Unpausing** brings the waiting jobs back as ready work at the next poll, and the
    model is wanted again as new work. Their age starts there: the scheduler's "oldest
    ready" time is when the class's ready count last went from zero to something, so
    the time spent paused (or waiting before a restart of this process) does not count
    toward `age_threshold`.
  - A model that is serving when a queue is unpaused simply sees ready work again; nothing
    is interrupted.
- **The Restate ingress is trusted-network only.** The public handlers have no authentication.
