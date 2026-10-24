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

What is carried out and what is not:

- **Allowing, draining and pausing** take effect through the allowed set: KEDA converges
  replicas on it. The set is published in process on a `tokio::sync::watch` channel
  (`Shared::subscribe_allowed`, a `watch::Receiver<Option<PublishedAllowed>>`, `None` until
  the first tick of the process). The KEDA scaler that serves it is a later phase; until it
  exists nothing outside the process reads the set.
- **Power on, power off, stopping in-flight work and recovery actions have no actuator.**
  The sequencer still takes these steps (its state moves on as if they happened), and each
  one is reported as not executed: listed in the tick's `not_executed`, marked
  `[NOT EXECUTED: ...]` in the reason, and logged as a warning. See Known blind spots.
- Every observer is read-only.

Run it locally against a Restate server:

```sh
restate-server &
PAPERLESS_API_TOKEN=... SPEAKR_API_TOKEN=... \
  cargo run -p sched-service -- --config config/example.yaml --backstop 30s
restate deployments register http://localhost:9080
```

- `--config` (or `SCHED_CONFIG`) is re-read every `--reload-interval`; an invalid file is
  logged and the last good config stays active. In the cluster this is a mounted ConfigMap.
- Tokens come from environment variables or files named in a source's `auth`, never from the config.
- Backstop: `arm_backstop` starts a self-scheduling `backstop` chain (delayed send to itself,
  every `--backstop`); the binary calls it through `--restate-ingress` until Restate accepts.
  Changed observations ask for an extra `tick` the same way, and so does every lease change.

## How state persists

The object keeps its state in Restate, under these keys:

| Key | Holds |
| --- | --- |
| `sched_state` | The sequencer state: per resource the power phase (off, powering on, on, shutting down) and mode phase (idle, loading, serving with its wind-down, draining), with when each began. |
| `leases` | The lease book: the leases held and the counter lease ids are drawn from. |
| `reported_sources` | Which configured sources have ever reported values. |
| `last_decision` | The latest tick report (what `last_decision` returns). |
| `backstop_epoch` | The current backstop chain. |

A tick loads `sched_state`, `leases` and `reported_sources`, decides inside a journaled
step (`ctx.run`, named `decide`), stores the new state, and only then publishes the allowed
set in process. A retried or replayed invocation gets the journaled decision back instead
of deciding again, so what is published and what is stored agree. A tick that fails (no
decision) keeps the previous state and the previous publication.

The observation cache is not persisted: it lives in the process. After a restart the
scheduler continues from the persisted state (a model that was draining is still draining,
a serving model keeps its dwell clock) and holds every safety-critical step (allowing a
model, powering off) until the observers have reported fresh readings again. Which sources
had reported before is seeded from `reported_sources`, so a push-fed source that went
silent before the restart still reads as `demand_unknown`, not as never reported.

## Handlers

All are called as `POST {ingress}/Scheduler/main/{handler}`.

| Handler | Ingress | What it does |
| --- | --- | --- |
| `tick` | yes | One decision; returns the tick report. |
| `arm_backstop` | yes | Starts a new backstop chain (the binary calls it). |
| `backstop` | yes | One link of the chain; not meant to be called by hand. |
| `last_decision` | yes (shared) | The latest tick report, without waiting behind a tick. |
| `acquire_model_lease` | yes | Acquires a manual model lease. |
| `release_lease` | yes | Releases a lease by id. |
| `leases` | yes (shared) | Every lease held. |
| `observe` | **no** (ingress-private) | Pushed observations, from other Restate handlers only. |

`observe` is registered as private: the ingress answers
`400 {"code":400,"message":"the invoked service is not public","source":"ingress"}`. The
in-process observers do not use it (they write to the cache directly), so no pusher outside
Restate can forge a reading such as `pods: 0` through the ingress any more.

### Manual model leases

A manual model lease hard-pins a model on its resource until it is released: the planner
targets that model, batch work on the resource is cut short (intake paused at once,
in-flight work given `drain_timeout`, or a class's own `kill_timeout`, then stopped), and
the model stays loaded however much other work waits. Rules:

- The model must exist in the active config; the lease is on the model's resource.
- At most one manual model lease per exclusive resource. Asking again for the leased model
  returns the existing lease (`"created": false`); asking for another model of the same
  resource is refused with `409` until the first lease is released. An unknown model is
  refused with `400`, an unknown lease id on release with `404`.
- Lease ids are `manual-model-<n>`, from a counter in the object's state.
- A lease whose model or resource later disappears from the config is kept but ignored
  (listed in the tick's `ignored_leases` and logged).
- Other lease kinds (interactive, reclaim, manual host power, shared holds) have no API yet.
- A manual model lease also asks for its host to be up, and the scheduler then takes
  responsibility for powering it down later (see Known blind spots).

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

- `sched_tick_decisions_total{kind}`: ticks by decision kind.
- `sched_resource_phase_age_seconds{resource,axis,phase}`: how long each resource has been
  in its current phase at the last tick, one series per axis: `axis="power"` with `phase`
  `off`, `powering_on`, `on` or `shutting_down`, and `axis="mode"` with `phase` `idle`,
  `loading`, `serving`, `winding_down` (a serving model whose intake is paused) or
  `draining`. When the phase changes, the old phase's series is removed. Ages come from the
  persisted state, so they survive restarts; `off` and `idle` have no recorded start and are
  measured from when this process first saw them. Alert on stuck transitions, for example
  `sched_resource_phase_age_seconds{phase=~"loading|draining|winding_down|powering_on|shutting_down"} > 900`.
- `sched_observation_age_seconds{source,capability}`: infinite if that capability never reported.
- `sched_source_has_observer{source}`: 1 if an observer runs for the source, else 0.
- `sched_source_errors_total{source}`: failed reads reported by a source.
- `sched_rejected_reports_total{reason}`: reports (`unknown_source`) or values
  (`unknown_subject`) dropped because the config has no place for them.
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
  `power_off`, `kill_in_flight`, `run_recovery`, `wait`), `idle`, `error`, and
  `demand_unknown` (something should be reporting the demand, an observer runs or the source
  has reported before, and it is silent or stale), `demand_unsupported` (the source kind has
  no observer in this build, e.g. schedule and session sources, and nothing has ever pushed
  to it) and `demand_unpolled` (an observable kind configured without `params`: nothing polls
  it and nothing has pushed to it). `sched_source_has_observer{source}` is 0 for the last two.
  All three still mean "not idle". A source that has reported once and then goes quiet is
  always `demand_unknown`, also after a restart. The counter was called
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
- **Unknown is not idle**: if a class's demand source never reported or is stale, the tick is
  `demand_unknown` (listed in `unknown_demand`), never `idle`.

## Known blind spots

Read these before trusting a "no demand" or "idle" line, or a phase.

- **Power, kill and recovery steps are not executed.** The sequencer records them as taken:
  - `PowerOn` moves the host to "powering on"; it reads as on once it is observed up (a
    person has to power it on: manual power only).
  - `PowerOff` moves the host to "shutting down". A host that stays up is not read back as
    on (only a host seen down moves on, to off), so it stays "shutting down" and no model
    is allowed on it until it is seen down. The scheduler takes power-off responsibility for
    a host once a manual model lease asked for it to be up (or once it emitted a power-on
    for it), and powers it off after `idle_grace` with no model loaded, no lease holding
    it and every class on it showing known, zero demand.
    With the example config this cannot happen yet, because `llm-sessions` (a `session`
    source with no observer, which nothing can push to through the ingress) keeps the
    interactive LLM's demand unknown. Watch `sched_resource_phase_age_seconds{phase="shutting_down"}`.
    Pinned by the test `known_gap_an_unexecuted_power_off_leaves_a_running_gpu_shutting_down`.
  - `KillInFlight` marks the class as stopped and stops waiting for it; the model is drained
    in the same tick. The in-flight work actually ends when KEDA scales the model's pods to
    zero, which the sequencer waits for before allowing the next model.
  - `RunRecovery` is emitted once, after the drained model reaches zero pods, and then
    forgotten: failed jobs (speakr) stay failed until re-queued by hand.
- **Failed speakr transcriptions count as demand for 15 minutes.** `speakr-pending` lists
  FAILED recordings updated in the last 15m and counts those with `has_summary: false` as
  ready `speakr-transcription` work (speakr stores a failed summary's error in `summary`,
  every other failure's in `transcription`), so a whisperX outage brings whisperX in. 15m is
  below `age_threshold` (30m), so a failure never reaches starvation priority. A failure
  counts until it is re-queued or 15m pass; any later edit of a FAILED recording (tags,
  title, inbox, highlight, share) bumps its `updated_at` and restarts the window; permanent
  failures (bad file, codec) count too until the window passes; a failure older than 15m is
  not seen at all. Automatic re-queueing (a later M3 phase) is capped at 2 per recording,
  after which the recording stays FAILED and ages out. A reprocess-transcription failure on
  a recording that already has a summary is missed; a failed merge or stitch job counts as
  transcription. More than 100 failures in the window fails the poll rather than under-counting.
- **Queued speakr summary, merge and stitch jobs count as transcription demand.** The speakr
  `/stats` queue counts jobs of every type, so this is a deliberate, conservative overcount.
- **speakr `/stats` is scoped to the token's user.** Recordings of other users are not seen.
- **basestar's reachability is one Prometheus query, and the probe is not set up yet.**
  `basestar-probe` reads `probe_success{instance="basestar"}`: 1 is up, 0 is down. Until a
  blackbox probe of basestar exists the source fails every read. If Prometheus is unreachable,
  the series is missing or there are several (one per blackbox module or job: pin the module/job
  label in the query), or the value is anything else (including `-0`), the read fails and the
  reading goes stale (decisions needing NFS wait) rather than reading "down". It shows what the
  blackbox probe checks (the host answering), not that the NFS export works. Up/down flips
  only after 3 agreeing readings.
- **immich demand is the three ML queues only, through an alpha API.** `immich-queues` reads
  `GET /api/queues/{smartSearch,faceDetection,ocr}` (added in v2.4.0 and marked alpha there,
  `.alpha('v2.4.0')`; this deployment runs v4.0 or later) with an admin API key (`queue.read`,
  `IMMICH_API_KEY`). An immich older than v2.4.0 answers 404 and a changed response shape
  fails the poll. A paused queue (`isPaused: true`) counts as zero, ready and in-flight; from
  v4.0 a paused queue reports its jobs under `waiting`, so without that check they would count.
- **The Restate ingress is trusted-network only.** The public handlers have no authentication.
