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
# Scheduler service (shadow mode)

`sched-service` observes the cluster and the queue sources and **logs what it would do**.
It has no actuator: every observer is read-only and nothing is scaled, powered or called.
It runs as the `Scheduler` Restate virtual object (key `main`); a tick is
snapshot, then planner, then sequencer, then a log line with the decision, its reason, the
config generation and each input's age (per source and capability).

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
  Changed observations ask for an extra `tick` the same way.
- Metrics (`/metrics`, `--metrics-listen`), the complete set (checked against a scrape and
  the registrations in `metrics.rs`; labelled counters appear after their first event):
  - `sched_tick_decisions_total{kind}`: ticks by decision kind.
  - `sched_observation_age_seconds{source,capability}`: infinite if that capability never reported.
  - `sched_source_has_observer{source}`: 1 if an observer runs for the source, else 0.
  - `sched_source_errors_total{source}`: failed reads reported by a source.
  - `sched_rejected_reports_total{reason}`: reports (`unknown_source`) or values
    (`unknown_subject`) dropped because the config has no place for them.
  - `sched_tick_duration_seconds` (histogram), `sched_last_tick_timestamp_seconds`,
    `sched_tick_trigger_failures_total`.
  - `sched_config_generation`, `sched_config_reloads_total{outcome}`.
- **Trust boundary**: the Restate ingress (and the service port, which Restate calls) is
  assumed to be reachable only from a trusted network. The handlers (`observe`, `tick`, ...)
  have no authentication of their own; do not expose the ingress beyond the cluster.
  `observe` is bounded all the same: unknown sources and values with no place in the
  config are dropped and counted (`sched_rejected_reports_total`).
- **Metric rename**: the decisions counter is now `sched_tick_decisions_total{kind}` (it was
  `sched_decisions_total` in the first cut). It counts ticks, not actions; kinds include
  `demand_unknown` (something should be reporting the demand, an observer runs or the source
  has reported before, and it is silent or stale), `demand_unsupported` (the source kind has
  no observer in this build, e.g. schedule and session sources, and nothing has ever pushed
  to it) and `demand_unpolled` (an observable kind configured without `params`: nothing polls
  it and nothing has pushed to it). `sched_source_has_observer{source}` is 0 for the last two.
  All three still mean "not idle". A source that has reported once and then goes quiet is
  always `demand_unknown`. Dashboards or alerts on the old name need updating.
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
  up, refused or timed out is down, anything else is a source error. `basestar-probe` uses
  it. `prometheus` (params `url`, `query`, `resource`, `timeout`, optional `auth`,
  `reachability_confirmations`) stays available for hosts a Prometheus probe covers: one
  instant query per resync that must return exactly one series, `1` up and `0` down, every
  other answer a source error. No source in the example config uses it (no blackbox exporter
  probes basestar); the example shows it as a commented source. Both may declare only
  `reachability`.
- Handlers: `observe` (push observations; dated on receipt, never by the sender), `tick`,
  `last_decision`, e.g. `curl -X POST localhost:8080/Scheduler/main/last_decision`.

## What shadow output can and cannot show

Every tick starts the sequencer from a **fresh state**; nothing is carried from one tick to
the next (log field `shadow_stateless=true`, reasons begin `[shadow: next step from a fresh
sequencer state]`). Nothing is executed, so carrying a simulated state would only make the
sequencer wait for effects that never happen. The consequences, stated plainly:

- **Dwell and idle grace never elapse**, because they are measured from state the sequencer
  does not have. Decisions that wait on them (minimum dwell, idle grace before unloading or
  powering off) are not reproduced.
- **Drain, pause intake, kill and power-off steps essentially cannot appear**: they follow a
  model being loaded or a host being held, which is state. Do not read their absence as "the
  scheduler would never do this".
- **`allow_model` repeats on every tick** for as long as the demand lasts, since the model is
  never "loaded" in the sequencer's state.
- `sched_tick_decisions_total{kind}` (renamed from `sched_decisions_total`) counts **ticks** by kind (once per distinct kind in a
  tick), not actions. Ticks also run on every change and on the backstop.
- Leases are not modelled (the snapshot has none).
- **Unknown is not idle**: if a class's demand source never reported or is stale, the tick is
  `demand_unknown` (listed in `unknown_demand`), never `idle`.

What shadow output does show well: which model the planner wants given current demand, whether
inputs are fresh (`inputs` with an age per source and capability, `inf` for a capability that
never reported), and whether a source is failing (`sched_source_errors_total`).

## Known blind spots

What shadow output (and the M2 sources) cannot see or get wrong. Read these before trusting a
"no demand" or "idle" line.

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
- **basestar's reachability is one TCP connect, the check power.py makes.**
  `basestar-probe` (kind `tcp_probe`) connects to `basestar:2049` (NFS) once per resync,
  with a 2s timeout, and closes the connection at once without sending anything. Connected
  is up; refused, or no answer within the timeout, is down. A name that does not resolve (or
  whose lookup outlasts the timeout), no address, or any other connect error ("no route to
  host", "network unreachable", an address that cannot be connected to) fails the read and
  the reading goes stale (decisions needing NFS wait) rather than reading "down". Up/down
  flips only after 3 agreeing readings; a failed read is not a reading. What it misses:
  - It shows that something accepts connections on port 2049, not that the NFS export
    works: a hung `nfsd` that still completes the TCP handshake, or a stale or full export,
    reads as up.
  - Another host taking basestar's address, or anything else listening on that port there,
    reads as up.
  - A firewall in the way that drops the connection, or rejects it with a TCP reset or an
    ICMP port-unreachable, reads as down even with basestar running; one that rejects it
    with ICMP host- or admin-prohibited is a failed read ("no route to host").
  - The name is resolved where the scheduler runs (its pod, through cluster DNS), not where
    power.py runs. If cluster DNS does not resolve the bare name `basestar`, every read
    fails and basestar's reachability stays stale; set `host` to an IP address or a name
    the pod can resolve. Not checked against the cluster.
  - If the name `basestar` stops resolving while the host is off (a name served only by the
    host itself or by DHCP leases, for example), the probe fails instead of reading down,
    and basestar's reachability goes stale rather than "down".
  - With default kernel settings (3 ARP requests 1s apart), a host on the local network that
    does not answer ARP, such as a powered-off basestar, is reported as "no route to host"
    after about 3s. The 2s timeout fires first, so it reads as down; a `timeout` of 3s or
    more would turn it into a failed read. This ordering is reasoned from the kernel
    defaults, not observed against basestar.
  - Several addresses for the name are tried in order within the one timeout: any connection
    is up, any refusal (or the timeout) is down; only when every address fails another way is
    the read failed.
- **immich demand is the three ML queues only, through an alpha API.** `immich-queues` reads
  `GET /api/queues/{smartSearch,faceDetection,ocr}` (added in v2.4.0 and marked alpha there,
  `.alpha('v2.4.0')`; this deployment runs v4.0 or later) with an admin API key (`queue.read`,
  `IMMICH_API_KEY`). An immich older than v2.4.0 answers 404 and a changed response shape
  fails the poll. A paused queue (`isPaused: true`) counts as zero, ready and in-flight; from
  v4.0 a paused queue reports its jobs under `waiting`, so without that check they would count.
- **"Reported, then silent" memory resets on restart.** The scheduler remembers that a source
  has reported only in memory; after a restart a silent push-fed source reads as
  "never pushed" (`demand_unsupported` / `demand_unpolled`) until it reports again.
- **Shadow output is stateless** (fresh sequencer state every tick): no dwell, idle grace,
  drain, pause or power-off steps appear. See "What shadow output can and cannot show".
- **The Restate ingress is trusted-network only.** The handlers have no authentication.
