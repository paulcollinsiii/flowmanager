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
- Metrics (`/metrics`, `--metrics-listen`): `sched_decisions_total{kind}`,
  `sched_observation_age_seconds{source}`, `sched_source_errors_total{source}`,
  `sched_tick_duration_seconds`, `sched_config_generation`, `sched_config_reloads_total{outcome}`.
- **Trust boundary**: the Restate ingress (and the service port, which Restate calls) is
  assumed to be reachable only from a trusted network. The handlers (`observe`, `tick`, ...)
  have no authentication of their own; do not expose the ingress beyond the cluster.
  `observe` is bounded all the same: unknown sources and values with no place in the
  config are dropped and counted (`sched_rejected_reports_total`).
- **Metric rename**: the decisions counter is now `sched_tick_decisions_total{kind}` (it was
  `sched_decisions_total` in the first cut). It counts ticks, not actions; kinds include
  `demand_unknown` (an observed source is silent or stale) and `demand_unsupported` (the
  source kind has no observer in this build, e.g. schedule and session sources;
  `sched_source_has_observer{source}` is 0 for these). Dashboards or alerts on the old name
  need updating.
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
