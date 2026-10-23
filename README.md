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
config generation and each input's age. Sequencer state is not carried between ticks (nothing is
executed, so each tick answers "what would I do given what I see now").

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
- Handlers: `observe` (push observations; dated on receipt, never by the sender), `tick`,
  `last_decision`, e.g. `curl -X POST localhost:8080/Scheduler/main/last_decision`.
