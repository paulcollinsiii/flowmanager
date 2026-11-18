# Operations

Running flowmanager day to day: what to call, what to watch, what to do when something is
stuck. Terms are in [concepts.md](concepts.md); install and flags are in
[install.md](install.md). Upgrade and rollback effects are in
[CHANGELOG.md](../CHANGELOG.md).

## Restate handlers

Everything a person can do goes through the Restate ingress to the `Scheduler` object (one
instance, key `main`), or the `Recovery` (key: class id) and `Power` (key: resource id)
objects. `POST {ingress}/{Object}/{key}/{handler}` with a JSON body (add `/send` for a
one-way call). The ingress has no authentication of its own: keep it on a trusted network
([install.md](install.md#9-istio-and-network-policy)). A refused call is an HTTP status with
`{"code": N, "message": "..."}`. Ingress-private handlers (`Scheduler/observe`,
`Recovery/run`, `Power/run`) are not listed; the ingress refuses them.

```sh
INGRESS=http://restate.restate.svc.cluster.local:8080
call() { curl -sS -X POST "$INGRESS/$1" -H 'content-type: application/json' ${2:+-d "$2"}; }
```

| Handler | Input | What it does and answers |
| --- | --- | --- |
| `Scheduler/main/last_decision` | none | The latest tick report this process published: inputs and their ages, `reason`, `kinds`, steps, `not_executed`, allowed set, `leases`, `ignored_leases`, `hosts` (one status each) and `events`. `null` until the first tick after a restart. Start here for "why is nothing running" |
| `Scheduler/main/leases` | none | Every lease, as an array: `id`, `resource`, `kind` (`type`: `manual_model`, `manual_host`, `lockout`, `shared_hold`), `granted_at` (ms), `origin`. Ids are `manual-model-N`, `manual-host-N`, `lockout-N`, `hold-job-<job>-<resource>` |
| `Scheduler/main/acquire_model_lease` | `{"model": id}` | Pin a model on its resource until released; batch work is cut short. `{"lease": ..., "created": bool}`. `400` unknown model or a model of a shared resource; `409` another model of the resource is pinned |
| `Scheduler/main/release_lease` | `{"id": lease id}` | Release any lease by id and return it. `404` unknown id. A job's hold is derived from its run and returns on the next tick |
| `Scheduler/main/acquire_host_lease` | `{"resource": id}` | Manual host lease: a managed host that is off is powered on and kept up. `{"lease", "created"}`. `400` unknown resource or no `power` section |
| `Scheduler/main/release_host_lease` | `{"resource": id, "drain"?: "graceful" or "immediate"}` | Release the host's manual lease (an adopted one too); returns it. `404` none held. With `drain`, also a requested shutdown, as `drain_host`, and the lease stays if a graceful one is refused (`409`). Without it the host goes down only when nothing holds it |
| `Scheduler/main/drain_host` | `{"resource": id, "drain": "graceful" or "immediate"}` | Requested shutdown, with or without a lease held ([Actions](architecture/power.md#actions)). `{"resource", "released", "drain"?, "wakers"?, "refused"?}`; `drain` is absent when the host is not up under managed control. A graceful request with wakers records nothing and answers `refused: true`. `400` as above |
| `Scheduler/main/lockout` | `{"resource": id}` | Lock the host out. `{"lease", "created"}`. `400` as above |
| `Scheduler/main/clear_lockout` | `{"resource": id}` | Clear it; returns the lease. `404` none held |
| `Scheduler/main/resume` | `{"resource": id}` | After fixing a host by hand: clear a failed power phase or stopped shutdown. `{"resource", "cleared"}`, `cleared` being `power_on_failed`, `shutdown_failed` or `shutdown_stopped_by_veto`. `400` unknown or no `power` section; `409` not stopped |
| `Scheduler/main/jobs` | none | Every configured job: `id`, `cron`, `timezone`, `missed_run`, `retries`, `next_due`, `run` (`scheduled_at`, `trigger`, `attempt`, `phase`, `job_name`, `retry_at`), `lingering`, `last` (`outcome`, `attempts`, `reason`, `ended_at`), `last_success_at` |
| `Scheduler/main/trigger_job` | `{"id": job id}` | Start a run now; same answer as one `jobs` entry. `404` unknown job; `409` the previous run is pending or running, or a Job may still be active. Never queued |
| `Scheduler/main/discard_state` | `{"key": ..., "force"?: bool}` | Delete `scheduler_state`, `lease_book` or `job_book` ([below](#discarding-state)). `{"key", "previous"}`, `previous` being the stored text: keep it. `400` unknown key; `409` the document reads fine and `force` is not set |
| `Scheduler/main/tick` | none | Run one decision now. `{"config_generation", "at", "kinds", "error"}` |
| `Scheduler/main/arm_backstop`, `backstop` | none, `{"epoch": N}` | The backstop chain, called by the process itself. Do not call `backstop` with the current epoch: it would fork the chain |
| `Recovery/{class}/attempts` | none | The stored attempt book as text, `null` if none |
| `Recovery/{class}/discard_attempts` | `{"force"?: bool}` | Delete it. `409` if it reads fine and no `force` |
| `Power/{resource}/last_action` | none | The last power action: `action`, `at`, `outcome` (`requested` or `failed`), `error`. `null` if none |

Any handler that reads a document it cannot decode answers `503` and counts it in
`sched_persisted_state_errors_total`. A lease whose model or resource has left the config is
kept in the book but ignored: it is listed in `ignored_leases` and logged at each tick.
Examples:

```sh
call Scheduler/main/acquire_model_lease '{"model": "llm"}'     # pin the LLM
call Scheduler/main/release_lease '{"id": "manual-model-1"}'   # release it
call Scheduler/main/drain_host '{"resource": "basestar-nfs", "drain": "graceful"}'
call Scheduler/main/lockout '{"resource": "cuda-central"}'     # keep away from the GPU host
call Scheduler/main/clear_lockout '{"resource": "cuda-central"}'
call Scheduler/main/resume '{"resource": "basestar-nfs"}'      # after fixing it by hand
call Scheduler/main/trigger_job '{"id": "backup"}'
call Scheduler/main/discard_state '{"key": "job_book", "force": true}'
```

## Metrics

`/metrics` on `--metrics-listen` (`/healthz` answers `ok`). A labelled counter appears after its
first event. A resource label is a config id; the toggle and webhook counters never carry a name.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `sched_last_tick_timestamp_seconds` | gauge | | Unix time of the last tick |
| `sched_tick_duration_seconds` | histogram | | Time to snapshot, plan and sequence one tick |
| `sched_tick_decisions_total` | counter | `kind` | Ticks by decision kind, once per distinct kind: the step kinds `allow_model`, `withdraw_model`, `pause_intake`, `power_on`, `power_off`, `kill_in_flight`, `run_recovery`, `wait`; and `idle`, `error`, `pinned`, `shutdown_stopped`, `power_failed`, `demand_unknown`, `demand_offline`, `demand_unsupported`, `demand_unpolled`. A replayed tick is not counted again |
| `sched_tick_trigger_failures_total` | counter | | Failed requests to the Restate ingress for a tick |
| `sched_config_generation` | gauge | | Generation of the active config |
| `sched_config_reloads_total` | counter | `outcome` | `applied`, `unchanged`, `rejected`, `unreadable` |
| `sched_persisted_state_errors_total` | counter | `key` | A tick or request could not read a stored document |
| `sched_observation_age_seconds` | gauge | `source`, `capability` | Age of the newest reading at the last tick; infinite if never reported. `capability` is as in `observe`, or `none` |
| `sched_source_has_observer` | gauge | `source` | 1 if this build runs an observer for the source |
| `sched_source_errors_total` | counter | `source` | Failed reads |
| `sched_rejected_reports_total` | counter | `reason` | Reports dropped for lack of a place in the config: `unknown_source`, `unknown_subject` |
| `sched_resource_phase_age_seconds` | gauge | `resource`, `axis`, `phase` | Age of the current phase: `axis="power"` (`off`, `powering_on`, `on`, `shutting_down`, `failed`) or `axis="mode"` (`idle`, `serving`, `winding_down`). `off` and `idle` are measured from when this process first saw them |
| `sched_resource_power_failed` | gauge | `resource`, `during` | 1 while the host is in the failed power phase; `during` is `powering_on` or `shutting_down` |
| `sched_power_requests_total` | counter | `resource`, `action`, `outcome` | `action` `on` or `off`; `outcome` `requested`, `failed` (never retried) or `not_wired` (not managed in the active config) |
| `sched_power_timeouts_total` | counter | `resource`, `during` | Power transitions that timed out |
| `sched_resource_veto` | gauge | `resource`, `reading`, `state` | `reading` `mounts` or `pods`; `waiting` is 1 on the readings still blocking a shutdown, `stopped` is 1 on every reading once `veto_timeout` ran out. Only hosts with a veto reading |
| `sched_veto_timeouts_total` | counter | `resource`, `reading` | Veto waits that timed out, one per blocking reading |
| `sched_resource_min_off_remaining_seconds` | gauge | `resource` | Time left of `min_off`; 0 outside it. Only managed hosts with `min_off` |
| `sched_manual_lease_age_seconds` | gauge | `kind`, `target` | Age of each manual lease: `kind` `model` (target the model), `host` (an adopted lease included) or `lockout` (target the resource) |
| `sched_recovery_runs_total` | counter | `class`, `outcome` | `ran`, `skipped_interval`, `skipped_not_allowed`, `not_wired`, `list_failed`, `state_error` |
| `sched_recovery_requeues_total` | counter | `class`, `outcome` | `attempted`, `queued`, `failed`, `skipped_cap` (once per stuck item per run), `skipped_not_allowed`, `deferred` (over `max_per_run`) |
| `sched_job_events_total` | counter | `scheduled_job`, `event` | `triggered`, `started`, `succeeded`, `retried`, `caught_up`, and the alerts `failed`, `missed`, `skipped_overlap`, `start_late`, `create_failed` |
| `sched_job_running` | gauge | `scheduled_job` | 1 while a run exists (waiting, creating, running or in backoff) |
| `sched_job_last_success_timestamp_seconds` | gauge | `scheduled_job` | Unix time of the last success; absent until one |
| `sched_job_next_due_timestamp_seconds` | gauge | `scheduled_job` | Unix time of the next scheduled run |
| `sched_toggle_requests_total` | counter | `endpoint`, `outcome` | Toggle requests; `endpoint` such as `model_on`, `resource_off_now`, `lockout_get`, `list`, `unmatched`; `outcome` `ok`, `unauthorized`, `bad_request`, `not_found`, `method_not_allowed`, `conflict`, `error` |
| `sched_webhook_requests_total` | counter | `outcome` | `accepted`, `unauthorized`, `not_found`, `method_not_allowed`, `payload_too_large`, `bad_request`, `unavailable` |
| `sched_webhook_nudges_total` | counter | | Sources asked to poll by accepted webhooks |

### Suggested alerts

Thresholds are starting points, not tested against a live cluster. A tick of kind `error`
(`sched_tick_decisions_total{kind="error"}` increasing) is worth an alert too.

```yaml
- alert: FlowmanagerNoTicks            # Restate registration, ingress or the process
  expr: time() - sched_last_tick_timestamp_seconds > 180   # the backstop ticks every 30 s
  for: 5m
- alert: FlowmanagerCannotReachRestate
  expr: increase(sched_tick_trigger_failures_total[10m]) > 0
- alert: FlowmanagerPersistedStateError  # nothing is published while it lasts
  expr: increase(sched_persisted_state_errors_total[10m]) > 0
- alert: FlowmanagerStuckPhase           # a busy `none` class waits for ever by design
  expr: sched_resource_phase_age_seconds{phase=~"winding_down|powering_on|shutting_down"} > 900
- alert: FlowmanagerSourceBlind          # reachability never reported, or silent
  expr: sched_observation_age_seconds{capability="reachability"} > 120
  for: 5m
- alert: FlowmanagerSourceErrors
  expr: increase(sched_source_errors_total[15m]) > 5
- alert: FlowmanagerNeedsAttention       # failed power phase or stopped shutdown: fix the host, then resume
  expr: sched_resource_power_failed == 1 or sched_resource_veto{state="stopped"} == 1
- alert: FlowmanagerJobProblem
  expr: sum by (scheduled_job, event) (increase(sched_job_events_total{event=~"failed|missed|skipped_overlap|start_late|create_failed"}[30m])) > 0
- alert: FlowmanagerBackupStale          # a job that never succeeded has no series; tune per job
  expr: time() - (sched_job_last_success_timestamp_seconds{scheduled_job="backup"} or on() vector(0)) > 8 * 86400
- alert: FlowmanagerLeaseHeldTooLong     # a forgotten lease or lockout; an adopted host lease counts
  expr: sched_manual_lease_age_seconds > 6 * 3600
  for: 10m
- alert: FlowmanagerConfigRejected       # the last good config stays active
  expr: increase(sched_config_reloads_total{outcome=~"rejected|unreadable"}[15m]) > 0
- alert: FlowmanagerRecoveryFailing
  expr: increase(sched_recovery_requeues_total{outcome="failed"}[30m]) > 0
        or increase(sched_recovery_runs_total{outcome=~"list_failed|state_error"}[30m]) > 0
- alert: FlowmanagerRecoveryCapReached   # failed items that used all their attempts: re-queue by hand
  expr: increase(sched_recovery_requeues_total{outcome="skipped_cap"}[30m]) > 0
```

## Logs

The process logs plain text to standard output at `--log-level`; [install.md](install.md#10-check-it-works) says
what a healthy start looks like. Key lines:

| Line | Level | Key fields |
| --- | --- | --- |
| `tick` | info | `generation`, `at_ms`, `decision`, `reason`, `allowed_models`, `allowed_classes`, `leases`, `forced`, `unknown_demand`, `offline_demand`, `unsupported_demand`, `unpolled_demand`, `drains` (`resource:cause:mode:scaled_down`), `inputs`, `input_changed`, `replayed` |
| `tick failed; the previous state and allowed set stay` | error | `generation`, `at_ms`, `decision`, `reason`, `inputs`, `replayed`; nothing was published |
| `leases ignored: ...` | warn | `leases`: ids kept in the book but ignored because their model or resource left the config |
| `event=queue` | info | `class`, `ready`, `in_flight`, `failed`, `oldest_ready_age_ms`, `known`, `offline`. One per class per tick, not on a replay |
| `event=drain` | info | `resource`, `mode`, `cause`, `since_ms`, `scaled_down`, `replayed` |
| `event=drain_requested` | info | `resource`, `mode`, and `lease_released` (`drain_host`) or `lease` (`release_host_lease` with `drain`) |
| `event=host_adopted`, `host_adoption_ended` | info | `resource` |
| `event=power_timeout`, `shutdown_veto_timeout` | error | `resource`; `during` (`power_timeout`), `readings` and `blocking` (`shutdown_veto_timeout`). A replayed tick logs `<event>_replayed` at info, so a log alert fires once |
| `event=power_requested` | info | `resource`, `action` |
| `event=power_request_failed` | error | `resource`, `action`, `error`; not retried |
| `event=power_not_wired` | warn | `resource`, `action` |
| `event=power_resumed` | warn | `resource`, `cleared` |
| `event=job_started`, `job_succeeded`, ... | info | `job`, `scheduled_at_ms`, `detail`. The alert-grade ones (`job_failed`, `job_missed`, `job_skipped_overlap`, `job_start_late`, `job_create_failed`) are errors, `ALERT: ...`, and `<event>_replayed` at info on a replay |
| `event=toggle` | info on 200, else warn | `kind`, `name`, `action`, `outcome`, `status`, `suppressed`. Rate-limited; `suppressed` counts the lines left out; never the token |
| `event=webhook` | info on 202, else warn | `path`, `outcome`, `status`, `sources`, `nudged`, `suppressed`. Rate-limited as `toggle` |
| `event=state_version_discarded`, `legacy_adopted_lease_dropped`, `job_book_unreadable` | warn, warn, error | See [state](#reading-and-changing-state) |

## Reading and changing state

What is persisted is in [architecture.md](architecture.md#durable-state). Each document is
stored as `{"version": N, "data": ...}`. This build reads `scheduler_state` version 4,
`lease_book` version 2 and the others version 1.

**Reading.** `last_decision`, `leases`, `jobs` and `Recovery/{class}/attempts` answer from the
running process. The stored documents are shown by the Restate CLI: `restate state get Scheduler
main`, `restate state get Recovery <class>`. To query the KEDA scaler by hand,
`cargo run -p sched-keda --example scaler_probe -- <addr> model=<id>` calls `IsActive`,
`GetMetricSpec` and `GetMetrics` once (`--stream` follows `StreamIsActive`).

**A document that cannot be read** (corrupt, or written by another build) never fails an
invocation, which Restate would retry for ever. Instead the tick reports an `error`, logs it,
counts it in `sched_persisted_state_errors_total{key}` and stores and publishes nothing, so
KEDA keeps what it had. The exceptions:
- An *older* `scheduler_state` is not an error ([architecture.md](architecture.md#durable-state)):
  look for one `state_version_discarded` warning at the first tick. An older `lease_book`
  (version 1) is read, with a `legacy_adopted_lease_dropped` warning per adopted lease.
- An unreadable `job_book` switches off only the job step (an error `job_book_unreadable`).
- With only `scheduler_state` unreadable, the lease handlers keep working, except `drain_host`,
  `resume` and a `release_host_lease` with `drain`, which read it too and answer `503` like
  every handler that reads an unreadable document. An unreadable `lease_book` makes every lease
  handler and `leases` answer `503`.

The fix is the build that wrote the document, or discarding it.

### Discarding state

| Key | Discard with | Effect |
| --- | --- | --- |
| `scheduler_state` | `discard_state` | The sequencer starts from observation. No model is allowed until the planner picks one (KEDA scales down a running pod of any other), dwell, idle and `min_off` clocks restart, a requested shutdown or a stopped one under way is forgotten, and the record of which sources ever reported is dropped. A host that is up is found running and adopted; a host under a lockout is shut down a second time |
| `lease_book` | `discard_state` | Drops every lease: a model pin, host leases, lockouts. Job holds are derived from `job_book` and return; an up host is adopted afresh |
| `job_book` | `discard_state` | Forgets every run, `lingering` mark and next time. A Job still running is then unknown and the next run can start beside it: discard only when no Job of flowmanager is active |
| `recovery_attempts` | `Recovery/{class}/discard_attempts` | Every failed item gets a fresh set of attempts |
| `power_last_action` | `restate state`; informational, never read to decide | The `last_action` answer is empty |
| `backstop_epoch` | `restate state clear Scheduler/main` clears all of the object's state | The chain ends and the process re-arms it by itself once no backstop has run for three intervals plus 10 s, checked every interval: about 100 to 130 s at the default 30 s |

A document that reads fine is discarded only with `"force": true`. Discard is for an unreadable
document or a deliberate fresh start; a single key can be edited with `restate state patch`.
Keep the `previous` text the handler returns.

### The first tick after a restart

The observation cache is empty, so the scheduler continues from the stored state (a serving
model stays allowed and keeps its dwell clock; a wind-down goes on) and holds every step that
needs a fresh reading (invariant 5, [Safety invariants](architecture.md#safety-invariants))
until the observers report, seconds later. Until then the scaler answers `Unavailable` and a
ScaledObject with no `fallback` keeps its count. The stored "on" phase keeps the serving model
and every gate class allowed meanwhile, even if a host went down; a fresh "down" reading closes
them at its tick. The backstop is armed again within seconds (`arm_backstop` is retried every
5 s until Restate accepts). `last_decision` is `null` until the first tick.

## Recovery actions

Keys are in [workloads.md](configuration/workloads.md#recovery). The `Recovery` object of a
class runs the action, so a slow service never delays a tick.

- **When it runs.** The tick sends the object a request while the class is in the allowed set
  and a fresh reading reports failed items for it, except while a fresh pods reading shows a
  model of the class with no ready pod (a request sent before the pod serves would be lost and
  spend an attempt). A missing or stale pods reading does not hold it back. The run re-checks
  that the class is allowed before listing and before every request.
- **A run.** `min_interval` has passed; one list request; then, for each of at most
  `max_per_run` items with attempts left, the attempt is recorded durably and then the request is
  sent. A crash after the request and before its result is journaled can send it once more for
  the same attempt.
- **The attempt book.** `Recovery/{class}/attempts` shows
  `{"version":1,"data":{"last_run_at":..., "attempts":{"<id>":{"count":N,"last_at":...}}}}`.
  A request counts whether or not it succeeded. Records are kept `retention` after the latest
  attempt, listed or not. Metrics for a class key that is not in the config use
  `class="unknown"`.
- **A stuck item.** One with `max_attempts` used is skipped (`skipped_cap`, once per run) and
  stays failed until it leaves the list's `since` window; re-queue it by hand. An item edited
  more than `retention` after its last attempt gets a fresh set. It stays in the `failed`
  count of the class's `queue` line while it is listed.
- **Failures** (network, 4xx, 5xx, an answer that is not JSON or fails `expect`) are logged
  and counted in `sched_recovery_requeues_total{outcome="failed"}`, never fatal. A list that
  fails sends nothing and leaves the attempts alone (`list_failed`).
- **Keep two lists identical.** The demand source and the recovery list are written separately.
  A source that counts failures the recovery would not re-queue brings the model in for
  nothing; a wider recovery list re-queues extra items only while the source counts something.

## Scheduled jobs

Keys are in [jobs.md](configuration/jobs.md). `jobs` shows every job; `trigger_job` starts a
run now (the same path as a scheduled run: it waits for its needs, then the Job is created).

- **A run.** `phase` is `waiting` (needs not up; the hold wakes a managed host), `creating`,
  `running` (`job_name` set), or `backoff` (`retry_at`). Each Job is `<id>-<scheduled epoch
  seconds>-a<attempt>`, labelled `flowmanager/job: <id>`, so `kubectl get jobs -l
  flowmanager/job=<id>` shows them. The holds are `hold-job-<id>-<resource>` leases, derived
  from the runs on every tick and released when a run ends.
- **A failed run.** A failed Job is retried after the backoff, keeping the hold. After the last
  retry the run is `failed` (`job_failed`, an alert) and `last.outcome` says so. A Job deleted
  by hand fails the run without a retry. A Job past its `deadline` is alerted and failed but not
  deleted, and the job stays blocked, `lingering`, while the Job is active.
- **A missed run.** Needs that did not come up within `start_window` make a run `missed`, using
  no retry (`job_missed`). A `catch_up` run alerts once (`job_start_late`) and runs late if the
  needs come up before the next time. Times missed during downtime are dropped in one `missed`
  event ([jobs.md](configuration/jobs.md#missed-runs)).
- **An overlap.** A run that comes due while the previous is waiting, running, in backoff or
  `lingering` is skipped, logged and alerted (`job_skipped_overlap`), never queued. A job
  removed from the config while its Job may be active keeps a `lingering` tombstone, so adding
  it back does not start a second instance; a reading that shows the Job not active frees it.
- **Timing.** A scheduled time is noticed up to one backstop interval late (`--backstop`).
- **Readings.** A Job's state comes from the `jobs` source each `resync`. A stale or failed
  reading never means finished; a Job absent from a reading is deleted only if the reading came
  more than 20 s after the creation call returned. A Job whose `ttlSecondsAfterFinished` expires
  before flowmanager saw it finish reads as deleted: failed, no retry, alerted, though it
  succeeded. Keep the TTL above `resync` plus 20 s plus a backstop interval and any outage you
  want to ride out, or leave it off.
- **Unsure creation.** A creation that errored may still have made the Job. A run that then
  gives up is `missed` and leaves the `lingering` mark, freed by a later reading that does not
  list the Job as active. If `cronjob.namespace` is changed while a run is active, its readings
  are ignored until it is restored; the run ends at its `deadline`.

## Known blind spots

Read these before trusting a "no demand" or "idle" line, or a phase.

| Blind spot | What you see | What to do |
| --- | --- | --- |
| Power timeouts are wall-clock only (`power_on_timeout`, `shutdown_timeout`) | After a restart or a long outage the host reads `failed` and `power_timeout` alerts before a reading could show it in the wanted state; a fresh reading then clears it | Check the host before acting; `resume` if it is fixed |
| A failed power request is remembered in the process | After a restart only the log, `sched_power_requests_total{outcome="failed"}` and `Power/{resource}/last_action` have it. `powering_on` or `shutting_down` means a request was decided, not accepted | Read `last_action` and the log |
| The mounts veto does not count a claim not bound to a volume | A shutdown proceeds while such a pod is pending | None needed: it cannot mount yet |
| A host flowmanager powered on that goes down, then is powered on by a person, is still its own | It is not adopted and goes down when idle | `on` it from the toggle (a manual lease) |
| A false "off" reading during a lockout's withdrawal counts as the host seen off | The toggle shows `left_alone` and the host stays up | Reachability debouncing makes it unlikely; power the host off yourself |
| A `scheduler_state` discard during a lockout | An up host is shut down a second time (`locked_down` is lost, the lockout lease is kept) | Expect it |
| `kill_in_flight` and `pause_intake` steps are only recorded (`not_executed` in `last_decision`) | In-flight work ends when KEDA scales the old model's pods down, not when the step is taken | None: the allowed set is the control |
| Demand that cannot be observed counts as none (`session` and `sql` kinds, or a source with no `params`) | `demand_unsupported` or `demand_unpolled` in the tick, `sched_source_has_observer` 0 | Use a kind with an observer ([sources.md](configuration/sources.md#kinds-and-capabilities)) |
| flowmanager does not see GPU contention | Two models can run at once if a GPU workload lacks `nvidia.com/gpu: 1` | Invariant 2 ([architecture.md](architecture.md#safety-invariants)) is a deployment requirement |
| A failed Job whose TTL expired first | `job_deleted`, failed, though it succeeded | Raise the TTL, as under [Scheduled jobs](#scheduled-jobs) |
| `sched_manual_lease_age_seconds` counts adopted host leases | `FlowmanagerLeaseHeldTooLong` fires for a host someone powered on and left | `release` the lease, or `off` the host |
| The Restate ingress has no authentication | Anyone who reaches it can call every handler | Keep it on a trusted network |
