# Concepts

The one place each term is defined. Config keys are in `code`; handler and API names are given
where a person calls them. A term used before its entry links to the section that defines it.

## Hosts and power

**Resource** (`resources[]`). A host the scheduler knows about, with an id (`cuda-central`,
`basestar-nfs`) and a `kind`. An optional `power` section says how it can be switched. Without
one, flowmanager never powers it.

**Exclusive resource** (`kind: exclusive`). Holds at most one model at a time, such as the GPU
node. **Shared resource** (`kind: shared`). Any number of holders, reference counted: an NFS
server, or an always-on node.

**Power control** (`power.control`). Who changes a host's power.
- `manual` (default): a person does. flowmanager never emits a power step for the host. Its
  power phase follows observed reachability, and work that needs it waits until it is up.
- `managed`: flowmanager powers it on and off. It needs both `wake_on_lan` and `shutdown`.

**Power methods**. `power.wake_on_lan`: the OPNsense router API sends the magic packet.
`power.shutdown`: SSH with a forced-command key to a pinned host key.

**Power timeouts** (`power_on_timeout` 10m, `shutdown_timeout` 15m, `veto_timeout` 30m). How
long a managed host may take to be observed up, to be observed down, or to pass the
[veto](#shutdown). When one expires flowmanager raises an alert and stops. It never retries;
a person fixes the host and calls `resume`.

**Minimum off time** (`min_off`, default 0). After flowmanager confirms a shutdown it started,
it does not power the host on again by itself for this long. A manual lease overrides it.

**What wakes a host**. Only under managed control, and only when something wants it: a manual
host lease, a scheduled job's `nfs` hold, or a *waker*.

**Waker**. A class that would power a host on if it went down now: a class with
`may_change_power: true` that has queued or in-flight work, or a [switched class](#workloads)
reading `on`. A class without `may_change_power` waits for the host to be up.

**Ownership and adoption**. A host flowmanager powers on is flowmanager's, whatever wanted it,
and goes down by the [idle cause](#shutdown). A host something else started is *adopted*:
flowmanager takes an adopted manual host [lease](#leases-and-manual-control) for the user.
This covers a power button, a manual wake, a host found running after state loss, and a host
that came up after flowmanager's own power-on timed out. The adopted lease ends when the host
is seen down.

## Workloads

**Model** (`models[]`). A mode of an exclusive resource, implemented by one or more Kubernetes
workloads (`workloads`). `load_time` is the estimated load time, used as the cost of
switching. `namespaces` lists where the workloads run, for the pods veto.

**Workload class** (`classes[]`). A unit of demand with `requires` (resources, and for an
exclusive one optionally a model), a `priority`, a `preemptibility`, `may_change_power`, and
optionally `demand`, `switch`, `recovery` and `kill_timeout`. A class is *allowed* when every
requirement is met. Kinds:
- **Demand class**: names a model and a demand source. Its queue drives which model is chosen.
- **Gate class**: requires an exclusive resource with no model, which means only "that host is
  up". It has no demand and never influences model choice.
- **Plain class**: no demand, only shared resources. It runs whenever they are up.
- **Switched class**: requires hosts only and has a [switch](#workloads) instead of demand.

**Priority** (`priority`). Higher wins when choosing which ready model to load next. It never
orders the steps of one piece of work.

**Precedence**. The order of steps for one piece of work, such as OCR before tagging. It is
handled by readiness: a step is not counted as ready until its upstream step is done.

**Preemptibility** (`preemptibility`). What a class's busy work does to a model switch the
planner wants.
- `none`: while the class is busy the planner does not switch away. That wait has no timeout.
  It does not hold against a *forced* wind-down: a lockout, a requested `off` or `off-now`, or
  a manual model lease. Those skip the wait and go straight to the [drain](#choosing-and-serving).
- `pause_intake`: intake pauses and in-flight work is given the drain.
- `kill`: the same, for work that is safe to retry.

**Recovery** (`recovery: {action, source, params}`). The one action is `requeue_failed`: it
re-queues failed items through the service's API while the class's model is allowed. It runs
in its own Restate object, `Recovery`, so it cannot delay a decision. The per-item attempt cap
is durable.

**Switch** (`switch: {source}`). The control of a switched class, an app a person turns on and
off, usually from Home Assistant. The reading is `on`, `off` (graceful) or `off_now`
(immediate). A missing, stale or unmapped reading is `off_now`, so the class fails closed. The
class is allowed while its hosts are up and the reading is `on`, or while a graceful `off`
waits for in-flight work of its *dependents*, at most one drain. A dependent is a class whose
demand source has `reads_from: <switched class>`. Once the class is off, its dependents'
demand is [offline](#demand-and-observation).

## Demand and observation

**Demand**. The work waiting for a class, in three buckets: *ready* (can start), *in-flight*
(running) and *failed*. Failed items wait for the class's recovery and never age into
starvation priority. They make the model wanted but never hold the serving model.

**Unknown demand**. No usable reading: missing, or older than the staleness limit. It is not
zero. It holds the model and host as if work existed, and it wakes nothing.

**Offline demand**. No work, and the class holds nothing. It applies when the demand reading
is missing or stale and a required host is freshly seen down (a fresh reading always wins), or
when the switched class the source `reads_from` is off and its graceful wait is over.

**Source** (`sources[]`). An observer adapter with a `kind`, the `observe` capabilities it
declares (only those are accepted from it) and, on a demand source, an optional `reads_from`
naming a switched class whose app the source polls. Kinds with an observer: `kubernetes_watch`
(pods, reachability, mounts, jobs), `http_json` (reachability, demand, switch), `prometheus`
(reachability, demand) and `tcp_probe` (reachability).

**Capabilities** (`observe`).
- `demand`: queue sizes of classes.
- `reachability`: whether a host is up.
- `pods`: pod counts of models. Two decisions read them: recovery waits for a ready pod, and
  the shutdown veto waits for zero pods of the host's models. Nothing else decides from pods.
- `mounts`: how many pods mount an NFS host.
- `jobs`: the state of Jobs flowmanager created.
- `switch`: a switched class's reading.

**Staleness**. A reading older than its limit counts as unknown. Reachability, mounts, pods
and job runs must be fresher than `host_reading_stale_after`; demand and switch readings
fresher than `queue_reading_stale_after`.

**Webhook listener** (`--webhook-listen`). Lets a service nudge an `http_json` source to poll
now, instead of waiting for its `resync`.

## Choosing and serving

**Tick**. One decision cycle: a snapshot of observations, leases and state, then the planner
(pure), then the sequencer (pure), then publication. The **planner** decides what is wanted:
which model next, which hosts to power. The **sequencer** turns that into ordered, safe steps
and the allowed set, and carries transitions such as withdrawals and shutdowns.

**Allowed set**. What each tick publishes: the allowed models (at most one per exclusive
resource) and the allowed classes. A KEDA ScaledObject with an `external` or `external-push`
trigger naming `model: <id>` or `class: <id>` is active only while that subject is allowed.
Replicas are 0 or 1.

**GPU exclusivity**. Enforced by Kubernetes (`nvidia.com/gpu: 1`, one GPU exposed, a tainted
node), not by flowmanager. On a switch the old model leaves and the new one enters the allowed
set in the same tick, and the new pod stays Pending until the old one frees the GPU.

**Exhaustive, bounded service**. A loaded model keeps serving while it has ready work. It
yields only when it has been allowed for `min_dwell` and either has served longer than
`max_dwell` or another queue is *starved* (its oldest ready item is older than
`age_threshold`), and only to a better competing candidate. With no competitor it serves on.

**Hysteresis**. `min_dwell` counts from the tick the model was allowed, so it includes load
time. `idle_grace` is how long a model or host sits idle before it is unloaded or released.

**Drain**. How long in-flight work may run once a wind-down starts, before it is cut off and
the model withdrawn. It is `drain_timeout` for a planner switch, a manual model lease and a
graceful `off`; `lockout_timeout` (default `drain_timeout`) for a lockout; none for `off-now`.
A class's `kill_timeout` replaces the timeout for that class, except under a graceful `off`,
which always gets exactly `drain_timeout`.

## Leases and manual control

**Lease**. A durable claim recorded in Restate state, with an origin of `requested` (by a
person) or `adopted`. Kinds:
- **Shared hold**: a running consumer, such as a scheduled job, holds a shared resource.
- **Manual host lease**: a person's `on`. It keeps the host up. Releasing it does not shut the
  host down; the host then goes down as any other (the idle cause).
- **Manual model lease**: pins a model and cuts batch work short: intake stops and in-flight
  work is drained. It has no expiry; a person releases it.
- **Lockout**: see below.

**Lockout**. A person tells flowmanager to keep away from a host (shared or exclusive, with a
power section) despite pending work. Intake stops at once, in-flight work is drained, and
pending work stays queued. The host is shut down once (power actions only under managed
control). After that nothing wakes, adopts or schedules it until the person clears it, and a
host that boots again under the lockout is left alone. Clearing it while the host is up makes
it adopted; while it is off, the host is woken only if something wants it. Handlers:
`lockout`, `clear_lockout`.

**Manual actions**. On any host with a power section, shared or exclusive: `on`
(`acquire_host_lease`), `release`, `off`, `off-now`, and `resume`, which clears a stopped power
transition after a person fixed the host. `release` drops the lease with no shutdown request;
`off` and `off-now` are [shutdown](#shutdown) causes. HTTP: `/v1/resources/{resource}/<action>`;
lockouts: `/v1/lockouts/{resource}[/on|/off]`.

## Shutdown

**Causes**. *Idle*: nothing holds the host and `idle_grace` has passed. *Requested*: `off`
(graceful: in-flight work gets the drain) or `off-now` (immediate: consumers are withdrawn at
once). *Lockout*.

**Stages**.
1. **Withdrawing**: consumers leave the allowed set. On an exclusive host this is the model
   wind-down.
2. **Vetoing**: a fresh reading must show that nothing uses the host: no pods mounting an NFS
   host (`mounts`), no pods of the host's models (`pods`). An unknown reading counts as in
   use. Expiry of `veto_timeout` is a power timeout and the host stays up.
3. **Power-off**: the SSH forced command runs.
4. **Confirmed down**: the host is observed down.

**Cancelling**. Any hold arriving cancels an idle shutdown. A requested shutdown is cancelled
by a new manual lease, and a graceful one also by a waker appearing.

**Refusal**. A graceful `off` is refused while the host has wakers, and the refusal names
them. `off-now` is never refused; with wakers it warns that they will power the host back on
after `min_off`.

## Scheduled jobs

**Scheduled job** (`jobs[]`). A cron schedule (five fields, `timezone` default UTC) that
creates a Kubernetes Job from the `jobTemplate` of a referenced CronJob (`cronjob`).
- `needs`: currently `nfs: {resource}` only. A shared hold that keeps the host up while the run
  waits and runs, and wakes it first if it is managed and off.
- `retries`, with a `retry_backoff` that doubles after each retry.
- `missed_run`: `skip` (default) drops a run that could not start in time. `catch_up` runs it
  late, at most once, and only before the next scheduled time.
- `start_window`: how long after its time a run may still start.
- `deadline`: a longer run is alerted and counted as failed. The Job is not deleted.

## Decisions and durability

**Scheduler**. The Restate virtual object (key `main`) that runs ticks and holds state.
Restate is a durable-execution server: it keeps flowmanager's decisions, leases and in-progress
transitions, so they survive restarts.

**KEDA scaler**. Serves the allowed set to KEDA. After a restart it answers `Unavailable`
until the first tick, and a ScaledObject with no `fallback` keeps its replica count, so a
restart scales nothing.
