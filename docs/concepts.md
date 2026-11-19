# Concepts

The one place each term is defined. Config keys are in `code`; handler and API names are given
where a person calls them. A term used before its entry links to the section that defines it.

The scheduling knobs (`min_dwell`, `max_dwell`, `idle_grace`, `age_threshold`, `drain_timeout`,
`lockout_timeout`, `host_reading_stale_after`, `queue_reading_stale_after`) are keys of the
config's `policy` section, defined where used below.

## Hosts and power

**Resource** (`resources[]`). A host the scheduler knows about, with an id and a `kind`
(example names: `cuda-central`, a GPU node, and `basestar-nfs`, an NFS server). An optional
`power` section says how it can be switched; without one, flowmanager never powers it.

**Exclusive resource** (`kind: exclusive`). Holds at most one model at a time, such as the GPU
node. **Shared resource** (`kind: shared`). Any number of holders, reference counted: an NFS
server, or an always-on node.

**Power control** (`power.control`). Who changes a host's power.
- `manual`: a person does. flowmanager never emits a power step for the host, its
  state follows observed reachability, and work that needs it waits until it is up.
- `managed`: flowmanager powers it on with `power.wake_on_lan` (the OPNsense router API sends
  the magic packet) and off with `power.shutdown` (SSH, forced-command key, pinned host key).
  Both are required.

**Power timeouts** (`power_on_timeout`, `shutdown_timeout`, `veto_timeout`). How
long a managed host may take to be observed up, to be observed down, or to pass the
[veto](#shutdown). When one expires flowmanager raises an alert and stops. It never retries;
a person fixes the host and calls [`resume`](#leases-and-manual-control).

**Minimum off time** (`min_off`). After flowmanager confirms a shutdown it started,
it does not power the host on again by itself for this long. A manual lease overrides it.

**Waker**. A class with `may_change_power: true` that requires a host and is busy, or is a
switched class reading `on`: it would power the host back on. See
[Wakers](architecture/power.md#wakers).

**What wakes a host**. Only under managed control: a manual lease, a scheduled job's `nfs`
hold, or a waker. See [Ownership and adoption](architecture/power.md#ownership-and-adoption).

**Ownership and adoption**. A host flowmanager powers on is its own and goes down when idle. A
host something else started is *adopted*: flowmanager takes a manual host lease for the user.
See [Ownership and adoption](architecture/power.md#ownership-and-adoption).

## Demand and observation

**Source** (`sources[]`). An observer adapter with a `kind` and the `observe` capabilities it
declares (only those are accepted from it). Kinds with an observer: `kubernetes_watch`,
`http_json`, `prometheus` and `tcp_probe`. A demand source may also have `reads_from`, naming
a [switched class](#workloads) whose app the source polls.

**Capabilities** (`observe`).
- `demand`: queue sizes of classes (`http_json`).
- `reachability`: whether a host is up (`kubernetes_watch`, `tcp_probe`, `prometheus`).
- `pods`: pod counts of models (`kubernetes_watch`), read only by recovery and the veto.
- `mounts`: how many pods mount an NFS host (`kubernetes_watch`).
- `jobs`: the state of Jobs flowmanager created (`kubernetes_watch`).
- `switch`: a switched class's reading (`http_json`).

**Staleness**. A reading older than its limit is *unknown*: `host_reading_stale_after` for
reachability, mounts, pods and job runs, `queue_reading_stale_after` for demand and switch.

**Demand**. The work waiting for a class, in three buckets: *ready* (can start), *in-flight*
(running) and *failed*. Failed items wait for the class's recovery and never age into
[starvation priority](#workloads). They make the model wanted but never hold the serving model.

**Unknown demand**. No usable reading. It is not zero: it holds the model and host as if work
existed, and it wakes nothing. Two cases are not unknown: [offline demand](#demand-and-observation)
(next), and a source that can never report, which counts as no demand.

**Offline demand**. No work, and nothing held. It applies when the demand reading is unknown
and a required host is freshly seen down (a fresh reading wins), or when the switched class
the source `reads_from` is off and its graceful wait is over.

## Workloads

**Model** (`models[]`). A mode of an exclusive resource, implemented by one or more Kubernetes
workloads (`workloads`). `load_time` is its estimated load time, used only as a tie-break
between candidates (see [Priority](#workloads)). `namespaces` lists where the workloads run, for the veto.

**Workload class** (`classes[]`). A unit of demand with `requires` (resources, and for an
exclusive one optionally a model), a `priority`, a `preemptibility`, `may_change_power`, and
optionally `demand`, `switch`, `recovery`, `kill_timeout` and `allowed`. A class is *allowed* when every
requirement is met. Kinds:
- **Demand class**: names a model and a demand source. Its queue drives which model is chosen.
- **Gate class**: requires an exclusive resource with no model, meaning only "that host is
  up". No demand; it never influences model choice.
- **Plain class**: no demand, only shared resources. It runs whenever they are up.
- **Switched class**: hosts only, with a [switch](#workloads) instead of demand.

A demand class with `allowed: with_work` is allowed only while it has
work (ready, in-flight or failed items, or an unknown count; offline demand is none) or within
`idle_grace` after a tick last saw it with work. The workloads that scale on it then follow its
own queue rather than its model, which may stay allowed for other classes. With
`idle_grace: 0` there is no linger: the class is allowed only on ticks that see it with work.

**Priority** (`priority`). A model's priority is the highest among its classes with ready (or
failed) work, and the higher tier wins. A *starved* model, whose oldest ready item is older
than `age_threshold`, outranks every tier. Within a tier the longest-waiting item wins, then
the lower `load_time`, then the lower id. Priority never orders the steps of one piece of
work: that is *precedence* (OCR before tagging), handled by readiness, so a step is not
counted as ready until its upstream step is done.

**Preemptibility** (`preemptibility`). What a class's busy work does to a model switch the
planner wants.
- `none`: the planner does not switch away while the class is busy, with no timeout. A
  [forced wind-down](#choosing-and-serving) skips that wait and goes straight to the drain.
- `pause_intake`: the work is drained. `kill`: the same, for work that is safe to retry.

**Recovery** (`recovery: {action, source, params}`). The one action is `requeue_failed`: it
re-queues failed items through the service's API while the class's model is allowed, in its
own Restate object (`Recovery`) so it cannot delay a decision.

**Switch** (`switch: {source}`). The control of a switched class, an app a person turns on and
off, usually from Home Assistant (a home-automation server). The reading is `on`, `off` or
`off_now`; `off` and `off_now` are the [graceful and immediate](#shutdown) forms. A missing or
unknown reading, or one the source cannot map, is `off_now`, so the class fails closed. The
class is allowed while its hosts are up and the reading is `on`, or during a graceful wait.
The wait opens when `off` is first read, only if every required host is up, and lasts while
a *dependent* has in-flight or unknown demand, at most `drain_timeout`. A dependent is a class
whose demand source has [`reads_from`](#demand-and-observation) the switched class. Once the
class is off, its dependents' demand is offline.

## Choosing and serving

**Tick**. One decision cycle: a snapshot, then the planner (pure), the sequencer (pure) and
publication. The **planner** decides what is wanted: which model next, which hosts to power.
The **sequencer** turns that into safe steps and the allowed set, and waits for a down host.
What triggers a tick is in [Tick and events](architecture.md#tick-and-events).

**Allowed set**. What each tick publishes: the allowed models (at most one per exclusive
resource) and the allowed classes. KEDA (the Kubernetes event-driven autoscaler, for which
flowmanager is the external scaler) scales a workload through a ScaledObject with an
`external` or `external-push` trigger naming `model: <id>` or `class: <id>`; it is active only
while that subject is allowed. Replicas are 0 or 1 because the ScaledObject sets
`maxReplicaCount: 1`.

**GPU exclusivity**. Kubernetes, not flowmanager, keeps one model on the GPU: invariant 2 in
[Safety invariants](architecture.md#safety-invariants). A busy old model is drained first.

**Dwell and idle grace**. `min_dwell` counts from the tick a model was allowed, so it includes
load time. A model with ready work keeps serving. It yields only when it has served for
`min_dwell` and either longer than `max_dwell` or another queue is starved, and only to a
better competing candidate; with none it serves on. A model with no work of its own gives way
only after it has been idle for `idle_grace` and served for `min_dwell`, even if another
model has ready work. This is intended: it prevents the GPU thrashing between models.

**Wind-down and drain**. A wind-down removes a model or host's *consumers* (the classes that
use it) from the allowed set. It starts for a planner switch or, *forced*, for a [manual model
lease, lockout](#leases-and-manual-control) or requested shutdown. Intake stops, and in-flight
work runs for a bounded time, the drain, before it is cut off. The time depends on the cause
and host kind: see [Timeouts](architecture/power.md#timeouts).

## Leases and manual control

**Lease**. A durable claim recorded in Restate state, with an origin of `requested` (by a
person) or `adopted`. Every lease except a lockout is a *hold*: it keeps its host up. Kinds:
- **Shared hold**: a running consumer, such as a scheduled job, holds a shared resource.
- **Manual host lease**: a person's `on`. Releasing it does not shut the host down; the host
  then goes down as any other (the idle cause).
- **Manual model lease**: pins a model and cuts batch work short with a drain. It has no
  expiry; a person releases it.
- **Lockout**: see below.

**Lockout**. A person tells flowmanager to keep away from a host despite pending work: it is
shut down once, and nothing wakes, adopts or schedules it until the lockout is cleared.
Handlers: `lockout`, `clear_lockout`. See [Actions](architecture/power.md#actions).

**Manual actions**. What a person can do to a host: `on`, `release`, `off`, `off-now`,
`resume` and lockout, defined in [Actions](architecture/power.md#actions). HTTP:
`/v1/resources/{resource}/<action>`; lockouts: `/v1/lockouts/{resource}[/on|/off]`.

## Shutdown

**Causes**. *Idle*: nothing holds the host and `idle_grace` has passed. *Requested*: `off` or
`off-now`. *Lockout*. See [State](architecture/power.md#state).

**Graceful and immediate**. A graceful shutdown gives in-flight work the drain; an immediate
one withdraws the consumers at once. See [Actions](architecture/power.md#actions).

**Veto**. A fresh reading, before power-off, that nothing uses the host; an unknown reading
counts as in use. See [Veto](architecture/power.md#veto).

**Stages**: withdrawing (the wind-down), vetoing, power-off (SSH), then confirmed down. See
[Lifecycle](architecture/power.md#lifecycle).

## Scheduled jobs

**Scheduled job** (`jobs[]`). A cron schedule (five fields, `timezone` default UTC) that
creates a Kubernetes Job from the `jobTemplate` of a referenced CronJob (`cronjob`).
- `needs`: a list of `{ type: nfs, resource: <id> }`; `nfs` is the only need type. It is a
  shared hold while the run waits and runs.
- `retries`, with a `retry_backoff` that doubles after each retry.
- `missed_run`: `skip` (default) or `catch_up`.
- `start_window`: how long after its time a run may still start. Past it, `skip` drops the
  run; `catch_up` alerts and keeps waiting, running it late at most once and only before the
  next scheduled time.
- `deadline`: a longer run is alerted and counted as failed. The Job is not deleted.

## Decisions and durability

**Scheduler**. The Restate virtual object (key `main`) that runs ticks. Restate is a
durable-execution server: it keeps decisions, leases and in-progress transitions across
restarts.

**KEDA scaler**. Serves the allowed set to KEDA. After a restart it answers `Unavailable`
until the first tick, and a ScaledObject with no `fallback` keeps its replica count.
