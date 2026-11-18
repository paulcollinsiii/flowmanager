# Concepts

The one place each term is defined. Config keys are in `code`; handler and API names are given
where a person calls them. A term used before its entry links to the section that defines it.

The scheduling knobs `min_dwell`, `max_dwell`, `idle_grace`, `age_threshold`, `drain_timeout`,
`lockout_timeout`, `host_reading_stale_after` and `queue_reading_stale_after` are keys of the
config's `policy` section, defined where used below.

## Hosts and power

**Resource** (`resources[]`). A host the scheduler knows about, with an id and a `kind`
(example names: `cuda-central`, a GPU node, and `basestar-nfs`, an NFS server). An optional
`power` section says how it can be switched; without one, flowmanager never powers it.

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
a person fixes the host and calls [`resume`](#leases-and-manual-control).

**Minimum off time** (`min_off`, default 0). After flowmanager confirms a shutdown it started,
it does not power the host on again by itself for this long. A manual lease overrides it.

**What wakes a host**. Only under managed control, and only when something wants it: a manual
host or model lease, a scheduled job's `nfs` hold, or a *waker*.

**Waker**. A class that would power a host on if it went down now: a class with
`may_change_power: true` that has queued or in-flight work, or a [switched class](#workloads)
reading `on`. A class without `may_change_power` waits for the host to be up.

**Ownership and adoption**. A host flowmanager powers on is flowmanager's, whatever wanted it,
and goes down by the [idle cause](#shutdown). Under managed control, a host something else
started is *adopted*: flowmanager takes an adopted manual host
[lease](#leases-and-manual-control) for the user. This covers a power button, a manual wake, a
host found running after state loss, and a host that came up after flowmanager's own power-on
timed out. The adopted lease ends when the host is seen down.

## Demand and observation

**Source** (`sources[]`). An observer adapter with a `kind` and the `observe` capabilities it
declares (only those are accepted from it). Kinds with an observer: `kubernetes_watch`,
`http_json`, `prometheus` and `tcp_probe`. A demand source may also have `reads_from`, naming
a [switched class](#workloads) whose app the source polls.

**Capabilities** (`observe`).
- `demand`: queue sizes of classes (`http_json`, `prometheus`).
- `reachability`: whether a host is up (all four kinds).
- `pods`: pod counts of models (`kubernetes_watch`), read only by recovery and the veto.
- `mounts`: how many pods mount an NFS host (`kubernetes_watch`).
- `jobs`: the state of Jobs flowmanager created (`kubernetes_watch`).
- `switch`: a switched class's reading (`http_json`).

**Staleness**. A reading older than its limit is *unknown*: `host_reading_stale_after` for
reachability, mounts, pods and job runs, `queue_reading_stale_after` for demand and switch.

**Demand**. The work waiting for a class, in three buckets: *ready* (can start), *in-flight*
(running) and *failed*. Failed items wait for the class's recovery and never age into
starvation priority. They make the model wanted but never hold the serving model.

**Unknown demand**. No usable reading. It is not zero: it holds the model and host as if work
existed, and it wakes nothing.

**Offline demand**. No work, and nothing held. It applies when the demand reading is unknown
and a required host is freshly seen down (a fresh reading wins), or when the switched class
the source `reads_from` is off and its graceful wait is over.

## Workloads

**Model** (`models[]`). A mode of an exclusive resource, implemented by one or more Kubernetes
workloads (`workloads`). `load_time` is the estimated load time, used as the cost of
switching. `namespaces` lists where the workloads run, for the veto.

**Workload class** (`classes[]`). A unit of demand with `requires` (resources, and for an
exclusive one optionally a model), a `priority`, a `preemptibility`, `may_change_power`, and
optionally `demand`, `switch`, `recovery` and `kill_timeout`. A class is *allowed* when every
requirement is met. Kinds:
- **Demand class**: names a model and a demand source. Its queue drives which model is chosen.
- **Gate class**: requires an exclusive resource with no model, which means only "that host is
  up". It has no demand and never influences model choice.
- **Plain class**: no demand, only shared resources. It runs whenever they are up.
- **Switched class**: hosts only, with a [switch](#workloads) instead of demand.

**Priority** (`priority`). Higher wins when choosing which ready model to load next. It never
orders the steps of one piece of work: that is *precedence* (OCR before tagging), handled by
readiness, so a step is not counted as ready until its upstream step is done.

**Preemptibility** (`preemptibility`). What a class's busy work does to a model switch the
planner wants.
- `none`: the planner does not switch away while the class is busy, with no timeout. A
  *forced* wind-down (a lockout, a requested `off` or `off-now`, or a manual model lease)
  skips that wait and goes straight to the [drain](#choosing-and-serving).
- `pause_intake`: the work is drained.
- `kill`: the same, for work that is safe to retry.

**Recovery** (`recovery: {action, source, params}`). The one action is `requeue_failed`: it
re-queues failed items through the service's API while the class's model is allowed, in its
own Restate object (`Recovery`) so it cannot delay a decision. The attempt cap is durable.

**Switch** (`switch: {source}`). The control of a switched class, an app a person turns on and
off, usually from Home Assistant (a home-automation server). The reading is `on`, `off` or
`off_now`; `off` and `off_now` are the [graceful and immediate](#shutdown) forms. A missing or
unknown reading, or one the source cannot map, is `off_now`, so the class fails closed. The
class is allowed while its hosts are up and the reading is `on`, or while a graceful `off`
waits, for at most `drain_timeout`, for its *dependents* to finish in-flight work or report
unknown demand. A dependent is a class whose demand source has
[`reads_from`](#demand-and-observation) the switched class. Once the class is off, its
dependents' demand is offline.

## Choosing and serving

**Tick**. One decision cycle: a snapshot, then the planner (pure), the sequencer (pure) and
publication. The **planner** decides what is wanted: which model next, which hosts to power.
The **sequencer** turns that into ordered, safe steps and the allowed set.

**Allowed set**. What each tick publishes: the allowed models (at most one per exclusive
resource) and the allowed classes. KEDA (the Kubernetes event-driven autoscaler, for which
flowmanager is the external scaler) scales a workload through a ScaledObject with an
`external` or `external-push` trigger naming `model: <id>` or `class: <id>`; it is active only
while that subject is allowed. Replicas are 0 or 1 because the ScaledObject sets
`maxReplicaCount: 1`.

**GPU exclusivity**. Enforced by Kubernetes (`nvidia.com/gpu: 1`, one GPU exposed, a tainted
node), not by flowmanager. On a switch the old model leaves and the new one enters the allowed
set in the same tick; the new pod stays Pending until the GPU is free.

**Dwell and idle grace**. `min_dwell` counts from the tick a model was allowed, so it includes
load time. A model with ready work keeps serving. It yields only when it has served for
`min_dwell` and either longer than `max_dwell` or another queue is *starved* (its oldest ready
item is older than `age_threshold`), and only to a better competing candidate; with none it
serves on. A model with no work of its own gives way only after it has been idle for
`idle_grace` and served for `min_dwell`, even if another model has ready work. `idle_grace` also
applies to a host: how long it sits idle before release.

**Drain**. When a wind-down of a model or host starts (a planner switch, a manual model
lease, a lockout, a requested shutdown) intake stops, and in-flight work runs for a bounded
time before it is cut off and the model withdrawn. The time is `drain_timeout`, or
`lockout_timeout` (default `drain_timeout`) for a lockout, or none for `off-now`. On an
exclusive host's model wind-down a class's `kill_timeout` replaces it for that class, except
under a graceful `off`, which always gets `drain_timeout`. A shared host's drain uses only
`drain_timeout` and `lockout_timeout`.

## Leases and manual control

**Lease**. A durable claim recorded in Restate state, with an origin of `requested` (by a
person) or `adopted`. Kinds:
- **Shared hold**: a running consumer, such as a scheduled job, holds a shared resource.
- **Manual host lease**: a person's `on`. It keeps the host up. Releasing it does not shut the
  host down; the host then goes down as any other (the idle cause).
- **Manual model lease**: pins a model and cuts batch work short with a drain. It has no
  expiry; a person releases it.
- **Lockout**: see below.

**Lockout**. A person tells flowmanager to keep away from a host (shared or exclusive, with a
power section) despite pending work, which stays queued. A drain starts at once. The host is
shut down once (power actions only under managed control). After that nothing wakes, adopts
or schedules it until the person clears it, and a host that boots again under the lockout is
left alone. Clearing it while the host is up makes it adopted; while it is off, the host is
woken only if something wants it. Handlers: `lockout`, `clear_lockout`.

**Manual actions**. On any host with a power section, shared or exclusive: `on`
(`acquire_host_lease`), `release` (drops the lease, with no shutdown request), `off` and
`off-now` ([shutdown](#shutdown) requests, which apply only to a managed host that is up and
not locked out), and `resume`, which clears a stopped power transition after a person fixed the
host. HTTP: `/v1/resources/{resource}/<action>`; lockouts: `/v1/lockouts/{resource}[/on|/off]`.

## Shutdown

**Causes**. *Idle*: nothing holds the host and `idle_grace` has passed. *Requested*: `off` or
`off-now`. *Lockout*.

**Graceful and immediate**. A graceful shutdown gives in-flight work the drain. An immediate
one withdraws the consumers at once. A graceful `off` is refused while the host has wakers, and
the refusal names them. `off-now` is never refused; with wakers it warns that they will power
the host back on after `min_off`.

**Stages**.
1. **Withdrawing**: consumers leave the allowed set. On an exclusive host this is the model
   wind-down.
2. **Vetoing**: a fresh reading must show that nothing uses the host: no pods mounting an NFS
   host (`mounts`), no pods of the host's models (`pods`). An unknown reading counts as in
   use. Expiry of `veto_timeout` is a power timeout and the host stays up.
3. **Power-off**: the SSH forced command runs.
4. **Confirmed down**: the host is observed down.

**Cancelling**. Any hold arriving cancels an idle shutdown. A new manual lease cancels a
requested one, and a waker appearing also cancels a graceful one.

## Scheduled jobs

**Scheduled job** (`jobs[]`). A cron schedule (five fields, `timezone` default UTC) that
creates a Kubernetes Job from the `jobTemplate` of a referenced CronJob (`cronjob`).
- `needs`: a list of `{ type: nfs, resource: <id> }`; `nfs` is the only need type. It is a
  shared hold that keeps the host up while the run waits and runs, and wakes it first if it is
  managed and off.
- `retries`, with a `retry_backoff` that doubles after each retry.
- `missed_run`: `skip` (default) drops a run that could not start in time. `catch_up` runs it
  late, at most once, and only before the next scheduled time.
- `start_window`: how long after its time a run may still start.
- `deadline`: a longer run is alerted and counted as failed. The Job is not deleted.

## Decisions and durability

**Scheduler**. The Restate virtual object (key `main`) that runs ticks. Restate is a
durable-execution server: it keeps decisions, leases and in-progress transitions across
restarts.

**KEDA scaler**. Serves the allowed set to KEDA. After a restart it answers `Unavailable`
until the first tick, and a ScaledObject with no `fallback` keeps its replica count.
