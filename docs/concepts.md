# Concepts

The one place each term is defined. Config keys are in `code`; handler and API names are given
where a person calls them.

- [Hosts and power](#hosts-and-power)
- [Workloads](#workloads)
- [Demand and observation](#demand-and-observation)
- [Choosing and serving](#choosing-and-serving)
- [Leases and manual control](#leases-and-manual-control)
- [Shutdown](#shutdown)
- [Scheduled jobs](#scheduled-jobs)
- [Decisions and durability](#decisions-and-durability)

## Hosts and power

**Resource** (`resources[]`). A host the scheduler knows about, with an id and a `kind`. An
optional `power` section says how it can be switched. Without one, flowmanager never powers it.

**Exclusive resource** (`kind: exclusive`). A resource that holds at most one model at a time,
such as the GPU node.

**Shared resource** (`kind: shared`). A resource with any number of holders, reference counted:
an NFS host, or an always-on node.

**Power control** (`power.control`). Who changes a host's power.
- `manual` (default): a person does. flowmanager never emits a power step for the host. Its
  power phase follows observed reachability, and work that needs it waits until it is up.
- `managed`: flowmanager powers it on and off. It needs both `wake_on_lan` and `shutdown`.

**Wake-on-LAN** (`power.wake_on_lan`). Power-on: the OPNsense router API sends the magic
packet.

**Forced-command shutdown** (`power.shutdown`). Power-off: an SSH connection with a
forced-command key, to a host key that is pinned.

**Power timeouts** (`power_on_timeout` 10m, `shutdown_timeout` 15m, `veto_timeout` 30m). How
long a managed host may take to come up, go down, or pass the [veto](#shutdown). When one
expires, flowmanager raises an alert and stops. It never retries; a person fixes the host and
calls `resume`.

**Minimum off time** (`min_off`, default 0). After flowmanager confirms a shutdown it started,
it does not power the host on again by itself for this long. A manual lease overrides it.

**Waker**. A class that would power a host on if it went down now: a class with
`may_change_power: true` that has queued or in-flight work, or a [switched class](#workloads)
reading `on`. A host's wakers matter only under managed control. Unknown demand wakes nothing.
A class without `may_change_power` waits for the host to be up.

**Ownership and adoption**. A host flowmanager powers on is flowmanager's, whatever wanted it,
and it goes down once nothing holds it and `idle_grace` has passed. A host that something else
started is *adopted*: flowmanager takes an adopted manual host [lease](#leases-and-manual-control)
for the user. This covers a power button, a manual wake, a host found running after state loss,
and a host that came up after flowmanager's own power-on timed out. The adopted lease ends when
the host is seen down.

## Workloads

**Model** (`models[]`). A mode of an exclusive resource, implemented by one or more Kubernetes
workloads (`workloads`). `load_time` is the estimated load time, used as the cost of switching.
`namespaces` lists where the workloads run, for the pods veto.

**Workload class** (`classes[]`). A unit of demand. It has `requires` (resources, and for an
exclusive one optionally a model), a `priority`, a `preemptibility`, `may_change_power`, and
optionally `demand`, `switch`, `recovery` and `kill_timeout`. A class is *allowed* when every
requirement is met. Kinds of class:
- **Demand class**: names a model and a demand source. Its queue drives which model is chosen.
- **Gate class**: requires an exclusive resource with no model, which means only "that host is
  up". It has no demand and never influences model choice.
- **Plain class**: no demand, only shared resources. It runs whenever they are up.
- **Switched class**: requires hosts only and has `switch: {source}` instead of demand. See
  [switch](#switch) below.

**Priority** (`priority`). Higher wins when choosing which ready model to load next. It is
never used to order steps of one piece of work.

**Precedence**. The order of steps for one piece of work, such as OCR before tagging. It is
handled by readiness: a step is not counted as ready until its upstream step is done.

**Preemptibility** (`preemptibility`). What happens to a class's in-flight work when another
model is wanted.
- `none`: never interrupted while it has work. The wait has no timeout.
- `pause_intake`: intake pauses, in-flight work gets `drain_timeout` (or the class's
  `kill_timeout`), then is cut off.
- `kill`: the same, for work that is safe to retry.

**Recovery** (`recovery: {action, source, params}`). The one action is `requeue_failed`, which
re-queues failed items through the service's API while the class's model is allowed. It runs
in its own Restate object, `Recovery`, so it cannot delay a decision. The per-item attempt cap
is durable.

**Switch** (`switch: {source}`). The control of a switched class: an app a person turns on and off, usually from Home Assistant. The
reading is `on`, `off` (graceful) or `off-now` (immediate). A missing, stale or unmapped
reading is `off-now`, so the class fails closed. The class is allowed while its hosts are up
and the reading is `on`, or while a graceful `off` waits for in-flight work of its
*dependents*, at most `drain_timeout`. A dependent is a class whose demand source has
`reads_from: <switched class>`. Once the class is off, its dependents' demand is
[offline](#demand-and-observation).

## Demand and observation

**Demand**. The work waiting for a class, in three buckets: *ready* (can start), *in-flight*
(running) and *failed*. Failed items wait for the class's recovery and never age into
starvation priority. They make the model wanted but never hold the serving model.

**Unknown demand**. No usable reading: the source is missing or its reading is stale. Unknown
is not zero. It holds the model and host as if work existed, and it wakes nothing.

**Offline demand**. A fresh reading shows a required host is down, or the source's
`reads_from` class is switched off. There is no work, and the class holds nothing.

**Source** (`sources[]`). An observer adapter with a `kind` and the `observe` capabilities it
declares. Only declared capabilities are accepted from it. Kinds with an observer:
`kubernetes_watch` (pods, reachability, mounts, jobs), `http_json` (reachability, demand,
switch), `prometheus` (reachability, demand) and `tcp_probe` (reachability).

**Capabilities** (`observe`). What a source reports:
- `demand`: queue sizes of classes.
- `reachability`: whether a host is up.
- `pods`: pod counts of models.
- `mounts`: how many pods mount an NFS host.
- `jobs`: the state of Jobs flowmanager created.
- `switch`: a switched class's `on`, `off` or `off_now`.

**Staleness**. A reading older than its limit counts as unknown. Safety-critical readings
(reachability, mounts, pods) must be fresher than `host_reading_stale_after`. Scheduling inputs
(demand, switch) must be fresher than `queue_reading_stale_after`.

**Webhook listener** (`--webhook-listen`). An HTTP endpoint on which a service can nudge an
`http_json` source to poll now, instead of waiting for its `resync`.

**Snapshot**. The observations, leases and state a decision reads. Each tick plans from one
snapshot.

## Choosing and serving

**Tick**. One decision cycle: snapshot, then planner (pure), then sequencer (pure), then
publication. The pure crates do no I/O.

**Planner**. Decides what is wanted: which model next, which hosts to power. **Sequencer**.
Turns that into ordered, safe steps and the allowed set, and carries transitions such as
withdrawals and shutdowns.

**Allowed set**. What each tick publishes: the allowed models (at most one per exclusive
resource) and the allowed classes. A KEDA ScaledObject with an `external` or `external-push`
trigger naming `model: <id>` or `class: <id>` is active only while that subject is allowed.
Replicas are 0 or 1.

**GPU exclusivity**. Enforced by Kubernetes (`nvidia.com/gpu: 1`, one GPU exposed, a tainted
node), not by flowmanager. On a switch the old model leaves and the new one enters the allowed
set in the same tick, and the new pod stays Pending until the old one frees the GPU.
flowmanager decides from queues, leases and policy, not pod state. The one exception is the
shutdown veto.

**Exhaustive, bounded service**. A loaded model keeps serving while it has ready work, bounded
by `max_dwell` or by another queue's oldest ready item exceeding `age_threshold`.

**Hysteresis**. `min_dwell` is the least time a model stays allowed, counted from the tick it
was allowed, so it includes load time. `idle_grace` is how long a model or host sits idle
before it is unloaded or released.

**Drain** (`drain_timeout`). How long in-flight work may run after a manual model lease cuts
batch work short, or after the planner wants another model, before it is cut off.

## Leases and manual control

**Lease**. A durable claim recorded in Restate state. Its origin is `requested` (by a person)
or `adopted`. Kinds:
- **Shared hold**: a running consumer, such as a scheduled job, holds a shared resource.
- **Manual host lease**: a person's `on`. It always keeps the host up. Releasing it does not
  shut the host down by itself; the host goes down like any other once nothing holds it and
  `idle_grace` has passed.
- **Manual model lease**: pins a model. It cuts batch work short: intake stops and in-flight
  work finishes or is stopped after `drain_timeout`. It has no expiry and a person releases it.
- **Lockout**: see below.

**Lockout**. A person tells flowmanager to keep away from a host (shared or exclusive, with a
power section) despite pending work. Intake stops at once. In-flight work finishes or is
stopped after `lockout_timeout` (default `drain_timeout`). Pending work stays queued. The host
is shut down once (power actions only under `managed`). After that nothing wakes, adopts or
schedules it until the person clears it, and if it boots again under the lockout it is left
alone. Clearing it while the host is up makes it adopted. Clearing it while it is off wakes it
only if something wants it. Handlers: `lockout`, `clear_lockout`.

**Manual actions**. On any host with a power section: `on` (`acquire_host_lease`), `release`,
`off`, `off-now`, `lockout` and `resume`. `off` and `off-now` use the same verbs for shared and
exclusive hosts. `resume` clears a stopped power transition after a person fixed the host.
These are also available as HTTP calls on the toggle API (`/v1/resources/{resource}/...`).

## Shutdown

**Causes**. *Idle*: nothing holds the host and `idle_grace` has passed. *Requested*: `off`
(graceful) or `off-now` (immediate). *Lockout*.

**Stages**.
1. **Withdrawing**: consumers leave the allowed set. On an exclusive host this is the model
   wind-down. Graceful waits for in-flight work up to the timeout; immediate does not.
2. **Vetoing**: a fresh reading must show that nothing uses the host. No pods may mount an NFS
   host (`mounts`), and no pods of the host's models may exist (`pods`). An unknown reading
   counts as in use. On `veto_timeout` flowmanager stops and alerts, and the host stays up.
3. **Power-off**: the SSH forced command runs.
4. **Confirmed down**: the host is observed down.

**Cancelling**. Any hold arriving cancels an idle shutdown. A requested shutdown is cancelled
by a new manual lease, and a graceful one also by a waker appearing.

**Refusal**. A graceful `off` is refused while the host has wakers, and the refusal names them.
`off-now` is never refused; if the host has wakers it warns that they will power the host back
on after `min_off`.

## Scheduled jobs

**Scheduled job** (`jobs[]`). A cron schedule (five fields, `timezone` default UTC) that
creates a Kubernetes Job from the `jobTemplate` of a referenced CronJob (`cronjob`).
- `needs`: currently `nfs: {resource}` only. It is a shared hold that keeps the host up while
  the run waits and runs. If the host is managed and off, it is woken first.
- `retries` with a `retry_backoff` that doubles after each retry.
- `missed_run`: `skip` (default) drops a run that could not start in time. `catch_up` keeps
  waiting and runs it late.
- `start_window`: how long after its time a run may still start.
- `deadline`: a longer run is alerted and counted as failed. The Job is not deleted.

## Decisions and durability

**Scheduler**. The Restate virtual object (key `main`) that runs ticks and holds state.
Decisions, leases and in-progress transitions are Restate state, so they survive restarts.

**KEDA scaler**. The gRPC external scaler that serves the allowed set. After a restart it
answers `Unavailable` until the first tick, and a ScaledObject with no `fallback` then keeps
its replica count, so a restart scales nothing.
