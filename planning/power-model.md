# Host power model: plan

Status: **decisions complete; all five phases built on the branch `refactor/power-model`** (based on main @ 689ab82; phase 1 documents, phases 2, 3, 4a, 4b and 5 code). Phase 5 (one derived `HostStatus`, section 2.4) is done and awaits review. The code behaves as sections 2 and 3 describe; section 5 is the one hand-off to the homesvr agent.
All decisions are recorded in section 3 (2026-10-30).

This document is the single description of how flowmanager owns and changes a host's
power. It exists because five features (adoption, drain on release, `drain_host`, idle
drain, exclusive host lease) were added one at a time and each was reviewed only against
itself. Accepted on 2026-10-30, it replaces the power parts of `architecture.md` and is the
model that every later power change is reviewed against. Section 1 is a short note on what
the refactor replaced; sections 2 and 3 are the model and the decisions, as built.

## 1. What the refactor replaced

Only hosts with a `power` section under `control: managed` take part; under `manual` the
phase just follows observation and nothing below applies.

Before the refactor, one lifecycle was encoded in six loosely related fields of
`ResourceState` (`power`, a `managed` flag, `host_idle_since`, a `drain` with an origin, a
`mounts_veto`, `shutdown_confirmed_at`), ownership was held in two places (the flag and an
adopted lease recognised by an id prefix, minted by the service), the consumers were
withdrawn by two separate mechanisms (the exclusive mode wind-down in `exclusive.rs` and the
shared host drain in the service's `power.rs`), only the NFS mounts were checked before a
power-off (cuda-central relied on the `idle_grace` margin for its GPU pods to be gone), and
every output (report fields, reason text, metrics, the toggle JSON) was rebuilt by diffing
state in its own place. The shapes that replaced them are in 2.2; where each output now comes
from is in 2.4.

What wants a host up has not changed (`intent.power_up`, planner `fifo.rs`): a `ManualHost` or
`ManualModel` lease (`Manual`), a job's `SharedHold` and a busy `may_change_power` class
(`Workload`). A reclaim, now a lockout, removes the want.

## 2. The model

### 2.1 One lifecycle

```
             want (lease, hold, workload)            fresh "up"
   Off ─────────────────────────────▶ PoweringOn ─────────────▶ On
    ▲   (waits min_off unless Manual)      │ timeout                │
    │                                      ▼                        │ nothing holds it and
    │                                   Failed ◀──────┐             │ idle_grace over,
    │                                                 │ timeout     │ or a person asks for off
    │      fresh "down"                               │             ▼
    └──────────────── ShuttingDown ◀── Vetoing ◀── Withdrawing ◀────┘
                                          │
                                          └─ timeout ─▶ Stopped (stays up, alerts)
```

- **Withdrawing**: the host's consumers leave the allowed set. On an exclusive host this
  *is* the mode wind-down; on a shared host it is scaling the classes that need it down.
  It has one bound: graceful = wait for in-flight work up to the timeout, immediate = at once.
- **Vetoing**: a fresh reading must show nothing still uses the host: mounting pods for an
  NFS host, the models' pods for a GPU host. Unknown counts as "in use". Bounded; on expiry
  flowmanager stops and alerts (today's `mounts_timeout` behaviour, generalised).
- **Any hold arriving** during Withdrawing or Vetoing cancels an *idle* shutdown. A
  *requested* shutdown is cancelled only by a new manual lease (as today).

### 2.1b Ownership: who started it decides

- **A host flowmanager powers on is flowmanager's**, whatever wanted it (a scheduled job's
  hold, a workload, a manual lease). It goes down when nothing holds it any more, after
  `idle_grace`, through the shutdown stages. A backup that woke basestar therefore lets it
  power down when the job ends. A short blip down and back does not change this.
- **A host something else started is adopted**: a power button, a manual Wake-on-LAN, a
  running host first seen after flowmanager lost its state, or one that came up after
  flowmanager's own power-on timed out (an alert is already out, so a person decides). An
  adopted host is held by the user through an adopted lease (D3) until released or switched
  off.
- **A manual lease always keeps a host up**, on top of anything else. Taking one while a
  backup runs keeps basestar up after the job.
- **Under a lockout nothing is adopted or woken** (2.3).

Tests that must exist after phase 4: a job-woken host shuts down after the job with no
lease; the same host with a manual lease taken mid-job stays up; a host found running while
a job holds it is adopted and stays up after the job (D6). They exist since 4b, end to end
through the `Scheduler` tick with the real job machine
(`crates/sched-service/src/jobs/scenarios/ownership.rs`).

### 2.2 State as types, not flags

The types as built (`crates/sched-model/src/state.rs`, trimmed to the fields):

```rust
pub struct ResourceState {
    pub power: PowerPhase,
    pub mode: ModePhase,                       // exclusive model: idle / serving (+ wind_down)
    pub idle_since: Option<Millis>,            // the idle_grace clock
    pub pending_recovery: BTreeSet<ClassId>,
    pub shutdown: Option<Shutdown>,            // only while On, or stopped on the way
    pub off_since: Option<Millis>,             // for min_off; only while Off after our shutdown
    pub locked_down: bool,                     // see below
}
pub enum PowerPhase {
    Off { ours: bool },
    PoweringOn { since: Millis },
    On { since: Millis, ours: bool },
    ShuttingDown { since: Millis },
    Failed { since: Millis, during: PowerTransition },
}
pub struct Shutdown {                          // fields private; every transition is a method
    cause: ShutdownCause, since: Millis, withdrawal: Withdrawal, veto: Veto,
}
pub enum ShutdownCause { Idle, Requested { mode: DrainMode }, Lockout }
pub enum Withdrawal { Pending, Done, NotNeeded }
pub enum Veto { None, Vetoing { since: Millis }, Stopped { since: Millis } }
```

The two axes of a `Shutdown` (withdrawal, then veto) replace the single stage an earlier
sketch had, so a combination that cannot happen cannot be built; the combinations that exist
are tabled on `Shutdown`. The 2.1 diagram's Withdrawing, Vetoing and Stopped are those axes read
as one path.

**`locked_down: bool`** (per resource, default false): the host was
seen off while it was locked out, so the lockout has done its one shutdown (2.3). It is set
when the host is seen off under a lockout (also a lockout taken while the host is already
off), cleared the tick no lockout applies, and never set under `control: manual`. While it is
set and the lockout stands, a host that is up is left alone: not powered off, not adopted
(`adopt::found_running` also skips any host with an active lockout lease, on either kind), not
given work. It is a bool beside the `Shutdown` rather than a `Shutdown` stage because it
outlives the shutdown (which ends when the host is seen off) and applies to a host that is up
with no shutdown at all.

**The `managed` flag shrinks to a marker on `On` and `Off`.** An earlier draft said it goes
away entirely; that traced only its writes and was wrong. Its reads (checked at 4de1266) show
one fact the ownership rule (2.1b) needs and nothing else records: whether a host is
flowmanager's. Three cases rest on it:

- a host flowmanager woke that blips down and back is still flowmanager's, not found running,
  so the fact must survive while the host is off;
- a host switched from `control: manual` to `managed` while it is up is found running, not
  flowmanager's to power down when idle;
- a host whose stopped shutdown is cleared by `resume` is found running.

So `PowerPhase` becomes `Off { ours: bool }` and `On { since, ours: bool }`, and the
free-standing `managed` field goes. It cannot exist on `PoweringOn`, `ShuttingDown` or
`Failed`, and no transition has to remember to clear it there. `ours` is set by flowmanager's
own power-on and by a manual want. It is carried across a blip (`On` to `Off` to `On`) and false
otherwise: after a confirmed shutdown, after a failed transition, under manual control, on a
stopped veto, and when an adopted lease ends because the host went down. "Is this host found
running?" then reads directly as "up, under managed control, and not `ours`", instead of
guessing from a combination of other fields.

*As built in 4b.* The stored shape is `{"phase":"off","ours":false}` and
`{"phase":"on","since":N,"ours":true}` (always written). A hand-written state, such as a
scenario file, may omit `ours`: it defaults to `false`, which is what omitting the old
`managed` meant, so a host that must be ours says `ours: true` inside its `power` object.
`powering_on`, `shutting_down` and `failed` refuse the key. `powering_on` seen up gives `on`
ours; a blip keeps it; `shutting_down` seen down, a failed phase cleared either way and manual
control give not ours; the veto timeout and the locked-down branch clear it; a manual want and
a person's drain request set it. An adopted lease ended by the host going down clears `ours` and
the idle clock in the previous state the tick decides from. `found_running` is: up, a power
section under managed control, not `powering_on` or ours, no shutdown stopped by the veto, no
manual host lease yet, no active lockout lease.

### 2.3 One vocabulary for both kinds

| Action | Meaning on any managed host |
| --- | --- |
| `on` | take a manual host lease (power on if off) |
| `release` | drop the lease; the host goes down when idle like any other |
| `off` | requested shutdown, graceful |
| `off-now` | requested shutdown, immediate |
| `resume` | clear a `Stopped` shutdown or a failed power transition |
| `lockout` on / off | see below (replaces reclaim) |

On an exclusive host, Withdrawing is the mode wind-down, so `off`/`off-now` cost nothing
new.

**Lockout** (D1) means "flowmanager, keep away from this host until I clear it". It forces the
withdrawal (in-flight work is cut off after `lockout_timeout`, today's `reclaim_timeout`),
shuts the host down, and then nothing wakes or schedules it: no workload, job hold or
manual lease. It is valid on any managed host. While locked out the host may be up running
something else (another OS, physical work on it); flowmanager neither adopts it nor powers
it off. Clearing the lockout returns the host to the normal model: it is woken only if
something wants it. On basestar it deliberately blocks the backup job (for a physical move,
say): a due run does not start while the host is locked out, up or not (it waits as for a host that is not up, so its `missed_run` rules apply); a run already in progress is not killed and keeps its hold, which the lockout overrides (the veto waits for its pods, bounded by `veto_timeout`). `off-now` differs in that anything may wake the host again afterwards.

**A lockout takes over a shutdown in progress**, on either kind of host. The shutdown becomes
the lockout's (cause `lockout`): a withdrawal still pending gives in-flight work
`lockout_timeout` counted from the takeover, whatever cause the shutdown had; a withdrawal
already done stays done. An `immediate` requested shutdown whose withdrawal is still pending
stays the person's. Because the shutdown is the lockout's, clearing the lockout drops it: a
person's earlier `off` does not outlive the lockout.

**A due job under a lockout.** The job does not start, the host up or not. Its run waits as
for a host that is not up, and keeps its own hold (`hold-job-<id>-<resource>`): the hold is
inert while the lockout stands (nothing is woken for it, and the lockout overrides it). When
the lockout clears, the hold is an ordinary job hold again: it may wake a host that is down so
the run can catch up (per the job's `missed_run` rules), after which the host is flowmanager's
(2.1b) and powers down when the job is done. A run already running when the lockout arrives
is not killed and keeps its hold until it ends.

### 2.4 Who decides what (built)

- **Adoption** is a pure ownership decision in the sequencer (`sched_sequencer::adopt::adopt`,
  phase 4b), and still results in an adopted lease with a typed `origin: adopted` (D3), so
  `/v1/leases` and Home Assistant see it. It no longer depends on whether the job book was
  readable (`jobs_off`).
- **One `HostStatus` per resource** is derived by one pure function,
  `sched_sequencer::status::derive` (phase 5), from the config, the snapshot the tick decided
  on, the decided state and steps. `TickReport`, `reason`, the logs, the metrics and the toggle
  listener read it instead of each diffing state. It says, in this document's terms:
  - the phase (`power`: with its timestamps, `ours`, the failed transition) and the exclusive
    `mode` (idle, serving, winding down);
  - the `shutdown` in progress, if any: `cause` (idle, requested with its mode, lockout),
    `withdrawal` (pending, done, not needed) and `veto` (clear, waiting, stopped; the readings
    that are blocking are in the waiting and stopped variants), and `veto_readings`, the
    readings this host's veto consults;
  - the `lockout` (held, withdrawing, left alone) when a lockout lease applies;
  - the manual host `lease` and its origin (`requested`, `adopted`);
  - `min_off` (since, remaining, until) for a managed host with a minimum off time;
  - the current `wait` reason, from the decision's wait step for the host;
  - `attention`: a person has to act (failed power phase, or the veto stopped the shutdown);
  - plus what a reader needs to place it: `kind`, `has_power`, `managed`, `ours`.
  It is derived for every resource in the decided state, not only those with a power section
  (the others are always up and the rest of their status is plain).

  The types as built (`crates/sched-sequencer/src/status.rs`; all but `Drain` serialize into
  `TickReport`):

  ```rust
  pub struct HostStatus {
      resource: ResourceId, kind: ResourceKind, has_power: bool, managed: bool,
      power: PowerPhase, ours: bool, mode: HostMode,
      shutdown: Option<ShutdownStatus>,   // { cause: ShutdownCause, since, withdrawal: Withdrawal, veto: VetoStatus }
      veto_readings: Vec<VetoReading>,
      lease: Option<LeaseOrigin>,         // Requested | Adopted
      lockout: Option<LockoutStatus>,
      min_off: Option<MinOff>,            // { since: Option<Millis>, remaining: Millis, until: Millis }
      wait: Option<WaitReason>, attention: bool,
  }
  pub enum HostMode { Idle, Serving { model }, WindingDown { model } }
  pub enum VetoStatus { Clear, Waiting { since, blocking: Vec<VetoBlocker> }, Stopped { since, blocking: Vec<VetoBlocker> } }
  pub struct VetoBlocker { reading: VetoReading, state: BlockerState /* InUse | Unknown */ }
  pub enum LockoutStatus { Held, Withdrawing, LeftAlone }
  pub enum HostEvent {
      PowerTimeout { resource, during: PowerTransition, started_at: Option<Millis>, at: Millis },
      VetoTimeout { resource, readings: Vec<VetoReading>, blocking: Vec<VetoBlocker>, waiting_since: Option<Millis>, at: Millis },
      Adopted { resource }, AdoptionEnded { resource },
  }
  // Derived view, from HostStatus::drain(); not stored.
  pub struct Drain { cause: DrainCause /* Requested | Idle */, mode: DrainMode, since: Millis, scaled_down: bool }
  ```

  A *drain* is only a host's requested or idle shutdown that has a withdrawal to report; a
  lockout's shutdown is not a drain and is reported as `lockout` (and `locked_out` in the HTTP
  JSON), so `drain.cause` is `requested` or `idle`, never `lockout`.
- **One-time events** (`HostEvent`: a power timeout, a veto timeout, an adoption, an adoption
  ended) are derived in the same function from the previous and the next state (adoption from
  what `adopt` did before the tick), one list in `TickReport.events`. An event is an edge, a
  status is a level: a timeout is an event in the tick that makes it and a level (`attention`)
  after. The events are part of the decided outcome Restate journals, so a replayed tick has the
  same events and the service alerts (logs at error level, counts) only when it decided the tick,
  logging a replay at info level under `..._replayed` and counting nothing: the alert-once rule
  has one definition, in `publish`.
- **A failed tick** (the core refused to decide) carries the statuses of the carried previous
  state at the tick's time (no wait, since no step was decided) and no events; the gauges and
  the toggle JSON are computed from them as for any tick.
- **The toggle listener** routes and translates only: it formats statuses (the status text of a
  switch, the `drain` and `wait` objects, `needs_attention`) and holds no status logic beyond
  that. It does not branch on resource kind.
- Request failures (a power call that failed) are not derived from state: they are the service's
  record of what the `Power` object reported, so `TickReport.power_request_failures` stays a
  field of its own.

## 3. Decisions

### Decided (2026-10-30, user)

- **Ownership rule (2.1b):** a host flowmanager woke is flowmanager's and powers down when
  the job that wanted it ends; only a manual lease keeps it up afterwards. Already today's
  behaviour; the refactor must keep it and test it.
- **D6. A host something else started is adopted at once, even while a job holds it**
  (assumed from the ownership rule; the user may overrule). Today adoption waits for the hold
  to end, with the same end state (adopted, stays up). The only visible change is the
  dashboard showing it held during the backup. A host the backup itself woke is never
  adopted.
- **D1. Reclaim becomes `lockout`, on any managed host** (section 2.3). The HA dashboard
  gives it help text, since the intent is clear once explained but not from the name alone.
  homesvr's `power.py` is ignored: it will be dropped once cuda-central's power is managed.
- **D2. GPU veto: yes.** cuda-central's shutdown waits for a fresh reading of zero model
  pods, bounded, and stops and alerts on timeout, as the mount veto does
  (`fresh_model_pods` exists).
- **D3. Adopted hosts keep a lease**, marked with a typed `origin: adopted` field instead of
  the id prefix. The HA dashboards and templates are open to refactoring for clarity
  (homesvr `kubernetes/apps/home-assistant/ha-config/`; the homesvr agent makes the changes).
- **D4. No migration or back-compat.** Bump the state version, start from observation (leases
  and job book kept), and drop the "reads and writes as before" serde shims. A later
  multi-deployment future may want migrations; not now.
- **D5. Observable output may break for clarity.** Log lines, reason strings, `TickReport`
  fields and metric labels may change in breaking ways when that makes them clearer. The
  homesvr agent updates Grafana dashboards and alerts. Each phase's report lists every such
  change for hand-off.

## 4. Refactor phases (after the decisions)

Each phase goes through the three-role process. From now on the Reviewer is also given
this document and checks the phase against it ("does this still fit the model").

1. **Model document.** This file, accepted; `architecture.md` power sections and invariants
   rewritten to point here; one canonical invariant list, with a mapping from the old
   numbers (`architecture.md`, *Mapping from the old numbers*). Documents only.
2. **State types.** `Shutdown{cause, stage}`, `off_since`, `idle_since`; drop `drain`,
   `mounts_veto`, `shutdown_confirmed_at`, `host_idle_since`. Behaviour preserved; the
   invariant checker and scenario tests are the safety net. State version bumped, no
   migration, serde back-compat shims dropped (D4). The invariant numbers in
   `invariants.rs` and in the `sched-sequencer` doc comment are renumbered from the mapping
   table. `managed` stays until phase 4: two of the cases it still covers (a host found
   running under a job hold, adoption skipped with an unreadable job book) go only with
   D6 and the `jobs_off` decoupling.
3. **One withdrawal, one veto.** Behaviour changes on purpose here (GPU veto; requested
   shutdown supported on exclusive hosts in the sequencer); everything else is preserved.
   - *Two axes.* `Shutdown { cause, since, withdrawal, veto }`: `withdrawal` is the progress
     of taking the consumers away (pending, done, not needed), `veto` is `None`,
     `Vetoing{since}` or `Stopped{since}`. Replaces phase 2's stage plus `withdrawn` booleans,
     so the combinations the phase 2 review listed as impossible cannot be built. Test fixtures
     that hand-build impossible states are rewritten to reachable ones.
   - *Withdrawal, both kinds.* Shared host: the classes that need it leave the allowed set
     (today's drain). Exclusive host: the mode machine withdraws the model, as for a reclaim;
     a requested `graceful` shutdown gives in-flight work up to `drain_timeout`, `immediate`
     cuts it off at once (both forced: a busy unpreemptible class does not hold it). An idle
     shutdown of an exclusive host has nothing to withdraw (the model is already gone). The
     sequencer supports a requested shutdown on an exclusive host; the API and listener keep
     refusing it until phase 4.
   - *One veto.* Before any power-off (idle, requested, reclaim) a fresh reading must show the
     host unused: no pod mounts it (NFS, where a mounts source names it) and no pod of its
     models exists (GPU, where a pods source covers its models, i.e. watches every namespace the model declares in `models[].namespaces`) (D2). Unknown counts as in
     use; the wait is bounded by `power.veto_timeout` (renamed from `mounts_timeout`) and then
     stops (`Stopped`, alert, no retry, `resume` clears it). A host with neither reading has
     no veto.
   - Metric and log names that say "mounts" for the veto are renamed to the general veto,
     with the reading (`mounts`, `pods`) as a label or field; listed for hand-off.
4. **Vocabulary and adoption**, in two sub-phases.
   - **4a. Vocabulary and lockout.**
     - `off` / `off-now` on both kinds through the API (`drain_host`, `release_host_lease`
       with a drain) and the listener; the sequencer already supports them (phase 3). No
       kind branch remains in the toggle listener or the lease checks.
     - Reclaim becomes lockout everywhere: lease kind, intent, handlers, `/v1/lockouts`
       route (replaces `/v1/reclaims`), `lockout_timeout` (replaces `reclaim_timeout`),
       wait reason, metrics, logs, docs. Accepted on any host with a power section (as
       reclaim today, minus the exclusive-only rule); power actions only under managed
       control.
     - On a shared host the lockout's withdrawal is the drain with in-flight work given
       `lockout_timeout`; on an exclusive host it is today's reclaim wind-down. Then the veto
       and the shutdown. Nothing wakes or schedules a locked-out host: no workload, job hold
       or manual lease (a manual lease does not cancel a lockout; a lockout does not remove
       the lease either).
     - **The lockout shuts the host down once.** After the host has been seen off under the
       lockout, flowmanager leaves it alone: a later boot (another OS, physical work) is
       neither powered off, nor adopted, nor given work, until the lockout is cleared.
       Clearing it while the host is up makes it a host found running (adopted, 2.1b); while
       it is off, the host is woken only if something wants it. A lockout taken while the
       host is already off just keeps it off.
     - *As built in 4a.* A shared host's lockout is a `Shutdown` with cause `lockout` and
       withdrawal `pending` (consumers keep running while in-flight work gets
       `lockout_timeout`, then `done`); an exclusive host's keeps today's wind-down and
       starts with nothing to withdraw. A drain already under way is carried on as the
       lockout's. A person's `off` / `off-now` on a locked-out host records nothing (the
       lockout decides). A lockout with `control: manual` withdraws the consumers (shared) or
       winds the model down (exclusive) but never powers anything. An adopted lease is
       dropped when the host is seen down, as for any host, so a lockout shutdown also ends
       an adoption; a manual lease is never dropped. The lockout shutdown overrides the
       `idle_grace`, holds and leases, as the reclaim did.
   - **4b. Ownership.**
     - Adoption decided entirely in the sequencer crate (pure), as one function the service
       calls and applies. The service no longer edits the previous state itself.
     - A typed `origin: adopted` on the lease replaces the id prefix (D3).
     - A host found running is adopted even while a job holds it (D6).
     - Adoption no longer skipped with an unreadable job book (`jobs_off`).
     - `managed` replaced by `ours` on `On` and `Off` (2.2).
     - The pure policy still in the service moves to the sequencer crate: the rules of
       `drain::request` and the lockout rule of `jobs::needs_up`.
     - The ownership tests of 2.1b.
     - *As built in 4b.* `sched_sequencer::adopt::adopt(config, snapshot, prev, leases, now)`
       returns `Adoption { leases, prev, adopted, ended }`: the lease book to use (adopted
       leases of hosts seen down dropped, a lease minted for each host found running), the
       previous state to decide from (hosts whose adoption ended: not ours, idle clock
       cleared) and what was adopted and ended; `tick.rs` applies it and edits nothing.
       `now` is the snapshot's time, passed explicitly for the lease's `granted_at`. A typed
       `origin` (`requested`, the default, or `adopted`) is a field of every lease; the id
       `adopted-host-<resource>` is only a name. `jobs_off` no longer touches adoption: the
       lease book is stored on every tick, the job book and the job creation only when it was
       readable. The rules of `drain::request` (`sched_sequencer::drain`) and `needs_up`
       (`sched_sequencer::jobs`) are in the sequencer crate with their tests; the service
       keeps `RecordedDrain` (the report view of a requested shutdown) and the Restate and
       persistence glue.
5. **HostStatus and reporting.** One derived status; `tick.rs` report weaving, `reason`,
   metrics and toggle `resource_state` consume it. README power section shrunk to the model
   plus tables, caveats moved to *Known blind spots*.
   - *As built.* `sched_sequencer::status::derive` returns `(Vec<HostStatus>, Vec<HostEvent>)`
     (2.4). `TickReport` has `hosts` and `events` and lost `power_failed`, `power_timeouts`,
     `veto_held`, `veto_timeouts`, `min_off_waits`, `drains`, `adopted` and `adoptions_ended`;
     `power_request_failures` stays. The service's `power_report`, `veto_report`,
     `min_off_report` and `drain_report`, the `reason::with_power`, `with_veto` and
     `with_min_off` and the `if error.is_none()` cascade are gone; `reason::with_hosts` is the
     one place that writes the power words into the kinds and reason, `Metrics::set_host_gauges`
     the one that sets the per-host gauges and `log_host_event` the one that logs an event.
     No decision changed: the planner, the sequencer's decisions and the jobs machine are
     untouched apart from the new pure module. The output changes are in section 5.
6. **Switch status and the remaining duplicates.** Phase 5 left status decisions in the toggle
   listener, against 2.4 ("routes and translates only"), and some questions answered in more
   than one place. A pure function in the sequencer crate decides what each switch says, from
   a `HostStatus` plus the inputs only the listener has (the fresh lease book, a drain just
   recorded, whether the last tick saw the lease); the listener fetches, calls and serializes.
   One drain type replaces `RecordedDrain`. One lockout check serves the planner, the
   sequencer and the listener; `invariants.rs` keeps its own on purpose, as an independent
   check. Toggle JSON does not change, except that a model switch no longer reads `locked_out`
   for a lockout lease on a host without a power section, which the planner ignores.

## 5. Hand-off to homesvr

One list for the homesvr agent, grouped by what it must change. It collects what phases 2 to 5
changed in everything outside the flowmanager repository (D4, D5: breaking changes were accepted
for clarity). Names are the ones the code has now. The README (*Power actions*, *Known blind
spots*, *Versions, and recovering the persisted state*, *Metrics*) and `docs/kubernetes_setup.md`
describe the behaviour.

### 5.1 Upgrade steps (in this order)

1. **Clear every reclaim before the new build starts.** A stored lease of kind `reclaim` no
   longer parses (the lease book is not versioned away): list the leases (`leases`) and clear
   each with the old build's `unreclaim` (`{"resource": "cuda-central"}`), or the lease book
   read fails, every tick publishes nothing (the scaler keeps what it had) and the lease
   handlers answer `503`. If the upgrade was done first: roll back, `unreclaim`, roll forward;
   or discard `lease_book` (`discard_state`, which drops every lease, job holds are re-derived).
2. **Config renames and additions** (5.2): `policy.reclaim_timeout` is `lockout_timeout` (the old
   key is refused at load); `resources[].power.mounts_timeout` is `veto_timeout`.
3. **Persisted state: nothing to migrate, nothing to do.** `scheduler_state` is version 4. An
   older state (1, 2 or 3) is discarded on read: one warning
   (`event="state_version_discarded"`), the sequencer starts afresh from observation, leases and
   the job book are kept. `lease_book` is version 2 (see below). A rollback needs
   `scheduler_state` and `lease_book` discarded and the config rolled back too.
4. **What the first tick after the upgrade does.** A host that is up is not ours, so a host under
   managed control is adopted (a manual host lease with `origin: adopted`), also one a job woke
   before the upgrade, which then stays up until a person switches it off; a requested shutdown
   under way is forgotten; the idle and `min_off` clocks start over.
5. **Lease book version 2.** Every lease has `origin` (`"requested"` or `"adopted"`). A version 1
   book is read with its adopted leases (a `manual_host` lease whose id begins `adopted-host-`)
   dropped, one warning each (`event="legacy_adopted_lease_dropped"`); nothing is lost, the hosts
   that are up are adopted afresh at the first tick. No operator step.
6. **State shape history** (for anyone reading `last_decision`): version 2 replaced each
   resource's `drain`, `mounts_veto`, `host_idle_since` and `shutdown_confirmed_at` with
   `shutdown { cause, since, withdrawal, veto }`, `idle_since` and `off_since`; version 3 renamed
   the shutdown cause `reclaim` to `lockout` and the wind-down flag `reclaim` to `lockout`, and
   added `locked_down`; version 4 replaced the per-resource `managed` flag with `ours` on the
   `off` and `on` phases (`{"phase":"off","ours":bool}`, `{"phase":"on","since":N,"ours":bool}`).

### 5.2 flowmanager config (homesvr `kubernetes/apps/flowmanager/config.yaml`)

- `resources[].power.mounts_timeout` is `veto_timeout` (same default 30m and bounds; no alias).
- `policy.reclaim_timeout` is `policy.lockout_timeout` (same default, `drain_timeout`, same bounds;
  no alias). It now also applies to a shared host's withdrawal.
- New `models[].namespaces`: required, for a managed exclusive host whose models are covered by a
  `pods` source, to list the namespaces of each model's workloads, all watched by that source;
  otherwise the config is refused at load.
- **Turning the GPU veto on in the deployed config** (checked read-only on 2026-10-30): no source
  there observes `pods` today (`cluster-nodes` has `observe: [reachability]` and nodes-only RBAC),
  so cuda-central has no GPU veto until homesvr (1) adds `pods` to a `kubernetes_watch` source with
  `namespaces: [ollama, paperless-ngx, speakr]`, (2) grants that service account pods list/watch in
  those namespaces, and (3) declares `namespaces` on the models: `mineru` -> `paperless-ngx`, `llm`
  -> `ollama`, `whisperx` -> `speakr`. Until then behaviour is as before (no veto on cuda-central),
  which is safe. With it on, cuda-central's power-off waits for zero pods of every model (`wait ...
  veto_in_use` or `veto_unknown`), up to `veto_timeout`, then stops and alerts
  (`shutdown_veto_timeout`, `sched_resource_veto{state="stopped"} == 1`).
- `power.min_off` is unchanged (opt-in, default 0).

### 5.3 Home Assistant (homesvr `kubernetes/apps/home-assistant/ha-config/`)

Routes and switches:

- The reclaim switch is a **lockout switch**: resource `/v1/lockouts/cuda-central` (unique id
  `flowmanager_lockout_cuda_central`, was `flowmanager_reclaim_cuda_central`). `/v1/reclaims/...`
  is a `404`. Give it the help text, suggested: *"Lockout: flowmanager stops all work on this
  host, shuts it down, and will not power it on or schedule anything on it until the lockout is
  cleared. Use it before rebooting into another OS or doing physical work on the machine. Backups
  that need the host wait too."* Add the same switch for basestar (`/v1/lockouts/basestar-nfs`; it
  deliberately blocks the backup job). `/v1/leases` has `lockouts` where it had `reclaims`, one
  entry per resource with a power section, each with `kind: "lockout"` and `supported: true`.
- Expose `off` / `off-now` / `release` / lockout for cuda-central like basestar:
  `POST /v1/resources/cuda-central/off`, `.../off-now`, `.../release`; they answer `200` with
  `state: "draining"` and a `drain` object while the host is up. Drop the stale "cuda-central is
  powered by hand" dashboard note. (homesvr's `power.py` is ignored: it will be dropped once
  cuda-central's power is managed.)
- The "shutdown stopped" sensor must not assume the host is basestar: cuda-central can be stopped
  by the veto too.

Fields the sensors read (JSON of `/v1/resources/{r}`, `/v1/models/{m}`, `/v1/lockouts/{r}`,
`/v1/leases`):

- `origin` (`"requested"` or `"adopted"`) is on every switch entry while a lease is held; it means
  only the lease's origin. Use it to tell an adopted lease from a person's.
- **`drain.origin` is now `drain.cause`** (`requested` or `idle`; phase 5). The object is
  `{mode, cause, since_ms, scaled_down}`. A lockout is not a drain: a locked-out host shows
  `locked_out`, never `cause: lockout`.
- `locked_out` (boolean, true while a lockout lease is held on that host) on the resource entries,
  with a `wait` entry `{"reason": "locked_out"}` (created even before the first tick); `state` reads
  `locked_out` instead of `off` while the last report does not show the host off and no lease is
  held (`draining` and `on` keep their meaning). `off`, `off-now` and `on` still answer `200`: the
  request is recorded or ignored as before and the reply says the lockout decides.
- Lockout switch `status` gains `left_alone` (the host booted after the lockout shut it down) and
  `draining` also covers a shared host's pending withdrawal; sensors reading it should expect
  `left_alone`. The model switch's `status` `reclaimed` is `locked_out` (its `detail` says "locked
  out").
- The `wait` entry of the veto is `shutdown_held_by_veto` (was `shutdown_held_by_mounts`) with
  `readings`, `blocking` (each reading's `state`: `in_use` or `unknown`) and `stopped`; resume's
  `cleared` is `shutdown_stopped_by_veto` (was `shutdown_stopped_by_mounts`). `needs_attention` is
  true for a failed power phase or a stopped shutdown, on either host.

### 5.4 Grafana and alerts

Metrics (names, values and labels; phase 5 changed none, see the end of this group):

- `sched_resource_mounts_veto{resource,state}` is `sched_resource_veto{resource,reading,state}`,
  one series per applicable reading (`mounts`, `pods`): `waiting` is 1 on the blocking reading(s),
  `stopped` is 1 on every reading of the host. cuda-central now has series too.
- `sched_mounts_timeouts_total{resource}` is `sched_veto_timeouts_total{resource,reading}`, one count
  per blocking reading.
- `cuda-central` therefore belongs in the stopped and needs-attention alerts that already cover
  basestar (`sched_resource_veto{state="stopped"} == 1`, `sched_resource_power_failed == 1`, and
  `needs_attention` in the toggle JSON).
- `sched_manual_lease_age_seconds{kind="reclaim"}` is `kind="lockout"`;
  `sched_toggle_requests_total{endpoint}` values `reclaim_get`, `reclaim_on`, `reclaim_off`,
  `reclaim_set` are `lockout_get`, `lockout_on`, `lockout_off`, `lockout_set`.
- **Phase 5:** no metric name, label or value changed; the per-host gauges
  (`sched_resource_power_failed`, `sched_resource_veto`, `sched_resource_min_off_remaining_seconds`)
  keep updating on a tick that failed to decide, from the carried state, as before. The alert-once
  counters (`sched_power_timeouts_total`, `sched_veto_timeouts_total`) count exactly as before, once
  per timeout, never for a replayed tick.
- **Decision kind rename:** the step `DrainModel` is `WithdrawModel`, so the kind label
  `sched_tick_decisions_total{kind="drain_model"}` is `kind="withdraw_model"` (also in
  `TickReport.steps` JSON as `type: withdraw_model` and in scenario YAML). "Drain" now means only a
  host's requested or idle shutdown. Update dashboards and alerts that name `drain_model`. (The model
  switch status string `draining` that Home Assistant reads is unchanged.)

Logs (for log-based alerts and queries):

- `shutdown_mounts_timeout` is `shutdown_veto_timeout` (and `..._replayed`), with `readings`
  (joined with `+`) and `blocking` (`mounts:in_use,pods:unknown`) fields.
- `host_adopted` is not emitted for a locked-out host. No other event was added or removed in 4a
  and 4b: the toggle `event="toggle"` line has `kind="lockout"` where it had `kind="reclaim"`.
- **Phase 5:** the `event="drain"` line (every tick while a requested or idle shutdown is
  withdrawing consumers) has `cause` where it had `origin` (values `requested`, `idle`), and the
  `tick` line's `drains` field entries read `resource:cause:mode:scaled_down|running` (the second
  part was the origin). `host_adopted` and `host_adoption_ended` are now emitted from the tick's
  events list (same text, same fields); the alert events (`power_timeout`,
  `shutdown_veto_timeout`) and their `_replayed` twins are unchanged in name, fields and text.
- Adoption (`host_adopted`, `host_adoption_ended`) and the adoption events of the report run with an
  unreadable job book too (`jobs_off`); the lease book is stored as the tick decided it. Before, no
  adoption happened then (and the lease book was not written).
- The invariant checker's message for 7 reads "powered off X, which flowmanager did not start (not
  ours)" (test harness output only).

`last_decision` / `TickReport` JSON, `reason` text and wait reasons:

- Wait reasons (`TickReport.steps`, scenario YAML, `reason` text): `pods_mounted` is
  `veto_in_use`, `mounts_unknown` is `veto_unknown`, `reclaimed` is `locked_out`,
  `reclaim_unsupported` is `lockout_unsupported`; `shutdown_stopped` is unchanged. A locked-out host
  that is up after its one shutdown reports `locked_out` every tick (before: `power_off` again every
  tick). `reason` text: ", reclaim [..]" is ", lockout [..]" and the wait texts say "locked out".
- `intent.reclaim` is `intent.lockout` (`last_decision`, scenario files).
- `mounts_held` was `veto_held` and `mounts_timeouts` `veto_timeouts` after phase 3; **phase 5
  removes the per-feature fields**: `power_failed`, `power_timeouts`, `veto_held`, `veto_timeouts`,
  `min_off_waits`, `drains`, `adopted`, `adoptions_ended` are gone from `TickReport`. In their place
  `hosts` (one status per resource: `resource`, `kind`, `has_power`, `managed`, `power`, `ours`,
  `mode`, `shutdown` {`cause`, `since`, `withdrawal`, `veto`: `clear` / `waiting` / `stopped` with
  `since` and the `blocking` readings}, `veto_readings`, `lease` origin, `lockout`, `min_off`
  {`since`, `remaining`, `until`}, `wait`, `attention`) and `events` (a list tagged by `event`:
  `power_timeout`, `veto_timeout`, `adopted`, `adoption_ended`). `power_request_failures` stays. A
  failed tick has `hosts` of the carried state and empty `events`. The reason text, the kinds (`power_failed`,
  `shutdown_stopped`) and their order are unchanged.
- `Lease` JSON (`TickReport.leases`, `last_decision`, Restate replies such as `LeaseGrant`) has
  `origin`.
- Restate handlers: `Scheduler/main/reclaim` is `lockout` and `unreclaim` is `clear_lockout` (same
  request `{"resource": ...}`, same `LeaseGrant` / `Lease` replies; new lease ids `lockout-<n>`, kind
  `lockout`). `drain_host` and `release_host_lease` with `drain` now accept an exclusive host (they
  answered `400` "not a shared resource"); on a locked-out host `drain_host` records nothing
  (`drain` absent in its reply). The `400` for a resource without a power section reads "...so a
  lease, a lockout or a drain on it would do nothing"; there is no longer a "cannot be reclaimed"
  refusal.

Behaviour changes that show on dashboards:

- A host found running is adopted at once, even while a job holds it (D6; before: only after the
  hold ended, same end state), so the dashboard shows it held during the backup. A host a job woke is
  never adopted.
- Only a shutdown stopped by the veto blocks adoption of a not-ours host that is up. So a not-ours
  host carrying the lockout's shutdown is adopted at the very tick its lockout lease disappears (the
  build before waited one tick); the adopted lease then cancels the leftover shutdown.
- A scheduled job whose needs include a locked-out host does not start (host up or not): its run waits
  in `Waiting` as for a host that is not up, so `job_events` show no `started`; `start_late` and
  `missed` follow the job's `missed_run` policy and a `started` (with `caught_up`) comes once the
  lockout is cleared. The waiting run's own hold (`hold-job-<id>-<resource>`) is inert under the
  lockout; when it clears the hold may wake a host that is down so the run can catch up, after which
  the host is flowmanager's and powers down when the job is done. A run already running is not
  killed and keeps its hold; the lockout shuts the host down regardless (the veto waits for the job's
  pods, bounded by `veto_timeout`).
- A lockout taking over a shutdown in progress (any host) makes it the lockout's: `shutdown.cause` is
  `lockout` (it stays `requested` for an immediate request whose withdrawal is still pending); a
  pending graceful withdrawal restarts at the takeover with `lockout_timeout`; clearing the lockout
  drops the shutdown, so an earlier `off` on an exclusive host no longer survives it.
- `off` and `off-now` on `/v1/resources/cuda-central` behave as on basestar (`200`, `state:
  "draining"` and a `drain` object while the host is up; before: plain release, and `400` for
  `off-now`).
