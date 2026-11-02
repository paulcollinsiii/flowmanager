# Host power model: plan

Status: **decisions complete; phase 1 (documents) written, phases 2, 3 and 4a (code) done on the branch (4a awaiting review); 4b and 5 not started**, branch `refactor/power-model`, based on main @ 689ab82.
All decisions are recorded in section 3 (2026-10-30).

This document is the single description of how flowmanager owns and changes a host's
power. It exists because five features (adoption, drain on release, `drain_host`, idle
drain, exclusive host lease) were added one at a time and each was reviewed only against
itself. Accepted on 2026-10-30, it replaces the power parts of `architecture.md` and is the
model that every later power change is reviewed against. Section 1 is what the code does today; section 2 is the target.

## 1. The model today, as the code behaves

Only hosts with a `power` section under `control: managed` take part; under `manual` the
phase just follows observation and nothing below applies.

**State per host** (`ResourceState`, `sched-model/src/state.rs`), six fields that together
encode one lifecycle:

| Field | Meaning in practice |
| --- | --- |
| `power` | `Off`, `PoweringOn`, `On`, `ShuttingDown`, `Failed{during}` |
| `managed` | "flowmanager may power this down". Set by a power-on or a manual want; cleared by a shutdown, a failed transition, a mount-veto timeout and the end of an adoption |
| `host_idle_since` | start of the `idle_grace` clock |
| `drain` | `{mode: graceful/immediate, origin: requested/idle, scaled_down, since}` |
| `mounts_veto` | `Waiting{since}` / `Stopped{since}` (shared hosts with a mounts source) |
| `shutdown_confirmed_at` | starts the `min_off` wait after a confirmed shutdown |

**What wants a host up** (`intent.power_up`, planner `fifo.rs`): a `ManualHost` or
`ManualModel` lease (`Manual`), a job's `SharedHold` and a busy `may_change_power` class
(`Workload`). A reclaim removes the want.

**Ownership, held in two places:** the `managed` flag, and, for a host found running, an
*adopted* `ManualHost` lease minted by the service (`adopt.rs`, applied in `tick.rs`) with a
magic id prefix.

**Taking a host down, by kind:**

| | Shared (basestar-nfs) | Exclusive (cuda-central) |
| --- | --- | --- |
| Idle (nothing holds it, `idle_grace` over) | idle drain: scale consumers down, mount veto, power off | model already withdrawn (mode idle), power off; no pod check |
| Listener `off` | requested drain, graceful | plain release of the host lease |
| Listener `off-now` | requested drain, immediate | refused |
| Forced, despite work | the drain | reclaim lease: forced wind-down with `reclaim_timeout`, shut down, not woken until unreclaimed |
| Release only | `release` | `release` |

**Withdrawing consumers is implemented twice:** the exclusive mode wind-down
(`exclusive.rs`: `drain_timeout`, a class's `kill_timeout`, `reclaim_timeout`), and the
shared host drain (`power.rs::drain_step`: `drain_timeout`).

**Pre-shutdown check:** only the NFS mount veto (bounded by `mounts_timeout`, stops on
expiry). cuda-central relies on the `idle_grace` margin for its GPU pods to be gone.

## 2. Target model

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
a job holds it is adopted and stays up after the job (D6).

### 2.2 State as types, not flags

A sketch (names not final):

```rust
struct ResourceState {
    power: PowerPhase,                    // Off | PoweringOn | On | ShuttingDown | Failed
    mode: ModePhase,                      // unchanged: exclusive model serving/idle/wind-down
    shutdown: Option<Shutdown>,           // only while On
    idle_since: Option<Millis>,
    off_since: Option<Millis>,            // for min_off; only while Off after our shutdown
    pending_recovery: BTreeSet<ClassId>,  // unchanged
}
struct Shutdown { cause: Cause, stage: Stage, since: Millis }
enum Cause { Idle, Requested { mode: DrainMode }, Lockout }
enum Stage { Withdrawing, Vetoing { since: Millis }, Stopped { since: Millis } }
```

**Phase 4a adds one field, `locked_down: bool`** (per resource, default false): the host was
seen off while it was locked out, so the lockout has done its one shutdown (2.3). It is set
when the host is seen off under a lockout (also a lockout taken while the host is already
off), cleared the tick no lockout applies, and never set under `control: manual`. While it is
set and the lockout stands, a host that is up is left alone: not powered off, not adopted
(`adopt::found_running` also skips any host with an active lockout lease, on either kind), not
given work. It is a bool beside the `Shutdown` rather than a `Shutdown` stage because it
outlives the shutdown (which ends when the host is seen off) and applies to a host that is up
with no shutdown at all.

**The `managed` flag goes away.** Checked on 689ab82: outside tests it is written in eight
places (set: power-on, a manual want, `drain_host`; cleared: shutdown, failed transition,
mount-veto timeout, manual control, end of an adoption). Since adoption mints a lease, an up
host under managed control has `managed = false` in only two lasting cases: the shutdown
stopped on a veto timeout (now `Stage::Stopped`) and a host found running while a job holds
it (adoption waits for the hold to end; see D6). A third, transient case (adoption skipped
while the job book is unreadable) goes when phase 4 removes that coupling. So "may
flowmanager power this down?" becomes "is the host up under managed control and not
`Stopped`?", and the adoption predicate no longer has to guess why a host is in a given
state.

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
say). `off-now` differs in that anything may wake the host again afterwards.

### 2.4 Who decides what

- **Adoption** moves into the sequencer as a pure ownership decision, and still results in
  an adopted lease with a typed `origin: adopted` (D3), so `/v1/leases` and Home Assistant
  see it.
- **One `HostStatus` per resource** comes out of the sequencer (phase, stage, wait reason,
  attention). `TickReport`, `reason`, metrics and the toggle listener all read it instead of
  each diffing state.
- **The toggle listener** routes and translates only. It does not branch on resource kind.
- Adoption no longer depends on whether the job book was readable (`jobs_off`).

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
   - **4b. Ownership.** Adoption decided in the sequencer (pure) with a typed
     `origin: adopted` on the lease (D3; the service only applies it), a host found running
     adopted even while a job holds it (D6), adoption no longer skipped with an unreadable
     job book (`jobs_off`), `managed` removed (2.2), and the ownership tests of 2.1b.
5. **HostStatus and reporting.** One derived status; `tick.rs` report weaving, `reason`,
   metrics and toggle `resource_state` consume it. README power section shrunk to the model
   plus tables, caveats moved to *Known blind spots*.

## 5. Hand-off to homesvr (collected as phases land)

The homesvr agent owns these; each phase adds what it changed.

- HA: rename the reclaim switch and sensors to lockout, with help text on the dashboard.
  Suggested: *"Lockout: flowmanager stops all work on this host, shuts it down, and will not
  power it on or schedule anything on it until the lockout is cleared. Use it before
  rebooting into another OS or doing physical work on the machine. Backups that need the host
  wait too."*
- HA: expose `off` / `off-now` / `release` / lockout for cuda-central like basestar; update
  the stale "cuda-central is powered by hand" dashboard note.
- Grafana and alerts: every breaking log, reason or metric change listed in phase reports (D5).
- **Phase 2 and 3 output changes (breaking, D5).** Phase 4 and 5 will add theirs here.
  - *Persisted state.* `scheduler_state` is version 2. A state of an older version is
    discarded on read and the sequencer starts afresh from observation (leases and the job
    book are kept); nothing to migrate. Its shape changed in version 2: per resource `drain`,
    `mounts_veto`, `host_idle_since` and `shutdown_confirmed_at` are gone, replaced by
    `shutdown { cause, since, withdrawal, veto }` and `off_since`. Phase 3 changed the stored
    `shutdown` again (`stage` and the `withdrawn` flags became `withdrawal` and `veto`), still
    version 2 because nothing was deployed at version 2 before it.
  - *Config.* `resources[].power.mounts_timeout` is renamed `veto_timeout` (same default 30m
    and bounds; no alias). New `models[].namespaces`: required, for a managed exclusive host
    whose models are covered by a `pods` source, to list the namespaces of each model's
    workloads, all watched by that source; otherwise the config is refused at load.
  - *Wait reasons* (`TickReport.steps`, scenario YAML, `reason` text): `pods_mounted` is
    `veto_in_use`, `mounts_unknown` is `veto_unknown`; `shutdown_stopped` is unchanged.
  - *TickReport.* `mounts_held` is `veto_held` and `mounts_timeouts` is `veto_timeouts`.
    Entries carry `readings` (those that apply: `mounts`, `pods`) and `blocking` (the readings
    holding the shutdown now, each with `state` `in_use` or `unknown`).
  - *Metrics.* `sched_resource_mounts_veto{resource,state}` is
    `sched_resource_veto{resource,reading,state}`, one series per applicable reading
    (`waiting` is 1 on the blocking reading(s), `stopped` is 1 on every reading of the host);
    `sched_mounts_timeouts_total{resource}` is `sched_veto_timeouts_total{resource,reading}`,
    one count per blocking reading. cuda-central now has series too.
  - *Log events.* `shutdown_mounts_timeout` is `shutdown_veto_timeout` (and `..._replayed`),
    with `readings` and `blocking` fields.
  - *HTTP JSON* (toggle listener): the wait `shutdown_held_by_mounts` is
    `shutdown_held_by_veto` with `readings` and `blocking`; resume's `cleared`
    `shutdown_stopped_by_mounts` is `shutdown_stopped_by_veto`.
  - *New GPU behaviour.* cuda-central's power-off now waits for zero pods of every model
    (`wait ... veto_in_use` or `veto_unknown`), up to `veto_timeout`, then stops and alerts
    (`shutdown_veto_timeout`, `sched_resource_veto{state="stopped"} == 1`). cuda-central
    therefore belongs in the stopped and needs-attention alerts that already cover basestar
    (`sched_resource_veto{state="stopped"} == 1`, and `needs_attention` in the toggle JSON),
    and the "shutdown stopped" Home Assistant sensor must not assume the host is basestar.
  - *Requested shutdown on an exclusive host* exists in the sequencer only; the API and
    toggle listener still refuse it until phase 4.
  - *Turning the GPU veto on in the deployed config* (checked read-only against homesvr
    `kubernetes/apps/flowmanager/config.yaml` on 2026-10-30): no source there observes
    `pods` today (`cluster-nodes` has `observe: [reachability]` and nodes-only RBAC), so
    cuda-central has no GPU veto until homesvr (1) adds `pods` to a `kubernetes_watch`
    source with `namespaces: [ollama, paperless-ngx, speakr]`, (2) grants that service
    account pods list/watch in those namespaces, and (3) declares `namespaces` on the
    models: `mineru` -> `paperless-ngx`, `llm` -> `ollama`, `whisperx` -> `speakr`. Until
    then behaviour is as before (no veto on cuda-central), which is safe.
- **Phase 4a output changes (breaking, D5).** Reclaim is now lockout on any host with a power
  section, and `off` / `off-now` exist on both kinds. The phase 3 bullet saying the API and
  listener refuse a requested shutdown on an exclusive host is superseded.
  - *Before deploying.* A stored lease of kind `reclaim` no longer parses (the lease book is
    not versioned away): clear every reclaim (`unreclaim`) before the new build starts, or the
    lease book read fails. A config with `policy.reclaim_timeout` is refused at load (unknown
    key): rename it first.
  - *Persisted state.* `scheduler_state` is version 3 (older: discarded on read, start from
    observation, leases and job book kept). Changes: the shutdown cause `{"kind":"reclaim"}` is
    `{"kind":"lockout"}`, the wind-down flag `reclaim` is `lockout`, and a new per-resource
    `locked_down` (bool, default false, see 2.2). A lockout shutdown can now also carry
    withdrawal `pending` or `done` (a shared host).
  - *Config.* `policy.reclaim_timeout` is renamed `policy.lockout_timeout` (same default,
    `drain_timeout`, same bounds; no alias). It now applies to a shared host's withdrawal too.
    A lockout is valid on any resource with a power section (it was exclusive only).
  - *Restate handlers.* `Scheduler/main/reclaim` is `lockout` and `unreclaim` is
    `clear_lockout` (same request `{"resource": ...}`, same `LeaseGrant` / `Lease` replies;
    new lease ids `lockout-<n>`, kind `lockout`). `drain_host` and `release_host_lease` with
    `drain` now accept an exclusive host (they answered `400` "not a shared resource"); on a
    locked-out host `drain_host` records nothing (`drain` absent in its reply). The `400` for a
    resource without a power section reads "...so a lease, a lockout or a drain on it would do
    nothing"; there is no longer a "cannot be reclaimed" refusal.
  - *Toggle listener routes and JSON.* `/v1/reclaims/{resource}[/on|/off]` is
    `/v1/lockouts/{resource}[/on|/off]` (the old route is a `404`). `/v1/leases` has `lockouts`
    where it had `reclaims`, one entry per resource with a power section (basestar-nfs now
    too), each with `kind: "lockout"` and `supported: true`. `off` and `off-now` on
    `/v1/resources/cuda-central` behave as on basestar: `200`, `state: "draining"` with a
    `drain` object while the host is up (before: plain release, and `400` for `off-now`).
    The lockout switch's `status` gains `left_alone` (the host booted after the lockout shut
    it down) and `draining` also covers a shared host's pending withdrawal; the model switch's
    `status` `reclaimed` is `locked_out` (its `detail` says "locked out").
  - *Wait reasons* (`TickReport.steps`, scenario YAML, `reason` text): `reclaimed` is
    `locked_out`, `reclaim_unsupported` is `lockout_unsupported`. A locked-out host that is up
    after its one shutdown now reports `locked_out` every tick (before: `power_off` again every
    tick while a reclaim was held and the host seen up). `reason` text: ", reclaim [..]" is
    ", lockout [..]" and the wait texts say "locked out".
  - *Intent and TickReport JSON.* `intent.reclaim` is `intent.lockout` (where the intent is
    serialized: `last_decision`, scenario files).
  - *Metrics.* `sched_manual_lease_age_seconds{kind="reclaim"}` is `kind="lockout"`;
    `sched_toggle_requests_total{endpoint}` values `reclaim_get`, `reclaim_on`, `reclaim_off`,
    `reclaim_set` are `lockout_get`, `lockout_on`, `lockout_off`, `lockout_set`. No new metric.
  - *Log events.* No event was added or removed. The toggle `event="toggle"` line has
    `kind="lockout"` where it had `kind="reclaim"`; `host_adopted` is not emitted for a
    locked-out host.
  - *Home Assistant.* The reclaim switch is a lockout switch: resource
    `/v1/lockouts/cuda-central` (unique id `flowmanager_lockout_cuda_central`, was
    `flowmanager_reclaim_cuda_central`), with the help text suggested above shown on the
    dashboard. Add the same switch for basestar (`/v1/lockouts/basestar-nfs`; it deliberately
    blocks the backup job). Add `off` and `off-now` for cuda-central as for basestar
    (`POST /v1/resources/cuda-central/off`, `.../off-now`) and drop the "cuda-central is
    powered by hand" note. Sensors reading the lockout switch's `status` should expect
    `left_alone`.
