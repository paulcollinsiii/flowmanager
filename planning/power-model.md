# Host power model: plan (draft v0)

Status: **draft for discussion**, branch `refactor/power-model`, based on main @ 689ab82.
Nothing here is decided until the open decisions at the end are answered.

This document is the single description of how flowmanager owns and changes a host's
power. It exists because five features (adoption, drain on release, `drain_host`, idle
drain, exclusive host lease) were added one at a time and each was reviewed only against
itself. Once accepted, it replaces the power parts of `architecture.md` and becomes the
model that every later power change is reviewed against.

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

- **Adoption** moves into the sequencer as a pure ownership change. Whether it still mints a
  lease (so `/v1/leases` and Home Assistant see it) is open decision D3.
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
   rewritten to point here; invariants renumbered.
2. **State types.** `HandsOff`, `Shutdown{cause, stage}`, `off_since`; drop `managed`,
   `drain`, `mounts_veto`, `shutdown_confirmed_at`. Behaviour preserved; the invariant
   checker and scenario tests are the safety net.
3. **One withdrawal, one veto.** The shared drain and the exclusive wind-down share the
   Withdrawing stage; the mount veto becomes a veto with a per-host reading (adds the GPU
   pod veto if D2 is accepted).
4. **Vocabulary and adoption.** `off`/`off-now` on both kinds; reclaim renamed to lockout
   and accepted on any managed host (D1: lease kind, handlers, `/v1/lockouts` route,
   `lockout_timeout`, metrics, docs); adoption in the sequencer with typed origin (D3, D6);
   no `jobs_off` coupling; toggle without kind branches.
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
