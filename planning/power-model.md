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

### 2.2 State as types, not flags

A sketch (names not final):

```rust
struct ResourceState {
    power: PowerPhase,                    // Off | PoweringOn | On | ShuttingDown | Failed
    mode: ModePhase,                      // unchanged: exclusive model serving/idle/wind-down
    hands_off: Option<HandsOff>,          // flowmanager will not power this host down by itself
    shutdown: Option<Shutdown>,           // only while On
    idle_since: Option<Millis>,
    off_since: Option<Millis>,            // for min_off; only while Off after our shutdown
    pending_recovery: BTreeSet<ClassId>,  // unchanged
}
enum HandsOff { FailedTransition, VetoTimedOut, FoundRunningUnderHold }
struct Shutdown { cause: Cause, stage: Stage, since: Millis }
enum Cause { Idle, Requested { mode: DrainMode }, Reclaim }
enum Stage { Withdrawing, Vetoing { since: Millis } }
```

Hypothesis to verify before committing to it: since adoption landed, every host that is up
under managed control is either flowmanager's (powered on by it or held by a lease, adopted
or not) or deliberately left alone after something went wrong. If so, `managed: bool` is
really "not hands-off", and the explicit `HandsOff` reason replaces the flag and the
adoption predicate's guesswork about why a host is in a given state.

### 2.3 One vocabulary for both kinds

| Action | Meaning on any managed host |
| --- | --- |
| `on` | take a manual host lease (power on if off) |
| `release` | drop the lease; the host goes down when idle like any other |
| `off` | requested shutdown, graceful |
| `off-now` | requested shutdown, immediate |
| `resume` | clear a hands-off state |

On an exclusive host, Withdrawing is the mode wind-down, so `off`/`off-now` cost nothing
new. Whether reclaim stays a separate thing is open decision D1.

### 2.4 Who decides what

- **Adoption** moves into the sequencer as a pure ownership change. Whether it still mints a
  lease (so `/v1/leases` and Home Assistant see it) is open decision D3.
- **One `HostStatus` per resource** comes out of the sequencer (phase, stage, wait reason,
  attention). `TickReport`, `reason`, metrics and the toggle listener all read it instead of
  each diffing state.
- **The toggle listener** routes and translates only. It does not branch on resource kind.
- Adoption no longer depends on whether the job book was readable (`jobs_off`).

## 3. Open decisions

- **D1. Reclaim.** Reclaim exists because cuda-central gets rebooted into Windows: off now,
  and not woken until handed back. Options:
  (a) keep reclaim as its own lease, now meaning `off-now` + *parked*;
  (b) make *parked* a separate flag any host can have, set by a reclaim;
  (c) leave reclaim exactly as it is and only unify `off`/`off-now`.
  Recommendation: (a). Parking stays exclusive-only, which avoids the Oct 29 worry that
  parking basestar would block the backup job from waking it.
- **D2. GPU veto.** Gate cuda-central's shutdown on a fresh reading of zero model pods,
  bounded, stop on timeout? Recommendation: yes. It comes almost free with the generalised
  veto, and the pods reading already exists (`fresh_model_pods`).
- **D3. Adopted hosts and the lease book.** Keep minting an adopted `ManualHost` lease (one
  source of truth for "held by the user", HA unchanged), but mark it with a field instead of
  an id prefix? Or record it only in state and have the listener derive "held"?
  Recommendation: keep the lease and add a typed `origin: adopted`.
- **D4. Persisted state.** This is a single deployment. Bump the state version and start
  from observation (one tick re-derives everything; leases and the job book are kept), or
  write a migration? Recommendation: version bump, no migration, and drop the
  "reads and writes as before" serde shims.
- **D5. Behaviour changes allowed?** Phases 2–5 below are meant to keep behaviour except
  where D1/D2 change it on purpose. Confirm that the refactor may change *observable* output
  (log reason strings, `TickReport` fields, metric labels) where they only mirror internal
  structure, as long as Home Assistant and the documented alerts keep working.

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
4. **Vocabulary and adoption.** `off`/`off-now` on both kinds (D1), adoption in the
   sequencer with typed origin (D3), no `jobs_off` coupling, toggle without kind branches.
5. **HostStatus and reporting.** One derived status; `tick.rs` report weaving, `reason`,
   metrics and toggle `resource_state` consume it. README power section shrunk to the model
   plus tables, caveats moved to *Known blind spots*.
