//! One derived status per host, and the one-time events between two ticks
//! (`docs/architecture/power.md`, *Who decides what*).
//!
//! The sequencer's state, steps and the readings of the tick say what a host is doing, but in
//! pieces: a power phase, a shutdown with two axes, a lease book, a wait step, config. Logs,
//! the reason text, metrics and the toggle listener all need the same answer in the model's
//! words (`docs/architecture/power.md`). [`derive`] makes it once, purely, and everything else reads it:
//!
//! - [`HostStatus`]: what is true of the host after the tick (a level: it is reported every
//!   tick for as long as it holds);
//! - [`HostEvent`]: what became true in this tick (an edge: a timeout firing, a failed
//!   power-on, a veto stopping, an adoption). A timeout is derived from the previous and the
//!   next state, an adoption passed through from `adopt`, all in one place, so alert-once
//!   behaviour has one definition; the service logs
//!   and counts an event only when the tick is decided, never when it is replayed from the
//!   journal.
//!
//! Nothing here decides anything: the same steps, waits and state are produced with or without
//! it.

use sched_model::{
    Config, DrainMode, LeaseOrigin, Millis, ModePhase, ModelId, PowerPhase, PowerTransition,
    ReadingUse, ResourceId, ResourceKind, SchedState, Shutdown, ShutdownCause, Snapshot, Step,
    VetoReading, WaitReason, Waker, Withdrawal, manual_host_lease,
};
use serde::{Deserialize, Serialize};

/// What a host's exclusive mode is doing (a shared host is always `Idle`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum HostMode {
    /// No model is allowed.
    Idle,
    /// The model is allowed.
    Serving { model: ModelId },
    /// The model is being switched away from or withdrawn: its in-flight work is awaited.
    WindingDown { model: ModelId },
}

/// Why a veto reading is not showing the host unused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockerState {
    /// A fresh reading shows a pod.
    InUse,
    /// No fresh reading: counts as in use.
    Unknown,
}

impl BlockerState {
    pub fn as_str(self) -> &'static str {
        match self {
            BlockerState::InUse => "in_use",
            BlockerState::Unknown => "unknown",
        }
    }
}

/// A reading of the veto that is not showing the host unused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct VetoBlocker {
    pub reading: VetoReading,
    pub state: BlockerState,
}

impl VetoBlocker {
    /// `mounts:in_use`, `pods:unknown`, as joined in logs and reasons.
    pub fn label(&self) -> String {
        format!("{}:{}", self.reading.as_str(), self.state.as_str())
    }
}

/// The blockers joined for a log field or a reason: `mounts:in_use,pods:unknown`.
pub fn blockers_label(blocking: &[VetoBlocker]) -> String {
    blocking
        .iter()
        .map(VetoBlocker::label)
        .collect::<Vec<_>>()
        .join(",")
}

/// The readings joined for a log field: `mounts+pods`.
pub fn readings_label(readings: &[VetoReading]) -> String {
    readings
        .iter()
        .map(|r| r.as_str())
        .collect::<Vec<_>>()
        .join("+")
}

/// Where the veto on the power-off stands (see `sched_model::Veto`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum VetoStatus {
    /// Not (yet) waiting on the readings.
    Clear,
    /// The shutdown is held back since `since`; `blocking` are the readings that say so.
    Waiting {
        since: Millis,
        blocking: Vec<VetoBlocker>,
    },
    /// The wait outlasted `veto_timeout` at `since`: flowmanager stopped, the host stays up
    /// and a person must act. `blocking` is empty once the pods are gone: it stays stopped.
    Stopped {
        since: Millis,
        blocking: Vec<VetoBlocker>,
    },
}

/// The shutdown of an up host, as reported (see `sched_model::Shutdown`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShutdownStatus {
    pub cause: ShutdownCause,
    /// When it was requested (an idle one: started).
    pub since: Millis,
    pub withdrawal: Withdrawal,
    pub veto: VetoStatus,
}

/// Who started a drain: the cause of a shutdown that has a withdrawal to report (not a
/// lockout's, which the lockout status reports).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DrainCause {
    /// A person asked (and what an absent `cause` in a stored [`Drain`] means).
    #[default]
    Requested,
    /// The scheduler started it when the host went idle.
    Idle,
}

impl DrainCause {
    pub fn as_str(self) -> &'static str {
        match self {
            DrainCause::Requested => "requested",
            DrainCause::Idle => "idle",
        }
    }

    fn is_requested(&self) -> bool {
        *self == DrainCause::Requested
    }
}

/// A drain under way on a host: a shutdown taking the consumers away (see [`Drain::of`]).
///
/// Serialized in the reply of `drain_host`, which is journaled and only ever holds a requested
/// drain ([`Drain::requested`]): `cause` is left out when it is `requested`, so that reply is
/// `{mode, since, scaled_down}`, and an entry journaled in that shape reads back as requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Drain {
    #[serde(default, skip_serializing_if = "DrainCause::is_requested")]
    pub cause: DrainCause,
    /// How in-flight work is treated. An idle drain scales the consumers down at once.
    pub mode: DrainMode,
    pub since: Millis,
    /// Whether the host's consumers have been scaled down (left the allowed set).
    pub scaled_down: bool,
}

impl Drain {
    /// The drain a shutdown is, if any: one a person asked for, or the scheduler's idle one,
    /// that has a withdrawal to report. A lockout's shutdown is not a drain.
    pub fn of(shutdown: &Shutdown) -> Option<Drain> {
        Drain::from_parts(shutdown.cause(), shutdown.since(), shutdown.withdrawal())
    }

    /// The drain of a shutdown a person requested. What a request answers with: an idle
    /// shutdown under way is not what was asked for, and is not recorded as if it were.
    pub fn requested(shutdown: &Shutdown) -> Option<Drain> {
        Drain::of(shutdown).filter(|d| d.cause == DrainCause::Requested)
    }

    /// The one derivation, from a shutdown as stored or as reported.
    fn from_parts(cause: ShutdownCause, since: Millis, withdrawal: Withdrawal) -> Option<Drain> {
        if withdrawal == Withdrawal::NotNeeded {
            return None;
        }
        let (cause, mode) = match cause {
            ShutdownCause::Requested { mode } => (DrainCause::Requested, mode),
            ShutdownCause::Idle => (DrainCause::Idle, DrainMode::Immediate),
            ShutdownCause::Lockout => return None,
        };
        Some(Drain {
            cause,
            mode,
            since,
            scaled_down: withdrawal == Withdrawal::Done,
        })
    }
}

/// What a lockout lease on the host is doing (absent from [`HostStatus`] without one).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LockoutStatus {
    /// Held: the host is being shut down, is down, or (under `control: manual`) is left for a
    /// person; nothing wakes or schedules it.
    Held,
    /// The host is up and its work is awaited for at most the lockout timeout, then cut off.
    Withdrawing,
    /// The host booted again after the lockout shut it down (another OS, say): it is left
    /// alone until the lockout is cleared.
    LeftAlone,
}

/// The `min_off` wait of a managed host with a minimum off time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MinOff {
    /// When the host was seen down after flowmanager shut it down (absent when it was not).
    pub since: Option<Millis>,
    /// How much of `min_off` is left at the tick's time (zero: nothing to wait for).
    pub remaining: Millis,
    /// The instant the wait ends (the earliest an automatic power-on can be decided).
    pub until: Millis,
}

/// Everything true of one host after a tick, in the model's terms.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostStatus {
    pub resource: ResourceId,
    pub kind: ResourceKind,
    /// Whether the resource has a power section: only then can it be leased, drained or
    /// locked out. Without one the host is always up and the rest of the status is plain.
    pub has_power: bool,
    /// Whether flowmanager controls the power (`control: managed`).
    pub managed: bool,
    /// The lifecycle phase, with its timestamps, `ours` and the failed transition.
    pub power: PowerPhase,
    /// Whether the host is flowmanager's (a host it powers on, or a manual want).
    pub ours: bool,
    pub mode: HostMode,
    /// The shutdown in progress, if any.
    pub shutdown: Option<ShutdownStatus>,
    /// The readings the veto of this host consults, in a fixed order; empty: no veto.
    pub veto_readings: Vec<VetoReading>,
    /// The manual host lease holding the host up and who made it: `requested` (a person) or
    /// `adopted` (flowmanager, for a host it found running).
    pub lease: Option<LeaseOrigin>,
    /// A lockout lease applies to the host.
    pub lockout: Option<LockoutStatus>,
    /// The `min_off` of a managed host that has one.
    pub min_off: Option<MinOff>,
    /// Why the host holds something back this tick (the decision's wait step for it).
    pub wait: Option<WaitReason>,
    /// The classes that would power the host back on if it went down now (`docs/architecture/power.md`, *Wakers*),
    /// judged on the state and readings of this tick. Empty for a host without wakers, a
    /// locked-out one, and one without a power section.
    #[serde(default)]
    pub wakers: Vec<Waker>,
    /// A person has to act: a power transition failed, or the veto stopped the shutdown.
    pub attention: bool,
}

impl HostStatus {
    /// Whether the host is up.
    pub fn is_up(&self) -> bool {
        matches!(self.power, PowerPhase::On { .. })
    }

    /// Whether the host is off.
    pub fn is_off(&self) -> bool {
        matches!(self.power, PowerPhase::Off { .. })
    }

    /// The power phase's name (`off`, `powering_on`, `on`, `shutting_down`, `failed`).
    pub fn phase(&self) -> &'static str {
        match self.power {
            PowerPhase::Off { .. } => "off",
            PowerPhase::PoweringOn { .. } => "powering_on",
            PowerPhase::On { .. } => "on",
            PowerPhase::ShuttingDown { .. } => "shutting_down",
            PowerPhase::Failed { .. } => "failed",
        }
    }

    /// The veto wait or stop of the shutdown, if it has reached the veto.
    pub fn veto(&self) -> Option<&VetoStatus> {
        self.shutdown
            .as_ref()
            .map(|s| &s.veto)
            .filter(|v| !matches!(v, VetoStatus::Clear))
    }

    /// Whether the shutdown was stopped by the veto.
    pub fn stopped(&self) -> bool {
        matches!(self.veto(), Some(VetoStatus::Stopped { .. }))
    }

    /// The readings of the veto that are blocking now (empty without a veto wait or stop).
    pub fn blocking(&self) -> &[VetoBlocker] {
        match self.veto() {
            Some(VetoStatus::Waiting { blocking, .. } | VetoStatus::Stopped { blocking, .. }) => {
                blocking
            }
            _ => &[],
        }
    }

    /// The drain under way, if the shutdown is one (see [`Drain::of`]).
    pub fn drain(&self) -> Option<Drain> {
        let s = self.shutdown.as_ref()?;
        Drain::from_parts(s.cause, s.since, s.withdrawal)
    }
}

/// Something that became true in one tick, alerted once. Derived by [`derive`] from the
/// previous and the next state (and, for adoption, what [`crate::adopt::adopt`] did).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum HostEvent {
    /// A power transition outlasted its timeout in this tick: the host is now failed.
    PowerTimeout {
        resource: ResourceId,
        during: PowerTransition,
        /// When the transition began, if the previous state said.
        started_at: Option<Millis>,
        at: Millis,
    },
    /// The veto wait outlasted `veto_timeout` in this tick: the shutdown is now stopped.
    VetoTimeout {
        resource: ResourceId,
        /// The readings the veto of this host consults.
        readings: Vec<VetoReading>,
        /// The readings that were still blocking when the wait ran out.
        blocking: Vec<VetoBlocker>,
        /// When the wait began, if the previous state said.
        waiting_since: Option<Millis>,
        at: Millis,
    },
    /// A host found running was adopted: a manual host lease was minted for it.
    Adopted { resource: ResourceId },
    /// An adopted lease was dropped because the host was seen down.
    AdoptionEnded { resource: ResourceId },
}

/// What [`derive`] reads: one decided tick.
pub struct Decided<'a> {
    pub config: &'a Config,
    /// The snapshot the tick decided on (its leases are the ones the core was given).
    pub snapshot: &'a Snapshot,
    /// The state the previous tick left.
    pub prev: &'a SchedState,
    /// The state this tick decided.
    pub next: &'a SchedState,
    /// The decision's steps.
    pub steps: &'a [Step],
    pub now: Millis,
    /// What [`crate::adopt::adopt`] did before the tick: hosts adopted and adoptions ended.
    pub adopted: &'a [ResourceId],
    pub ended: &'a [ResourceId],
}

/// The statuses of every resource in `next`, and the events of this tick: the adoptions that
/// [`crate::adopt::adopt`] did before it (passed through, as `adopted` and `ended`), and the
/// timeouts, which are derived here from the change between `prev` and `next`. Pure.
pub fn derive(d: &Decided<'_>) -> (Vec<HostStatus>, Vec<HostEvent>) {
    let Decided {
        config,
        snapshot,
        prev,
        next,
        steps,
        now,
        adopted,
        ended,
    } = *d;
    let hosts: Vec<HostStatus> = next
        .resources
        .iter()
        .filter_map(|(id, st)| {
            let spec = config.resource(id)?;
            // The wakers are read from the state as the tick began, as the planner reads them.
            Some(HostStatus {
                wakers: sched_model::wakers(config, snapshot, prev, id),
                ..host_status(config, snapshot, steps, now, spec.kind, id, st)
            })
        })
        .collect();
    let mut events: Vec<HostEvent> = adopted
        .iter()
        .map(|r| HostEvent::Adopted {
            resource: r.clone(),
        })
        .chain(ended.iter().map(|r| HostEvent::AdoptionEnded {
            resource: r.clone(),
        }))
        .collect();
    for h in &hosts {
        let before = prev.resource(&h.resource);
        if let PowerPhase::Failed { since, during } = h.power
            && !matches!(before.map(|p| p.power), Some(PowerPhase::Failed { .. }))
        {
            events.push(HostEvent::PowerTimeout {
                resource: h.resource.clone(),
                during,
                started_at: match before.map(|p| p.power) {
                    Some(PowerPhase::PoweringOn { since } | PowerPhase::ShuttingDown { since }) => {
                        Some(since)
                    }
                    _ => None,
                },
                at: since,
            });
        }
        if let Some(VetoStatus::Stopped { since, blocking }) = h.veto() {
            let was = before.and_then(|p| p.shutdown);
            if was.is_none_or(|b| b.stopped_since().is_none()) {
                events.push(HostEvent::VetoTimeout {
                    resource: h.resource.clone(),
                    readings: h.veto_readings.clone(),
                    blocking: blocking.clone(),
                    waiting_since: was.and_then(|b| b.vetoing_since()),
                    at: *since,
                });
            }
        }
    }
    (hosts, events)
}

fn host_status(
    config: &Config,
    snapshot: &Snapshot,
    steps: &[Step],
    now: Millis,
    kind: ResourceKind,
    id: &ResourceId,
    st: &sched_model::ResourceState,
) -> HostStatus {
    let spec = config.resource(id);
    let has_power = spec.is_some_and(|r| r.power.is_some());
    let managed = config.power_managed(id);
    let blocking = || -> Vec<VetoBlocker> {
        snapshot
            .host_use(config, id, config.policy().host_reading_stale_after)
            .blocking()
            .into_iter()
            .map(|(reading, state)| VetoBlocker {
                reading,
                state: match state {
                    ReadingUse::InUse => BlockerState::InUse,
                    _ => BlockerState::Unknown,
                },
            })
            .collect()
    };
    let shutdown = st.shutdown.map(|s: Shutdown| ShutdownStatus {
        cause: s.cause(),
        since: s.since(),
        withdrawal: s.withdrawal(),
        veto: if let Some(since) = s.vetoing_since() {
            VetoStatus::Waiting {
                since,
                blocking: blocking(),
            }
        } else if let Some(since) = s.stopped_since() {
            VetoStatus::Stopped {
                since,
                blocking: blocking(),
            }
        } else {
            VetoStatus::Clear
        },
    });
    let mode = match &st.mode {
        ModePhase::Idle => HostMode::Idle,
        ModePhase::Serving {
            model,
            wind_down: Some(_),
            ..
        } => HostMode::WindingDown {
            model: model.clone(),
        },
        ModePhase::Serving { model, .. } => HostMode::Serving {
            model: model.clone(),
        },
    };
    let lease = manual_host_lease(&snapshot.leases, id).map(|l| l.origin);
    let lockout = config
        .locked_out(&snapshot.leases, id, now)
        .then(|| match st.power {
            PowerPhase::On { .. } => {
                let withdrawing = matches!(mode, HostMode::WindingDown { .. })
                    || st
                        .shutdown
                        .is_some_and(|s| s.is_lockout() && s.withdrawal() == Withdrawal::Pending);
                if withdrawing {
                    LockoutStatus::Withdrawing
                } else if st.locked_down {
                    LockoutStatus::LeftAlone
                } else {
                    LockoutStatus::Held
                }
            }
            _ => LockoutStatus::Held,
        });
    let min_off = spec
        .filter(|_| managed)
        .and_then(|r| r.power.as_ref())
        .map(|p| p.min_off())
        .filter(|m| *m > Millis::ZERO)
        .map(|m| {
            let remaining = st.min_off_remaining(m, now);
            MinOff {
                since: st.off_since,
                remaining,
                until: now.saturating_add(remaining),
            }
        });
    let wait = steps.iter().find_map(|s| match s {
        Step::Wait { resource, reason } if resource == id => Some(*reason),
        _ => None,
    });
    HostStatus {
        resource: id.clone(),
        kind,
        has_power,
        managed,
        power: st.power,
        ours: st.ours(),
        mode,
        attention: matches!(st.power, PowerPhase::Failed { .. })
            || shutdown
                .as_ref()
                .is_some_and(|s| matches!(s.veto, VetoStatus::Stopped { .. })),
        shutdown,
        veto_readings: config.veto_readings(id),
        lease,
        lockout,
        min_off,
        wait,
        wakers: Vec::new(),
    }
}
