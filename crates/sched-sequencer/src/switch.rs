//! What each switch of the toggle listener says (`planning/power-model.md`, 2.4): a pure
//! function per switch (a host's manual lease, a model's lease, a lockout) from a host's
//! [`HostStatus`] plus what only the listener knows: the fresh lease book, what a request has
//! just done, whether the last tick saw the lease.
//!
//! Every precedence rule is here; the listener fetches the inputs, calls and serializes. The
//! results are types and not words: the status strings and detail text are the listener's,
//! a matter of its JSON.
//!
//! A status is always of the last tick and a lease book is fresher. So each function says how
//! the two are weighed: a drain a request just recorded wins over the report's, a lockout in
//! the book answers before any report, and a lease the last tick has not seen has no status
//! from it.

use sched_model::{Config, Millis, ModelId, PowerPhase, VetoReading, WaitReason};

use crate::status::{Drain, HostMode, HostStatus, LockoutStatus, VetoBlocker, VetoStatus};

/// What `state` reads on a switch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwitchState {
    On,
    Off,
    /// A host's switch with no lease held while a drain is under way.
    Draining,
    /// A host's switch with no lease held, locked out, and not seen off.
    LockedOut,
}

impl SwitchState {
    /// A switch that says only whether its lease is held: a model's and a lockout's.
    pub fn of_lease(held: bool) -> SwitchState {
        if held {
            SwitchState::On
        } else {
            SwitchState::Off
        }
    }
}

/// What a request has just done to a host, beyond what the last report can know.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostRequest {
    /// Nothing: a read. The last report says what drain is under way.
    Read,
    /// A handler answered: this drain or none is the one, whatever the older report says.
    Answered { drain: Option<Drain> },
    /// The manual lease was released with no drain: the host stays as it is until the next
    /// ticks decide, so a host the report shows up still reads on.
    Released,
}

/// Why a host's switch says something is held back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostWait {
    /// The veto holds the shutdown back, or stopped it.
    Veto {
        readings: Vec<VetoReading>,
        blocking: Vec<VetoBlocker>,
        stopped: bool,
    },
    /// The host's `min_off` holds its power-on back.
    MinOff { remaining: Millis },
    /// A lockout is held on the host.
    LockedOut,
}

/// What a host's switch says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostSwitch {
    pub state: SwitchState,
    /// The drain to show.
    pub drain: Option<Drain>,
    /// `None`: nothing can be said (no status yet, and no lockout in the book).
    pub waits: Option<Vec<HostWait>>,
}

/// A host's switch. `host` is its status in the last report (`None`: before the first tick, or
/// no state for it yet), `held` whether a manual host lease is held after the request,
/// `locked_out` whether the fresh lease book locks the host out.
///
/// `state`, the first rule that applies:
/// 1. `On` with a lease held, whatever else is going on, and after a plain release while the
///    host is up, locked out or not: nothing has taken it down;
/// 2. `Draining` with a drain to show;
/// 3. `LockedOut` while locked out and the host is not seen off: the lockout is shutting it
///    down, or leaving it alone after another boot;
/// 4. `Off`.
///
/// The drain to show is the request's answer; on a read, the host's, only while it is up (a
/// host on its way down or down has none, whatever the status still carries).
///
/// The waits, in this order: the veto's (waiting or stopped), `min_off` (when that is the
/// host's wait this tick and it has a minimum), and last the lockout's (with or without a
/// status: a lockout is said at once).
pub fn host_switch(
    host: Option<&HostStatus>,
    held: bool,
    locked_out: bool,
    request: HostRequest,
) -> HostSwitch {
    let up = host.is_some_and(HostStatus::is_up);
    let drain = match request {
        HostRequest::Read => host.filter(|h| h.is_up()).and_then(HostStatus::drain),
        HostRequest::Answered { drain } => drain,
        HostRequest::Released => None,
    };
    let state = if held || (request == HostRequest::Released && up) {
        SwitchState::On
    } else if drain.is_some() {
        SwitchState::Draining
    } else if locked_out && !host.is_some_and(HostStatus::is_off) {
        SwitchState::LockedOut
    } else {
        SwitchState::Off
    };
    let mut waits = host.map(host_waits);
    if locked_out {
        waits.get_or_insert_default().push(HostWait::LockedOut);
    }
    HostSwitch {
        state,
        drain,
        waits,
    }
}

fn host_waits(h: &HostStatus) -> Vec<HostWait> {
    let mut waits = Vec::new();
    if let Some(v) = h.veto() {
        waits.push(HostWait::Veto {
            readings: h.veto_readings.clone(),
            blocking: h.blocking().to_vec(),
            stopped: matches!(v, VetoStatus::Stopped { .. }),
        });
    }
    if let (Some(WaitReason::MinOff), Some(m)) = (h.wait, h.min_off) {
        waits.push(HostWait::MinOff {
            remaining: m.remaining,
        });
    }
    waits
}

/// Whether a person has to act on any host with a switch, the roll-up of a listing: among
/// `hosts`, the statuses of the last report, any whose resource has a power section in `config`
/// with `attention` set. `config` is the current one and not the status's `has_power`: a switch
/// exists by the config in force, which a reload may have changed since the tick.
pub fn any_needs_attention(config: &Config, hosts: &[HostStatus]) -> bool {
    hosts.iter().any(|h| {
        h.attention
            && config
                .resource(&h.resource)
                .is_some_and(|r| r.power.is_some())
    })
}

/// What the last tick's report knows of a switch's host and lease.
#[derive(Debug, Clone, Copy)]
pub struct LastTick<'a> {
    /// The host's status (`None`: the report has no state for it yet).
    pub host: Option<&'a HostStatus>,
    /// Whether the lease of the switch is among those the tick decided on.
    pub saw_lease: bool,
}

/// What a held model lease has come to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelStatus {
    /// A lockout is held on the model's resource: the planner gives it no model. `phase` is
    /// its power phase's name, if a status says it.
    LockedOut { phase: Option<&'static str> },
    /// The last tick was decided before the lease was taken.
    LeaseNotSeen,
    /// The model is in the allowed set.
    Active,
    /// The report has no state for the host yet.
    NoHostState,
    /// A power transition of the host failed.
    HostFailed,
    /// The host is not up: `phase` is its power phase's name.
    WaitingForHost { phase: &'static str },
    /// Another model is winding down on the host.
    OtherWindingDown { model: ModelId },
    /// Another model is still allowed on the host.
    OtherServing { model: ModelId },
    /// The host is up and idle, and the model is not allowed yet.
    HostIdle,
}

/// A model switch's status: `None` with no lease held, with the lease book unread
/// (`locked_out` is `None`: nothing can be said for the lockout, so nothing is), and before the
/// first tick unless locked out.
///
/// The first rule that applies: a lockout in the book, whatever the report says (the lease
/// cannot become active until it is released); a lease the tick did not see; `allowed`, the
/// model in the tick's allowed set; no state for the host; the host failed; the host not up;
/// then what the host's mode says.
pub fn model_status(
    held: bool,
    locked_out: Option<bool>,
    tick: Option<LastTick<'_>>,
    allowed: bool,
) -> Option<ModelStatus> {
    if !held {
        return None;
    }
    if locked_out? {
        let phase = tick.and_then(|t| t.host).map(HostStatus::phase);
        return Some(ModelStatus::LockedOut { phase });
    }
    let tick = tick?;
    if !tick.saw_lease {
        return Some(ModelStatus::LeaseNotSeen);
    }
    if allowed {
        return Some(ModelStatus::Active);
    }
    let Some(h) = tick.host else {
        return Some(ModelStatus::NoHostState);
    };
    Some(match (&h.power, &h.mode) {
        (PowerPhase::Failed { .. }, _) => ModelStatus::HostFailed,
        (PowerPhase::On { .. }, HostMode::WindingDown { model }) => ModelStatus::OtherWindingDown {
            model: model.clone(),
        },
        (PowerPhase::On { .. }, HostMode::Serving { model }) => ModelStatus::OtherServing {
            model: model.clone(),
        },
        (PowerPhase::On { .. }, HostMode::Idle) => ModelStatus::HostIdle,
        _ => ModelStatus::WaitingForHost { phase: h.phase() },
    })
}

/// What a held lockout lease has come to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockoutSwitchStatus {
    /// The last tick was decided before the lockout was taken.
    LeaseNotSeen,
    /// The report has no state for the host yet.
    NoHostState,
    /// A power transition of the host failed.
    HostFailed,
    /// The host is off and not woken.
    PoweredOff,
    ShuttingDown,
    /// The host is powering on; it is shut down once a tick decides it.
    PoweringOn,
    /// The model is winding down; its work is cut off after the lockout timeout.
    ModelWindingDown {
        model: ModelId,
    },
    /// The consumers are given the lockout timeout to finish, then withdrawn.
    Withdrawing,
    /// The host is up again after the lockout shut it down.
    LeftAlone,
    /// The host is up under `control: manual`: flowmanager does not shut it down.
    LeftForPerson,
    /// The host is up and the lockout has not shut it down yet.
    NotYetShutDown,
}

/// A lockout switch's status: `None` with no lease held, or before the first tick.
///
/// The first rule that applies: a lease the tick did not see; no state for the host; then the
/// power phase. While up: a model winding down (with or without a lockout in the status, whose
/// `Withdrawing` it refines), the status's `Withdrawing`, its `LeftAlone`, `control: manual`,
/// and otherwise not shut down yet.
pub fn lockout_status(held: bool, tick: Option<LastTick<'_>>) -> Option<LockoutSwitchStatus> {
    if !held {
        return None;
    }
    let tick = tick?;
    if !tick.saw_lease {
        return Some(LockoutSwitchStatus::LeaseNotSeen);
    }
    let Some(h) = tick.host else {
        return Some(LockoutSwitchStatus::NoHostState);
    };
    Some(match h.power {
        PowerPhase::Failed { .. } => LockoutSwitchStatus::HostFailed,
        PowerPhase::Off { .. } => LockoutSwitchStatus::PoweredOff,
        PowerPhase::ShuttingDown { .. } => LockoutSwitchStatus::ShuttingDown,
        PowerPhase::PoweringOn { .. } => LockoutSwitchStatus::PoweringOn,
        PowerPhase::On { .. } => match (&h.mode, h.lockout) {
            (HostMode::WindingDown { model }, _) => LockoutSwitchStatus::ModelWindingDown {
                model: model.clone(),
            },
            (_, Some(LockoutStatus::Withdrawing)) => LockoutSwitchStatus::Withdrawing,
            (_, Some(LockoutStatus::LeftAlone)) => LockoutSwitchStatus::LeftAlone,
            _ if !h.managed => LockoutSwitchStatus::LeftForPerson,
            _ => LockoutSwitchStatus::NotYetShutDown,
        },
    })
}
