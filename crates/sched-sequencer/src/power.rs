//! Power phases for hosts that have a power section, and the reference count of what holds
//! them up. Resources without a power section are always on.

use sched_model::{
    ClassSpec, ModePhase, PowerPhase, PowerTransition, PowerWant, ResourceState, Step, WaitReason,
    class_work,
};

use crate::Cx;

/// State for a resource the sequencer has not seen before, from observation.
pub(crate) fn initial(cx: &Cx, steps: &mut Vec<Step>) -> Option<ResourceState> {
    let now = cx.now();
    if cx.res.power.is_none() {
        return Some(ResourceState::new(PowerPhase::On { since: now }));
    }
    match cx.snapshot.fresh_resource_up(
        cx.config,
        &cx.res.id,
        cx.config.policy().host_reading_stale_after,
    ) {
        Some(true) => Some(ResourceState::new(PowerPhase::On { since: now })),
        Some(false) => Some(ResourceState::new(PowerPhase::Off)),
        None => {
            wait(cx, steps, WaitReason::StaleObservation);
            None
        }
    }
}

/// Brings the power phase in line with a fresh reachability observation.
///
/// Under `control: manual` the scheduler is never responsible for the host's power
/// (`managed` is cleared, also in state persisted by a build or config that managed it), and
/// the phase is simply what is observed: on or off. A transitional phase left from managed
/// control (powering on, shutting down) converges to the observed one.
pub(crate) fn reconcile(cx: &Cx, st: &mut ResourceState) {
    if cx.res.power.is_none() {
        return;
    }
    let manual = !cx.config.power_managed(&cx.res.id);
    let was_failed = matches!(st.power, PowerPhase::Failed { .. });
    if manual {
        st.managed = false;
        st.host_idle_since = None;
    }
    let now = cx.now();
    let observed = cx.snapshot.fresh_resource_up(
        cx.config,
        &cx.res.id,
        cx.config.policy().host_reading_stale_after,
    );
    if let Some(up) = observed {
        st.power = match (st.power, up) {
            (PowerPhase::On { since }, true) => PowerPhase::On { since },
            (_, true) if manual => PowerPhase::On { since: now },
            (_, false) if manual => PowerPhase::Off,
            (PowerPhase::Off | PowerPhase::PoweringOn { .. }, true) => {
                PowerPhase::On { since: now }
            }
            (PowerPhase::On { .. }, false) => PowerPhase::Off,
            (PowerPhase::ShuttingDown { .. }, false) => PowerPhase::Off,
            // The failed state clears only on the state the transition was after.
            (
                PowerPhase::Failed {
                    during: PowerTransition::PoweringOn,
                    ..
                },
                true,
            ) => PowerPhase::On { since: now },
            (
                PowerPhase::Failed {
                    during: PowerTransition::ShuttingDown,
                    ..
                },
                false,
            ) => PowerPhase::Off,
            (p, _) => p,
        };
    }
    // A transition that outlasts its timeout is not retried: the resource enters the failed
    // state, which only the observation (or a person, or manual control) leaves. The check
    // does not need a fresh reading: it is about elapsed time. Manual control has no
    // transitions to time out.
    if !manual && let Some(power) = &cx.res.power {
        st.power = match st.power {
            PowerPhase::PoweringOn { since }
                if now.saturating_sub(since) >= power.power_on_timeout() =>
            {
                PowerPhase::Failed {
                    since: now,
                    during: PowerTransition::PoweringOn,
                }
            }
            PowerPhase::ShuttingDown { since }
                if now.saturating_sub(since) >= power.shutdown_timeout() =>
            {
                PowerPhase::Failed {
                    since: now,
                    during: PowerTransition::ShuttingDown,
                }
            }
            p => p,
        };
    }
    // A failed transition leaves the host not managed by an earlier action of ours: whatever
    // brings it back (a person, a slow boot) is treated like a host found running, so no
    // automatic power-off follows until a new want powers it on through the normal path.
    if was_failed || matches!(st.power, PowerPhase::Failed { .. }) {
        st.managed = false;
        st.host_idle_since = None;
    }
    if !st.is_on() {
        st.mode = ModePhase::Idle;
        st.host_idle_since = None;
    }
}

pub(crate) fn wait(cx: &Cx, steps: &mut Vec<Step>, reason: WaitReason) {
    steps.push(Step::Wait {
        resource: cx.res.id.clone(),
        reason,
    });
}

/// What keeps a host from being powered down.
pub(crate) struct Holds {
    pub count: usize,
    /// Some requiring class has demand that cannot be read fresh, so holds are unknown.
    pub unknown: bool,
}

/// Active leases plus classes with queued or in-flight work that need the host.
pub(crate) fn holds(cx: &Cx) -> Holds {
    let now = cx.now();
    let mut h = Holds {
        count: 0,
        unknown: false,
    };
    h.count += cx
        .snapshot
        .leases
        .iter()
        .filter(|l| l.resource == cx.res.id && l.holds_host(now))
        .count();
    for class in cx.config.classes().iter().filter(|c| needs(c, cx)) {
        if class.demand.is_none() {
            continue;
        }
        let w = class_work(cx.config, cx.snapshot, class);
        if !w.known {
            h.unknown = true;
        } else if w.busy() {
            h.count += 1;
        }
    }
    h
}

fn needs(class: &ClassSpec, cx: &Cx) -> bool {
    class.requires.iter().any(|r| r.resource == cx.res.id)
}

/// Power transitions. `acted` is true if the resource already emitted its one step this tick.
pub(crate) fn step(cx: &Cx, st: &mut ResourceState, steps: &mut Vec<Step>, acted: bool) {
    if cx.res.power.is_none() {
        return;
    }
    let now = cx.now();
    let policy = cx.config.policy();
    let want = cx.intent.power_up.get(&cx.res.id).copied();
    if !cx.config.power_managed(&cx.res.id) {
        manual_step(cx, st, steps, acted, want.is_some());
        return;
    }
    match st.power {
        PowerPhase::Off => {
            if acted {
                return;
            }
            if cx.reclaimed() {
                wait(cx, steps, WaitReason::Reclaimed);
            } else if want.is_some() {
                steps.push(Step::PowerOn {
                    resource: cx.res.id.clone(),
                });
                st.power = PowerPhase::PoweringOn { since: now };
                st.managed = true;
                st.host_idle_since = None;
            } else if cx
                .intent
                .models
                .get(&cx.res.id)
                .is_some_and(|m| m.is_some())
            {
                wait(cx, steps, WaitReason::HostNotUp);
            }
        }
        PowerPhase::PoweringOn { .. } => {
            if !acted {
                wait(cx, steps, WaitReason::PoweringOn);
            }
        }
        PowerPhase::ShuttingDown { .. } => {
            if !acted {
                wait(cx, steps, WaitReason::ShuttingDown);
            }
        }
        // Stopped: no power step is ever emitted from here (see `PowerPhase::Failed`).
        PowerPhase::Failed { .. } => {
            if !acted {
                wait(cx, steps, WaitReason::PowerFailed);
            }
        }
        PowerPhase::On { .. } => {
            if acted {
                return;
            }
            let idle_mode = matches!(st.mode, ModePhase::Idle);
            if cx.reclaimed() {
                if idle_mode {
                    power_off(cx, st, steps);
                }
                return;
            }
            if want == Some(PowerWant::Manual) {
                st.managed = true;
            }
            let h = holds(cx);
            if st.managed && idle_mode && h.count == 0 {
                if h.unknown {
                    wait(cx, steps, WaitReason::StaleDemand);
                    return;
                }
                let since = *st.host_idle_since.get_or_insert(now);
                if now.saturating_sub(since) >= policy.idle_grace {
                    power_off(cx, st, steps);
                }
            } else {
                st.host_idle_since = None;
            }
        }
    }
}

/// Under `control: manual`: never a power step. Work that needs a host that is not up
/// waits for a person to power it on.
fn manual_step(cx: &Cx, st: &ResourceState, steps: &mut Vec<Step>, acted: bool, wanted: bool) {
    if acted {
        return;
    }
    let needed = wanted
        || cx
            .intent
            .models
            .get(&cx.res.id)
            .is_some_and(|m| m.is_some());
    match st.power {
        PowerPhase::On { .. } => {}
        _ if cx.reclaimed() => wait(cx, steps, WaitReason::Reclaimed),
        // Only left from managed control while reachability is not fresh.
        PowerPhase::PoweringOn { .. } => wait(cx, steps, WaitReason::PoweringOn),
        PowerPhase::ShuttingDown { .. } => wait(cx, steps, WaitReason::ShuttingDown),
        PowerPhase::Failed { .. } => wait(cx, steps, WaitReason::PowerFailed),
        PowerPhase::Off if needed => wait(cx, steps, WaitReason::HostNotUp),
        PowerPhase::Off => {}
    }
}

/// Shuts the host down, provided a fresh reading shows it up. It is only reached with no
/// model allowed (mode idle); pod counts are not consulted.
fn power_off(cx: &Cx, st: &mut ResourceState, steps: &mut Vec<Step>) {
    let max_age = cx.config.policy().host_reading_stale_after;
    if cx
        .snapshot
        .fresh_resource_up(cx.config, &cx.res.id, max_age)
        != Some(true)
    {
        wait(cx, steps, WaitReason::StaleObservation);
        return;
    }
    steps.push(Step::PowerOff {
        resource: cx.res.id.clone(),
    });
    st.power = PowerPhase::ShuttingDown { since: cx.now() };
    st.managed = false;
    st.host_idle_since = None;
    st.mode = ModePhase::Idle;
}
