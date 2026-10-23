//! Power phases for hosts that have a power section, and the reference count of what holds
//! them up. Resources without a power section are always on.

use sched_model::{
    ClassSpec, ModePhase, PowerPhase, PowerWant, ResourceKind, ResourceState, Step, WaitReason,
    class_work,
};

use crate::{Cx, others_zero};

/// State for a resource the sequencer has not seen before, from observation.
pub(crate) fn initial(cx: &Cx, steps: &mut Vec<Step>) -> Option<ResourceState> {
    let now = cx.now();
    if cx.res.power.is_none() {
        return Some(ResourceState::new(PowerPhase::On { since: now }));
    }
    match cx
        .snapshot
        .fresh_resource_up(&cx.res.id, cx.config.policy().safety_max_age)
    {
        Some(true) => Some(ResourceState::new(PowerPhase::On { since: now })),
        Some(false) => Some(ResourceState::new(PowerPhase::Off)),
        None => {
            wait(cx, steps, WaitReason::StaleObservation);
            None
        }
    }
}

/// Brings the power phase in line with a fresh reachability observation.
pub(crate) fn reconcile(cx: &Cx, st: &mut ResourceState) {
    if cx.res.power.is_none() {
        return;
    }
    let now = cx.now();
    let Some(up) = cx
        .snapshot
        .fresh_resource_up(&cx.res.id, cx.config.policy().safety_max_age)
    else {
        return;
    };
    st.power = match (st.power, up) {
        (PowerPhase::Off | PowerPhase::PoweringOn { .. }, true) => PowerPhase::On { since: now },
        (PowerPhase::On { .. }, false) => PowerPhase::Off,
        (PowerPhase::ShuttingDown { .. }, false) => PowerPhase::Off,
        (p, _) => p,
    };
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

/// Shuts the host down, provided the observations that make it safe are fresh.
fn power_off(cx: &Cx, st: &mut ResourceState, steps: &mut Vec<Step>) {
    let max_age = cx.config.policy().safety_max_age;
    if cx.snapshot.fresh_resource_up(&cx.res.id, max_age) != Some(true) {
        wait(cx, steps, WaitReason::StaleObservation);
        return;
    }
    if cx.res.kind == ResourceKind::Exclusive
        && let Err(reason) = others_zero(cx, None)
    {
        wait(cx, steps, reason);
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
