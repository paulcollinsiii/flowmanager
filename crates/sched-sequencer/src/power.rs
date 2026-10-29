//! Power phases for hosts that have a power section, and the reference count of what holds
//! them up. Resources without a power section are always on.

use sched_model::{
    ClassSpec, Drain, DrainMode, Millis, ModePhase, MountsVeto, PowerPhase, PowerTransition,
    PowerWant, ResourceState, Step, WaitReason, class_work,
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
        st.mounts_veto = None;
        st.drain = None;
    }
    let now = cx.now();
    let before = st.power;
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
    // The stamp of a confirmed shutdown: the instant the phase reached `off` from a shutdown
    // flowmanager started (also a failed one: the host was observed down after it). A host
    // found down, gone down by itself, or under manual control has none, and the stamp ends
    // with the phase `off`. A stamp from the future (the clock went back) is made again at
    // now, so the wait is never longer than `min_off`. Without a `min_off` (zero, the default)
    // there is no stamp at all, so the stored state is exactly what it was before the key.
    let min_off = cx.res.power.as_ref().map_or(Millis::ZERO, |p| p.min_off());
    st.shutdown_confirmed_at = match (st.power, st.shutdown_confirmed_at) {
        _ if manual || min_off == Millis::ZERO => None,
        (PowerPhase::Off, _)
            if matches!(
                before,
                PowerPhase::ShuttingDown { .. }
                    | PowerPhase::Failed {
                        during: PowerTransition::ShuttingDown,
                        ..
                    }
            ) =>
        {
            Some(now)
        }
        (PowerPhase::Off, Some(at)) => Some(at.min(now)),
        _ => None,
    };
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
        st.mounts_veto = None;
        // A drain ends when the host is no longer up: seen off, or on its way there.
        st.drain = None;
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

/// Active leases plus classes with queued or in-flight work that need the host. During a
/// drain (`ignore_work`) only the leases count: the person asked for the host to go down, so
/// work does not hold it (its consumers are scaled down instead, see [`drain_step`]).
pub(crate) fn holds(cx: &Cx, ignore_work: bool) -> Holds {
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
    if ignore_work {
        return h;
    }
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
            } else if want.is_some()
                && want != Some(PowerWant::Manual)
                && st.min_off_remaining(
                    cx.res.power.as_ref().map_or(Millis::ZERO, |p| p.min_off()),
                    now,
                ) > Millis::ZERO
            {
                // flowmanager shut this host down a moment ago. A person who asked for it
                // (a manual lease) does not wait; anything else does, and decides afresh
                // when the time has passed (a delay, never a retry).
                wait(cx, steps, WaitReason::MinOff);
            } else if want.is_some() {
                steps.push(Step::PowerOn {
                    resource: cx.res.id.clone(),
                });
                st.power = PowerPhase::PoweringOn { since: now };
                st.shutdown_confirmed_at = None;
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
                // No `idle` check: a reclaim exists only for an exclusive resource, whose mode
                // step has run just before this (the host is on) and always acts while a model
                // is allowed (`acted`, returned above), so a host that gets here has no model.
                power_off(cx, st, steps);
                return;
            }
            if want == Some(PowerWant::Manual) {
                st.managed = true;
                // A new manual request takes the host over again: an earlier stop (pods
                // still mounted it) is over, and the next idle period decides afresh.
                if matches!(st.mounts_veto, Some(MountsVeto::Stopped { .. })) {
                    st.mounts_veto = None;
                }
            }
            let h = holds(cx, st.drain.is_some());
            if let Some(d) = st.drain
                && st.managed
                && idle_mode
            {
                drain_shutdown(cx, st, steps, d, h.count);
            } else if st.managed && idle_mode && h.count == 0 {
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
                match st.mounts_veto {
                    // Something holds the host (or it is no longer ours to power off): the
                    // shutdown this wait was for is not due any more.
                    Some(MountsVeto::Waiting { .. }) => st.mounts_veto = None,
                    Some(MountsVeto::Stopped { .. }) => {
                        wait(cx, steps, WaitReason::ShutdownStopped);
                    }
                    None => {}
                }
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

/// The pre-shutdown veto: whether no pod mounts the host. This is the one place a pod
/// reading decides anything, and only to hold a shutdown back. A reading that is missing,
/// stale or failed is unknown, and unknown counts as mounted. While pods mount (or may mount)
/// the host the shutdown waits, bounded by the resource's `mounts_timeout`; when that runs
/// out the scheduler stops (the host stays up, no retry) until the host is seen down, a person
/// resets it, or a new manual request takes it over.
fn mounts_clear(cx: &Cx, st: &mut ResourceState, steps: &mut Vec<Step>) -> bool {
    if matches!(st.mounts_veto, Some(MountsVeto::Stopped { .. })) {
        wait(cx, steps, WaitReason::ShutdownStopped);
        return false;
    }
    let now = cx.now();
    let reading = cx.snapshot.fresh_resource_mounts(
        cx.config,
        &cx.res.id,
        cx.config.policy().host_reading_stale_after,
    );
    if reading == Some(0) {
        st.mounts_veto = None;
        return true;
    }
    let since = match st.mounts_veto {
        Some(MountsVeto::Waiting { since }) => since,
        _ => now,
    };
    let timeout = cx
        .res
        .power
        .as_ref()
        .map_or(sched_model::config::DEFAULT_MOUNTS_TIMEOUT, |p| {
            p.mounts_timeout()
        });
    if now.saturating_sub(since) >= timeout {
        st.mounts_veto = Some(MountsVeto::Stopped { since: now });
        st.managed = false;
        st.host_idle_since = None;
        wait(cx, steps, WaitReason::ShutdownStopped);
    } else {
        st.mounts_veto = Some(MountsVeto::Waiting { since });
        let why = if reading.is_none() {
            WaitReason::MountsUnknown
        } else {
            WaitReason::PodsMounted
        };
        wait(cx, steps, why);
    }
    false
}

/// Shuts the host down, provided a fresh reading shows it up and (for a host whose mounts
/// are checked) no pod mounts it. It is only reached with no model allowed (mode idle);
/// pod counts of models are not consulted.
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
    if cx.config.mounts_checked(&cx.res.id) && !mounts_clear(cx, st, steps) {
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

/// Whether every class that needs the host and has a demand source is known to have nothing
/// in flight (the work the consumers would be cut off from is already done).
fn no_work_in_flight(cx: &Cx) -> bool {
    cx.config
        .classes()
        .iter()
        .filter(|c| needs(c, cx) && c.demand.is_some())
        .all(|c| {
            let w = class_work(cx.config, cx.snapshot, c);
            w.known && w.in_flight == 0
        })
}

/// Advances a drain on a host that is up under managed control (run before the power step):
/// a new manual lease cancels it; otherwise the consumers that need the host are scaled down
/// (the classes leave the allowed set, see `class_allowed`) at once for `immediate`, and for
/// `graceful` when their in-flight work is done or `policy.drain_timeout` has passed since the
/// request, whichever is first. Once scaled down it stays so until the drain ends.
pub(crate) fn drain_step(cx: &Cx, st: &mut ResourceState) {
    let Some(mut d) = st.drain else {
        return;
    };
    if !st.is_on() || !cx.config.power_managed(&cx.res.id) {
        st.drain = None;
        return;
    }
    if cx.intent.power_up.get(&cx.res.id) == Some(&PowerWant::Manual) {
        st.drain = None;
        return;
    }
    if !d.scaled_down {
        let waited = cx.now().saturating_sub(d.since);
        d.scaled_down = d.mode == DrainMode::Immediate
            || waited >= cx.config.policy().drain_timeout
            || no_work_in_flight(cx);
        st.drain = Some(d);
    }
}

/// The shutdown of a host with a drain: it waits for the consumers to be scaled down, for
/// every lease that holds the host (`lease_holds`: a job's hold, say; the drain overrides
/// none). Then `power_off` applies the mount veto as for any shutdown (a managed shared host
/// always has a mounts source, so there is a pod reading to wait for; no `idle_grace` applies).
fn drain_shutdown(
    cx: &Cx,
    st: &mut ResourceState,
    steps: &mut Vec<Step>,
    d: Drain,
    lease_holds: usize,
) {
    if !d.scaled_down {
        st.host_idle_since = None;
        wait(cx, steps, WaitReason::Draining);
        return;
    }
    if lease_holds > 0 {
        st.host_idle_since = None;
        wait(cx, steps, WaitReason::DrainHeld);
        return;
    }
    power_off(cx, st, steps);
}
