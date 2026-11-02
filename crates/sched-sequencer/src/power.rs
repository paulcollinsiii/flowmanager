//! Power phases for hosts that have a power section, and the reference count of what holds
//! them up. Resources without a power section are always on.

use sched_model::{
    ClassSpec, DrainMode, Millis, ModePhase, PowerPhase, PowerTransition, PowerWant, ResourceKind,
    ResourceState, Shutdown, ShutdownCause, Step, Veto, WaitReason, Withdrawal, class_work,
};

use crate::Cx;

/// State for a resource the sequencer has not seen before, from observation.
pub(crate) fn initial(cx: &Cx, steps: &mut Vec<Step>) -> Option<ResourceState> {
    let now = cx.now();
    if cx.res.power.is_none() {
        return Some(ResourceState::new(PowerPhase::On {
            since: now,
            ours: false,
        }));
    }
    match cx.snapshot.fresh_resource_up(
        cx.config,
        &cx.res.id,
        cx.config.policy().host_reading_stale_after,
    ) {
        Some(true) => Some(ResourceState::new(PowerPhase::On {
            since: now,
            ours: false,
        })),
        Some(false) => Some(ResourceState::new(PowerPhase::Off { ours: false })),
        None => {
            wait(cx, steps, WaitReason::StaleObservation);
            None
        }
    }
}

/// Brings the power phase in line with a fresh reachability observation.
///
/// Under `control: manual` the scheduler is never responsible for the host's power
/// (`ours` is cleared, also in state persisted by a build or config that managed it), and
/// the phase is simply what is observed: on or off. A transitional phase left from managed
/// control (powering on, shutting down) converges to the observed one.
///
/// `ours` follows the phase: a host flowmanager powered on is ours when it is seen up (the
/// `powering_on` phase has no marker, it is ours by construction), and a blip (`on`, `off`,
/// `on`) keeps it. It is not ours after a confirmed shutdown or a failed transition.
pub(crate) fn reconcile(cx: &Cx, st: &mut ResourceState) {
    if cx.res.power.is_none() {
        return;
    }
    let manual = !cx.config.power_managed(&cx.res.id);
    let was_failed = matches!(st.power, PowerPhase::Failed { .. });
    if manual {
        st.set_ours(false);
        st.idle_since = None;
        st.shutdown = None;
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
            (PowerPhase::On { since, ours }, true) => PowerPhase::On { since, ours },
            (_, true) if manual => PowerPhase::On {
                since: now,
                ours: false,
            },
            (_, false) if manual => PowerPhase::Off { ours: false },
            (PowerPhase::Off { ours }, true) => PowerPhase::On { since: now, ours },
            // Our own power-on completed.
            (PowerPhase::PoweringOn { .. }, true) => PowerPhase::On {
                since: now,
                ours: true,
            },
            // A blip keeps the marker.
            (PowerPhase::On { ours, .. }, false) => PowerPhase::Off { ours },
            // A confirmed shutdown: the host is no longer ours.
            (PowerPhase::ShuttingDown { .. }, false) => PowerPhase::Off { ours: false },
            // The failed state clears only on the state the transition was after; whatever
            // brought the host there is not flowmanager's action, so it is not ours.
            (
                PowerPhase::Failed {
                    during: PowerTransition::PoweringOn,
                    ..
                },
                true,
            ) => PowerPhase::On {
                since: now,
                ours: false,
            },
            (
                PowerPhase::Failed {
                    during: PowerTransition::ShuttingDown,
                    ..
                },
                false,
            ) => PowerPhase::Off { ours: false },
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
    // there is no stamp at all.
    let min_off = cx.res.power.as_ref().map_or(Millis::ZERO, |p| p.min_off());
    st.off_since = match (st.power, st.off_since) {
        _ if manual || min_off == Millis::ZERO => None,
        (PowerPhase::Off { .. }, _)
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
        (PowerPhase::Off { .. }, Some(at)) => Some(at.min(now)),
        _ => None,
    };
    // A failed transition leaves the host not ours (cleared above as the phase leaves `failed`): whatever
    // brings it back (a person, a slow boot) is treated like a host found running, so no
    // automatic power-off follows until a new want powers it on through the normal path.
    if was_failed || matches!(st.power, PowerPhase::Failed { .. }) {
        st.idle_since = None;
    }
    if !st.is_on() {
        st.mode = ModePhase::Idle;
        st.idle_since = None;
        // A shutdown ends when the host is no longer up: seen off, or on its way there.
        st.shutdown = None;
    }
    // A lockout shuts the host down once. Seen off under a lockout (also a lockout taken
    // while the host is off), it is done: the host is left alone, whatever boots it later,
    // until the lockout is cleared. Flowmanager is not responsible for it any more either.
    if manual || !cx.locked_out() {
        st.locked_down = false;
        // A shutdown the lockout forced is over with it; the host, if up, is found running.
        if st.shutdown.is_some_and(|s| s.is_lockout()) {
            st.shutdown = None;
        }
    } else if matches!(st.power, PowerPhase::Off { .. }) {
        st.locked_down = true;
        st.set_ours(false);
        st.idle_since = None;
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
        PowerPhase::Off { .. } => {
            if acted {
                return;
            }
            if cx.locked_out() {
                wait(cx, steps, WaitReason::LockedOut);
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
                st.off_since = None;
                st.idle_since = None;
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
            if cx.locked_out() && st.locked_down {
                // The lockout's one shutdown is done (the host was seen off under it): a
                // later boot is left alone, not powered off, adopted or given work.
                st.shutdown = None;
                st.idle_since = None;
                st.set_ours(false);
                if !acted {
                    wait(cx, steps, WaitReason::LockedOut);
                }
                return;
            }
            if acted {
                return;
            }
            let idle_mode = matches!(st.mode, ModePhase::Idle);
            if cx.locked_out() {
                if cx.res.kind == ResourceKind::Exclusive {
                    // No `idle` check: the mode step has run just before this (the host is
                    // on) and always acts while a model is allowed (`acted`, returned
                    // above), so a host that gets here has no model.
                    power_off(cx, st, steps, Shutdown::lockout_without_consumers(now));
                } else {
                    shared_lockout(cx, st, steps);
                }
                return;
            }
            if want == Some(PowerWant::Manual) {
                st.set_ours(true);
                // A new manual request takes the host over again: an earlier stop (pods
                // still used it) is over, and the next idle period decides afresh.
                if st.shutdown.is_some_and(|s| s.stopped_since().is_some()) {
                    st.shutdown = None;
                }
            }
            // A requested drain ignores queued and in-flight work; an idle one ignores nothing.
            let idle_drain = st
                .shutdown
                .is_some_and(|s| s.is_drain() && s.cause() == ShutdownCause::Idle);
            let h = holds(cx, st.shutdown.is_some_and(|s| s.is_drain()) && !idle_drain);
            if idle_drain && h.count > 0 {
                // Something holds the host again (a lease, or work for a class that needs it):
                // the consumers are allowed again and the idle clock starts from scratch.
                st.shutdown = None;
                st.idle_since = None;
            }
            if let Some(s) = st.shutdown.filter(Shutdown::is_drain)
                && st.ours()
                && idle_mode
            {
                if s.cause() == ShutdownCause::Idle && h.unknown {
                    // As for any shutdown: not while a demand reading is unknown (the drain
                    // stays, nothing is shut down).
                    wait(cx, steps, WaitReason::StaleDemand);
                    return;
                }
                drain_shutdown(cx, st, steps, s, h.count);
            } else if st.ours() && idle_mode && h.count == 0 {
                if h.unknown {
                    wait(cx, steps, WaitReason::StaleDemand);
                    return;
                }
                let since = *st.idle_since.get_or_insert(now);
                if now.saturating_sub(since) >= policy.idle_grace {
                    if cx.res.kind == ResourceKind::Shared {
                        // The consumers go first: an idle drain scales them down (they leave
                        // the allowed set in this very tick), and the shutdown goes on through
                        // the usual veto, which waits while pods still mount the host.
                        // A shared host never reaches here carrying a shutdown: one with a
                        // withdrawal took the branch above, and a stopped one is not ours.
                        let s = Shutdown::idle_withdrawn(now);
                        st.shutdown = Some(s);
                        drain_shutdown(cx, st, steps, s, 0);
                    } else {
                        power_off(cx, st, steps, Shutdown::idle_without_consumers(now));
                    }
                }
            } else {
                st.idle_since = None;
                if let Some(s) = st.shutdown {
                    match s.veto() {
                        // Something holds the host (or it is no longer ours to power off):
                        // the shutdown this wait was for is not due any more.
                        Veto::Vetoing { .. } => st.shutdown = s.without_veto(),
                        Veto::Stopped { .. } => {
                            wait(cx, steps, WaitReason::ShutdownStopped);
                        }
                        Veto::None => {}
                    }
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
        _ if cx.locked_out() => wait(cx, steps, WaitReason::LockedOut),
        // Only left from managed control while reachability is not fresh.
        PowerPhase::PoweringOn { .. } => wait(cx, steps, WaitReason::PoweringOn),
        PowerPhase::ShuttingDown { .. } => wait(cx, steps, WaitReason::ShuttingDown),
        PowerPhase::Failed { .. } => wait(cx, steps, WaitReason::PowerFailed),
        PowerPhase::Off { .. } if needed => wait(cx, steps, WaitReason::HostNotUp),
        PowerPhase::Off { .. } => {}
    }
}

/// The pre-shutdown veto: whether nothing still uses the host. Before any power-off (idle,
/// requested, lockout) every reading that applies to the host must be fresh and show it
/// unused: no pod mounts its NFS server (a `mounts` source names it) and no pod of its models
/// exists (a `pods` source covers them, all of its models). This is the one place a pod
/// reading decides anything, and only to hold a shutdown back. A reading that is missing,
/// stale or failed is unknown, and unknown counts as in use. While the host is (or may be) in
/// use the shutdown waits (veto `Vetoing`), bounded by the resource's `veto_timeout`; when
/// that runs out the scheduler stops (veto `Stopped`: the host stays up, no retry) until the
/// host is seen down, a person resets it, or a new manual request takes it over.
///
/// `shutdown` is the shutdown this power-off belongs to; it is recorded only when the host
/// has none yet (an exclusive host's idle or lockout shutdown starts here).
fn veto_clear(cx: &Cx, st: &mut ResourceState, steps: &mut Vec<Step>, shutdown: Shutdown) -> bool {
    if st.shutdown.is_some_and(|s| s.stopped_since().is_some()) {
        wait(cx, steps, WaitReason::ShutdownStopped);
        return false;
    }
    let now = cx.now();
    let in_use = cx.snapshot.host_use(
        cx.config,
        &cx.res.id,
        cx.config.policy().host_reading_stale_after,
    );
    if in_use.is_unused() {
        st.shutdown = st.shutdown.and_then(Shutdown::without_veto);
        return true;
    }
    let since = st.shutdown.and_then(|s| s.vetoing_since()).unwrap_or(now);
    let timeout = cx
        .res
        .power
        .as_ref()
        .map_or(sched_model::config::DEFAULT_VETO_TIMEOUT, |p| {
            p.veto_timeout()
        });
    let current = st.shutdown.unwrap_or(shutdown);
    if now.saturating_sub(since) >= timeout {
        // The stopped state holds the host as for any shutdown (no longer ours, so no
        // idle drain starts again).
        st.shutdown = Some(current.stopped(now));
        st.set_ours(false);
        st.idle_since = None;
        wait(cx, steps, WaitReason::ShutdownStopped);
    } else {
        st.shutdown = Some(current.vetoing(since));
        let why = if in_use.is_in_use() {
            WaitReason::VetoInUse
        } else {
            WaitReason::VetoUnknown
        };
        wait(cx, steps, why);
    }
    false
}

/// Shuts the host down, provided a fresh reading shows it up and the veto allows it (see
/// `veto_clear`; a host with no reading that applies has none). It is only reached with no
/// model allowed (mode idle). `shutdown` is the shutdown this belongs to, for a host that has
/// none recorded yet.
fn power_off(cx: &Cx, st: &mut ResourceState, steps: &mut Vec<Step>, shutdown: Shutdown) {
    let max_age = cx.config.policy().host_reading_stale_after;
    if cx
        .snapshot
        .fresh_resource_up(cx.config, &cx.res.id, max_age)
        != Some(true)
    {
        wait(cx, steps, WaitReason::StaleObservation);
        return;
    }
    if !cx.config.veto_readings(&cx.res.id).is_empty() && !veto_clear(cx, st, steps, shutdown) {
        return;
    }
    steps.push(Step::PowerOff {
        resource: cx.res.id.clone(),
    });
    st.power = PowerPhase::ShuttingDown { since: cx.now() };
    st.idle_since = None;
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
/// a new manual lease cancels it; otherwise, on a shared host, the consumers that need the
/// host are scaled down (the classes leave the allowed set, see `class_allowed`) at once for
/// `immediate`, and for `graceful` when their in-flight work is done or `policy.drain_timeout`
/// has passed since the request, whichever is first. Once scaled down it stays so until the
/// drain ends.
pub(crate) fn drain_step(cx: &Cx, st: &mut ResourceState) {
    let Some(mut s) = st.shutdown.filter(Shutdown::is_drain) else {
        return;
    };
    if !st.is_on() || !cx.config.power_managed(&cx.res.id) {
        st.shutdown = None;
        return;
    }
    if cx.intent.power_up.get(&cx.res.id) == Some(&PowerWant::Manual) {
        st.shutdown = None;
        return;
    }
    // A lockout taking over a shutdown in progress makes it the lockout's own, on either kind
    // of host (see `Shutdown::under_lockout`): a pending withdrawal gives in-flight work
    // `lockout_timeout` from now, whichever cause the shutdown had; a done one stays done;
    // an `immediate` request whose withdrawal is pending stays as it is (the exclusive mode
    // machine keeps it immediate). Clearing the lockout then drops the shutdown, so a
    // person's earlier `off` does not outlive it.
    if cx.locked_out() {
        s = s.under_lockout(cx.now());
        st.shutdown = Some(s);
    }
    // An exclusive host's withdrawal is the mode machine's (see `withdrawal_step`).
    if cx.res.kind == ResourceKind::Shared
        && s.withdrawal() == Withdrawal::Pending
        && graceful_wait_over(cx, s, no_work_in_flight(cx))
    {
        s = s.withdrawn();
        st.shutdown = Some(s);
    }
}

/// Whether a shutdown's wait for in-flight work is over: it is `immediate`, or the timeout
/// has passed since the shutdown started (`policy.drain_timeout`, or `policy.lockout_timeout`
/// while the host is locked out), or `work_done` (nothing in flight).
fn graceful_wait_over(cx: &Cx, s: Shutdown, work_done: bool) -> bool {
    let waited = cx.now().saturating_sub(s.since());
    let immediate = matches!(
        s.cause(),
        ShutdownCause::Requested {
            mode: DrainMode::Immediate
        }
    );
    let policy = cx.config.policy();
    let timeout = if cx.locked_out() {
        policy.lockout_timeout()
    } else {
        policy.drain_timeout
    };
    immediate || waited >= timeout || work_done
}

/// The lockout of a shared host that is up under managed control: the consumers are
/// withdrawn (in-flight work gets `lockout_timeout`, see [`drain_step`]), then the veto and
/// the power-off, whatever leases or work hold the host. A drain already under way (idle or
/// requested) is carried on as the lockout's; one the veto gave up on stays stopped.
fn shared_lockout(cx: &Cx, st: &mut ResourceState, steps: &mut Vec<Step>) {
    st.idle_since = None;
    if st.shutdown.is_some_and(|s| s.stopped_since().is_some()) {
        wait(cx, steps, WaitReason::ShutdownStopped);
        return;
    }
    let s = match st.shutdown.filter(Shutdown::is_drain) {
        Some(s) => s,
        None => {
            let s = Shutdown::lockout_withdrawing(cx.now());
            st.shutdown = Some(s);
            s
        }
    };
    drain_shutdown(cx, st, steps, s, 0);
}

/// Advances the withdrawal of an exclusive host's requested shutdown (run after the mode
/// step): it is done once the mode machine has taken the model away. The mode machine does
/// the waiting (graceful: in-flight work gets `drain_timeout`; immediate: none), told by the
/// shutdown being requested (see `exclusive::step`). The classes that need only the host
/// (gates) have no demand, so no work to wait for.
pub(crate) fn withdrawal_step(cx: &Cx, st: &mut ResourceState) {
    if cx.res.kind != ResourceKind::Exclusive || !matches!(st.mode, ModePhase::Idle) {
        return;
    }
    if let Some(s) = st.shutdown {
        st.shutdown = Some(s.withdrawn());
    }
}

/// The shutdown of a host with a drain: it waits for the consumers to be scaled down, for
/// every lease that holds the host (`lease_holds`: a job's hold, say; the drain overrides
/// none). Then `power_off` applies the veto as for any shutdown (a managed shared host
/// always has a mounts source, so there is a reading to wait for; no `idle_grace` applies).
fn drain_shutdown(
    cx: &Cx,
    st: &mut ResourceState,
    steps: &mut Vec<Step>,
    s: Shutdown,
    lease_holds: usize,
) {
    if s.withdrawal() == Withdrawal::Pending {
        st.idle_since = None;
        wait(cx, steps, WaitReason::Draining);
        return;
    }
    if lease_holds > 0 {
        st.idle_since = None;
        wait(cx, steps, WaitReason::DrainHeld);
        return;
    }
    power_off(cx, st, steps, s);
}
