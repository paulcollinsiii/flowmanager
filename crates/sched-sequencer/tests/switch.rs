//! What each switch says (`sched_sequencer::switch`): every precedence rule, on statuses built
//! by hand for the host they are about.

use sched_model::{
    DrainMode, Millis, ModelId, PowerPhase, PowerTransition, ResourceKind, ShutdownCause,
    VetoReading, WaitReason, Withdrawal,
};
use sched_sequencer::status::{
    BlockerState, Drain, DrainCause, HostMode, HostStatus, LockoutStatus, MinOff, ShutdownStatus,
    VetoBlocker, VetoStatus,
};
use sched_sequencer::switch::{
    HostRequest, HostSwitch, HostWait, LastTick, LockoutSwitchStatus as L, ModelStatus as M,
    SwitchState as S, host_switch, lockout_status, model_status,
};

const ON: PowerPhase = PowerPhase::On {
    since: Millis(1),
    ours: false,
};
const OFF: PowerPhase = PowerPhase::Off { ours: false };
const POWERING_ON: PowerPhase = PowerPhase::PoweringOn { since: Millis(1) };
const SHUTTING_DOWN: PowerPhase = PowerPhase::ShuttingDown { since: Millis(1) };
const FAILED: PowerPhase = PowerPhase::Failed {
    since: Millis(1),
    during: PowerTransition::PoweringOn,
};

fn host(power: PowerPhase) -> HostStatus {
    HostStatus {
        resource: "h".into(),
        kind: ResourceKind::Exclusive,
        has_power: true,
        managed: true,
        power,
        ours: false,
        mode: HostMode::Idle,
        shutdown: None,
        veto_readings: vec![VetoReading::Mounts],
        lease: None,
        lockout: None,
        min_off: None,
        wait: None,
        attention: false,
    }
}

fn model(m: &str) -> ModelId {
    m.into()
}

/// A host in `power` with a graceful requested shutdown, its withdrawal still pending.
fn draining(power: PowerPhase) -> HostStatus {
    with_shutdown(
        power,
        ShutdownCause::Requested {
            mode: DrainMode::Graceful,
        },
        Withdrawal::Pending,
        VetoStatus::Clear,
    )
}

fn with_shutdown(
    power: PowerPhase,
    cause: ShutdownCause,
    withdrawal: Withdrawal,
    veto: VetoStatus,
) -> HostStatus {
    HostStatus {
        shutdown: Some(ShutdownStatus {
            cause,
            since: Millis(2),
            withdrawal,
            veto,
        }),
        ..host(power)
    }
}

/// The drain of [`draining`].
fn its_drain() -> Drain {
    Drain {
        cause: DrainCause::Requested,
        mode: DrainMode::Graceful,
        since: Millis(2),
        scaled_down: false,
    }
}

/// A drain a request answered with, different from [`its_drain`].
fn answered() -> Drain {
    Drain {
        cause: DrainCause::Requested,
        mode: DrainMode::Immediate,
        since: Millis(9),
        scaled_down: true,
    }
}

fn read(h: Option<&HostStatus>, held: bool, locked: bool) -> HostSwitch {
    host_switch(h, held, locked, HostRequest::Read)
}

#[test]
fn a_lease_held_reads_on_whatever_else_is_going_on() {
    for h in [host(ON), host(OFF), draining(ON)] {
        for locked in [false, true] {
            assert_eq!(read(Some(&h), true, locked).state, S::On, "{h:?}");
        }
    }
    assert_eq!(read(None, true, true).state, S::On);
    // A drain shown beside a held lease does not change that.
    let s = read(Some(&draining(ON)), true, false);
    assert_eq!((s.state, s.drain), (S::On, Some(its_drain())));
    let answered_now = HostRequest::Answered {
        drain: Some(answered()),
    };
    assert_eq!(host_switch(None, true, false, answered_now).state, S::On);
}

#[test]
fn with_no_lease_a_drain_reads_draining_ahead_of_a_lockout() {
    let s = read(Some(&draining(ON)), false, false);
    assert_eq!((s.state, s.drain), (S::Draining, Some(its_drain())));
    let s = read(Some(&draining(ON)), false, true);
    assert_eq!((s.state, s.drain), (S::Draining, Some(its_drain())));
    let s = host_switch(
        None,
        false,
        true,
        HostRequest::Answered {
            drain: Some(answered()),
        },
    );
    assert_eq!((s.state, s.drain), (S::Draining, Some(answered())));
}

#[test]
fn a_lockout_reads_locked_out_unless_the_host_is_seen_off() {
    for h in [
        host(ON),
        host(POWERING_ON),
        host(SHUTTING_DOWN),
        host(FAILED),
    ] {
        assert_eq!(read(Some(&h), false, true).state, S::LockedOut, "{h:?}");
        assert_eq!(read(Some(&h), false, false).state, S::Off, "{h:?}");
    }
    // No status: not seen off.
    assert_eq!(read(None, false, true).state, S::LockedOut);
    assert_eq!(read(Some(&host(OFF)), false, true).state, S::Off);
    assert_eq!(read(None, false, false).state, S::Off);
}

#[test]
fn a_read_shows_the_drain_of_the_status_only_while_the_host_is_up() {
    for power in [OFF, POWERING_ON, SHUTTING_DOWN, FAILED] {
        let s = read(Some(&draining(power)), false, false);
        assert_eq!((s.state, s.drain), (S::Off, None), "{power:?}");
    }
    // A lockout's shutdown is not a drain, nor is one with nothing to withdraw.
    for (cause, w) in [
        (ShutdownCause::Lockout, Withdrawal::Pending),
        (ShutdownCause::Lockout, Withdrawal::NotNeeded),
        (ShutdownCause::Idle, Withdrawal::NotNeeded),
    ] {
        let h = with_shutdown(ON, cause, w, VetoStatus::Clear);
        let s = read(Some(&h), false, false);
        assert_eq!((s.state, s.drain), (S::Off, None), "{cause:?} {w:?}");
    }
    // An idle one with a withdrawal is, whoever started it.
    let h = with_shutdown(ON, ShutdownCause::Idle, Withdrawal::Done, VetoStatus::Clear);
    let s = read(Some(&h), false, false);
    assert_eq!(s.state, S::Draining);
    assert_eq!(s.drain.map(|d| d.cause), Some(DrainCause::Idle));
}

#[test]
fn the_answer_to_a_request_wins_over_the_status_either_way() {
    // None recorded: the older report's drain is not shown, and the switch is off.
    let none = HostRequest::Answered { drain: None };
    let s = host_switch(Some(&draining(ON)), false, false, none);
    assert_eq!((s.state, s.drain), (S::Off, None));
    // Recorded: shown, also on a host the report does not show up (it is not filtered).
    let got = HostRequest::Answered {
        drain: Some(answered()),
    };
    let s = host_switch(Some(&host(OFF)), false, false, got);
    assert_eq!((s.state, s.drain), (S::Draining, Some(answered())));
    let s = host_switch(Some(&draining(ON)), false, false, got);
    assert_eq!(s.drain, Some(answered()));
}

#[test]
fn a_plain_release_reads_on_while_the_report_shows_the_host_up_even_locked_out() {
    let released =
        |h: Option<&HostStatus>, locked| host_switch(h, false, locked, HostRequest::Released);
    for locked in [false, true] {
        let s = released(Some(&host(ON)), locked);
        assert_eq!((s.state, s.drain), (S::On, None), "{locked}");
        // The drain under way in the report is not shown, and does not read draining.
        let s = released(Some(&draining(ON)), locked);
        assert_eq!((s.state, s.drain), (S::On, None), "{locked}");
    }
    // Not up: as for any other answer with no drain.
    assert_eq!(released(Some(&host(OFF)), false).state, S::Off);
    assert_eq!(released(Some(&host(OFF)), true).state, S::Off);
    assert_eq!(released(Some(&host(SHUTTING_DOWN)), false).state, S::Off);
    assert_eq!(
        released(Some(&host(SHUTTING_DOWN)), true).state,
        S::LockedOut
    );
    assert_eq!(released(None, false).state, S::Off);
    assert_eq!(released(None, true).state, S::LockedOut);
}

#[test]
fn the_waits_are_the_veto_then_min_off_then_the_lockout_and_absent_only_with_nothing_to_say() {
    assert_eq!(read(None, false, false).waits, None);
    assert_eq!(
        read(None, false, true).waits,
        Some(vec![HostWait::LockedOut])
    );
    assert_eq!(read(Some(&host(ON)), false, false).waits, Some(vec![]));

    let blocking = vec![VetoBlocker {
        reading: VetoReading::Mounts,
        state: BlockerState::InUse,
    }];
    let min_off = MinOff {
        since: None,
        remaining: Millis(61_000),
        until: Millis(9),
    };
    let veto = |stopped| HostWait::Veto {
        readings: vec![VetoReading::Mounts],
        blocking: blocking.clone(),
        stopped,
    };
    let waiting = VetoStatus::Waiting {
        since: Millis(3),
        blocking: blocking.clone(),
    };
    let stopped = VetoStatus::Stopped {
        since: Millis(3),
        blocking: blocking.clone(),
    };
    let mut h = with_shutdown(ON, ShutdownCause::Idle, Withdrawal::Done, waiting);
    h.wait = Some(WaitReason::MinOff);
    h.min_off = Some(min_off);
    assert_eq!(
        read(Some(&h), false, true).waits,
        Some(vec![
            veto(false),
            HostWait::MinOff {
                remaining: Millis(61_000)
            },
            HostWait::LockedOut,
        ])
    );
    let h = with_shutdown(ON, ShutdownCause::Lockout, Withdrawal::Done, stopped);
    assert_eq!(read(Some(&h), false, false).waits, Some(vec![veto(true)]));

    // A minimum is said only when it is the host's wait this tick, and only with a minimum.
    let mut h = host(OFF);
    h.min_off = Some(min_off);
    assert_eq!(read(Some(&h), false, false).waits, Some(vec![]));
    h.wait = Some(WaitReason::LockedOut);
    assert_eq!(read(Some(&h), false, false).waits, Some(vec![]));
    let mut h = host(OFF);
    h.wait = Some(WaitReason::MinOff);
    assert_eq!(read(Some(&h), false, false).waits, Some(vec![]));
}

fn seen(h: Option<&HostStatus>) -> Option<LastTick<'_>> {
    Some(LastTick {
        host: h,
        saw_lease: true,
    })
}

fn not_seen(h: Option<&HostStatus>) -> Option<LastTick<'_>> {
    Some(LastTick {
        host: h,
        saw_lease: false,
    })
}

#[test]
fn a_model_switch_says_nothing_without_a_lease_a_lease_book_or_a_tick() {
    let h = host(ON);
    assert_eq!(model_status(false, Some(true), seen(Some(&h)), true), None);
    assert_eq!(model_status(true, None, seen(Some(&h)), true), None);
    assert_eq!(model_status(true, Some(false), None, true), None);
}

#[test]
fn a_lockout_in_the_book_answers_for_a_model_ahead_of_any_report() {
    let h = host(ON);
    let locked = |tick, allowed| model_status(true, Some(true), tick, allowed);
    let on = Some(M::LockedOut { phase: Some("on") });
    assert_eq!(locked(seen(Some(&h)), true), on);
    assert_eq!(locked(not_seen(Some(&h)), true), on);
    // No tick yet, or no state: said all the same, with no phase.
    let bare = Some(M::LockedOut { phase: None });
    assert_eq!(locked(None, false), bare);
    assert_eq!(locked(seen(None), false), bare);
}

#[test]
fn a_model_status_is_the_first_of_not_seen_allowed_no_state_failed_not_up_and_the_mode() {
    fn status(tick: Option<LastTick<'_>>, allowed: bool) -> Option<M> {
        model_status(true, Some(false), tick, allowed)
    }
    let failed = host(FAILED);
    assert_eq!(status(not_seen(Some(&failed)), true), Some(M::LeaseNotSeen));
    assert_eq!(status(not_seen(None), true), Some(M::LeaseNotSeen));
    // Allowed goes ahead of the host: a report whose host failed still says active.
    assert_eq!(status(seen(Some(&failed)), true), Some(M::Active));
    assert_eq!(status(seen(None), true), Some(M::Active));
    assert_eq!(status(seen(None), false), Some(M::NoHostState));
    assert_eq!(status(seen(Some(&failed)), false), Some(M::HostFailed));
    for (power, phase) in [
        (OFF, "off"),
        (POWERING_ON, "powering_on"),
        (SHUTTING_DOWN, "shutting_down"),
    ] {
        let mut h = host(power);
        // Not up: the mode is not read.
        h.mode = HostMode::WindingDown { model: model("o") };
        assert_eq!(
            status(seen(Some(&h)), false),
            Some(M::WaitingForHost { phase })
        );
    }
    let mut h = host(ON);
    assert_eq!(status(seen(Some(&h)), false), Some(M::HostIdle));
    h.mode = HostMode::Serving { model: model("o") };
    assert_eq!(
        status(seen(Some(&h)), false),
        Some(M::OtherServing { model: model("o") })
    );
    h.mode = HostMode::WindingDown { model: model("o") };
    assert_eq!(
        status(seen(Some(&h)), false),
        Some(M::OtherWindingDown { model: model("o") })
    );
}

#[test]
fn a_lockout_switch_says_nothing_without_a_lease_or_a_tick() {
    let h = host(ON);
    assert_eq!(lockout_status(false, seen(Some(&h))), None);
    assert_eq!(lockout_status(true, None), None);
}

#[test]
fn a_lockout_status_is_not_seen_then_no_state_then_the_power_phase() {
    let failed = host(FAILED);
    fn status(tick: Option<LastTick<'_>>) -> Option<L> {
        lockout_status(true, tick)
    }
    assert_eq!(status(not_seen(Some(&failed))), Some(L::LeaseNotSeen));
    assert_eq!(status(not_seen(None)), Some(L::LeaseNotSeen));
    assert_eq!(status(seen(None)), Some(L::NoHostState));
    for (power, want) in [
        (FAILED, L::HostFailed),
        (OFF, L::PoweredOff),
        (SHUTTING_DOWN, L::ShuttingDown),
        (POWERING_ON, L::PoweringOn),
    ] {
        // Not up: neither the mode nor the lockout status is read.
        let mut h = host(power);
        h.mode = HostMode::WindingDown { model: model("o") };
        h.lockout = Some(LockoutStatus::LeftAlone);
        h.managed = false;
        assert_eq!(status(seen(Some(&h))), Some(want), "{power:?}");
    }
}

#[test]
fn a_lockout_on_an_up_host_is_the_wind_down_then_the_status_then_manual_then_not_yet() {
    let say = |mode: HostMode, lockout, managed| {
        let mut h = host(ON);
        h.mode = mode;
        h.lockout = lockout;
        h.managed = managed;
        lockout_status(true, seen(Some(&h))).unwrap()
    };
    let down = || HostMode::WindingDown { model: model("o") };
    let wound = || L::ModelWindingDown { model: model("o") };
    // A wind-down goes ahead of whatever the status says, and of manual control.
    for lockout in [
        None,
        Some(LockoutStatus::Held),
        Some(LockoutStatus::Withdrawing),
        Some(LockoutStatus::LeftAlone),
    ] {
        for managed in [true, false] {
            assert_eq!(say(down(), lockout, managed), wound(), "{lockout:?}");
        }
    }
    // Both ahead of manual control.
    for managed in [true, false] {
        assert_eq!(
            say(HostMode::Idle, Some(LockoutStatus::Withdrawing), managed),
            L::Withdrawing
        );
        assert_eq!(
            say(HostMode::Idle, Some(LockoutStatus::LeftAlone), managed),
            L::LeftAlone
        );
    }
    let serving = || HostMode::Serving { model: model("o") };
    for lockout in [None, Some(LockoutStatus::Held)] {
        assert_eq!(say(serving(), lockout, false), L::LeftForPerson);
        assert_eq!(say(HostMode::Idle, lockout, false), L::LeftForPerson);
        assert_eq!(say(serving(), lockout, true), L::NotYetShutDown);
        assert_eq!(say(HostMode::Idle, lockout, true), L::NotYetShutDown);
    }
}

#[test]
fn a_model_and_a_lockout_switch_read_on_or_off_by_their_lease() {
    assert_eq!(S::of_lease(true), S::On);
    assert_eq!(S::of_lease(false), S::Off);
}
