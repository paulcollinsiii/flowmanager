//! The rules for recording a shutdown a person asked for (`release_host_lease` with a drain,
//! `drain_host`): pure, so they are tested without Restate. The service only reads the state,
//! applies [`request`] and persists the result.
//!
//! The request is recorded in the host's state (`ResourceState::shutdown`, persisted with it,
//! so it survives a restart); the sequencer does the rest (see `power`: withdraw the
//! consumers, wait for the leases that hold the host, apply the veto, shut the host down). A
//! new manual host lease cancels it; the host being seen down ends it.

use sched_model::{
    Config, DrainMode, Millis, ResourceId, SchedState, Shutdown, ShutdownCause, Waker,
};

/// What a drain request did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Requested {
    /// The drain is recorded.
    Recorded,
    /// A graceful request was refused because the host has wakers (`docs/architecture/power.md`, *Wakers*): nothing
    /// was recorded or changed.
    Refused,
    /// Nothing was recorded: the host's power is under `control: manual` (flowmanager never
    /// shuts it down), it has no state yet, it is not up (nothing consumes it), or it is
    /// locked out (the lockout shuts it down, or leaves it alone, whatever is asked).
    NotApplicable,
}

/// Records a drain of `resource` requested at `now` in `state`.
///
/// The person is taking the host down, so the scheduler may shut it down: the host becomes
/// ours (`ours`), and a shutdown it had stopped at (pods still mounting after `veto_timeout`)
/// is tried afresh, as a new manual request would. A drain already under way keeps its start
/// and its scale-down: a second request only changes the mode: `immediate` after `graceful`
/// ends the wait, and `graceful` after `immediate` leaves it immediate. A person's request
/// takes over an idle drain: it is a requested drain from now on (immediate, as the idle
/// one was), with the start and the scale-down kept.
///
/// `wakers` are the host's wakers as of the last tick (`HostStatus::wakers`). A `graceful`
/// request is refused while there are any (a graceful off must never end in a bounce, see `docs/architecture/power.md`, *Wakers*):
/// [`Requested::Refused`], with nothing changed. An `immediate` one is never refused. A request
/// that is not applicable is reported as such, whatever the wakers.
pub fn request(
    state: &mut SchedState,
    config: &Config,
    resource: &ResourceId,
    mode: DrainMode,
    now: Millis,
    wakers: &[Waker],
) -> Requested {
    if !config.power_managed(resource) {
        return Requested::NotApplicable;
    }
    let Some(st) = state.resources.get_mut(resource) else {
        return Requested::NotApplicable;
    };
    if !st.is_on() {
        return Requested::NotApplicable;
    }
    // A lockout's shutdown is stricter than a request and is not taken over: the host is
    // being shut down already, and once it is down (or while it stays up and left alone) a
    // request changes nothing until the lockout is cleared.
    if st.shutdown.is_some_and(|s| s.is_lockout()) || st.locked_down {
        return Requested::NotApplicable;
    }
    if mode == DrainMode::Graceful && !wakers.is_empty() {
        return Requested::Refused;
    }
    // A person's request takes over a shutdown that has a withdrawal (a drain, idle or
    // requested), keeping its start and withdrawal; anything else starts a requested one.
    st.shutdown = Some(match st.shutdown.filter(Shutdown::is_drain) {
        Some(before) => {
            let before_mode = match before.cause() {
                ShutdownCause::Requested { mode } => mode,
                ShutdownCause::Idle | ShutdownCause::Lockout => DrainMode::Immediate,
            };
            // Immediate escalates; graceful asked of an immediate drain leaves it immediate.
            let mode = if before_mode == DrainMode::Immediate {
                DrainMode::Immediate
            } else {
                mode
            };
            // A stop is tried afresh, as for a new manual request.
            before
                .taken_over(mode)
                .unwrap_or_else(|| Shutdown::requested(mode, now))
        }
        // No drain yet; a stopped shutdown of another kind is tried afresh, as above.
        None => Shutdown::requested(mode, now),
    });
    st.set_ours(true);
    st.idle_since = None;
    Requested::Recorded
}

#[cfg(test)]
mod tests {
    use super::*;
    use sched_model::ResourceState;
    use sched_model::test_support::{example_config, example_config_managed};
    use sched_model::{PowerPhase, Veto, WakeReason, Withdrawal};

    /// A request with no wakers.
    fn ask(
        c: &mut SchedState,
        config: &Config,
        resource: &ResourceId,
        mode: DrainMode,
        now: Millis,
    ) -> Requested {
        request(c, config, resource, mode, now, &[])
    }

    fn waker() -> Vec<Waker> {
        vec![Waker {
            class: "gpu-batch".into(),
            why: WakeReason::Work,
        }]
    }

    #[test]
    fn a_graceful_request_is_refused_while_the_host_has_wakers_and_changes_nothing() {
        let (config, now) = (example_config_managed(), Millis(9));
        for before in [
            None,
            Some(Shutdown::requested(DrainMode::Immediate, Millis(2))),
        ] {
            let mut c = core(up());
            c.resources.get_mut(&nfs()).unwrap().shutdown = before;
            let was = c.clone();
            let r = request(&mut c, &config, &nfs(), DrainMode::Graceful, now, &waker());
            assert_eq!(r, Requested::Refused);
            assert_eq!(c, was, "nothing changes");
        }
    }

    #[test]
    fn an_immediate_request_is_never_refused_by_wakers() {
        let (config, now) = (example_config_managed(), Millis(9));
        let mut c = core(up());
        let r = request(&mut c, &config, &nfs(), DrainMode::Immediate, now, &waker());
        assert_eq!(r, Requested::Recorded);
        assert_eq!(mode_of(state(&c)), Some(DrainMode::Immediate));
    }

    #[test]
    fn a_request_that_does_not_apply_is_not_a_refusal() {
        let (config, now) = (example_config_managed(), Millis(9));
        let mut c = core(PowerPhase::Off { ours: false });
        let r = request(&mut c, &config, &nfs(), DrainMode::Graceful, now, &waker());
        assert_eq!(r, Requested::NotApplicable);
    }

    fn up() -> PowerPhase {
        PowerPhase::On {
            since: Millis(1),
            ours: false,
        }
    }

    fn core(power: PowerPhase) -> SchedState {
        let mut c = SchedState::default();
        c.resources
            .insert("basestar-nfs".into(), ResourceState::new(power));
        c
    }

    fn nfs() -> ResourceId {
        "basestar-nfs".into()
    }

    fn state(c: &SchedState) -> &ResourceState {
        c.resource(&nfs()).unwrap()
    }

    fn mode_of(st: &ResourceState) -> Option<DrainMode> {
        match st.shutdown?.cause() {
            ShutdownCause::Requested { mode } => Some(mode),
            _ => None,
        }
    }

    #[test]
    fn a_request_under_a_lockout_records_nothing() {
        let (config, now) = (example_config_managed(), Millis(9));
        // The lockout's own shutdown under way (shared host: withdrawal pending).
        let mut c = core(up());
        let lockout = Shutdown::lockout_withdrawing(Millis(2));
        c.resources.get_mut(&nfs()).unwrap().shutdown = Some(lockout);
        for mode in [DrainMode::Graceful, DrainMode::Immediate] {
            let r = ask(&mut c, &config, &nfs(), mode, now);
            assert_eq!(r, Requested::NotApplicable);
            assert_eq!(state(&c).shutdown, Some(lockout), "not taken over");
        }
        // The lockout's one shutdown done and the host booted again: left alone.
        let mut c = core(up());
        c.resources.get_mut(&nfs()).unwrap().locked_down = true;
        let r = ask(&mut c, &config, &nfs(), DrainMode::Immediate, now);
        assert_eq!(r, Requested::NotApplicable);
        assert_eq!(state(&c).shutdown, None);
        // Without a lockout the same host takes the request.
        let mut c = core(up());
        let r = ask(&mut c, &config, &nfs(), DrainMode::Immediate, now);
        assert_eq!(r, Requested::Recorded);
    }

    #[test]
    fn a_drain_is_recorded_on_a_host_that_is_up_and_takes_it_over() {
        let mut c = core(up());
        // An idle shutdown the scheduler gave up on (pods still mounted): tried afresh.
        c.resources.get_mut(&nfs()).unwrap().shutdown = Some(
            Shutdown::idle_withdrawn(Millis(1))
                .vetoing(Millis(1))
                .stopped(Millis(2)),
        );
        let r = ask(
            &mut c,
            &example_config_managed(),
            &nfs(),
            DrainMode::Graceful,
            Millis(9),
        );
        assert_eq!(r, Requested::Recorded);
        let st = state(&c);
        let s = st.shutdown.unwrap();
        assert_eq!(mode_of(st), Some(DrainMode::Graceful));
        assert_eq!(s.since(), Millis(9));
        assert!(!s.consumers_withdrawn());
        // The person is taking the host down: it is ours to power off.
        assert!(st.ours());
        assert_eq!(s.withdrawal(), Withdrawal::Pending);
    }

    #[test]
    fn a_second_request_keeps_the_start_and_the_scale_down_and_changes_the_mode() {
        let mut c = core(up());
        let config = example_config_managed();
        ask(&mut c, &config, &nfs(), DrainMode::Graceful, Millis(9));
        let withdrawn = state(&c).shutdown.unwrap().withdrawn();
        c.resources.get_mut(&nfs()).unwrap().shutdown = Some(withdrawn);
        ask(&mut c, &config, &nfs(), DrainMode::Immediate, Millis(50));
        let s = state(&c).shutdown.unwrap();
        assert_eq!(mode_of(state(&c)), Some(DrainMode::Immediate));
        assert_eq!((s.since(), s.consumers_withdrawn()), (Millis(9), true));
        // Graceful asked of an immediate drain leaves it immediate.
        ask(&mut c, &config, &nfs(), DrainMode::Graceful, Millis(60));
        assert_eq!(mode_of(state(&c)), Some(DrainMode::Immediate));
    }

    #[test]
    fn a_request_takes_over_an_idle_drain_keeping_its_start_and_scale_down() {
        let mut c = core(up());
        c.resources.get_mut(&nfs()).unwrap().shutdown = Some(Shutdown::idle_withdrawn(Millis(4)));
        ask(
            &mut c,
            &example_config_managed(),
            &nfs(),
            DrainMode::Graceful,
            Millis(50),
        );
        let s = state(&c).shutdown.unwrap();
        assert_eq!(mode_of(state(&c)), Some(DrainMode::Immediate));
        assert_eq!((s.since(), s.consumers_withdrawn()), (Millis(4), true));
    }

    #[test]
    fn a_request_after_a_stopped_requested_drain_keeps_it_withdrawn_and_tries_again() {
        let mut c = core(up());
        c.resources.get_mut(&nfs()).unwrap().shutdown = Some(
            Shutdown::requested(DrainMode::Graceful, Millis(4))
                .withdrawn()
                .vetoing(Millis(5))
                .stopped(Millis(8)),
        );
        ask(
            &mut c,
            &example_config_managed(),
            &nfs(),
            DrainMode::Graceful,
            Millis(50),
        );
        let after = state(&c).shutdown.unwrap();
        assert_eq!(
            (after.withdrawal(), after.veto()),
            (Withdrawal::Done, Veto::None)
        );
        assert_eq!(after.since(), Millis(4));
    }

    #[test]
    fn nothing_is_recorded_for_manual_control_an_unseen_host_or_one_that_is_not_up() {
        let mut manual = core(up());
        assert_eq!(
            ask(
                &mut manual,
                &example_config(),
                &nfs(),
                DrainMode::Immediate,
                Millis(9)
            ),
            Requested::NotApplicable
        );
        assert_eq!(state(&manual).shutdown, None);
        let mut none = SchedState::default();
        assert_eq!(
            ask(
                &mut none,
                &example_config_managed(),
                &nfs(),
                DrainMode::Immediate,
                Millis(9)
            ),
            Requested::NotApplicable
        );
        let mut off = core(PowerPhase::Off { ours: false });
        assert_eq!(
            ask(
                &mut off,
                &example_config_managed(),
                &nfs(),
                DrainMode::Immediate,
                Millis(9)
            ),
            Requested::NotApplicable
        );
    }
}
