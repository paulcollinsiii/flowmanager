//! `release_host_lease` with a drain: a person releases the manual lease of a shared host and
//! asks for it to go down, even though queued or in-flight work would otherwise keep it up.
//!
//! The request is recorded in the host's sequencer state (`ResourceState::shutdown`, persisted
//! with it, so it survives a restart); the sequencer does the rest (see `sched_sequencer`:
//! scale the consumers down, wait for the leases that hold the host, apply the mount veto,
//! shut the host down). A new manual host lease cancels it; the host being seen down ends it.
//!
//! The rules are pure, so they are tested without Restate; `object::release_host_lease`
//! journals them.

use sched_model::{Config, DrainMode, Millis, ResourceId, ResourceState, Shutdown, ShutdownCause};
use serde::{Deserialize, Serialize};

use crate::persist::CoreState;

/// What `drain_host` and the switch report of the drain a person requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedDrain {
    pub mode: DrainMode,
    /// When the drain was requested.
    pub since: Millis,
    /// Whether the classes that need the host have been taken out of the allowed set.
    pub scaled_down: bool,
}

impl RecordedDrain {
    /// The drain a person requested of the host, if its shutdown is one.
    pub fn of(st: &ResourceState) -> Option<RecordedDrain> {
        let s = st.shutdown?;
        let ShutdownCause::Requested { mode } = s.cause() else {
            return None;
        };
        Some(RecordedDrain {
            mode,
            since: s.since(),
            scaled_down: s.consumers_withdrawn(),
        })
    }
}

/// What a drain request did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Requested {
    /// The drain is recorded.
    Recorded,
    /// Nothing was recorded: the host's power is under `control: manual` (flowmanager never
    /// shuts it down), it has no state yet, or it is not up (nothing consumes it).
    NotApplicable,
}

/// Records a drain of `resource` requested at `now`.
///
/// The person is taking the host down, so the scheduler may shut it down: the host becomes
/// managed, and a shutdown it had stopped at (pods still mounting after `veto_timeout`) is
/// tried afresh, as a new manual request would. A drain already under way keeps its start and
/// its scale-down: a second request only changes the mode: `immediate` after `graceful`
/// ends the wait, and `graceful` after `immediate` leaves it immediate. A person's request
/// takes over an idle drain: it is a requested drain from now on (immediate, as the idle
/// one was), with the start and the scale-down kept.
pub fn request(
    core: &mut CoreState,
    config: &Config,
    resource: &ResourceId,
    mode: DrainMode,
    now: Millis,
) -> Requested {
    if !config.power_managed(resource) {
        return Requested::NotApplicable;
    }
    let Some(st) = core.state.resources.get_mut(resource) else {
        return Requested::NotApplicable;
    };
    if !st.is_on() {
        return Requested::NotApplicable;
    }
    // A person's request takes over a shutdown that has a withdrawal (a drain, idle or
    // requested), keeping its start and withdrawal; anything else starts a requested one.
    st.shutdown = Some(match st.shutdown.filter(Shutdown::is_drain) {
        Some(before) => {
            let before_mode = match before.cause() {
                ShutdownCause::Requested { mode } => mode,
                ShutdownCause::Idle | ShutdownCause::Reclaim => DrainMode::Immediate,
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
    st.managed = true;
    st.idle_since = None;
    Requested::Recorded
}

#[cfg(test)]
mod tests {
    use super::*;
    use sched_model::ResourceState;
    use sched_model::test_support::{example_config, example_config_managed};
    use sched_model::{PowerPhase, Veto, Withdrawal};

    fn core(power: PowerPhase) -> CoreState {
        let mut c = CoreState::default();
        c.state
            .resources
            .insert("basestar-nfs".into(), ResourceState::new(power));
        c
    }

    fn nfs() -> ResourceId {
        "basestar-nfs".into()
    }

    fn state(c: &CoreState) -> &ResourceState {
        c.state.resource(&nfs()).unwrap()
    }

    fn drain(st: &ResourceState) -> Option<RecordedDrain> {
        RecordedDrain::of(st)
    }

    #[test]
    fn a_drain_is_recorded_on_a_host_that_is_up_and_takes_it_over() {
        let mut c = core(PowerPhase::On { since: Millis(1) });
        // An idle shutdown the scheduler gave up on (pods still mounted): tried afresh.
        c.state.resources.get_mut(&nfs()).unwrap().shutdown = Some(
            Shutdown::idle_withdrawn(Millis(1))
                .vetoing(Millis(1))
                .stopped(Millis(2)),
        );
        let r = request(
            &mut c,
            &example_config_managed(),
            &nfs(),
            DrainMode::Graceful,
            Millis(9),
        );
        assert_eq!(r, Requested::Recorded);
        let st = state(&c);
        assert_eq!(
            drain(st),
            Some(RecordedDrain {
                mode: DrainMode::Graceful,
                since: Millis(9),
                scaled_down: false,
            })
        );
        assert!(st.managed);
        assert_eq!(st.shutdown.unwrap().withdrawal(), Withdrawal::Pending);
    }

    #[test]
    fn a_second_request_keeps_the_start_and_the_scale_down_and_changes_the_mode() {
        let mut c = core(PowerPhase::On { since: Millis(1) });
        let config = example_config_managed();
        request(&mut c, &config, &nfs(), DrainMode::Graceful, Millis(9));
        let withdrawn = state(&c).shutdown.unwrap().withdrawn();
        c.state.resources.get_mut(&nfs()).unwrap().shutdown = Some(withdrawn);
        request(&mut c, &config, &nfs(), DrainMode::Immediate, Millis(50));
        assert_eq!(
            drain(state(&c)),
            Some(RecordedDrain {
                mode: DrainMode::Immediate,
                since: Millis(9),
                scaled_down: true,
            })
        );
        // Graceful asked of an immediate drain leaves it immediate.
        request(&mut c, &config, &nfs(), DrainMode::Graceful, Millis(60));
        assert_eq!(drain(state(&c)).map(|d| d.mode), Some(DrainMode::Immediate));
    }

    #[test]
    fn a_request_takes_over_an_idle_drain_keeping_its_start_and_scale_down() {
        let mut c = core(PowerPhase::On { since: Millis(1) });
        c.state.resources.get_mut(&nfs()).unwrap().shutdown =
            Some(Shutdown::idle_withdrawn(Millis(4)));
        request(
            &mut c,
            &example_config_managed(),
            &nfs(),
            DrainMode::Graceful,
            Millis(50),
        );
        assert_eq!(
            drain(state(&c)),
            Some(RecordedDrain {
                mode: DrainMode::Immediate,
                since: Millis(4),
                scaled_down: true,
            })
        );
    }

    #[test]
    fn a_request_after_a_stopped_requested_drain_keeps_it_withdrawn_and_tries_again() {
        let mut c = core(PowerPhase::On { since: Millis(1) });
        c.state.resources.get_mut(&nfs()).unwrap().shutdown = Some(
            Shutdown::requested(DrainMode::Graceful, Millis(4))
                .withdrawn()
                .vetoing(Millis(5))
                .stopped(Millis(8)),
        );
        request(
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
        assert_eq!(drain(state(&c)).map(|d| d.since), Some(Millis(4)));
    }

    #[test]
    fn nothing_is_recorded_for_manual_control_an_unseen_host_or_one_that_is_not_up() {
        let mut up = core(PowerPhase::On { since: Millis(1) });
        assert_eq!(
            request(
                &mut up,
                &example_config(),
                &nfs(),
                DrainMode::Immediate,
                Millis(9)
            ),
            Requested::NotApplicable
        );
        assert_eq!(state(&up).shutdown, None);
        let mut none = CoreState::default();
        assert_eq!(
            request(
                &mut none,
                &example_config_managed(),
                &nfs(),
                DrainMode::Immediate,
                Millis(9)
            ),
            Requested::NotApplicable
        );
        let mut off = core(PowerPhase::Off);
        assert_eq!(
            request(
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
