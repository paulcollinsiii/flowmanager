//! `release_host_lease` with a drain: a person releases the manual lease of a shared host and
//! asks for it to go down, even though queued or in-flight work would otherwise keep it up.
//!
//! The request is recorded in the host's sequencer state (`ResourceState::drain`, persisted
//! with it, so it survives a restart); the sequencer does the rest (see `sched_sequencer`:
//! scale the consumers down, wait for the leases that hold the host, apply the mount veto,
//! shut the host down). A new manual host lease cancels it; the host being seen down ends it.
//!
//! The rules are pure, so they are tested without Restate; `object::release_host_lease`
//! journals them.

use sched_model::{Config, Drain, DrainMode, DrainOrigin, Millis, MountsVeto, ResourceId};

use crate::persist::CoreState;

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
/// managed, and a shutdown it had stopped at (pods still mounting after `mounts_timeout`) is
/// tried afresh, as a new manual request would. A drain already under way keeps its start and
/// its scale-down: a second request only changes the mode: `immediate` after `graceful`
/// ends the wait, and `graceful` after `immediate` leaves it immediate.
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
    let before = st.drain;
    st.drain = Some(Drain {
        // Immediate escalates; graceful asked of an immediate drain leaves it immediate.
        mode: if before.is_some_and(|d| d.mode == DrainMode::Immediate) {
            DrainMode::Immediate
        } else {
            mode
        },
        since: before.map_or(now, |d| d.since),
        scaled_down: before.is_some_and(|d| d.scaled_down),
        // A person's request takes over an idle drain: it is a requested drain from now on, with
        // the start and the scale-down kept.
        origin: DrainOrigin::Requested,
    });
    st.managed = true;
    st.host_idle_since = None;
    if matches!(st.mounts_veto, Some(MountsVeto::Stopped { .. })) {
        st.mounts_veto = None;
    }
    Requested::Recorded
}

#[cfg(test)]
mod tests {
    use super::*;
    use sched_model::PowerPhase;
    use sched_model::ResourceState;
    use sched_model::test_support::{example_config, example_config_managed};

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

    #[test]
    fn a_drain_is_recorded_on_a_host_that_is_up_and_takes_it_over() {
        let mut c = core(PowerPhase::On { since: Millis(1) });
        c.state.resources.get_mut(&nfs()).unwrap().mounts_veto =
            Some(MountsVeto::Stopped { since: Millis(2) });
        let r = request(
            &mut c,
            &example_config_managed(),
            &nfs(),
            DrainMode::Graceful,
            Millis(9),
        );
        assert_eq!(r, Requested::Recorded);
        let st = c.state.resource(&nfs()).unwrap();
        assert_eq!(
            st.drain,
            Some(Drain {
                mode: DrainMode::Graceful,
                since: Millis(9),
                scaled_down: false,
                origin: DrainOrigin::Requested
            })
        );
        assert!(st.managed);
        assert_eq!(st.mounts_veto, None);
    }

    #[test]
    fn a_second_request_keeps_the_start_and_the_scale_down_and_changes_the_mode() {
        let mut c = core(PowerPhase::On { since: Millis(1) });
        let config = example_config_managed();
        request(&mut c, &config, &nfs(), DrainMode::Graceful, Millis(9));
        c.state
            .resources
            .get_mut(&nfs())
            .unwrap()
            .drain
            .as_mut()
            .unwrap()
            .scaled_down = true;
        request(&mut c, &config, &nfs(), DrainMode::Immediate, Millis(50));
        assert_eq!(
            c.state.resource(&nfs()).unwrap().drain,
            Some(Drain {
                mode: DrainMode::Immediate,
                since: Millis(9),
                scaled_down: true,
                origin: DrainOrigin::Requested
            })
        );
        // Graceful asked of an immediate drain leaves it immediate.
        request(&mut c, &config, &nfs(), DrainMode::Graceful, Millis(60));
        assert_eq!(
            c.state.resource(&nfs()).unwrap().drain.map(|d| d.mode),
            Some(DrainMode::Immediate)
        );
    }

    #[test]
    fn a_request_takes_over_an_idle_drain_keeping_its_start_and_scale_down() {
        let mut c = core(PowerPhase::On { since: Millis(1) });
        c.state.resources.get_mut(&nfs()).unwrap().drain = Some(Drain {
            mode: DrainMode::Immediate,
            since: Millis(4),
            scaled_down: true,
            origin: DrainOrigin::Idle,
        });
        request(
            &mut c,
            &example_config_managed(),
            &nfs(),
            DrainMode::Graceful,
            Millis(50),
        );
        assert_eq!(
            c.state.resource(&nfs()).unwrap().drain,
            Some(Drain {
                mode: DrainMode::Immediate,
                since: Millis(4),
                scaled_down: true,
                origin: DrainOrigin::Requested
            })
        );
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
        assert_eq!(up.state.resource(&nfs()).unwrap().drain, None);
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
