//! The graceful off of a switched class (`switch_phase`), its stored lifecycle
//! (`switch_offs_next`) and the dependents' offline demand (`class_work`), on the example with
//! `nfs-app` (switched, on the NFS host) read by `paperless-ocr-pending`.

use super::*;
use crate::observation::{Fidelity, Observation};
use crate::test_support::example_config_switched_dependents;

const NOW: u64 = 100_000_000;
const MIN: u64 = 60_000;
/// `drain_timeout` of the example.
const DRAIN: u64 = 10 * MIN;

fn obs(source: &str, at: u64, value: ObservedValue) -> Observation {
    Observation {
        source: source.into(),
        observed_at: Millis(at),
        fidelity: Fidelity::Polled,
        value,
    }
}

fn sw_at(state: SwitchState, at: u64) -> Observation {
    obs(
        "ha-wikijs",
        at,
        ObservedValue::Switch {
            class: "nfs-app".into(),
            state,
        },
    )
}

fn sw(state: SwitchState) -> Observation {
    sw_at(state, NOW)
}

fn ocr(ready: u64, in_flight: u64) -> Observation {
    ocr_failed(ready, in_flight, 0)
}

fn ocr_failed(ready: u64, in_flight: u64, failed: u64) -> Observation {
    obs(
        "paperless-ocr-pending",
        NOW,
        ObservedValue::Demand {
            class: "paperless-ocr".into(),
            ready,
            in_flight,
            failed,
            oldest_ready_at: (ready > 0).then_some(Millis(NOW - 1000)),
        },
    )
}

fn resource_up(resource: &str, up: bool) -> Observation {
    obs(
        "cluster-pods",
        NOW,
        ObservedValue::ResourceUp {
            resource: resource.into(),
            up,
        },
    )
}

/// A snapshot at `NOW` in which the NFS host is up, plus `observations`.
fn snap(mut observations: Vec<Observation>) -> Snapshot {
    observations.push(resource_up("basestar-nfs", true));
    Snapshot {
        now: Millis(NOW),
        observations,
        ..Snapshot::default()
    }
}

fn pending(ago: u64) -> SchedState {
    let mut st = SchedState::default();
    st.classes.insert(
        "nfs-app".into(),
        SwitchOff {
            since: Millis(NOW - ago),
            withdrawal: Withdrawal::Pending,
        },
    );
    st
}

fn done(ago: u64) -> SchedState {
    let mut st = pending(ago);
    st.classes.get_mut(&"nfs-app".into()).unwrap().withdrawal = Withdrawal::Done;
    st
}

fn phase(snap: &Snapshot, state: &SchedState) -> SwitchPhase {
    let config = example_config_switched_dependents();
    let class = config.class(&"nfs-app".into()).unwrap();
    switch_phase(&config, snap, state, class).unwrap()
}

fn work(snap: &Snapshot, state: &SchedState) -> ClassWork {
    let config = example_config_switched_dependents();
    let class = config.class(&"paperless-ocr".into()).unwrap();
    class_work(&config, snap, state, class)
}

fn waiting(since: u64) -> SwitchPhase {
    SwitchPhase::Waiting {
        until: Millis(since + DRAIN),
    }
}

#[test]
fn off_with_a_dependent_in_flight_waits_until_the_timeout() {
    let s = snap(vec![sw(SwitchState::Off), ocr(0, 2)]);
    // First read: the wait starts now.
    assert_eq!(phase(&s, &SchedState::default()), waiting(NOW));
    // A stored wait keeps its own start...
    assert_eq!(phase(&s, &pending(3 * MIN)), waiting(NOW - 3 * MIN));
    // ...and ends at since + drain_timeout, not before.
    assert_eq!(phase(&s, &pending(DRAIN - 1)), waiting(NOW - DRAIN + 1));
    assert_eq!(phase(&s, &pending(DRAIN)), SwitchPhase::Off);
    assert_eq!(phase(&s, &pending(DRAIN + MIN)), SwitchPhase::Off);
}

#[test]
fn the_wait_ends_when_no_dependent_has_work_in_flight_and_ready_work_does_not_count() {
    let s = snap(vec![sw(SwitchState::Off), ocr(5, 0)]);
    assert_eq!(phase(&s, &SchedState::default()), SwitchPhase::Off);
    assert_eq!(phase(&s, &pending(MIN)), SwitchPhase::Off);
}

#[test]
fn no_dependents_means_off_at_once() {
    let config = example_config_switched_dependents();
    // Nothing reads from `wikijs`.
    let class = config.class(&"wikijs".into()).unwrap();
    assert_eq!(dependents(&config, &class.id).count(), 0);
    let s = snap(vec![
        obs(
            "ha-wikijs",
            NOW,
            ObservedValue::Switch {
                class: "wikijs".into(),
                state: SwitchState::Off,
            },
        ),
        ocr(0, 2),
    ]);
    assert_eq!(
        switch_phase(&config, &s, &SchedState::default(), class),
        Some(SwitchPhase::Off)
    );
    // The one with a dependent has exactly it.
    let ids: Vec<_> = dependents(&config, &"nfs-app".into())
        .map(|c| c.id.as_str().to_string())
        .collect();
    assert_eq!(ids, ["paperless-ocr"]);
}

#[test]
fn off_now_ends_the_wait_at_once() {
    let s = snap(vec![sw(SwitchState::OffNow), ocr(0, 2)]);
    assert_eq!(phase(&s, &SchedState::default()), SwitchPhase::Off);
    assert_eq!(phase(&s, &pending(MIN)), SwitchPhase::Off);
}

#[test]
fn an_unreadable_switch_ends_the_wait_at_once() {
    for observations in [
        vec![ocr(0, 2)],
        vec![sw_at(SwitchState::Off, NOW - 10 * MIN - 1), ocr(0, 2)],
        vec![sw_at(SwitchState::Off, NOW + 1), ocr(0, 2)],
    ] {
        let s = snap(observations);
        assert_eq!(phase(&s, &pending(MIN)), SwitchPhase::Off);
    }
}

#[test]
fn on_cancels_the_wait_and_removes_the_state() {
    let config = example_config_switched_dependents();
    let s = snap(vec![sw(SwitchState::On), ocr(0, 2)]);
    for st in [SchedState::default(), pending(MIN), done(MIN)] {
        assert_eq!(phase(&s, &st), SwitchPhase::On);
        assert!(!switch_offs_next(&config, &s, &st).contains_key(&"nfs-app".into()));
    }
}

#[test]
fn unknown_dependent_demand_counts_as_in_flight_and_is_bounded() {
    // No demand reading at all: unknown.
    let s = snap(vec![sw(SwitchState::Off)]);
    assert_eq!(phase(&s, &SchedState::default()), waiting(NOW));
    assert_eq!(phase(&s, &pending(DRAIN)), SwitchPhase::Off);
}

#[test]
fn a_dependent_that_is_host_offline_has_nothing_in_flight() {
    // paperless-ocr also requires cuda-central: freshly seen down, its demand is offline,
    // known to be none, so there is nothing to wait for even without a demand reading.
    let s = snap(vec![
        sw(SwitchState::Off),
        resource_up("cuda-central", false),
    ]);
    assert_eq!(phase(&s, &SchedState::default()), SwitchPhase::Off);
}

#[test]
fn a_class_that_was_not_running_has_nothing_to_wait_for() {
    let mut s = snap(vec![sw(SwitchState::Off), ocr(0, 2)]);
    s.observations
        .retain(|o| !matches!(&o.value, ObservedValue::ResourceUp { .. }));
    s.observations.push(resource_up("basestar-nfs", false));
    assert_eq!(phase(&s, &SchedState::default()), SwitchPhase::Off);
}

#[test]
fn a_stored_done_stays_off_whatever_the_dependents_do() {
    let s = snap(vec![sw(SwitchState::Off), ocr(0, 2)]);
    assert_eq!(phase(&s, &done(MIN)), SwitchPhase::Off);
}

fn next(config: &Config, s: &Snapshot, st: &SchedState) -> Option<SwitchOff> {
    switch_offs_next(config, s, st)
        .get(&"nfs-app".into())
        .copied()
}

#[test]
fn the_stored_state_follows_the_phase_and_keeps_its_start() {
    let config = example_config_switched_dependents();
    let waiting_obs = snap(vec![sw(SwitchState::Off), ocr(0, 2)]);
    // Created when off is first read, pending.
    assert_eq!(
        next(&config, &waiting_obs, &SchedState::default()),
        Some(SwitchOff {
            since: Millis(NOW),
            withdrawal: Withdrawal::Pending
        })
    );
    // Kept, with its original start, while the reading stays off.
    assert_eq!(
        next(&config, &waiting_obs, &pending(4 * MIN)),
        Some(SwitchOff {
            since: Millis(NOW - 4 * MIN),
            withdrawal: Withdrawal::Pending
        })
    );
    // Done when the class leaves (here: the timeout), and then kept done.
    let left = SwitchOff {
        since: Millis(NOW - DRAIN),
        withdrawal: Withdrawal::Done,
    };
    assert_eq!(next(&config, &waiting_obs, &pending(DRAIN)), Some(left));
    assert_eq!(next(&config, &waiting_obs, &done(DRAIN)), Some(left));
    // off-now is recorded too, done at once, so a later `off` does not start a wait.
    let now_obs = snap(vec![sw(SwitchState::OffNow), ocr(0, 2)]);
    let after_now = SchedState {
        classes: switch_offs_next(&config, &now_obs, &SchedState::default()),
        ..SchedState::default()
    };
    let done_now = SwitchOff {
        since: Millis(NOW),
        withdrawal: Withdrawal::Done,
    };
    assert_eq!(
        next(&config, &now_obs, &SchedState::default()),
        Some(done_now)
    );
    assert_eq!(next(&config, &waiting_obs, &after_now), Some(done_now));
    // An entry for a class that is no longer configured is dropped.
    let mut st = pending(MIN);
    st.classes.insert(
        "ghost".into(),
        SwitchOff {
            since: Millis(1),
            withdrawal: Withdrawal::Done,
        },
    );
    let out = switch_offs_next(&config, &waiting_obs, &st);
    // (`wikijs`, the example's other switched class, has no reading: off-now, done.)
    let keys: Vec<_> = out.keys().map(|k| k.as_str()).collect();
    assert_eq!(keys, ["nfs-app", "wikijs"]);
}

#[test]
fn a_dependent_is_offline_once_the_wait_is_over_even_with_a_fresh_reading_of_work() {
    let s = snap(vec![sw(SwitchState::Off), ocr(7, 3)]);
    // Wait over (stored done): offline, with nothing, whatever the source reports.
    let w = work(&s, &done(DRAIN));
    assert!(w.offline && w.switched_off, "{w:?}");
    assert_eq!((w.ready, w.in_flight, w.failed), (0, 0, 0));
    assert!(w.known && !w.busy() && !w.wanted());
    // Off with no dependent in flight is over at once; here ready work alone.
    let w = work(
        &snap(vec![sw(SwitchState::Off), ocr(7, 0)]),
        &SchedState::default(),
    );
    assert!(w.offline && w.switched_off && !w.busy(), "{w:?}");
    // off-now is the same at once.
    let w = work(
        &snap(vec![sw(SwitchState::OffNow), ocr(7, 3)]),
        &SchedState::default(),
    );
    assert!(w.offline && w.switched_off && !w.busy(), "{w:?}");
    // An unreadable switch too (fail closed).
    let w = work(&snap(vec![ocr(7, 3)]), &SchedState::default());
    assert!(w.offline && w.switched_off && !w.busy(), "{w:?}");
}

#[test]
fn a_dependent_with_failed_work_has_none_of_it_once_the_wait_is_over() {
    // Failed items, with ready and in-flight work, in a fresh reading.
    let reading = |s: SwitchState| snap(vec![sw(s), ocr_failed(4, 2, 5)]);
    // Wait over (stored done, timed out, off-now): no failed work, not wanted.
    for (name, s, st) in [
        ("done", reading(SwitchState::Off), done(MIN)),
        ("timed out", reading(SwitchState::Off), pending(DRAIN)),
        (
            "off-now",
            reading(SwitchState::OffNow),
            SchedState::default(),
        ),
    ] {
        let w = work(&s, &st);
        assert!(w.offline && w.switched_off, "{name}: {w:?}");
        assert_eq!(w.failed, 0, "{name}: {w:?}");
        assert!(!w.busy() && !w.wanted(), "{name}: {w:?}");
    }
    // The control: while the wait runs and while the switch is on, failed items are reported.
    assert_eq!(
        work(&reading(SwitchState::Off), &pending(MIN)).failed,
        5,
        "during the wait"
    );
    assert_eq!(
        work(&reading(SwitchState::On), &SchedState::default()).failed,
        5,
        "switch on"
    );
}

#[test]
fn a_dependent_during_the_wait_keeps_its_in_flight_work_but_its_ready_work_wants_nothing() {
    let s = snap(vec![sw(SwitchState::Off), ocr(7, 3)]);
    let w = work(&s, &pending(MIN));
    assert!(!w.offline && !w.switched_off, "{w:?}");
    assert_eq!((w.ready, w.in_flight, w.withheld), (0, 3, 7));
    assert_eq!(w.oldest_ready_at, None);
    assert!(w.busy());
}

#[test]
fn a_dependent_is_unaffected_while_the_switch_is_on() {
    let s = snap(vec![sw(SwitchState::On), ocr(7, 3)]);
    for st in [SchedState::default(), pending(MIN), done(MIN)] {
        let w = work(&s, &st);
        assert_eq!((w.ready, w.in_flight, w.withheld), (7, 3, 0));
        assert!(!w.offline);
    }
    // And a class that reads from nothing never is.
    let config = example_config_switched_dependents();
    let tagging = config.class(&"paperless-tagging".into()).unwrap();
    let s = snap(vec![sw(SwitchState::OffNow)]);
    assert!(!class_work(&config, &s, &SchedState::default(), tagging).switched_off);
}
