//! Invariant 20 in the loop: a bad config never replaces the good one, and a tick pinned to
//! an old generation is refused rather than run against the new config.

use sched_model::test_support::EXAMPLE_CONFIG;
use sched_model::{ConfigStore, Millis, SchedState, Snapshot};
use sched_planner::PlanError;
use sched_service::core::{CoreError, decide};

fn snapshot(generation: u64) -> Snapshot {
    Snapshot {
        config_generation: generation,
        now: Millis(1_000),
        observations: vec![],
        leases: vec![],
        unobservable: Default::default(),
    }
}

#[test]
fn ticks_follow_the_active_generation() {
    let mut store = ConfigStore::new(EXAMPLE_CONFIG).unwrap();
    let state = SchedState::default();
    assert!(decide(store.active(), &snapshot(1), &state).is_ok());

    // A rejected config changes nothing: the old generation still works.
    assert!(
        store
            .apply(&EXAMPLE_CONFIG.replace("min_dwell: 5m", "min_dwell: 5h"))
            .is_err()
    );
    assert_eq!(store.generation(), 1);
    assert!(decide(store.active(), &snapshot(1), &state).is_ok());

    // An accepted config moves the generation; a snapshot pinned to the old one is refused,
    // by the planner first and by the sequencer if the planner were bypassed.
    store
        .apply(&EXAMPLE_CONFIG.replace("idle_grace: 10m", "idle_grace: 15m"))
        .unwrap();
    assert_eq!(
        decide(store.active(), &snapshot(1), &state).unwrap_err(),
        CoreError::Plan(PlanError::GenerationMismatch {
            config: 2,
            snapshot: 1
        })
    );
    let intent = sched_model::Intent::default();
    assert_eq!(
        sched_sequencer::tick(store.active(), &snapshot(1), &state, &intent).unwrap_err(),
        sched_sequencer::TickError::GenerationMismatch {
            config: 2,
            snapshot: 1
        }
    );
    assert!(decide(store.active(), &snapshot(2), &state).is_ok());
}
