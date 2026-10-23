//! One scheduler tick as a pure function: plan, then sequence.

use sched_model::{Config, Intent, SchedState, Snapshot};
use sched_planner::PlanError;
use sched_sequencer::{Decision, TickError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoreError {
    Plan(PlanError),
    Tick(TickError),
}

impl std::fmt::Display for CoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CoreError::Plan(e) => write!(f, "planning failed: {e}"),
            CoreError::Tick(e) => write!(f, "sequencing failed: {e}"),
        }
    }
}

impl std::error::Error for CoreError {}

/// Plans an intent from the snapshot and state, then sequences it.
pub fn decide(
    config: &Config,
    snapshot: &Snapshot,
    state: &SchedState,
) -> Result<(Intent, Decision), CoreError> {
    let intent = sched_planner::plan(config, snapshot, state).map_err(CoreError::Plan)?;
    let decision =
        sched_sequencer::tick(config, snapshot, state, &intent).map_err(CoreError::Tick)?;
    Ok((intent, decision))
}
