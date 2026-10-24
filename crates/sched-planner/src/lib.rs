//! Policy strategies that turn a config, a snapshot and the sequencer's state into an
//! intent: which model each exclusive resource should serve and which hosts must be up.
//!
//! Pure: depends only on `sched-model`; no I/O, no async runtime, no wall-clock access.
//! Deterministic: the same inputs always produce the same intent. The planner reads the
//! sequencer's state (allowed-since, idle-since) but never changes it, and knows nothing of
//! pods or drain timing: it decides from queues, leases and policy alone.

use sched_model::{Config, Intent, SchedState, Snapshot};

mod fifo;

pub use fifo::FifoExhaustive;

/// A named scheduling policy.
pub trait Strategy: Sync {
    fn name(&self) -> &'static str;
    fn plan(&self, config: &Config, snapshot: &Snapshot, state: &SchedState) -> Intent;
}

static REGISTRY: &[&dyn Strategy] = &[&FifoExhaustive];

/// Looks a strategy up by the name used in `policy.strategy`.
pub fn strategy(name: &str) -> Option<&'static dyn Strategy> {
    REGISTRY.iter().copied().find(|s| s.name() == name)
}

/// Names of every registered strategy.
pub fn strategy_names() -> Vec<&'static str> {
    REGISTRY.iter().map(|s| s.name()).collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanError {
    UnknownStrategy(String),
    /// The snapshot was taken against a different config generation than the one given.
    GenerationMismatch {
        config: u64,
        snapshot: u64,
    },
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanError::UnknownStrategy(name) => write!(
                f,
                "unknown policy strategy {name:?}; registered: {}",
                strategy_names().join(", ")
            ),
            PlanError::GenerationMismatch { config, snapshot } => write!(
                f,
                "snapshot is pinned to config generation {snapshot} but the config is generation {config}"
            ),
        }
    }
}

impl std::error::Error for PlanError {}

/// Plans with the strategy named by the config's policy.
pub fn plan(config: &Config, snapshot: &Snapshot, state: &SchedState) -> Result<Intent, PlanError> {
    if snapshot.config_generation != config.generation() {
        return Err(PlanError::GenerationMismatch {
            config: config.generation(),
            snapshot: snapshot.config_generation,
        });
    }
    let name = &config.policy().strategy;
    let strategy = strategy(name).ok_or_else(|| PlanError::UnknownStrategy(name.clone()))?;
    Ok(strategy.plan(config, snapshot, state))
}
