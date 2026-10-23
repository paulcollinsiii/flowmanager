//! The flowmanager scheduler service. For now this holds the pure decision loop that wires
//! the planner to the sequencer, plus the replay harness and invariant checker used to test
//! it. Restate wiring, the KEDA endpoint and startup come in later phases.

pub mod core;
pub mod invariants;
pub mod replay;
