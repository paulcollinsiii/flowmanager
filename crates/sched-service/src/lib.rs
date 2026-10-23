//! The flowmanager scheduler service.
//!
//! This holds the pure decision loop that wires the planner to the sequencer, the replay
//! harness and invariant checker used to test it, and the I/O layer around it: the
//! observation cache, Restate hosting, configuration delivery, logs and metrics.
//!
//! The service currently runs in shadow mode: it observes and logs what it would do, and
//! never acts. There is no actuator, and every observer is read-only.

pub mod cache;
pub mod clock;
pub mod config_source;
pub mod core;
pub mod invariants;
pub mod metrics;
pub mod metrics_server;
pub mod reason;
pub mod replay;
pub mod runtime;
pub mod scheduler;
pub mod tick;
