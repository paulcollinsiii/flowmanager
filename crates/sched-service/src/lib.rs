//! The flowmanager scheduler service.
//!
//! This holds the pure decision loop that wires the planner to the sequencer, the replay
//! harness and invariant checker used to test it, and the I/O layer around it: the
//! observation cache, Restate hosting, configuration delivery, logs and metrics.
//!
//! The scheduler's state (the sequencer's state, leases, which sources have reported) is
//! durable in Restate, and each tick's allowed set is published for the KEDA external scaler,
//! which this process serves (`keda_scaler`). The recovery action `requeue_failed` runs in
//! its own Restate object (`recovery`), and power on and off in the `power` object (Wake-on-LAN
//! through the router, SSH shutdown); kill and pause steps have no actuator and are
//! reported as not executed. Every observer is read-only.

pub mod cache;
pub mod clock;
pub mod config_source;
pub mod core;
pub mod invariants;
pub mod keda_scaler;
pub mod leases;
pub mod metrics;
pub mod metrics_server;
pub mod object;
pub mod persist;
pub mod power;
pub mod reason;
pub mod recovery;
pub mod replay;
pub mod runtime;
pub mod scheduler;
pub mod tick;
