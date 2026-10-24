//! Observers: adapters that read external systems and push what they saw to the scheduler.
//!
//! An observer decides nothing and writes to nothing. It never stamps time: it hands the
//! scheduler a [`Report`] and the scheduler dates it on receipt with its own clock, so a
//! remote service's clock can neither wedge nor mislead a decision. A failed read is
//! reported as [`Report::Failed`], never as a reused old value.

pub mod http_json;
pub mod kube_watch;
mod report;
mod supervisor;

/// A local HTTP server for tests (here and in dependent crates).
#[cfg(any(test, feature = "test-support"))]
pub mod testserver;

pub use report::{Report, ReportTx};
pub use supervisor::{
    ObserverState, Observers, RestartBackoff, RestartPolicy, SharedBackoff, SpawnOptions, observer_available, observer_state,
    shared_backoff, spawn_observers, supervised,
};
