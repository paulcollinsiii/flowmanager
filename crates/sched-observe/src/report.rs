//! What observers send to the scheduler.

use sched_model::{Fidelity, ObservedValue, SourceId};
use tokio::sync::mpsc;

/// One message from an observer. It carries no timestamps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Report {
    /// Values read from `source`. `Demand::oldest_ready_at` is left unset: it is a remote
    /// timestamp, and the scheduler derives its own from when it first saw the work.
    Observed {
        source: SourceId,
        fidelity: Fidelity,
        values: Vec<ObservedValue>,
    },
    /// The source could not be read. Old values are not refreshed, so they age out.
    Failed { source: SourceId, error: String },
}

impl Report {
    pub fn source(&self) -> &SourceId {
        match self {
            Report::Observed { source, .. } | Report::Failed { source, .. } => source,
        }
    }
}

/// Where observers send their reports.
pub type ReportTx = mpsc::Sender<Report>;
