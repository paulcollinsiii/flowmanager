//! What `release_host_lease` with a drain and `drain_host` report of the shutdown a person
//! requested. The rules for recording it are pure and live in `sched_sequencer::drain`;
//! `object` journals the result and persists the state.

use sched_model::{DrainMode, Millis, ResourceState, ShutdownCause};
use serde::{Deserialize, Serialize};

/// What `drain_host` and the switch report of the drain a person requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedDrain {
    pub mode: DrainMode,
    /// When the drain was requested.
    pub since: Millis,
    /// Whether the classes that need the host have been taken out of the allowed set.
    pub scaled_down: bool,
}

impl RecordedDrain {
    /// The drain a person requested of the host, if its shutdown is one.
    pub fn of(st: &ResourceState) -> Option<RecordedDrain> {
        let s = st.shutdown?;
        let ShutdownCause::Requested { mode } = s.cause() else {
            return None;
        };
        Some(RecordedDrain {
            mode,
            since: s.since(),
            scaled_down: s.consumers_withdrawn(),
        })
    }
}
