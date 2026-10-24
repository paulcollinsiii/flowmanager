//! What the `Scheduler` object keeps in Restate, and how it is versioned.
//!
//! Each persisted document is stored as JSON `{"version": N, "data": ...}`. The handlers
//! read the raw bytes and decode them here, so a value that cannot be decoded, or that was
//! written by a newer build (an unknown version, e.g. after a rollback), never fails the
//! invocation (which Restate would retry forever, blocking the exclusive handlers): the
//! caller gets a [`StateError`] and decides what to do (see `object`).
//!
//! Bump a document's version whenever its shape changes in a way an older build cannot
//! read (a field added under `deny_unknown_fields`, a field renamed or retyped). A build
//! reads only its own version; an absent key reads as the default (a fresh start).

use std::collections::BTreeSet;

use sched_model::{SchedState, SourceId};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::leases::LeaseBook;

/// The sequencer state and which sources have ever reported ([`CoreState`]).
pub const STATE_KEY: &str = "scheduler_state";
/// The lease book ([`LeaseBook`]).
pub const LEASES_KEY: &str = "lease_book";
/// The backstop chain's epoch (a plain number, not versioned).
pub const EPOCH_KEY: &str = "backstop_epoch";

/// Version of the [`CoreState`] document this build reads and writes.
pub const STATE_VERSION: u32 = 1;
/// Version of the [`LeaseBook`] document this build reads and writes.
pub const LEASES_VERSION: u32 = 1;

/// The keys a tick or a lease request may fail to read, and the recovery handler may clear.
pub const DOCUMENT_KEYS: [&str; 2] = [STATE_KEY, LEASES_KEY];

/// The scheduler's own durable state, besides the leases.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoreState {
    #[serde(default)]
    pub state: SchedState,
    /// Configured sources that have reported values at some point.
    #[serde(default)]
    pub reported: BTreeSet<SourceId>,
}

/// A persisted document that cannot be used.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateError {
    pub key: String,
    pub problem: String,
}

impl std::fmt::Display for StateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.key, self.problem)
    }
}

#[derive(Serialize)]
struct EnvelopeOut<'a, T> {
    version: u32,
    data: &'a T,
}

#[derive(Deserialize)]
struct EnvelopeIn {
    version: Option<serde_json::Value>,
    data: Option<serde_json::Value>,
}

/// The stored form of `data` at `version`.
pub fn encode<T: Serialize>(version: u32, data: &T) -> Vec<u8> {
    serde_json::to_vec(&EnvelopeOut { version, data }).expect("persisted state serializes")
}

/// Reads a stored document of `version`; an absent value is the default.
pub fn decode<T: DeserializeOwned + Default>(
    key: &str,
    version: u32,
    raw: Option<&[u8]>,
) -> Result<T, StateError> {
    let err = |problem: String| StateError {
        key: key.to_string(),
        problem,
    };
    let Some(raw) = raw else {
        return Ok(T::default());
    };
    let env: EnvelopeIn =
        serde_json::from_slice(raw).map_err(|e| err(format!("not a versioned document: {e}")))?;
    let found = match env.version {
        Some(serde_json::Value::Number(n)) => n.as_u64(),
        _ => None,
    };
    match found {
        Some(v) if v == u64::from(version) => {}
        Some(v) => {
            return Err(err(format!(
                "version {v} is not the version this build reads ({version}); it was probably written by another build"
            )));
        }
        None => return Err(err("no version (written before versioning?)".to_string())),
    }
    let data = env
        .data
        .ok_or_else(|| err("version but no data".to_string()))?;
    serde_json::from_value(data).map_err(|e| err(format!("cannot decode version {version}: {e}")))
}

pub fn decode_state(raw: Option<&[u8]>) -> Result<CoreState, StateError> {
    decode(STATE_KEY, STATE_VERSION, raw)
}

pub fn decode_leases(raw: Option<&[u8]>) -> Result<LeaseBook, StateError> {
    decode(LEASES_KEY, LEASES_VERSION, raw)
}

pub fn encode_state(s: &CoreState) -> Vec<u8> {
    encode(STATE_VERSION, s)
}

pub fn encode_leases(b: &LeaseBook) -> Vec<u8> {
    encode(LEASES_VERSION, b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn documents_round_trip_and_an_absent_key_is_a_fresh_start() {
        let mut s = CoreState::default();
        s.reported.insert("cluster-pods".into());
        assert_eq!(decode_state(Some(&encode_state(&s))).unwrap(), s);
        assert_eq!(decode_state(None).unwrap(), CoreState::default());
        assert_eq!(decode_leases(None).unwrap(), LeaseBook::default());
        let text = String::from_utf8(encode_state(&s)).unwrap();
        assert!(text.starts_with("{\"version\":1,\"data\":"), "{text}");
    }

    #[test]
    fn unknown_versions_garbage_and_unversioned_values_are_errors_not_resets() {
        let newer = br#"{"version":2,"data":{"state":{},"reported":[],"later_field":1}}"#;
        let e = decode_state(Some(newer)).unwrap_err();
        assert_eq!(e.key, STATE_KEY);
        assert!(e.problem.contains("version 2"), "{e}");
        let e = decode_state(Some(b"not json")).unwrap_err();
        assert!(e.problem.contains("not a versioned document"), "{e}");
        // What the unversioned first cut stored.
        let e = decode_leases(Some(br#"{"next":1,"leases":[]}"#)).unwrap_err();
        assert!(e.problem.contains("no version"), "{e}");
        // The right version with a field this build does not know.
        let e = decode_state(Some(br#"{"version":1,"data":{"surprise":true}}"#)).unwrap_err();
        assert!(e.problem.contains("cannot decode version 1"), "{e}");
    }
}
