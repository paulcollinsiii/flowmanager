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

use sched_actuate::AttemptBook;
use sched_model::{JobBook, SchedState, SourceId};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::leases::LeaseBook;

/// The sequencer state and which sources have ever reported ([`CoreState`]).
pub const STATE_KEY: &str = "scheduler_state";
/// The lease book ([`LeaseBook`]).
pub const LEASES_KEY: &str = "lease_book";
/// The recovery action's attempts, kept by the `Recovery` object under the key of each
/// class ([`sched_actuate::AttemptBook`]).
pub const RECOVERY_KEY: &str = "recovery_attempts";
/// The last power action of a resource, kept by the `Power` object under the key of each
/// resource ([`crate::power::PowerLog`]). Informational: nothing reads it back to decide.
pub const POWER_KEY: &str = "power_last_action";
/// The scheduled jobs' state ([`JobBook`]).
pub const JOBS_KEY: &str = "job_book";
/// The backstop chain's epoch (a plain number, not versioned).
pub const EPOCH_KEY: &str = "backstop_epoch";

/// Version of the [`CoreState`] document this build reads and writes.
///
/// Version 2 replaced the per-resource `drain`, `mounts_veto`, `host_idle_since` and
/// `shutdown_confirmed_at` with `shutdown`, `idle_since` and `off_since`. There is no
/// migration: a document of version 1 is an error (see [`decode`]) until it is discarded, and
/// the sequencer then starts from observation. Version 3 (phase 4a) renamed the `reclaim`
/// shutdown cause and wind-down flag to `lockout` and added `locked_down`. Version 4 (phase 4b)
/// replaced the per-resource `managed` flag with `ours` on the `off` and `on` phases.
pub const STATE_VERSION: u32 = 4;
/// Version of the [`LeaseBook`] document this build reads and writes.
///
/// Version 2 (phase 4b) gave every lease an `origin` (`requested` or `adopted`); version 1
/// marked an adopted lease only by the prefix of its id (`adopted-host-`). A version 1 book is
/// read (see [`decode_leases`]), with those leases dropped.
pub const LEASES_VERSION: u32 = 2;
/// How a lease id began in a version 1 lease book when the lease was adopted.
const LEGACY_ADOPTED_PREFIX: &str = "adopted-host-";

/// Version of the [`AttemptBook`] document this build reads and writes.
pub const RECOVERY_VERSION: u32 = 1;

/// Version of the [`crate::power::PowerLog`] document this build reads and writes.
pub const POWER_VERSION: u32 = 1;

/// Version of the [`JobBook`] document this build reads and writes.
pub const JOBS_VERSION: u32 = 1;

/// The keys a tick or a lease request may fail to read, and the recovery handler may clear.
pub const DOCUMENT_KEYS: [&str; 3] = [STATE_KEY, LEASES_KEY, JOBS_KEY];

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

/// Reads the sequencer state. A document of an *older* version is not migrated: it reads as
/// an absent one (a fresh start, from observation), with one log line. A newer version (a
/// rollback) or anything unreadable is an error, as for every document.
pub fn decode_state(raw: Option<&[u8]>) -> Result<CoreState, StateError> {
    if let Some(old) = older_version(raw, STATE_VERSION) {
        tracing::warn!(
            event = "state_version_discarded",
            key = STATE_KEY,
            found = old,
            current = STATE_VERSION,
            "scheduler_state is version {old}, this build reads {STATE_VERSION} and does not migrate: discarded, starting from observation (leases and jobs are kept)"
        );
        return Ok(CoreState::default());
    }
    decode(STATE_KEY, STATE_VERSION, raw)
}

/// The version of a stored document if it is a number below `current`.
fn older_version(raw: Option<&[u8]>, current: u32) -> Option<u64> {
    let env: EnvelopeIn = serde_json::from_slice(raw?).ok()?;
    match env.version {
        Some(serde_json::Value::Number(n)) => n.as_u64().filter(|v| *v < u64::from(current)),
        _ => None,
    }
}

/// Reads the lease book. A book of version 1 is read as it is, except that its adopted leases
/// (a manual host lease whose id begins `adopted-host-`) are dropped, with one log line each:
/// version 1 had no `origin`, so such a lease would otherwise read as one a person asked for
/// and wake its host again after it went down. Nothing is lost by dropping them: the sequencer
/// state is discarded on the same upgrade (version 4), every host that is up is found running
/// and adopted afresh at the first tick, with an `adopted` origin, and one that is down needs
/// no lease. Any other version is an error, as for every document.
pub fn decode_leases(raw: Option<&[u8]>) -> Result<LeaseBook, StateError> {
    if older_version(raw, LEASES_VERSION) == Some(1) {
        let mut book: LeaseBook = decode(LEASES_KEY, 1, raw)?;
        book.leases.retain(|l| {
            let adopted = matches!(l.kind, sched_model::LeaseKind::ManualHost)
                && l.id.as_str().starts_with(LEGACY_ADOPTED_PREFIX);
            if adopted {
                tracing::warn!(
                    event = "legacy_adopted_lease_dropped",
                    lease = %l.id,
                    resource = %l.resource,
                    "lease_book is version 1: the adopted lease {} is dropped (a host that is up is adopted afresh)",
                    l.id
                );
            }
            !adopted
        });
        return Ok(book);
    }
    decode(LEASES_KEY, LEASES_VERSION, raw)
}

pub fn decode_attempts(raw: Option<&[u8]>) -> Result<AttemptBook, StateError> {
    decode(RECOVERY_KEY, RECOVERY_VERSION, raw)
}

pub fn encode_attempts(b: &AttemptBook) -> Vec<u8> {
    encode(RECOVERY_VERSION, b)
}

pub fn encode_state(s: &CoreState) -> Vec<u8> {
    encode(STATE_VERSION, s)
}

pub fn encode_leases(b: &LeaseBook) -> Vec<u8> {
    encode(LEASES_VERSION, b)
}

pub fn decode_jobs(raw: Option<&[u8]>) -> Result<JobBook, StateError> {
    decode(JOBS_KEY, JOBS_VERSION, raw)
}

pub fn encode_jobs(b: &JobBook) -> Vec<u8> {
    encode(JOBS_VERSION, b)
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
        assert!(text.starts_with("{\"version\":3,\"data\":"), "{text}");
    }

    #[test]
    fn the_off_stamp_and_a_shutdown_survive_a_restart() {
        use sched_model::{Millis, PowerPhase, ResourceState};
        let mut s = CoreState::default();
        let mut off = ResourceState::new(PowerPhase::Off { ours: false });
        off.off_since = Some(Millis(4_000));
        s.state.resources.insert("basestar-nfs".into(), off);
        s.state.resources.insert(
            "cuda-central".into(),
            ResourceState::new(PowerPhase::Off { ours: false }),
        );
        let bytes = encode_state(&s);
        assert_eq!(decode_state(Some(&bytes)).unwrap(), s);
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.starts_with("{\"version\":3,\"data\":"), "{text}");
        assert_eq!(text.matches("\"off_since\":4000").count(), 1, "{text}");
        // A shutdown, in the stage it has reached, is part of the stored state.
        s.state
            .resources
            .get_mut(&"basestar-nfs".into())
            .unwrap()
            .shutdown =
            Some(sched_model::Shutdown::idle_withdrawn(Millis(4_500)).vetoing(Millis(5_000)));
        assert_eq!(decode_state(Some(&encode_state(&s))).unwrap(), s);
    }

    #[test]
    fn a_state_of_an_older_version_reads_as_a_fresh_start_and_is_not_migrated() {
        // Version 1 stored `drain`, `mounts_veto`, `host_idle_since` and
        // `shutdown_confirmed_at`; this build neither reads nor converts them.
        let old = br#"{"version":1,"data":{"state":{"resources":{"b":{"power":{"phase":"on","since":1},"mode":{"phase":"idle"},"drain":{"mode":"graceful","since":9}}}},"reported":["x"]}}"#;
        assert_eq!(decode_state(Some(old)).unwrap(), CoreState::default());
        // Version 2 stored a shutdown cause `reclaim` and a wind-down flag `reclaim`.
        let v2 = br#"{"version":2,"data":{"state":{"resources":{"b":{"power":{"phase":"on","since":1},"mode":{"phase":"idle"},"shutdown":{"cause":{"kind":"reclaim"},"since":1,"withdrawal":"not_needed","veto":{"phase":"none"}}}}},"reported":["x"]}}"#;
        assert_eq!(decode_state(Some(v2)).unwrap(), CoreState::default());
        // The lease book is not versioned away: a stored `reclaim` lease (kind renamed
        // `lockout` in phase 4a, no alias) is an error until it is cleared.
        let reclaim_lease = br#"{"version":1,"data":{"next":1,"leases":[{"id":"reclaim-1","resource":"cuda-central","kind":{"type":"reclaim"},"granted_at":1}]}}"#;
        assert!(decode_leases(Some(reclaim_lease)).is_err());
        let renamed = String::from_utf8_lossy(reclaim_lease)
            .replace("reclaim", "lockout")
            .into_bytes();
        assert!(decode_leases(Some(&renamed)).is_ok());
        // Only the sequencer state is treated so: the other documents stay strict.
        assert!(decode_leases(Some(br#"{"version":0,"data":{}}"#)).is_err());
    }

    #[test]
    fn a_state_of_a_newer_version_is_still_an_error() {
        let newer = br#"{"version":4,"data":{"state":{},"reported":[]}}"#;
        let e = decode_state(Some(newer)).unwrap_err();
        assert_eq!(e.key, STATE_KEY);
        assert!(e.problem.contains("version 4"), "{e}");
    }

    #[test]
    fn attempts_are_a_versioned_document_of_their_own() {
        let mut b = AttemptBook::default();
        b.record("7", sched_model::Millis(5));
        let bytes = encode_attempts(&b);
        let text = String::from_utf8(bytes.clone()).unwrap();
        assert!(text.starts_with("{\"version\":1,\"data\":"), "{text}");
        assert_eq!(decode_attempts(Some(&bytes)).unwrap(), b);
        assert_eq!(decode_attempts(None).unwrap(), AttemptBook::default());
        let e = decode_attempts(Some(br#"{"version":2,"data":{}}"#)).unwrap_err();
        assert_eq!(e.key, RECOVERY_KEY);
        assert!(e.problem.contains("version 2"), "{e}");
    }

    #[test]
    fn unknown_versions_garbage_and_unversioned_values_are_errors_not_resets() {
        let newer = br#"{"version":4,"data":{"state":{},"reported":[],"later_field":1}}"#;
        let e = decode_state(Some(newer)).unwrap_err();
        assert_eq!(e.key, STATE_KEY);
        assert!(e.problem.contains("version 4"), "{e}");
        let e = decode_state(Some(b"not json")).unwrap_err();
        assert!(e.problem.contains("not a versioned document"), "{e}");
        // What the unversioned first cut stored.
        let e = decode_leases(Some(br#"{"next":1,"leases":[]}"#)).unwrap_err();
        assert!(e.problem.contains("no version"), "{e}");
        // The right version with a field this build does not know.
        let e = decode_state(Some(br#"{"version":3,"data":{"surprise":true}}"#)).unwrap_err();
        assert!(e.problem.contains("cannot decode version 3"), "{e}");
    }
}
