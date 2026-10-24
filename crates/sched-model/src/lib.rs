//! Shared types for the GPU scheduler: resources, models, workload classes, leases,
//! observations, snapshots, intents, steps, and the configuration schema.
//!
//! Pure: no I/O, no async runtime, no wall-clock access. Time is always an explicit
//! [`Millis`] value in the data, so everything built on this crate is deterministic and
//! can be tested by replaying recorded snapshots. Service-specific knowledge never
//! appears here; it lives in configuration.

pub mod config;
pub mod fixture;
pub mod ids;
pub mod lease;
pub mod observation;
pub mod plan;
pub mod source_params;
pub mod state;
pub mod store;
pub mod time;
pub mod validate;
pub mod work;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

pub use config::{
    Capability, ClassSpec, Config, ConfigError, DemandRef, ModelSpec, PolicyConfig, PowerSpec,
    Preemptibility, RecoverySpec, Requirement, ResourceKind, ResourceSpec, SourceKind, SourceSpec,
};
pub use fixture::{Fixture, FixtureError};
pub use ids::{ClassId, HolderId, LeaseId, ModelId, ResourceId, SourceId};
pub use lease::{Lease, LeaseKind, host_hold_count};
pub use observation::{Fidelity, Observation, ObservedValue, Snapshot};
pub use plan::{AllowedSet, Intent, PowerWant, Step, WaitReason};
pub use source_params::{
    AuthSpec, CountsAs, ElementFilter, HttpJsonParams, KubeWatchParams, RequestSpec, SinceSpec,
};
pub use state::{ModePhase, PowerPhase, ResourceState, SchedState, WindDown};
pub use store::ConfigStore;
pub use time::Millis;
pub use validate::Problem;
pub use work::{ClassWork, class_work, classes_of_model, host_is_up};
