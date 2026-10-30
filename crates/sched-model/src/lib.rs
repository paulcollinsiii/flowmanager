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
pub mod job_state;
pub mod jobs;
pub mod lease;
pub mod observation;
pub mod plan;
pub mod power_params;
pub mod recovery_params;
pub mod source_params;
pub mod state;
pub mod store;
pub mod time;
pub mod validate;
pub mod work;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

pub use config::{
    Capability, ClassSpec, Config, ConfigError, DemandRef, ModelSpec, PolicyConfig, PowerControl,
    PowerSpec, Preemptibility, RecoverySpec, Requirement, ResourceKind, ResourceSpec, SourceKind,
    SourceSpec,
};
pub use fixture::{Fixture, FixtureError};
pub use ids::{ClassId, HolderId, JobId, LeaseId, ModelId, ResourceId, SourceId};
pub use job_state::{
    CreateJob, JobBook, JobEvent, JobEventKind, JobRecord, LastRun, Run, RunOutcome, RunPhase,
    Trigger,
};
pub use jobs::{CronJobRef, JobNeed, JobSpec, MissedRun};
pub use lease::{ADOPTED_HOST_PREFIX, Lease, LeaseKind, host_hold_count};
pub use observation::{Fidelity, JobRunReading, JobRunState, Observation, ObservedValue, Snapshot};
pub use plan::{AllowedSet, Intent, PowerWant, PublishedAllowed, Step, WaitReason};
pub use power_params::{MacAddr, SshShutdown, WakeOnLan};
pub use recovery_params::{HttpMethod, ListSpec, REQUEUE_FAILED, RequeueFailedParams, RequeueSpec};
pub use source_params::{
    AuthSpec, CountsAs, ElementFilter, ExtraCount, FilterValue, HttpJsonParams, KubeWatchParams,
    PrometheusParams, RequestSpec, ResponseCondition, SinceSpec, TcpProbeParams, WebhookParams,
};
pub use state::{
    DrainMode, ModePhase, PowerPhase, PowerTransition, ResourceState, SchedState, Shutdown,
    ShutdownCause, ShutdownStage, WindDown,
};
pub use store::ConfigStore;
pub use time::Millis;
pub use validate::Problem;
pub use work::{ClassWork, class_work, classes_of_model, host_is_up};
