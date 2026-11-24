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
pub mod wakers;
pub mod work;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

pub use config::{
    Allowed, Capability, ClassSpec, Config, ConfigError, DemandRef, ModelSpec, PolicyConfig,
    PowerControl, PowerSpec, Preemptibility, RecoverySpec, Requirement, ResourceKind, ResourceSpec,
    SourceKind, SourceSpec, SwitchRef, VetoReading,
};
pub use fixture::{Fixture, FixtureError};
pub use ids::{ClassId, HolderId, JobId, LeaseId, ModelId, ResourceId, SourceId};
pub use job_state::{
    CreateJob, JobBook, JobEvent, JobEventKind, JobRecord, LastRun, Run, RunOutcome, RunPhase,
    Trigger,
};
pub use jobs::{CronJobRef, JobNeed, JobSpec, MissedRun};
pub use lease::{Lease, LeaseKind, LeaseOrigin, host_hold_count, lockout_lease, manual_host_lease};
pub use observation::{
    Fidelity, HostUse, JobRunReading, JobRunState, Observation, ObservedValue, ReadingUse,
    Snapshot, SwitchState,
};
pub use plan::{AllowedSet, Intent, PowerWant, PublishedAllowed, Step, WaitReason};
pub use power_params::{MacAddr, SshShutdown, WakeOnLan};
pub use recovery_params::{HttpMethod, ListSpec, REQUEUE_FAILED, RequeueFailedParams, RequeueSpec};
pub use source_params::{
    AuthSpec, CountsAs, ElementFilter, ElementFilters, ExtraCount, FilterValue, HttpJsonParams,
    KubeWatchParams, MapValue, PrometheusParams, RequestSpec, ResponseCondition, SinceSpec,
    TcpProbeParams, ValueMap, WebhookParams,
};
pub use state::{
    DrainMode, ModePhase, PowerPhase, PowerTransition, ResourceState, SchedState, Shutdown,
    ShutdownCause, SwitchOff, Veto, WindDown, Withdrawal,
};
pub use store::ConfigStore;
pub use time::Millis;
pub use validate::Problem;
pub use wakers::{WakeReason, Waker, wakers, wakers_label};
pub use work::{
    ClassSwitch, ClassWork, SwitchPhase, class_has_work, class_switch, class_work,
    classes_of_model, dependents, gated_by_work, host_is_up, reads_from_class, switch_offs_next,
    switch_phase, work_grace_holds, work_seen_next, work_stamp_allows,
};
