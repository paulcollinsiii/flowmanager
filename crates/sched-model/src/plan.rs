//! The planner's output (an intent), the sequencer's output (steps and the allowed set).

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::ids::{ClassId, ModelId, ResourceId};
use crate::time::Millis;

/// Why a host should be powered up. Only these two reasons may ever power a host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PowerWant {
    /// A manual lease from a user request. The host is then ours (`ours`): it is powered down
    /// again once nothing holds it.
    Manual,
    /// A workload class that may change power. Wakes a host that is off; a host that was
    /// already up is left as it was found.
    Workload,
}

/// What the system should converge to. Says nothing about how to get there.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Intent {
    /// Per exclusive resource, the model it should serve; `None` means no model.
    #[serde(default)]
    pub models: BTreeMap<ResourceId, Option<ModelId>>,
    /// Resources where reaching `models` is forced (a manual model lease or a lockout):
    /// batch work is cut short, and in-flight work gets a timeout instead of being awaited.
    #[serde(default)]
    pub forced: BTreeSet<ResourceId>,
    /// Hosts that must be powered up, and why.
    #[serde(default)]
    pub power_up: BTreeMap<ResourceId, PowerWant>,
    /// Resources the user has locked out (any resource with a power section, shared or
    /// exclusive): intake stops and they are shut down once, then neither woken, adopted nor
    /// scheduled until the lockout is cleared.
    #[serde(default)]
    pub lockout: BTreeSet<ResourceId>,
}

/// Why the sequencer is not moving a resource this tick. Pod readings gate one decision only:
/// a power-off, through the veto (`VetoInUse`, `VetoUnknown`). Kubernetes enforces GPU
/// exclusivity, so no model switch waits for pods.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitReason {
    /// A safety-critical observation (host reachability) is missing or older than
    /// `host_reading_stale_after`.
    StaleObservation,
    /// A scheduling observation needed to prove no work is in flight is missing or stale.
    StaleDemand,
    /// A lockout holds the host: it is shut down (or already is, and left alone) and nothing
    /// wakes or schedules it.
    LockedOut,
    /// A lockout lease names a resource that cannot be locked out (one without a
    /// power section, say after a config change). It is ignored.
    LockoutUnsupported,
    HostNotUp,
    PoweringOn,
    ShuttingDown,
    /// The host's power transition (on or off) timed out and the scheduler stopped: it takes
    /// no further power action for the host by itself and allows nothing on it until a
    /// reachability observation shows the state the transition was after, or a person acts.
    PowerFailed,
    /// The host is due to be powered off, but a pod still uses it (a fresh reading shows pods
    /// mounting its NFS server or running its models): the shutdown waits, bounded by the
    /// resource's `veto_timeout`.
    VetoInUse,
    /// As `VetoInUse`, but a reading the veto needs is unknown (none, or a stale or
    /// failed one). Unknown means possibly in use, never "unused".
    VetoUnknown,
    /// The veto wait outlasted `veto_timeout`: the scheduler stopped and
    /// will not shut the host down by itself (a person acts, or a new manual request takes the
    /// host over again). The host stays up.
    ShutdownStopped,
    /// The host was shut down by flowmanager and has not been off for `power.min_off` yet:
    /// an automatic power-on (a workload, a scheduled job's hold) waits for it to pass. A
    /// manual lease does not wait. The wait is a delay of a fresh decision, not a retry.
    MinOff,
    /// The allowed model has not been allowed for `min_dwell` yet.
    MinDwell,
    /// The allowed model's active work is not declared preemptible.
    NotPreemptible,
    /// In-flight work (as its queue reports it) is still running, and no timeout applies
    /// or it has not expired.
    InFlightWork,
    /// A drain was requested for the host (its manual lease was released with a drain mode)
    /// and the shutdown has not been decided yet: `graceful` in-flight work is still given its
    /// `drain_timeout` (the consumers are still allowed).
    Draining,
    /// A drain is under way (the consumers are scaled down) but a lease other than a manual
    /// one still holds the host (a scheduled job's hold, say): the shutdown waits for it.
    DrainHeld,
}

/// The next safe action toward an intent.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Step {
    /// Nothing to do on this resource yet; a precondition for the next move does not hold.
    Wait {
        resource: ResourceId,
        reason: WaitReason,
    },
    /// Allow a model: it enters the allowed set (KEDA then scales it up). In a switch this
    /// comes with the `WithdrawModel` of the previous model in the same tick; the new pod stays
    /// pending in Kubernetes until the old one has released the GPU.
    AllowModel {
        resource: ResourceId,
        model: ModelId,
    },
    /// Withdraw a model's allowance: it leaves the allowed set, and KEDA scales its pods
    /// down. Nothing waits for them to be gone.
    WithdrawModel {
        resource: ResourceId,
        model: ModelId,
    },
    PowerOn {
        resource: ResourceId,
    },
    PowerOff {
        resource: ResourceId,
    },
    /// A class leaves the allowed set because its model is being wound down. In-flight work
    /// continues. Consumers are not gated on their demand class (only gate classes, whose
    /// requirements are hosts, gate a consumer), so nothing actually stops the consumer
    /// taking new work: the step is recorded, not carried out.
    PauseIntake {
        class: ClassId,
    },
    /// Stop a class's in-flight work after its timeout ran out.
    KillInFlight {
        class: ClassId,
    },
    /// Run the class's declared recovery action after a kill.
    RunRecovery {
        class: ClassId,
    },
}

impl Step {
    /// The resource this step acts on, if it is resource-scoped.
    pub fn resource(&self) -> Option<&ResourceId> {
        match self {
            Step::Wait { resource, .. }
            | Step::AllowModel { resource, .. }
            | Step::WithdrawModel { resource, .. }
            | Step::PowerOn { resource }
            | Step::PowerOff { resource } => Some(resource),
            Step::PauseIntake { .. } | Step::KillInFlight { .. } | Step::RunRecovery { .. } => None,
        }
    }
}

/// What KEDA is told is currently permitted to run. Models are separate from classes: a
/// model is allowed from the tick it is allowed (while serving or winding down), at most
/// one per exclusive resource; a class is allowed only when every one of its requirements
/// is met (a model requirement: that model serving and not winding down; any other
/// requirement: that host up).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllowedSet {
    #[serde(default)]
    pub models: BTreeSet<ModelId>,
    #[serde(default)]
    pub classes: BTreeSet<ClassId>,
}

/// An allowed set as published for the KEDA scaler: the set one tick decided, with the
/// config generation that tick was pinned to and the tick's time.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedAllowed {
    pub config_generation: u64,
    /// The time of the tick that decided `allowed`.
    pub at: Millis,
    pub allowed: AllowedSet,
}
