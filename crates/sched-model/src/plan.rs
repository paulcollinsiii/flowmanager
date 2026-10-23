//! The planner's output (an intent), the sequencer's output (steps and the allowed set).

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::ids::{ClassId, ModelId, ResourceId};

/// Why a host should be powered up. Only these two reasons may ever power a host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PowerWant {
    /// A manual lease from a user request. The host is then managed: it is powered down
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
    /// Resources where reaching `models` is forced (a manual model lease or a reclaim):
    /// batch work is cut short, and in-flight work gets a timeout instead of being awaited.
    #[serde(default)]
    pub forced: BTreeSet<ResourceId>,
    /// Hosts that must be powered up, and why.
    #[serde(default)]
    pub power_up: BTreeMap<ResourceId, PowerWant>,
    /// Reclaimable resources the user has reclaimed (never a shared resource): intake stops and they are shut down, not woken.
    #[serde(default)]
    pub reclaim: BTreeSet<ResourceId>,
}

/// Why the sequencer is not moving a resource this tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitReason {
    /// A safety-critical observation is missing or older than `safety_max_age`.
    StaleObservation,
    /// A scheduling observation needed to prove no work is in flight is missing or stale.
    StaleDemand,
    Reclaimed,
    /// A reclaim lease names a resource that cannot be reclaimed (shared, or without a
    /// power section). It is ignored.
    ReclaimUnsupported,
    HostNotUp,
    PoweringOn,
    ShuttingDown,
    /// The minimum dwell of the loaded model has not elapsed.
    MinDwell,
    /// The loaded model's active work is not declared preemptible.
    NotPreemptible,
    /// In-flight work is still running and no timeout applies or it has not expired.
    InFlightWork,
    /// Another model of the resource still has pods.
    OtherModelPresent,
    Loading,
    Draining,
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
    /// Allow a model (KEDA then scales it up).
    AllowModel {
        resource: ResourceId,
        model: ModelId,
    },
    /// Withdraw a model's allowance so its pods drain.
    DrainModel {
        resource: ResourceId,
        model: ModelId,
    },
    PowerOn {
        resource: ResourceId,
    },
    PowerOff {
        resource: ResourceId,
    },
    /// Stop new intake for a class: it leaves the allowed set. In-flight work continues.
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
            | Step::DrainModel { resource, .. }
            | Step::PowerOn { resource }
            | Step::PowerOff { resource } => Some(resource),
            Step::PauseIntake { .. } | Step::KillInFlight { .. } | Step::RunRecovery { .. } => None,
        }
    }
}

/// What KEDA is told is currently permitted to run. Models are separate from classes: a
/// model's pods are allowed while it loads, serves or winds down; a class is allowed only
/// when every one of its requirements is met.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllowedSet {
    #[serde(default)]
    pub models: BTreeSet<ModelId>,
    #[serde(default)]
    pub classes: BTreeSet<ClassId>,
}
