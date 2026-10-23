//! The planner's output (an intent) and the sequencer's output (a step).

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::ids::{ClassId, ModelId, ResourceId};

/// What the system should converge to. Says nothing about how to get there.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Intent {
    /// Per exclusive resource, the model it should serve; `None` means idle.
    #[serde(default)]
    pub models: BTreeMap<ResourceId, Option<ModelId>>,
    /// Shared resources that must be held up.
    #[serde(default)]
    pub hold: BTreeSet<ResourceId>,
    /// Resources the user has reclaimed: intake stops and they are not woken.
    #[serde(default)]
    pub reclaim: BTreeSet<ResourceId>,
}

/// The next safe action toward an intent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Step {
    /// Do nothing yet; a precondition for the next move does not hold.
    Hold {
        resource: ResourceId,
    },
    /// Mark a model allowed (KEDA then scales it up).
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
    /// Stop new intake for a class without killing in-flight work.
    PauseIntake {
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
            Step::Hold { resource }
            | Step::AllowModel { resource, .. }
            | Step::DrainModel { resource, .. }
            | Step::PowerOn { resource }
            | Step::PowerOff { resource } => Some(resource),
            Step::PauseIntake { .. } | Step::RunRecovery { .. } => None,
        }
    }
}
