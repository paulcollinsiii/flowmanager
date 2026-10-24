//! The sequencer's own state: plain serializable data, carried from one tick to the next
//! (the service persists it between ticks). The planner reads it but never changes it.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::ids::{ClassId, ModelId, ResourceId};
use crate::time::Millis;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum PowerPhase {
    Off,
    PoweringOn { since: Millis },
    On { since: Millis },
    ShuttingDown { since: Millis },
}

/// A model that is being wound down: its classes' intake is stopped and the sequencer is
/// waiting for in-flight work to end.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WindDown {
    pub since: Millis,
    /// Set when the switch is forced: in-flight work gets this long (unless a class
    /// declares its own `kill_timeout`) and is then stopped.
    #[serde(default)]
    pub timeout: Option<Millis>,
    /// Whether the forcing cause is a reclaim (rather than a manual model lease), so that a
    /// different cause arriving later restarts the clock with its own timeout.
    #[serde(default)]
    pub reclaim: bool,
    /// Classes already stopped. Each class is stopped once, when its own timeout expires.
    #[serde(default)]
    pub killed: BTreeSet<ClassId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModePhase {
    /// No model is allowed.
    Idle,
    /// The model is allowed and its pods are coming up.
    Loading { model: ModelId, since: Millis },
    /// The model is ready. `since` is when it became ready, which dwell is measured from.
    Serving {
        model: ModelId,
        since: Millis,
        /// When the model last had no work; cleared as soon as work appears.
        #[serde(default)]
        idle_since: Option<Millis>,
        #[serde(default)]
        wind_down: Option<WindDown>,
    },
    /// The allowance is withdrawn; waiting for the model's pods to reach zero.
    Draining { model: ModelId, since: Millis },
}

impl ModePhase {
    /// The model currently allowed (loading, serving or winding down), if any.
    pub fn allowed_model(&self) -> Option<&ModelId> {
        match self {
            ModePhase::Loading { model, .. } | ModePhase::Serving { model, .. } => Some(model),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceState {
    pub power: PowerPhase,
    pub mode: ModePhase,
    /// Whether the scheduler may power this host down: set when it woke the host or took
    /// over a manual request, never for a host merely found running.
    #[serde(default)]
    pub managed: bool,
    /// When the host last had nothing holding it.
    #[serde(default)]
    pub host_idle_since: Option<Millis>,
    /// Classes whose in-flight work was stopped and whose recovery action has not run.
    #[serde(default)]
    pub pending_recovery: BTreeSet<ClassId>,
}

impl ResourceState {
    pub fn new(power: PowerPhase) -> Self {
        ResourceState {
            power,
            mode: ModePhase::Idle,
            managed: false,
            host_idle_since: None,
            pending_recovery: BTreeSet::new(),
        }
    }

    pub fn is_on(&self) -> bool {
        matches!(self.power, PowerPhase::On { .. })
    }
}

/// State of every resource. A resource absent from the map has not been seen yet; the
/// sequencer initialises it from observation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedState {
    #[serde(default)]
    pub resources: BTreeMap<ResourceId, ResourceState>,
}

impl SchedState {
    pub fn resource(&self, id: &ResourceId) -> Option<&ResourceState> {
        self.resources.get(id)
    }
}
