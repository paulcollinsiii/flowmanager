//! Observations and snapshots: what the scheduler knows at one instant.

use serde::{Deserialize, Serialize};

use crate::ids::{ClassId, ModelId, ResourceId, SourceId};
use crate::lease::Lease;
use crate::time::Millis;

/// How an observation was obtained, which bears on how far it can be trusted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Fidelity {
    /// Pushed by a watch stream or webhook-triggered re-read of the source.
    Event,
    /// Read by a periodic poll or resync.
    Polled,
    /// Inferred rather than read directly.
    Derived,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    pub source: SourceId,
    pub observed_at: Millis,
    pub fidelity: Fidelity,
    pub value: ObservedValue,
}

impl Observation {
    /// Age at `now`; zero if the observation is dated in the future.
    pub fn age(&self, now: Millis) -> Millis {
        now.saturating_sub(self.observed_at)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ObservedValue {
    /// Demand for one workload class. Only work whose upstream steps are done counts
    /// as `ready`.
    Demand {
        class: ClassId,
        ready: u64,
        #[serde(default)]
        in_flight: u64,
        /// When the oldest ready item became ready; absent if nothing is ready.
        #[serde(default)]
        oldest_ready_at: Option<Millis>,
    },
    /// Pods currently running for a model.
    ModelPods {
        model: ModelId,
        pods: u32,
        ready_pods: u32,
    },
    /// Whether a resource's host is reachable and up.
    ResourceUp { resource: ResourceId, up: bool },
}

/// Everything the planner and sequencer see for one tick.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub now: Millis,
    #[serde(default)]
    pub observations: Vec<Observation>,
    #[serde(default)]
    pub leases: Vec<Lease>,
}

impl Snapshot {
    /// The newest demand observation for `class`.
    pub fn demand(&self, class: &ClassId) -> Option<&Observation> {
        self.newest(|v| matches!(v, ObservedValue::Demand { class: c, .. } if c == class))
    }

    /// The newest pod-count observation for `model`.
    pub fn model_pods(&self, model: &ModelId) -> Option<&Observation> {
        self.newest(|v| matches!(v, ObservedValue::ModelPods { model: m, .. } if m == model))
    }

    /// The newest reachability observation for `resource`.
    pub fn resource_up(&self, resource: &ResourceId) -> Option<&Observation> {
        self.newest(|v| matches!(v, ObservedValue::ResourceUp { resource: r, .. } if r == resource))
    }

    fn newest(&self, pred: impl Fn(&ObservedValue) -> bool) -> Option<&Observation> {
        self.observations
            .iter()
            .filter(|o| pred(&o.value))
            .max_by_key(|o| o.observed_at)
    }
}
