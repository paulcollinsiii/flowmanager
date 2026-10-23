//! Observations and snapshots: what the scheduler knows at one instant.

use serde::{Deserialize, Serialize};

use crate::ids::{ClassId, ModelId, ResourceId, SourceId};
use crate::lease::Lease;
use crate::time::Millis;

/// How an observation was obtained, which bears on how far it can be trusted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Fidelity {
    /// Pushed by a watch stream or webhook-triggered re-read of the source.
    Event,
    /// Read by a periodic poll or resync.
    Polled,
    /// Inferred rather than read directly.
    Derived,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    pub source: SourceId,
    pub observed_at: Millis,
    pub fidelity: Fidelity,
    pub value: ObservedValue,
}

impl Observation {
    /// Age at `now`; zero if the observation is dated in the future. Use `is_fresh` to
    /// decide whether to rely on it.
    pub fn age(&self, now: Millis) -> Millis {
        now.saturating_sub(self.observed_at)
    }

    /// Whether the observation may be relied on: no older than `max_age` and not dated in
    /// the future (a future timestamp means a skewed clock, so it proves nothing).
    pub fn is_fresh(&self, now: Millis, max_age: Millis) -> bool {
        self.observed_at <= now && now.saturating_sub(self.observed_at) <= max_age
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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

impl ObservedValue {
    /// Safety-critical values (pod counts, host reachability) gate irreversible steps and
    /// are held to `safety_max_age`; everything else is a scheduling input.
    pub fn is_safety_critical(&self) -> bool {
        matches!(
            self,
            ObservedValue::ModelPods { .. } | ObservedValue::ResourceUp { .. }
        )
    }
}

/// Everything the planner and sequencer see for one tick.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    /// The config generation this tick is pinned to.
    #[serde(default)]
    pub config_generation: u64,
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

    /// Pod counts `(pods, ready_pods)` for `model` if the newest observation is fresh.
    ///
    /// Observations are reduced independently of their order in the snapshot: only the
    /// newest timestamp counts, and observations tied at it are read conservatively (the
    /// highest pod count, the lowest ready count). If the newest is stale or future-dated,
    /// the answer is `None` even when an older observation looks fresh.
    pub fn fresh_model_pods(&self, model: &ModelId, max_age: Millis) -> Option<(u32, u32)> {
        let group = self
            .newest_group(|v| matches!(v, ObservedValue::ModelPods { model: m, .. } if m == model));
        let first = group.first()?;
        if !first.is_fresh(self.now, max_age) {
            return None;
        }
        let mut pods = 0;
        let mut ready = u32::MAX;
        for o in &group {
            if let ObservedValue::ModelPods {
                pods: p,
                ready_pods: r,
                ..
            } = o.value
            {
                pods = pods.max(p);
                ready = ready.min(r);
            }
        }
        Some((pods, ready))
    }

    /// Reachability of `resource` if the newest observation is fresh. Observations tied at
    /// the newest timestamp that disagree read as "not up".
    pub fn fresh_resource_up(&self, resource: &ResourceId, max_age: Millis) -> Option<bool> {
        let group = self.newest_group(
            |v| matches!(v, ObservedValue::ResourceUp { resource: r, .. } if r == resource),
        );
        let first = group.first()?;
        if !first.is_fresh(self.now, max_age) {
            return None;
        }
        Some(
            group
                .iter()
                .all(|o| matches!(o.value, ObservedValue::ResourceUp { up: true, .. })),
        )
    }

    /// The observations matching `pred` that share the newest timestamp, sorted so the
    /// result does not depend on snapshot order.
    pub(crate) fn newest_group(&self, pred: impl Fn(&ObservedValue) -> bool) -> Vec<&Observation> {
        let matching = || self.observations.iter().filter(|o| pred(&o.value));
        let Some(newest) = matching().map(|o| o.observed_at).max() else {
            return Vec::new();
        };
        let mut group: Vec<&Observation> = matching().filter(|o| o.observed_at == newest).collect();
        group.sort();
        group
    }

    /// A canonical newest observation: ties are broken by the observations' own ordering,
    /// never by position. Not a safe reading on its own; the `fresh_*` methods are.
    fn newest(&self, pred: impl Fn(&ObservedValue) -> bool) -> Option<&Observation> {
        self.newest_group(pred).pop()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::test_support::example_config;
    use crate::work::class_work;

    fn pods(at: u64, p: u32, r: u32) -> Observation {
        Observation {
            source: "s".into(),
            observed_at: Millis(at),
            fidelity: Fidelity::Event,
            value: ObservedValue::ModelPods {
                model: "m".into(),
                pods: p,
                ready_pods: r,
            },
        }
    }

    fn up(at: u64, up: bool) -> Observation {
        Observation {
            source: "s".into(),
            observed_at: Millis(at),
            fidelity: Fidelity::Event,
            value: ObservedValue::ResourceUp {
                resource: "h".into(),
                up,
            },
        }
    }

    fn snap(observations: Vec<Observation>) -> Snapshot {
        Snapshot {
            config_generation: 0,
            now: Millis(1000),
            observations,
            leases: vec![],
        }
    }

    fn both_orders(a: Observation, b: Observation) -> [Snapshot; 2] {
        [snap(vec![a.clone(), b.clone()]), snap(vec![b, a])]
    }

    #[test]
    fn tied_pod_counts_read_as_most_pods_fewest_ready_in_any_order() {
        for s in both_orders(pods(900, 0, 0), pods(900, 2, 1)) {
            assert_eq!(s.fresh_model_pods(&"m".into(), Millis(500)), Some((2, 0)));
        }
        for s in both_orders(pods(900, 1, 1), pods(900, 1, 0)) {
            assert_eq!(s.fresh_model_pods(&"m".into(), Millis(500)), Some((1, 0)));
        }
    }

    #[test]
    fn tied_reachability_reads_as_down_in_any_order() {
        for s in both_orders(up(900, true), up(900, false)) {
            assert_eq!(s.fresh_resource_up(&"h".into(), Millis(500)), Some(false));
        }
    }

    #[test]
    fn only_the_newest_timestamp_counts() {
        let s = snap(vec![pods(800, 5, 5), pods(900, 0, 0)]);
        assert_eq!(s.fresh_model_pods(&"m".into(), Millis(500)), Some((0, 0)));
        // A stale newest observation is not rescued by an older one.
        let s = snap(vec![pods(100, 0, 0)]);
        assert_eq!(s.fresh_model_pods(&"m".into(), Millis(500)), None);
    }

    #[test]
    fn future_dated_observations_are_never_fresh() {
        assert!(pods(1000, 0, 0).is_fresh(Millis(1000), Millis(0)));
        assert!(!pods(1001, 0, 0).is_fresh(Millis(1000), Millis(10_000)));
        let s = snap(vec![pods(1001, 0, 0)]);
        assert_eq!(s.fresh_model_pods(&"m".into(), Millis(10_000)), None);
        // Even with an older, fresh reading beneath it: the newest is the one that counts.
        let s = snap(vec![pods(900, 0, 0), pods(1001, 0, 0)]);
        assert_eq!(s.fresh_model_pods(&"m".into(), Millis(10_000)), None);
        let s = snap(vec![up(1001, true)]);
        assert_eq!(s.fresh_resource_up(&"h".into(), Millis(10_000)), None);
    }

    fn demand(at: u64, ready: u64, in_flight: u64, oldest: Option<u64>) -> Observation {
        Observation {
            source: "paperless-ocr-pending".into(),
            observed_at: Millis(at),
            fidelity: Fidelity::Polled,
            value: ObservedValue::Demand {
                class: "paperless-ocr".into(),
                ready,
                in_flight,
                oldest_ready_at: oldest.map(Millis),
            },
        }
    }

    #[test]
    fn demand_ties_merge_conservatively_and_future_demand_is_unknown() {
        let config: Config = example_config();
        let class = config.class(&"paperless-ocr".into()).unwrap();
        let mut s = snap(vec![
            demand(900, 0, 0, None),
            demand(900, 3, 0, Some(500)),
            demand(900, 1, 2, Some(300)),
        ]);
        s.now = Millis(1000);
        let a = class_work(&config, &s, class);
        s.observations.reverse();
        assert_eq!(a, class_work(&config, &s, class));
        assert_eq!(
            (a.ready, a.in_flight, a.oldest_ready_at, a.known),
            (3, 2, Some(Millis(300)), true)
        );

        let future = snap(vec![demand(1001, 0, 0, None)]);
        assert!(!class_work(&config, &future, class).known);
        let missing = snap(vec![]);
        assert!(!class_work(&config, &missing, class).known);
    }
}
