//! Observations and snapshots: what the scheduler knows at one instant.

use serde::{Deserialize, Serialize};

use crate::config::Config;
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
    /// The newest demand observation for `class` from the source the config says supplies it.
    pub fn demand(&self, config: &Config, class: &ClassId) -> Option<&Observation> {
        let source = &config.class(class)?.demand.as_ref()?.source;
        self.demand_group(source, class).pop()
    }

    /// The newest pod-count observation for `model` from a source that can report cluster
    /// state.
    pub fn model_pods(&self, config: &Config, model: &ModelId) -> Option<&Observation> {
        self.pods_group(config, model).pop()
    }

    /// The newest reachability observation for `resource` from a source that can report
    /// cluster state.
    pub fn resource_up(&self, config: &Config, resource: &ResourceId) -> Option<&Observation> {
        self.up_group(config, resource).pop()
    }

    /// Pod counts `(pods, ready_pods)` for `model` if the newest observation is fresh.
    ///
    /// Only observations from configured sources that can report cluster state
    /// (`SourceKind::can_supply_cluster_state`) are considered; they are filtered by subject
    /// and source BEFORE the newest timestamp is chosen. A reading from any other source is
    /// ignored entirely: it can neither make the data fresh nor stale, so a stray reading
    /// can never wedge the scheduler, and since it never replaces the authoritative reading
    /// it cannot make a step look safe that the authoritative source says is unsafe. If no
    /// authoritative reading exists the answer is `None` (unknown), never a stray's value.
    ///
    /// Observations are reduced independently of their order in the snapshot: only the
    /// newest timestamp counts, and observations tied at it, whatever their source or
    /// fidelity, are read conservatively (the highest pod count, the lowest ready count).
    /// If the newest is stale or future-dated, the answer is `None` even when an older
    /// observation looks fresh.
    ///
    /// Freshness rule and clock skew: an observation dated after `now` is never fresh, and
    /// the newest one decides, so a future-dated reading makes the data unknown until the
    /// clock catches up. This is accepted: observation timestamps must be assigned by the
    /// scheduler at the moment it receives the observation (not copied from the observed
    /// system), so that clock skew between systems cannot occur in the first place.
    pub fn fresh_model_pods(
        &self,
        config: &Config,
        model: &ModelId,
        max_age: Millis,
    ) -> Option<(u32, u32)> {
        let group = self.pods_group(config, model);
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

    /// Reachability of `resource` if the newest observation is fresh, with the same source
    /// filtering and freshness rule as `fresh_model_pods`. Observations tied at the newest
    /// timestamp that disagree read as "not up".
    pub fn fresh_resource_up(
        &self,
        config: &Config,
        resource: &ResourceId,
        max_age: Millis,
    ) -> Option<bool> {
        let group = self.up_group(config, resource);
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

    /// Newest-timestamp demand observations for `class` from `source`, the source its
    /// config says supplies it; everything else is filtered out first.
    pub(crate) fn demand_group(&self, source: &SourceId, class: &ClassId) -> Vec<&Observation> {
        self.newest_group(|o| {
            &o.source == source
                && matches!(&o.value, ObservedValue::Demand { class: c, .. } if c == class)
        })
    }

    fn pods_group(&self, config: &Config, model: &ModelId) -> Vec<&Observation> {
        self.newest_group(|o| {
            config.supplies_cluster_state(&o.source)
                && matches!(&o.value, ObservedValue::ModelPods { model: m, .. } if m == model)
        })
    }

    fn up_group(&self, config: &Config, resource: &ResourceId) -> Vec<&Observation> {
        self.newest_group(|o| {
            config.supplies_cluster_state(&o.source)
                && matches!(&o.value, ObservedValue::ResourceUp { resource: r, .. } if r == resource)
        })
    }

    /// The observations matching `pred` (which must already select subject, value kind and
    /// source) that share the newest timestamp among those, sorted so the result does not
    /// depend on snapshot order. Filtering happens before the newest is chosen.
    fn newest_group(&self, pred: impl Fn(&Observation) -> bool) -> Vec<&Observation> {
        let matching = || self.observations.iter().filter(|o| pred(o));
        let Some(newest) = matching().map(|o| o.observed_at).max() else {
            return Vec::new();
        };
        let mut group: Vec<&Observation> = matching().filter(|o| o.observed_at == newest).collect();
        group.sort();
        group
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::test_support::example_config;
    use crate::work::class_work;

    fn cfg() -> Config {
        example_config()
    }

    fn pods(at: u64, p: u32, r: u32) -> Observation {
        Observation {
            source: "cluster-pods".into(),
            observed_at: Millis(at),
            fidelity: Fidelity::Event,
            value: ObservedValue::ModelPods {
                model: "mineru".into(),
                pods: p,
                ready_pods: r,
            },
        }
    }

    fn up(at: u64, up: bool) -> Observation {
        Observation {
            source: "cluster-pods".into(),
            observed_at: Millis(at),
            fidelity: Fidelity::Event,
            value: ObservedValue::ResourceUp {
                resource: "cuda-central".into(),
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
            assert_eq!(
                s.fresh_model_pods(&cfg(), &"mineru".into(), Millis(500)),
                Some((2, 0))
            );
        }
        for s in both_orders(pods(900, 1, 1), pods(900, 1, 0)) {
            assert_eq!(
                s.fresh_model_pods(&cfg(), &"mineru".into(), Millis(500)),
                Some((1, 0))
            );
        }
    }

    #[test]
    fn tied_reachability_reads_as_down_in_any_order() {
        for s in both_orders(up(900, true), up(900, false)) {
            assert_eq!(
                s.fresh_resource_up(&cfg(), &"cuda-central".into(), Millis(500)),
                Some(false)
            );
        }
    }

    #[test]
    fn only_the_newest_timestamp_counts() {
        let s = snap(vec![pods(800, 5, 5), pods(900, 0, 0)]);
        assert_eq!(
            s.fresh_model_pods(&cfg(), &"mineru".into(), Millis(500)),
            Some((0, 0))
        );
        // A stale newest observation is not rescued by an older one.
        let s = snap(vec![pods(100, 0, 0)]);
        assert_eq!(
            s.fresh_model_pods(&cfg(), &"mineru".into(), Millis(500)),
            None
        );
    }

    #[test]
    fn future_dated_observations_are_never_fresh() {
        assert!(pods(1000, 0, 0).is_fresh(Millis(1000), Millis(0)));
        assert!(!pods(1001, 0, 0).is_fresh(Millis(1000), Millis(10_000)));
        let s = snap(vec![pods(1001, 0, 0)]);
        assert_eq!(
            s.fresh_model_pods(&cfg(), &"mineru".into(), Millis(10_000)),
            None
        );
        // Even with an older, fresh reading beneath it: the newest is the one that counts.
        let s = snap(vec![pods(900, 0, 0), pods(1001, 0, 0)]);
        assert_eq!(
            s.fresh_model_pods(&cfg(), &"mineru".into(), Millis(10_000)),
            None
        );
        let s = snap(vec![up(1001, true)]);
        assert_eq!(
            s.fresh_resource_up(&cfg(), &"cuda-central".into(), Millis(10_000)),
            None
        );
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

    // ---- source filtering, cross-source/fidelity ties ----

    use crate::test_support::EXAMPLE_CONFIG;

    /// The example config plus a second cluster-state source, so ties can cross sources.
    fn two_cluster_sources() -> Config {
        let text = EXAMPLE_CONFIG.replace(
            "sources:\n  - id: cluster-pods\n",
            "sources:\n  - id: cluster-pods-b\n    kind: kubernetes_watch\n    resync: 10s\n  - id: cluster-pods\n",
        );
        assert_ne!(text, EXAMPLE_CONFIG);
        Config::from_yaml(&text).expect("config with a second cluster source is valid")
    }

    fn pods_from(source: &str, fidelity: Fidelity, at: u64, p: u32, r: u32) -> Observation {
        Observation {
            source: source.into(),
            fidelity,
            ..pods(at, p, r)
        }
    }

    fn up_from(source: &str, fidelity: Fidelity, at: u64, u: bool) -> Observation {
        Observation {
            source: source.into(),
            fidelity,
            ..up(at, u)
        }
    }

    fn all_orders<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
        if items.len() <= 1 {
            return vec![items.to_vec()];
        }
        let mut out = Vec::new();
        for i in 0..items.len() {
            let mut rest = items.to_vec();
            let head = rest.remove(i);
            for mut p in all_orders(&rest) {
                p.insert(0, head.clone());
                out.push(p);
            }
        }
        out
    }

    #[test]
    fn pod_ties_across_sources_and_fidelities_read_highest_pods_lowest_ready() {
        let cfg = two_cluster_sources();
        // The highest pod count and the lowest ready count sit in different readings and in
        // the middle of the canonical order (source, then fidelity), so a reduction that
        // takes the first, the last, or the minimum pod count cannot pass.
        let readings = [
            pods_from("cluster-pods", Fidelity::Event, 900, 1, 1),
            pods_from("cluster-pods", Fidelity::Derived, 900, 3, 3),
            pods_from("cluster-pods-b", Fidelity::Polled, 900, 2, 0),
            pods_from("cluster-pods-b", Fidelity::Event, 900, 2, 2),
        ];
        for order in all_orders(&readings) {
            let s = snap(order);
            assert_eq!(
                s.fresh_model_pods(&cfg, &"mineru".into(), Millis(500)),
                Some((3, 0))
            );
        }
        // Pairs that differ only in source, then only in fidelity.
        for (a, b) in [
            (
                pods_from("cluster-pods", Fidelity::Event, 900, 0, 0),
                pods_from("cluster-pods-b", Fidelity::Event, 900, 4, 4),
            ),
            (
                pods_from("cluster-pods", Fidelity::Event, 900, 4, 4),
                pods_from("cluster-pods", Fidelity::Derived, 900, 0, 0),
            ),
        ] {
            for s in both_orders(a, b) {
                assert_eq!(
                    s.fresh_model_pods(&cfg, &"mineru".into(), Millis(500)),
                    Some((4, 0))
                );
            }
        }
    }

    #[test]
    fn reachability_ties_across_sources_and_fidelities_read_as_down() {
        let cfg = two_cluster_sources();
        let readings = [
            up_from("cluster-pods", Fidelity::Event, 900, true),
            up_from("cluster-pods", Fidelity::Derived, 900, true),
            up_from("cluster-pods-b", Fidelity::Polled, 900, false),
            up_from("cluster-pods-b", Fidelity::Event, 900, true),
        ];
        for order in all_orders(&readings) {
            let s = snap(order);
            assert_eq!(
                s.fresh_resource_up(&cfg, &"cuda-central".into(), Millis(500)),
                Some(false)
            );
        }
    }

    #[test]
    fn readings_from_unexpected_sources_are_ignored_not_trusted_and_not_wedging() {
        let cfg = two_cluster_sources();
        let m = ModelId::from("mineru");
        let h = ResourceId::from("cuda-central");
        // Unknown source, and a configured source that cannot report cluster state.
        for stray in ["nowhere", "paperless-ocr-pending"] {
            // Newer strays do not hide a fresh authoritative reading ...
            let s = snap(vec![
                pods(900, 0, 0),
                pods_from(stray, Fidelity::Event, 950, 7, 7),
                up(900, true),
                up_from(stray, Fidelity::Event, 950, false),
            ]);
            assert_eq!(s.fresh_model_pods(&cfg, &m, Millis(500)), Some((0, 0)));
            assert_eq!(s.fresh_resource_up(&cfg, &h, Millis(500)), Some(true));
            // ... future-dated strays do not make it stale ...
            let s = snap(vec![
                pods(900, 0, 0),
                pods_from(stray, Fidelity::Event, 5000, 7, 7),
                up(900, true),
                up_from(stray, Fidelity::Event, 5000, false),
            ]);
            assert_eq!(s.fresh_model_pods(&cfg, &m, Millis(500)), Some((0, 0)));
            assert_eq!(s.fresh_resource_up(&cfg, &h, Millis(500)), Some(true));
            // ... and, however fresh and benign, never stand in for a missing or stale
            // authoritative reading.
            let s = snap(vec![
                pods_from(stray, Fidelity::Event, 990, 0, 0),
                up_from(stray, Fidelity::Event, 990, true),
            ]);
            assert_eq!(s.fresh_model_pods(&cfg, &m, Millis(500)), None);
            assert_eq!(s.fresh_resource_up(&cfg, &h, Millis(500)), None);
            let s = snap(vec![
                pods(100, 0, 0),
                pods_from(stray, Fidelity::Event, 990, 0, 0),
                up(100, true),
                up_from(stray, Fidelity::Event, 990, true),
            ]);
            assert_eq!(s.fresh_model_pods(&cfg, &m, Millis(500)), None);
            assert_eq!(s.fresh_resource_up(&cfg, &h, Millis(500)), None);
        }
    }

    #[test]
    fn demand_from_another_source_is_ignored() {
        let config: Config = example_config();
        let class = config.class(&"paperless-ocr".into()).unwrap();
        let stray = |at: u64, ready: u64| Observation {
            source: "speakr-pending".into(),
            ..demand(at, ready, 0, None)
        };
        // A newer stray observation of the class must not displace the configured source's.
        let s = snap(vec![demand(500, 3, 0, Some(400)), stray(900, 0)]);
        let w = class_work(&config, &s, class);
        assert_eq!((w.known, w.ready), (true, 3));
        // Nor does a future-dated one.
        let s = snap(vec![demand(500, 3, 0, Some(400)), stray(5000, 0)]);
        assert_eq!(class_work(&config, &s, class).ready, 3);
        // A stray alone is no demand reading at all.
        let s = snap(vec![stray(900, 5)]);
        assert!(!class_work(&config, &s, class).known);
        assert!(s.demand(&config, &class.id).is_none());
    }

    #[test]
    fn demand_ties_merge_across_every_field_in_any_order() {
        let config: Config = example_config();
        let class = config.class(&"paperless-ocr".into()).unwrap();
        // Most ready, most in flight and the oldest item each come from a different
        // reading, none of them first or last in the canonical order (fidelity, then value).
        let with = |f: Fidelity, o: Observation| Observation { fidelity: f, ..o };
        let readings = [
            with(Fidelity::Event, demand(900, 2, 1, Some(400))),
            with(Fidelity::Polled, demand(900, 5, 3, Some(100))),
            with(Fidelity::Derived, demand(900, 1, 0, Some(300))),
        ];
        for order in all_orders(&readings) {
            let w = class_work(&config, &snap(order), class);
            assert_eq!(
                (w.ready, w.in_flight, w.oldest_ready_at, w.known),
                (5, 3, Some(Millis(100)), true)
            );
        }
        // A reading with no oldest item does not hide the others' (ready 0 has none).
        for order in all_orders(&[
            with(Fidelity::Event, demand(900, 0, 0, None)),
            with(Fidelity::Polled, demand(900, 2, 0, Some(700))),
        ]) {
            let w = class_work(&config, &snap(order), class);
            assert_eq!((w.ready, w.oldest_ready_at), (2, Some(Millis(700))));
        }
    }

    /// Documents the accepted behaviour for clock skew: a future-dated newest observation
    /// makes the data unknown (not merely stale) until the clock catches up, even with a
    /// fresh older reading beneath it. Timestamps are meant to be assigned by the scheduler
    /// on receipt, so this should not arise from skew in production.
    #[test]
    fn future_dated_newest_makes_data_unknown_until_the_clock_catches_up() {
        let cfg = two_cluster_sources();
        let observations = vec![pods(990, 0, 0), pods(1200, 0, 0)];
        let mut s = snap(observations);
        s.now = Millis(1000);
        assert_eq!(
            s.fresh_model_pods(&cfg, &"mineru".into(), Millis(500)),
            None
        );
        s.now = Millis(1199);
        assert_eq!(
            s.fresh_model_pods(&cfg, &"mineru".into(), Millis(500)),
            None
        );
        s.now = Millis(1200);
        assert_eq!(
            s.fresh_model_pods(&cfg, &"mineru".into(), Millis(500)),
            Some((0, 0))
        );
    }
}
