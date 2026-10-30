//! Observations and snapshots: what the scheduler knows at one instant.

use serde::{Deserialize, Serialize};

use crate::config::{Capability, Config};
use crate::ids::{ClassId, JobId, ModelId, ResourceId, SourceId};
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
    ///
    /// Observation timestamps must be assigned by the scheduler at the moment it receives
    /// an observation, never copied from the observed system, so that clock skew between
    /// systems cannot occur. A future-dated newest observation therefore only happens if
    /// the scheduler's own clock steps backwards, and until the clock catches up it makes
    /// the data unknown (see `Snapshot::fresh_model_pods`).
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
        /// Items that failed and are waiting to be re-queued by the class's recovery action.
        /// Not `ready` work: they have no age for starvation (see `ClassWork::failed`).
        #[serde(default)]
        failed: u64,
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
    /// How many pods mount the resource's NFS server (pods that have not finished and have
    /// a volume on it, directly or through a claim). Read from the Kubernetes API.
    ResourceMounts { resource: ResourceId, pods: u32 },
    /// The state of the Kubernetes Jobs flowmanager created for scheduled job `job`, as read
    /// from the API: one entry per Job still listed (the newest few). An empty list is a
    /// fresh reading that found none; a reading that is stale or missing is unknown.
    JobRuns {
        job: JobId,
        runs: Vec<JobRunReading>,
    },
}

/// What a Job's status says.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobRunState {
    /// Neither complete nor failed (pending or running).
    Active,
    /// The `Complete` condition is true.
    Complete,
    /// The `Failed` condition is true.
    Failed,
}

/// One Job of a scheduled job as listed from the Kubernetes API.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobRunReading {
    pub name: String,
    pub state: JobRunState,
}

impl ObservedValue {
    /// Host reachability gates the safety-critical steps (allowing a model, powering a host
    /// off) and is held to `host_reading_stale_after`. Pod counts gate nothing any more (Kubernetes
    /// enforces GPU exclusivity) but keep the same bound, so their ages read alike.
    /// Everything else is a scheduling input.
    pub fn is_safety_critical(&self) -> bool {
        matches!(
            self,
            ObservedValue::ModelPods { .. }
                | ObservedValue::ResourceUp { .. }
                | ObservedValue::ResourceMounts { .. }
                | ObservedValue::JobRuns { .. }
        )
    }
}

/// Everything the planner and sequencer see for one tick.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Demand sources that can never report in this build (no observer for their kind, or
    /// an observable kind configured without params, and nothing can push to them) and
    /// that have never reported. A class fed by one of them, with no observation in the
    /// snapshot, counts as having no demand rather than unknown demand. Decided by the
    /// service, which knows which observers run; the core only reads it.
    #[serde(default)]
    pub unobservable: std::collections::BTreeSet<SourceId>,
}

impl Snapshot {
    /// The newest demand observation for `class` from the source the config says supplies it.
    pub fn demand(&self, config: &Config, class: &ClassId) -> Option<&Observation> {
        let source = &config.class(class)?.demand.as_ref()?.source;
        self.demand_group(source, class).pop()
    }

    /// A canonical newest pod-count observation for `model` among declaring sources. Not a
    /// safe reading on its own; `fresh_model_pods` is.
    pub fn model_pods(&self, config: &Config, model: &ModelId) -> Option<&Observation> {
        self.pods_groups(config, model).into_iter().flatten().max()
    }

    /// A canonical newest reachability observation for `resource` among declaring sources.
    /// Not a safe reading on its own; `fresh_resource_up` is.
    pub fn resource_up(&self, config: &Config, resource: &ResourceId) -> Option<&Observation> {
        self.up_groups(config, resource).into_iter().flatten().max()
    }

    /// Pod counts `(pods, ready_pods)` for `model`, or `None` when they are unknown.
    ///
    /// Only observations from configured sources that declare the `pods` capability in
    /// their `observe` list are considered, filtered by subject and source BEFORE any
    /// reduction. Readings from any other source are ignored entirely: they can neither
    /// make the data fresh nor stale, so a stray reading cannot wedge the scheduler or
    /// stand in for a missing authoritative reading.
    ///
    /// Reduction is two-level and independent of snapshot order:
    /// 1. Within each declaring source only its newest timestamp counts; readings tied at
    ///    it (any fidelity) are merged conservatively (highest pod count, lowest ready
    ///    count).
    /// 2. Across sources the per-source results are combined conservatively. A source's
    ///    reading is never shadowed by a newer reading from another source. If the newest
    ///    reading of ANY declaring source that has reported on the model is stale or
    ///    future-dated, the answer is `None` (unknown), even if other sources are fresh.
    ///    Otherwise the highest pod count and the lowest ready count over all sources win,
    ///    so a fresh nonzero from any source defeats a zero from another.
    ///
    /// A declaring source that has never reported on this model is ignored: coverage is
    /// checked per config at load, not per subject.
    ///
    /// Freshness rule and clock skew: an observation dated after `now` is never fresh, and
    /// a source's newest one decides, so a future-dated reading makes the data unknown
    /// until the clock catches up. This is accepted: observation timestamps must be
    /// assigned by the scheduler at the moment it receives the observation (not copied from
    /// the observed system), so that clock skew between systems cannot occur in the first
    /// place.
    pub fn fresh_model_pods(
        &self,
        config: &Config,
        model: &ModelId,
        max_age: Millis,
    ) -> Option<(u32, u32)> {
        let groups = self.pods_groups(config, model);
        if groups.is_empty() {
            return None;
        }
        let mut pods = 0;
        let mut ready = u32::MAX;
        for group in &groups {
            if !group[0].is_fresh(self.now, max_age) {
                return None;
            }
            for o in group {
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
        }
        Some((pods, ready))
    }

    /// Reachability of `resource`, or `None` when unknown, with the same source filtering
    /// (`reachability` capability), two-level reduction and freshness rule as
    /// `fresh_model_pods`. The host reads as up only if every reading of every declaring
    /// source that has reported on it is fresh and up; a fresh "down" from any source, or
    /// disagreeing readings tied at a source's newest timestamp, read as "not up". A stale
    /// or future-dated newest reading from any reporting source makes it `None`.
    /// "Not up" and "unknown" both block power-off, and neither allows anything.
    pub fn fresh_resource_up(
        &self,
        config: &Config,
        resource: &ResourceId,
        max_age: Millis,
    ) -> Option<bool> {
        let groups = self.up_groups(config, resource);
        if groups.is_empty() {
            return None;
        }
        let mut all_up = true;
        for group in &groups {
            if !group[0].is_fresh(self.now, max_age) {
                return None;
            }
            all_up &= group
                .iter()
                .all(|o| matches!(o.value, ObservedValue::ResourceUp { up: true, .. }));
        }
        Some(all_up)
    }

    /// How many pods mount `resource`'s NFS server, or `None` when that is unknown, with the
    /// same source filtering (`mounts` capability), two-level reduction and freshness rule as
    /// `fresh_model_pods`. Unknown is never zero: a missing, stale or future-dated newest
    /// reading from any reporting source makes it `None`, and otherwise the highest count of
    /// any source wins. A caller deciding whether a host may be shut down must treat `None`
    /// as "possibly mounted".
    pub fn fresh_resource_mounts(
        &self,
        config: &Config,
        resource: &ResourceId,
        max_age: Millis,
    ) -> Option<u32> {
        let groups = self.newest_per_source(|o| {
            config.source_observes(&o.source, Capability::Mounts)
                && matches!(&o.value, ObservedValue::ResourceMounts { resource: r, .. } if r == resource)
        });
        if groups.is_empty() {
            return None;
        }
        let mut most = 0;
        for group in &groups {
            if !group[0].is_fresh(self.now, max_age) {
                return None;
            }
            for o in group {
                if let ObservedValue::ResourceMounts { pods, .. } = o.value {
                    most = most.max(pods);
                }
            }
        }
        Some(most)
    }

    /// What every reading the veto before a power-off of `resource` needs says about it (see
    /// `Config::veto_readings`). A reading that does not apply to the host is `None`. For the
    /// pods reading all of the host's models must show a fresh count of zero pods; a model
    /// with no reading, or a stale or future-dated one, makes it unknown, and unknown is
    /// never "unused".
    pub fn host_use(&self, config: &Config, resource: &ResourceId, max_age: Millis) -> HostUse {
        let mounts = config.mounts_checked(resource).then(|| {
            match self.fresh_resource_mounts(config, resource, max_age) {
                Some(0) => ReadingUse::Unused,
                Some(_) => ReadingUse::InUse,
                None => ReadingUse::Unknown,
            }
        });
        let pods = config.pods_checked(resource).then(|| {
            let counts: Vec<Option<u32>> = config
                .models_of(resource)
                .map(|m| {
                    self.fresh_model_pods(config, &m.id, max_age)
                        .map(|(pods, _)| pods)
                })
                .collect();
            if counts.iter().any(|c| c.is_some_and(|n| n > 0)) {
                ReadingUse::InUse
            } else if counts.iter().any(|c| c.is_none()) {
                ReadingUse::Unknown
            } else {
                ReadingUse::Unused
            }
        });
        HostUse { mounts, pods }
    }

    /// The state of the Jobs of scheduled job `job` and when that reading was taken, or
    /// `None` when it is unknown, with the same source filtering (`jobs` capability) and
    /// freshness rule as `fresh_resource_mounts`. Unknown is never "no Jobs": a missing, stale
    /// or future-dated reading makes it `None`, and a caller must not read `None` as the Job
    /// having finished or vanished.
    pub fn fresh_job_runs(
        &self,
        config: &Config,
        job: &JobId,
        max_age: Millis,
    ) -> Option<(Millis, Vec<JobRunReading>)> {
        let groups = self.newest_per_source(|o| {
            config.source_observes(&o.source, Capability::Jobs)
                && matches!(&o.value, ObservedValue::JobRuns { job: j, .. } if j == job)
        });
        let mut found: Option<(Millis, Vec<JobRunReading>)> = None;
        for group in &groups {
            if !group[0].is_fresh(self.now, max_age) {
                return None;
            }
            // One source per job is the rule (validation); with more, the older reading wins
            // (the less recent view is the safer one to act on).
            if let ObservedValue::JobRuns { runs, .. } = &group[0].value
                && found
                    .as_ref()
                    .is_none_or(|(at, _)| group[0].observed_at < *at)
            {
                found = Some((group[0].observed_at, runs.clone()));
            }
        }
        found
    }

    /// Newest-timestamp demand observations for `class` from `source`, the source its
    /// config says supplies it; everything else is filtered out first.
    pub(crate) fn demand_group(&self, source: &SourceId, class: &ClassId) -> Vec<&Observation> {
        self.newest_group(|o| {
            &o.source == source
                && matches!(&o.value, ObservedValue::Demand { class: c, .. } if c == class)
        })
    }

    fn pods_groups(&self, config: &Config, model: &ModelId) -> Vec<Vec<&Observation>> {
        self.newest_per_source(|o| {
            config.source_observes(&o.source, Capability::Pods)
                && matches!(&o.value, ObservedValue::ModelPods { model: m, .. } if m == model)
        })
    }

    fn up_groups(&self, config: &Config, resource: &ResourceId) -> Vec<Vec<&Observation>> {
        self.newest_per_source(|o| {
            config.source_observes(&o.source, Capability::Reachability)
                && matches!(&o.value, ObservedValue::ResourceUp { resource: r, .. } if r == resource)
        })
    }

    /// For each source with a matching observation, in source order, that source's newest
    /// group (see `newest_group`). Sources are reduced separately so that a newer reading
    /// from one never hides an older reading from another.
    fn newest_per_source(&self, pred: impl Fn(&Observation) -> bool) -> Vec<Vec<&Observation>> {
        let sources: std::collections::BTreeSet<&SourceId> = self
            .observations
            .iter()
            .filter(|o| pred(o))
            .map(|o| &o.source)
            .collect();
        sources
            .into_iter()
            .map(|src| self.newest_group(|o| &o.source == src && pred(o)))
            .collect()
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

/// What one veto reading says about a host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadingUse {
    /// A fresh reading shows nothing using the host.
    Unused,
    /// A fresh reading shows something using it.
    InUse,
    /// No fresh reading: counts as in use for whether the host may go down.
    Unknown,
}

/// What the readings of the veto say about a host (see `Snapshot::host_use`): `None` where
/// the reading does not apply to the host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostUse {
    /// Pods mounting the host's NFS server.
    pub mounts: Option<ReadingUse>,
    /// Pods of the host's models.
    pub pods: Option<ReadingUse>,
}

impl HostUse {
    fn applying(&self) -> impl Iterator<Item = ReadingUse> {
        [self.mounts, self.pods].into_iter().flatten()
    }

    /// Whether the host may be powered off as far as the veto goes: every reading that applies
    /// is fresh and shows it unused. A host with no reading that applies has no veto.
    pub fn is_unused(&self) -> bool {
        self.applying().all(|u| u == ReadingUse::Unused)
    }

    /// Whether a fresh reading shows the host in use. Otherwise a host that is not unused has
    /// an unknown reading.
    pub fn is_in_use(&self) -> bool {
        self.applying().any(|u| u == ReadingUse::InUse)
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
            unobservable: Default::default(),
        }
    }

    fn mounts(source: &str, at: u64, pods: u32) -> Observation {
        Observation {
            source: source.into(),
            observed_at: Millis(at),
            fidelity: Fidelity::Polled,
            value: ObservedValue::ResourceMounts {
                resource: "basestar-nfs".into(),
                pods,
            },
        }
    }

    fn model_pods(model: &str, at: u64, p: u32) -> Observation {
        Observation {
            source: "cluster-pods".into(),
            observed_at: Millis(at),
            fidelity: Fidelity::Event,
            value: ObservedValue::ModelPods {
                model: model.into(),
                pods: p,
                ready_pods: p,
            },
        }
    }

    #[test]
    fn host_use_reads_every_model_of_a_gpu_host_and_nothing_for_a_host_without_readings() {
        let c = cfg();
        let gpu: ResourceId = "cuda-central".into();
        let all = |at: u64, p: u32| -> Vec<Observation> {
            ["mineru", "llm", "whisperx", "immich-ml"]
                .iter()
                .map(|m| model_pods(m, at, p))
                .collect()
        };
        assert!(c.pods_checked(&gpu) && !c.mounts_checked(&gpu));
        let use_of = |obs: Vec<Observation>| snap(obs).host_use(&c, &gpu, Millis(500));
        // Fresh zero for every model.
        let u = use_of(all(900, 0));
        assert_eq!(u.pods, Some(ReadingUse::Unused));
        assert_eq!(u.mounts, None);
        assert!(u.is_unused() && !u.is_in_use());
        // One model missing, or stale: unknown.
        let mut missing = all(900, 0);
        missing.pop();
        assert_eq!(use_of(missing).pods, Some(ReadingUse::Unknown));
        let mut stale = all(900, 0);
        stale[0] = model_pods("mineru", 100, 0);
        let u = use_of(stale);
        assert_eq!(u.pods, Some(ReadingUse::Unknown));
        assert!(!u.is_unused() && !u.is_in_use());
        // A pod of any model: in use, even beside an unknown one.
        let mut busy = all(900, 0);
        busy[2] = model_pods("whisperx", 900, 1);
        busy.pop();
        let u = use_of(busy);
        assert_eq!(u.pods, Some(ReadingUse::InUse));
        assert!(u.is_in_use());
        // No readings at all: unknown, never unused.
        assert_eq!(use_of(vec![]).pods, Some(ReadingUse::Unknown));
        // The NFS host has no models: only its mounts reading applies, and unknown is not
        // unused. A resource nothing reads has no veto.
        let nfs: ResourceId = "basestar-nfs".into();
        let u = snap(vec![]).host_use(&c, &nfs, Millis(500));
        assert_eq!((u.mounts, u.pods), (Some(ReadingUse::Unknown), None));
        assert!(!u.is_unused() && !c.pods_checked(&nfs));
        let none = snap(vec![]).host_use(&c, &"nowhere".into(), Millis(500));
        assert!(none.is_unused() && none.mounts.is_none() && none.pods.is_none());
    }

    #[test]
    fn mounts_are_unknown_when_missing_stale_future_dated_or_from_an_undeclared_source() {
        let (c, nfs) = (cfg(), ResourceId::new("basestar-nfs"));
        // Nothing reported: unknown, not zero.
        assert_eq!(
            snap(vec![]).fresh_resource_mounts(&c, &nfs, Millis(500)),
            None
        );
        let fresh = snap(vec![mounts("basestar-mounts", 900, 0)]);
        assert_eq!(fresh.fresh_resource_mounts(&c, &nfs, Millis(500)), Some(0));
        // Older than the bound, and dated after now: unknown.
        assert_eq!(fresh.fresh_resource_mounts(&c, &nfs, Millis(50)), None);
        let future = snap(vec![mounts("basestar-mounts", 1001, 0)]);
        assert_eq!(future.fresh_resource_mounts(&c, &nfs, Millis(500)), None);
        // A source that does not declare the capability is ignored, so it can neither
        // make the reading known nor hide a stale one.
        let stray = snap(vec![mounts("cluster-pods", 900, 0)]);
        assert_eq!(stray.fresh_resource_mounts(&c, &nfs, Millis(500)), None);
        // Only the newest reading of a source counts, and the highest count wins.
        let newest = snap(vec![
            mounts("basestar-mounts", 800, 3),
            mounts("basestar-mounts", 900, 1),
        ]);
        assert_eq!(newest.fresh_resource_mounts(&c, &nfs, Millis(500)), Some(1));
        assert!(
            ObservedValue::ResourceMounts {
                resource: nfs,
                pods: 0
            }
            .is_safety_critical()
        );
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
                failed: 0,
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

    /// The example config plus a second source declaring pods and reachability, so ties can cross sources.
    fn two_cluster_sources() -> Config {
        let text = EXAMPLE_CONFIG.replace(
            "sources:\n  - id: cluster-pods\n",
            "sources:\n  - id: cluster-pods-b\n    kind: kubernetes_watch\n    observe: [pods, reachability]\n    resync: 10s\n  - id: cluster-pods\n",
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
        // Unknown source, and a configured source that does not declare the capability.
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

    #[test]
    fn each_capability_is_honoured_only_from_sources_declaring_it() {
        let text = EXAMPLE_CONFIG.replace(
            "sources:\n  - id: cluster-pods\n",
            "sources:\n  - id: pods-only\n    kind: kubernetes_watch\n    observe: [pods]\n    resync: 10s\n  - id: cluster-pods\n",
        );
        let cfg = Config::from_yaml(&text).unwrap();
        let (m, h) = (ModelId::from("mineru"), ResourceId::from("cuda-central"));
        // `basestar-probe` declares only reachability; `pods-only` only pods.
        let s = snap(vec![
            pods_from("basestar-probe", Fidelity::Event, 990, 0, 0),
            up_from("pods-only", Fidelity::Event, 990, true),
        ]);
        assert_eq!(s.fresh_model_pods(&cfg, &m, Millis(500)), None);
        assert_eq!(s.fresh_resource_up(&cfg, &h, Millis(500)), None);
        let s = snap(vec![
            pods_from("pods-only", Fidelity::Event, 990, 2, 2),
            up_from("basestar-probe", Fidelity::Event, 990, true),
        ]);
        assert_eq!(s.fresh_model_pods(&cfg, &m, Millis(500)), Some((2, 2)));
        assert_eq!(s.fresh_resource_up(&cfg, &h, Millis(500)), Some(true));
    }

    #[test]
    fn an_older_reading_from_one_source_is_not_shadowed_by_a_newer_one_from_another() {
        let cfg = two_cluster_sources();
        let (m, h) = (ModelId::from("mineru"), ResourceId::from("cuda-central"));
        let ev = Fidelity::Event;
        // Pods: A still reports pods at 800, B says zero at 900.
        let readings = [
            pods_from("cluster-pods", ev, 800, 2, 1),
            pods_from("cluster-pods-b", ev, 900, 0, 0),
            pods_from("cluster-pods-b", Fidelity::Polled, 900, 0, 0),
        ];
        for order in all_orders(&readings) {
            let s = snap(order);
            assert_eq!(s.fresh_model_pods(&cfg, &m, Millis(500)), Some((2, 0)));
        }
        // The other way round.
        for order in all_orders(&[
            pods_from("cluster-pods", ev, 900, 0, 0),
            pods_from("cluster-pods-b", ev, 800, 3, 3),
        ]) {
            let s = snap(order);
            assert_eq!(s.fresh_model_pods(&cfg, &m, Millis(500)), Some((3, 0)));
        }
        // A stale or future-dated newest reading from any reporting source: unknown, even
        // though the other source is fresh (and whichever of the two is nonzero).
        for (a, b) in [
            (
                pods_from("cluster-pods", ev, 100, 2, 2),
                pods_from("cluster-pods-b", ev, 900, 0, 0),
            ),
            (
                pods_from("cluster-pods", ev, 900, 0, 0),
                pods_from("cluster-pods-b", ev, 100, 0, 0),
            ),
            (
                pods_from("cluster-pods", ev, 900, 0, 0),
                pods_from("cluster-pods-b", ev, 1200, 0, 0),
            ),
        ] {
            for s in both_orders(a, b) {
                assert_eq!(s.fresh_model_pods(&cfg, &m, Millis(500)), None);
            }
        }
        // A declaring source that never reported on the model is ignored.
        let s = snap(vec![pods_from("cluster-pods-b", ev, 900, 0, 0)]);
        assert_eq!(s.fresh_model_pods(&cfg, &m, Millis(500)), Some((0, 0)));

        // Reachability: any fresh "down" wins whichever source is newer; stale is unknown.
        for (a, b, want) in [
            (
                up_from("cluster-pods", ev, 800, false),
                up_from("cluster-pods-b", ev, 900, true),
                Some(false),
            ),
            (
                up_from("cluster-pods", ev, 900, true),
                up_from("cluster-pods-b", ev, 800, false),
                Some(false),
            ),
            (
                up_from("cluster-pods", ev, 800, true),
                up_from("cluster-pods-b", ev, 900, true),
                Some(true),
            ),
            (
                up_from("cluster-pods", ev, 100, true),
                up_from("cluster-pods-b", ev, 900, true),
                None,
            ),
            (
                up_from("cluster-pods", ev, 900, false),
                up_from("cluster-pods-b", ev, 1200, true),
                None,
            ),
        ] {
            for s in both_orders(a, b) {
                assert_eq!(s.fresh_resource_up(&cfg, &h, Millis(500)), want);
            }
        }
    }
}
