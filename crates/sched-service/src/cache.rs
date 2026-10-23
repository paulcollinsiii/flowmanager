//! The observation cache: the latest observation per source and subject, with its age, and
//! a dirty flag. It interprets nothing; it dates what observers report.
//!
//! Dating happens here, on receipt, with the scheduler's own clock. Nothing a remote
//! system says about time is kept, so a future-dated or skewed remote reading cannot make
//! the data look newer, older or unusable.

use std::collections::BTreeMap;

use sched_model::{
    ClassId, Config, Fidelity, Millis, ModelId, Observation, ObservedValue, ResourceId, Snapshot,
    SourceId,
};
use sched_observe::Report;

/// What an observation is about; one observation per source and subject is kept.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Subject {
    Demand(ClassId),
    Pods(ModelId),
    Up(ResourceId),
}

fn subject(v: &ObservedValue) -> Subject {
    match v {
        ObservedValue::Demand { class, .. } => Subject::Demand(class.clone()),
        ObservedValue::ModelPods { model, .. } => Subject::Pods(model.clone()),
        ObservedValue::ResourceUp { resource, .. } => Subject::Up(resource.clone()),
    }
}

/// How a source has been doing, as the scheduler saw it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SourceStatus {
    /// When its latest report of values arrived.
    pub last_report_at: Option<Millis>,
    /// Its latest failure, cleared by the next good report: when it arrived and what it said.
    pub last_error: Option<(Millis, String)>,
    pub errors_total: u64,
}

/// The result of taking in one report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ingest {
    /// Taken in; `changed` says whether any value (or the source's health) differs from
    /// before, i.e. whether a decision could now differ.
    Taken { changed: bool },
    /// The source is not in the active config; the report was dropped.
    UnknownSource,
}

/// One configured source's input age, for logs and metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputAge {
    pub source: SourceId,
    /// Age of the newest observation held from it; `None` if there is none.
    pub age: Option<Millis>,
    pub last_error: Option<String>,
}

#[derive(Debug, Default)]
pub struct ObservationCache {
    entries: BTreeMap<(SourceId, Subject), Observation>,
    /// When each class's demand from a source last went from nothing to something. The
    /// scheduler's own "oldest ready" estimate, in place of a remote timestamp.
    ready_since: BTreeMap<(SourceId, ClassId), Millis>,
    sources: BTreeMap<SourceId, SourceStatus>,
    dirty: bool,
}

impl ObservationCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes in a report received at `now`.
    pub fn ingest(&mut self, config: &Config, report: Report, now: Millis) -> Ingest {
        if config.source(report.source()).is_none() {
            return Ingest::UnknownSource;
        }
        let mut changed = false;
        match report {
            Report::Failed { source, error } => {
                let st = self.sources.entry(source).or_default();
                changed |= st.last_error.as_ref().map(|e| &e.1) != Some(&error);
                st.last_error = Some((now, error));
                st.errors_total += 1;
            }
            Report::Observed {
                source,
                fidelity,
                values,
            } => {
                let st = self.sources.entry(source.clone()).or_default();
                st.last_report_at = Some(now);
                changed |= st.last_error.take().is_some();
                for mut value in values {
                    if let ObservedValue::Demand {
                        class,
                        ready,
                        oldest_ready_at,
                        ..
                    } = &mut value
                    {
                        let key = (source.clone(), class.clone());
                        *oldest_ready_at = if *ready > 0 {
                            Some(*self.ready_since.entry(key).or_insert(now))
                        } else {
                            self.ready_since.remove(&key);
                            None
                        };
                    }
                    let key = (source.clone(), subject(&value));
                    changed |= self.entries.get(&key).map(|o| &o.value) != Some(&value);
                    self.entries.insert(
                        key,
                        Observation {
                            source: source.clone(),
                            observed_at: now,
                            fidelity,
                            value,
                        },
                    );
                }
            }
        }
        self.dirty |= changed;
        Ingest::Taken { changed }
    }

    /// Returns whether anything changed since the last call, and clears the flag.
    pub fn take_dirty(&mut self) -> bool {
        std::mem::take(&mut self.dirty)
    }

    /// Forgets everything from sources that the config no longer has.
    pub fn retain_sources(&mut self, config: &Config) {
        let keep = |s: &SourceId| config.source(s).is_some();
        self.entries.retain(|(s, _), _| keep(s));
        self.ready_since.retain(|(s, _), _| keep(s));
        self.sources.retain(|s, _| keep(s));
    }

    /// The snapshot for one tick, pinned to the generation of `config`. Only observations
    /// from sources `config` declares are included.
    pub fn snapshot(&self, config: &Config, now: Millis) -> Snapshot {
        Snapshot {
            config_generation: config.generation(),
            now,
            observations: self
                .entries
                .values()
                .filter(|o| config.source(&o.source).is_some())
                .cloned()
                .collect(),
            leases: Vec::new(),
        }
    }

    pub fn status(&self, source: &SourceId) -> Option<&SourceStatus> {
        self.sources.get(source)
    }

    /// Age of the newest observation from each configured source, with its last error.
    /// A source that never reported has no age; it is not reported as healthy.
    pub fn input_ages(&self, config: &Config, now: Millis) -> Vec<InputAge> {
        config
            .sources()
            .iter()
            .map(|s| InputAge {
                source: s.id.clone(),
                age: self
                    .entries
                    .values()
                    .filter(|o| o.source == s.id)
                    .map(|o| o.observed_at)
                    .max()
                    .map(|t| now.saturating_sub(t)),
                last_error: self
                    .sources
                    .get(&s.id)
                    .and_then(|st| st.last_error.as_ref())
                    .map(|e| e.1.clone()),
            })
            .collect()
    }
}

/// Fidelity assumed for a push that does not say.
pub const PUSH_FIDELITY: Fidelity = Fidelity::Event;

#[cfg(test)]
mod tests {
    use super::*;
    use sched_model::test_support::example_config;

    fn demand(class: &str, ready: u64, oldest: Option<u64>) -> ObservedValue {
        ObservedValue::Demand {
            class: class.into(),
            ready,
            in_flight: 0,
            oldest_ready_at: oldest.map(Millis),
        }
    }

    fn report(source: &str, values: Vec<ObservedValue>) -> Report {
        Report::Observed {
            source: source.into(),
            fidelity: Fidelity::Polled,
            values,
        }
    }

    fn held(c: &ObservationCache, cfg: &Config, now: u64) -> Vec<Observation> {
        c.snapshot(cfg, Millis(now)).observations
    }

    #[test]
    fn observations_are_dated_on_receipt_and_remote_timestamps_are_discarded() {
        let cfg = example_config();
        let mut c = ObservationCache::new();
        // A remote reading claiming to be from the far future (or the distant past).
        for remote in [u64::MAX, 5] {
            c.ingest(
                &cfg,
                report(
                    "paperless-ocr-pending",
                    vec![demand("paperless-ocr", 3, Some(remote))],
                ),
                Millis(1_000),
            );
            let o = held(&c, &cfg, 1_500);
            assert_eq!(o.len(), 1);
            assert_eq!(o[0].observed_at, Millis(1_000));
            // The only "oldest" time kept is the scheduler's own.
            assert!(matches!(
                o[0].value,
                ObservedValue::Demand {
                    oldest_ready_at: Some(Millis(1_000)),
                    ..
                }
            ));
        }
        // And it is fresh by the scheduler's clock, so decisions are not wedged.
        let snap = c.snapshot(&cfg, Millis(1_500));
        assert!(
            snap.demand(&cfg, &"paperless-ocr".into())
                .unwrap()
                .is_fresh(Millis(1_500), Millis(600_000))
        );
    }

    #[test]
    fn ready_since_is_when_the_scheduler_first_saw_work_and_resets_when_it_clears() {
        let cfg = example_config();
        let mut c = ObservationCache::new();
        let at = |c: &mut ObservationCache, now: u64, ready: u64| {
            c.ingest(
                &cfg,
                report(
                    "paperless-ocr-pending",
                    vec![demand("paperless-ocr", ready, None)],
                ),
                Millis(now),
            );
            match held(c, &cfg, now)[0].value {
                ObservedValue::Demand {
                    oldest_ready_at, ..
                } => oldest_ready_at,
                _ => unreachable!(),
            }
        };
        assert_eq!(at(&mut c, 100, 0), None);
        assert_eq!(at(&mut c, 200, 4), Some(Millis(200)));
        assert_eq!(at(&mut c, 300, 6), Some(Millis(200)));
        assert_eq!(at(&mut c, 400, 0), None);
        assert_eq!(at(&mut c, 500, 1), Some(Millis(500)));
    }

    #[test]
    fn a_source_that_stops_reporting_ages_and_is_never_refreshed_by_errors() {
        let cfg = example_config();
        let mut c = ObservationCache::new();
        c.ingest(
            &cfg,
            report(
                "paperless-ocr-pending",
                vec![demand("paperless-ocr", 3, None)],
            ),
            Millis(1_000),
        );
        c.ingest(
            &cfg,
            Report::Failed {
                source: "paperless-ocr-pending".into(),
                error: "HTTP 500".into(),
            },
            Millis(61_000),
        );
        let ages = c.input_ages(&cfg, Millis(61_000));
        let a = ages
            .iter()
            .find(|a| a.source.as_str() == "paperless-ocr-pending")
            .unwrap();
        assert_eq!(a.age, Some(Millis(60_000)));
        assert_eq!(a.last_error.as_deref(), Some("HTTP 500"));
        assert_eq!(
            c.status(&"paperless-ocr-pending".into())
                .unwrap()
                .errors_total,
            1
        );
        // Past demand_max_age (10m) the snapshot reading is stale.
        let snap = c.snapshot(&cfg, Millis(1_000 + 600_001));
        let o = snap.demand(&cfg, &"paperless-ocr".into()).unwrap();
        assert!(!o.is_fresh(snap.now, cfg.policy().demand_max_age));
        // A source that never reported has no age at all.
        let never = ages
            .iter()
            .find(|a| a.source.as_str() == "speakr-pending")
            .unwrap();
        assert_eq!(never.age, None);
    }

    #[test]
    fn dirty_is_set_by_changes_only_and_cleared_by_take() {
        let cfg = example_config();
        let mut c = ObservationCache::new();
        let r = || {
            report(
                "paperless-ocr-pending",
                vec![demand("paperless-ocr", 3, None)],
            )
        };
        assert_eq!(
            c.ingest(&cfg, r(), Millis(1)),
            Ingest::Taken { changed: true }
        );
        assert!(c.take_dirty());
        assert!(!c.take_dirty());
        // The same reading again refreshes the timestamp but is not a change.
        assert_eq!(
            c.ingest(&cfg, r(), Millis(2)),
            Ingest::Taken { changed: false }
        );
        assert!(!c.take_dirty());
        assert_eq!(held(&c, &cfg, 3)[0].observed_at, Millis(2));
        // A new error is a change, the same error again is not, recovery is.
        let err = || Report::Failed {
            source: "paperless-ocr-pending".into(),
            error: "boom".into(),
        };
        assert_eq!(
            c.ingest(&cfg, err(), Millis(3)),
            Ingest::Taken { changed: true }
        );
        assert_eq!(
            c.ingest(&cfg, err(), Millis(4)),
            Ingest::Taken { changed: false }
        );
        assert_eq!(
            c.ingest(&cfg, r(), Millis(5)),
            Ingest::Taken { changed: true }
        );
    }

    #[test]
    fn unknown_sources_are_dropped_and_removed_sources_are_forgotten() {
        let cfg = example_config();
        let mut c = ObservationCache::new();
        assert_eq!(
            c.ingest(
                &cfg,
                report("nowhere", vec![demand("paperless-ocr", 1, None)]),
                Millis(1)
            ),
            Ingest::UnknownSource
        );
        c.ingest(
            &cfg,
            report(
                "paperless-ocr-pending",
                vec![demand("paperless-ocr", 1, None)],
            ),
            Millis(1),
        );
        let without = sched_model::Config::from_yaml(
            &sched_model::test_support::EXAMPLE_CONFIG
                .replace(
                    "demand: { source: paperless-ocr-pending }",
                    "demand: { source: paperless-post-ocr }",
                )
                .replace("id: paperless-ocr-pending", "id: other-source"),
        )
        .unwrap();
        c.retain_sources(&without);
        assert!(held(&c, &without, 2).is_empty());
        assert!(c.status(&"paperless-ocr-pending".into()).is_none());
    }

    #[test]
    fn snapshot_is_pinned_to_the_config_generation_and_has_no_leases() {
        let cfg = example_config();
        let snap = ObservationCache::new().snapshot(&cfg, Millis(7));
        assert_eq!(snap.config_generation, cfg.generation());
        assert!(snap.leases.is_empty());
    }
}
