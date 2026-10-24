//! The shared scheduler context and one tick. A tick takes the cached snapshot, runs the
//! core (planner, then sequencer), and logs the decision and why. Nothing is executed: the
//! service has no actuator, and the observers only read.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use sched_model::{ClassId, Millis, ModelId, SchedState, Step, class_work};
use sched_observe::{Report, observer_available};
use serde::{Deserialize, Serialize};

use crate::cache::{Ingest, InputAge, ObservationCache};
use crate::clock::Clock;
use crate::config_source::ConfigHolder;
use crate::core::decide;
use crate::metrics::Metrics;
use crate::reason;

/// One source's input age in a [`TickReport`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputReport {
    pub source: String,
    /// The capability the age is for (`pods`, `reachability`, `demand`).
    pub capability: Option<String>,
    /// Milliseconds since the newest observation arrived; absent if none ever did.
    pub age_ms: Option<u64>,
    pub last_error: Option<String>,
}

impl From<&InputAge> for InputReport {
    fn from(a: &InputAge) -> Self {
        InputReport {
            source: a.source.to_string(),
            capability: a.capability.map(|c| c.to_string()),
            age_ms: a.age.map(Millis::as_u64),
            last_error: a.last_error.clone(),
        }
    }
}

/// What a tick decided, for logs and for the `last_decision` handler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickReport {
    pub config_generation: u64,
    pub at: Millis,
    /// Always true: each tick starts from a fresh sequencer state, so hysteresis (dwell,
    /// idle grace) and the steps that follow it (drain, pause, power off) cannot appear.
    pub shadow_stateless: bool,
    /// Classes whose demand is unknown although an observer exists for their source (it
    /// never reported or is stale): a real problem. Unknown is not zero: such a tick is
    /// never `idle`.
    pub unknown_demand: Vec<ClassId>,
    /// Classes whose demand is unknown because this build has no observer for their source
    /// kind (for example schedule or session sources), unless something pushed a value.
    /// Expected for now, and kept apart so it cannot drown `unknown_demand`.
    pub unsupported_demand: Vec<ClassId>,
    /// Distinct decision kinds (see `reason::step_kind`), `idle` for none, `error` on failure.
    pub kinds: Vec<String>,
    pub reason: String,
    pub steps: Vec<Step>,
    pub allowed_models: Vec<ModelId>,
    pub allowed_classes: Vec<ClassId>,
    pub inputs: Vec<InputReport>,
    /// Whether observations changed since the previous tick.
    pub input_changed: bool,
    pub error: Option<String>,
}

/// Everything ticks and observer plumbing share.
pub struct Shared {
    pub config: Arc<ConfigHolder>,
    pub cache: Mutex<ObservationCache>,
    pub metrics: Arc<Metrics>,
    pub clock: Arc<dyn Clock>,
    /// When the backstop last ran (0 if it has not), to notice a chain that died.
    pub last_backstop: AtomicU64,
    /// When this process started.
    pub started_at: Millis,
    /// Woken when a report changed what the scheduler knows, so a tick follows promptly.
    pub changed: tokio::sync::Notify,
}

impl Shared {
    pub fn new(config: Arc<ConfigHolder>, metrics: Arc<Metrics>, clock: Arc<dyn Clock>) -> Shared {
        metrics.set_config_generation(config.current().generation());
        Shared {
            config,
            cache: Mutex::new(ObservationCache::new()),
            metrics,
            started_at: clock.now(),
            clock,
            last_backstop: AtomicU64::new(0),
            changed: tokio::sync::Notify::new(),
        }
    }

    fn cache(&self) -> std::sync::MutexGuard<'_, ObservationCache> {
        self.cache.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Takes in a report, dating it `now` (the scheduler's receipt time). Returns whether it
    /// changed anything; if so, wakes the ticker.
    pub fn ingest(&self, report: Report, now: Millis) -> Ingest {
        let config = self.config.current();
        // Unknown sources are refused before anything else, so a pusher cannot make
        // metric series or log lines by inventing names.
        if config.source(report.source()).is_none() {
            self.metrics.rejected("unknown_source", 1);
            tracing::warn!("report from a source not in the config dropped");
            return Ingest::UnknownSource;
        }
        if let Report::Failed { source, error } = &report {
            self.metrics.source_error(source.as_str());
            tracing::warn!(source = %source, "source error: {error}");
        }
        let result = self.cache().ingest(&config, report, now);
        if let Ingest::Taken { changed, dropped } = &result {
            if *dropped > 0 {
                self.metrics.rejected("unknown_subject", *dropped as u64);
            }
            if *changed {
                self.changed.notify_one();
            }
        }
        result
    }

    /// Reads the clock for a report received now.
    pub fn ingest_now(&self, report: Report) -> Ingest {
        self.ingest(report, self.clock.now())
    }

    /// Drops cache entries of sources the active config no longer has.
    pub fn prune(&self) {
        let config = self.config.current();
        self.cache().retain_sources(&config);
    }

    /// Runs one tick at `now`. The config handle is taken once, so the whole tick sees one
    /// generation.
    pub fn tick(&self, now: Millis) -> TickReport {
        self.tick_pinned(now, || {})
    }

    /// As [`Shared::tick`]; `after_pin` runs right after the config is pinned, a seam for
    /// tests to change the config mid-tick.
    pub fn tick_pinned(&self, now: Millis, after_pin: impl FnOnce()) -> TickReport {
        let started = Instant::now();
        let config = self.config.current();
        after_pin();
        let (snapshot, ages, changed, now) = {
            let mut cache = self.cache();
            // Never run before something already received: that observation would be
            // dated after `now` and read as not fresh.
            let now = now.max(cache.latest_receipt());
            (
                cache.snapshot(&config, now),
                cache.input_ages(&config, now),
                cache.take_dirty(),
                now,
            )
        };
        let (mut unknown_demand, mut unsupported_demand) = (Vec::new(), Vec::new());
        for c in config.classes() {
            let Some(d) = &c.demand else { continue };
            if class_work(&config, &snapshot, c).known {
                continue;
            }
            let observed = config.source(&d.source).is_some_and(observer_available);
            if observed {
                &mut unknown_demand
            } else {
                &mut unsupported_demand
            }
            .push(c.id.clone());
        }
        // Shadow mode: the sequencer's state is not carried between ticks, because no step
        // is ever executed and carrying it would only let the scheduler wait for effects
        // that cannot happen. Each tick answers "what would I do given what I see now".
        let result = decide(&config, &snapshot, &SchedState::default());
        let (unknown, unsupported) = (!unknown_demand.is_empty(), !unsupported_demand.is_empty());
        let (kinds, reason, steps, allowed, error) = match result {
            Ok((intent, decision)) => (
                reason::kinds(&decision.steps, unknown, unsupported)
                    .into_iter()
                    .map(String::from)
                    .collect(),
                reason::reason(
                    &intent,
                    &decision.steps,
                    &unknown_demand,
                    &unsupported_demand,
                ),
                decision.steps,
                decision.allowed,
                None,
            ),
            Err(e) => (
                vec![reason::KIND_ERROR.to_string()],
                format!("{}no decision: {e}", reason::SHADOW_PREFIX),
                Vec::new(),
                Default::default(),
                Some(e.to_string()),
            ),
        };
        let report = TickReport {
            config_generation: config.generation(),
            at: now,
            shadow_stateless: true,
            unknown_demand,
            unsupported_demand,
            kinds,
            reason,
            steps,
            allowed_models: allowed.models.into_iter().collect(),
            allowed_classes: allowed.classes.into_iter().collect(),
            inputs: ages.iter().map(InputReport::from).collect(),
            input_changed: changed,
            error,
        };
        for kind in &report.kinds {
            self.metrics.decision(kind);
        }
        self.metrics.set_input_ages(&ages);
        self.metrics.tick_finished_at(now);
        self.metrics.tick_duration(started.elapsed().as_secs_f64());
        log_tick(&report, &ages);
        report
    }

    /// Records that the backstop ran at `now`.
    pub fn note_backstop(&self, now: Millis) {
        self.last_backstop.store(now.as_u64(), Ordering::SeqCst);
    }
}

fn log_tick(r: &TickReport, ages: &[InputAge]) {
    let kinds = r.kinds.join(",");
    let inputs = reason::format_inputs(ages);
    if r.error.is_some() {
        tracing::error!(
            generation = r.config_generation,
            at_ms = r.at.as_u64(),
            shadow_stateless = true,
            decision = %kinds,
            reason = %r.reason,
            inputs = %inputs,
            "shadow tick failed (nothing executed)"
        );
    } else {
        tracing::info!(
            generation = r.config_generation,
            at_ms = r.at.as_u64(),
            shadow_stateless = true,
            decision = %kinds,
            reason = %r.reason,
            inputs = %inputs,
            input_changed = r.input_changed,
            unknown_demand = ?r.unknown_demand.iter().map(|c| c.as_str()).collect::<Vec<_>>(),
            unsupported_demand = ?r.unsupported_demand.iter().map(|c| c.as_str()).collect::<Vec<_>>(),
            "shadow tick (nothing executed)"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ManualClock;
    use sched_model::test_support::EXAMPLE_CONFIG;
    use sched_model::{Fidelity, ObservedValue};

    const T0: u64 = 10_000_000;

    fn shared() -> (Arc<Shared>, Arc<ManualClock>) {
        let clock = Arc::new(ManualClock::new(Millis(T0)));
        let holder = Arc::new(ConfigHolder::new(EXAMPLE_CONFIG).unwrap());
        let s = Shared::new(holder, Arc::new(Metrics::new()), clock.clone());
        (Arc::new(s), clock)
    }

    fn push(s: &Shared, source: &str, values: Vec<ObservedValue>) {
        s.ingest_now(Report::Observed {
            source: source.into(),
            fidelity: Fidelity::Polled,
            values,
        });
    }

    fn cluster(s: &Shared) {
        let mut v: Vec<ObservedValue> = ["mineru", "llm", "whisperx", "immich-ml"]
            .iter()
            .map(|m| ObservedValue::ModelPods {
                model: (*m).into(),
                pods: 0,
                ready_pods: 0,
            })
            .collect();
        v.push(ObservedValue::ResourceUp {
            resource: "cuda-central".into(),
            up: true,
        });
        push(s, "cluster-pods", v);
        push(
            s,
            "basestar-probe",
            vec![ObservedValue::ResourceUp {
                resource: "basestar-nfs".into(),
                up: true,
            }],
        );
    }

    fn ocr_demand(s: &Shared, ready: u64) {
        push(
            s,
            "paperless-ocr-pending",
            vec![ObservedValue::Demand {
                class: "paperless-ocr".into(),
                ready,
                in_flight: 0,
                oldest_ready_at: None,
            }],
        );
    }

    #[test]
    fn demand_with_fresh_cluster_state_is_logged_as_a_decision_to_allow_a_model() {
        let (s, _) = shared();
        cluster(&s);
        ocr_demand(&s, 3);
        let r = s.tick(Millis(T0 + 1_000));
        assert_eq!(r.error, None);
        assert!(r.kinds.contains(&"allow_model".to_string()), "{r:?}");
        assert!(
            r.reason.contains("allow mineru on cuda-central"),
            "{}",
            r.reason
        );
        assert_eq!(r.config_generation, 1);
        assert!(r.input_changed);
        let again = s.tick(Millis(T0 + 2_000));
        assert!(!again.input_changed);
        assert!(
            s.metrics
                .render()
                .contains("sched_tick_decisions_total{kind=\"allow_model\"} 2")
        );
    }

    #[test]
    fn a_source_that_stops_reporting_makes_decisions_wait_instead_of_looking_healthy() {
        let (s, _) = shared();
        cluster(&s);
        ocr_demand(&s, 3);
        // 31s later nothing was refreshed: pod counts are older than safety_max_age (30s).
        let r = s.tick(Millis(T0 + 31_000));
        assert!(!r.kinds.contains(&"allow_model".to_string()), "{r:?}");
        let pods_age = r
            .inputs
            .iter()
            .find(|i| i.source == "cluster-pods")
            .unwrap();
        assert_eq!(pods_age.age_ms, Some(31_000));
        let never = r
            .inputs
            .iter()
            .find(|i| i.source == "speakr-pending")
            .unwrap();
        assert_eq!(never.age_ms, None);
    }

    #[test]
    fn adapter_errors_are_visible_in_the_tick_and_the_metrics_and_do_not_refresh_data() {
        let (s, _) = shared();
        cluster(&s);
        ocr_demand(&s, 3);
        s.ingest(
            Report::Failed {
                source: "paperless-ocr-pending".into(),
                error: "HTTP 503".into(),
            },
            Millis(T0 + 5_000),
        );
        let r = s.tick(Millis(T0 + 6_000));
        let i = r
            .inputs
            .iter()
            .find(|i| i.source == "paperless-ocr-pending")
            .unwrap();
        assert_eq!(i.last_error.as_deref(), Some("HTTP 503"));
        // The age is that of the last good reading, not of the failure.
        assert_eq!(i.age_ms, Some(6_000));
        assert!(
            s.metrics
                .render()
                .contains("sched_source_errors_total{source=\"paperless-ocr-pending\"} 1")
        );
    }

    #[test]
    fn each_tick_is_pinned_to_one_generation_and_follows_reloads() {
        let (s, _) = shared();
        assert_eq!(s.tick(Millis(T0)).config_generation, 1);
        s.config
            .apply_text(&EXAMPLE_CONFIG.replace("idle_grace: 10m", "idle_grace: 20m"));
        let r = s.tick(Millis(T0 + 1));
        assert_eq!((r.config_generation, r.error), (2, None));
    }

    #[test]
    fn an_empty_world_decides_without_error() {
        let (s, _) = shared();
        let r = s.tick(Millis(T0));
        assert_eq!(r.error, None);
        assert!(!r.kinds.is_empty());
        println!("{r:#?}");
    }

    #[test]
    fn a_demand_source_that_never_reported_is_unknown_not_idle() {
        let (s, _) = shared();
        cluster(&s);
        // Pods and reachability are fresh; no demand source has ever reported.
        let r = s.tick(Millis(T0 + 1_000));
        assert!(!r.kinds.contains(&"idle".to_string()), "{r:?}");
        assert!(r.kinds.contains(&"demand_unknown".to_string()), "{r:?}");
        assert!(
            r.unknown_demand
                .iter()
                .any(|c| c.as_str() == "paperless-ocr")
        );
        assert!(
            r.reason.contains("demand UNKNOWN") && r.reason.contains("not idle"),
            "{}",
            r.reason
        );
        // Once every demand source has reported (all zero), it really is idle.
        for (src, class) in [
            ("paperless-ocr-pending", "paperless-ocr"),
            ("paperless-post-ocr", "paperless-tagging"),
            ("speakr-pending", "speakr-transcription"),
            ("speakr-summaries", "speakr-summarization"),
            ("immich-queues", "immich-ml"),
            ("backup-schedule", "backup"),
            ("llm-sessions", "interactive-llm"),
        ] {
            push(
                &s,
                src,
                vec![ObservedValue::Demand {
                    class: class.into(),
                    ready: 0,
                    in_flight: 0,
                    oldest_ready_at: None,
                }],
            );
        }
        let r = s.tick(Millis(T0 + 2_000));
        assert!(r.unknown_demand.is_empty() && r.unsupported_demand.is_empty());
        assert_eq!(r.kinds, vec!["idle".to_string()], "{r:?}");
        // And stale demand is unknown again.
        let r = s.tick(Millis(T0 + 2_000 + 600_001));
        assert!(r.kinds.contains(&"demand_unknown".to_string()), "{r:?}");
    }

    #[test]
    fn every_tick_says_it_is_stateless_shadow_output() {
        let (s, _) = shared();
        let r = s.tick(Millis(T0));
        assert!(r.shadow_stateless);
        assert!(
            r.reason.starts_with(crate::reason::SHADOW_PREFIX),
            "{}",
            r.reason
        );
    }

    #[test]
    fn a_tick_never_runs_before_an_observation_it_can_read() {
        let (s, _) = shared();
        cluster(&s);
        // Received at T0 + 5s, after the time the tick was told: without the clamp the
        // observation would be dated in the future and read as not fresh.
        s.ingest(
            Report::Observed {
                source: "paperless-ocr-pending".into(),
                fidelity: Fidelity::Polled,
                values: vec![ObservedValue::Demand {
                    class: "paperless-ocr".into(),
                    ready: 3,
                    in_flight: 0,
                    oldest_ready_at: None,
                }],
            },
            Millis(T0 + 5_000),
        );
        let r = s.tick(Millis(T0));
        assert_eq!(r.at, Millis(T0 + 5_000));
        assert!(
            !r.unknown_demand
                .iter()
                .any(|c| c.as_str() == "paperless-ocr"),
            "{r:?}"
        );
        let age = r
            .inputs
            .iter()
            .find(|i| i.source == "paperless-ocr-pending")
            .unwrap();
        assert_eq!(age.age_ms, Some(0));
    }

    #[test]
    fn a_config_change_in_the_middle_of_a_tick_does_not_split_its_generation() {
        let (s, _) = shared();
        let changed = EXAMPLE_CONFIG.replace("idle_grace: 10m", "idle_grace: 20m");
        let r = s.tick_pinned(Millis(T0), || {
            assert!(matches!(
                s.config.apply_text(&changed),
                crate::config_source::ReloadOutcome::Applied(2)
            ));
        });
        // The whole tick ran on generation 1, snapshot and core alike: no mismatch error.
        assert_eq!((r.config_generation, r.error), (1, None));
        assert_eq!(s.tick(Millis(T0 + 1)).config_generation, 2);
    }

    #[test]
    fn unknown_sources_create_no_metric_series_and_stray_subjects_are_counted_not_stored() {
        let (s, _) = shared();
        let before = s.metrics.render();
        for i in 0..50 {
            let r = s.ingest_now(Report::Failed {
                source: format!("made-up-{i}").as_str().into(),
                error: "x".into(),
            });
            assert_eq!(r, Ingest::UnknownSource);
        }
        let after = s.metrics.render();
        assert!(!after.contains("made-up"), "{after}");
        assert!(
            after.contains("sched_rejected_reports_total{reason=\"unknown_source\"} 50"),
            "{after}"
        );
        assert_eq!(
            before.contains("sched_source_errors_total{"),
            after.contains("sched_source_errors_total{")
        );
        let r = s.ingest_now(Report::Observed {
            source: "paperless-ocr-pending".into(),
            fidelity: Fidelity::Polled,
            values: (0..20)
                .map(|i| ObservedValue::Demand {
                    class: format!("c{i}").as_str().into(),
                    ready: 1,
                    in_flight: 0,
                    oldest_ready_at: None,
                })
                .collect(),
        });
        assert_eq!(
            r,
            Ingest::Taken {
                changed: false,
                dropped: 20
            }
        );
    }

    #[test]
    fn demand_of_sources_without_an_observer_is_reported_apart_and_is_still_not_idle() {
        let (s, _) = shared();
        cluster(&s);
        for (src, class) in [
            ("paperless-ocr-pending", "paperless-ocr"),
            ("paperless-post-ocr", "paperless-tagging"),
            ("speakr-pending", "speakr-transcription"),
            ("speakr-summaries", "speakr-summarization"),
            ("immich-queues", "immich-ml"),
        ] {
            push(
                &s,
                src,
                vec![ObservedValue::Demand {
                    class: class.into(),
                    ready: 0,
                    in_flight: 0,
                    oldest_ready_at: None,
                }],
            );
        }
        // Everything that has an observer reports; only schedule and session sources are
        // silent, and nothing is built to observe them.
        let r = s.tick(Millis(T0 + 1_000));
        assert!(r.unknown_demand.is_empty(), "{:?}", r.unknown_demand);
        assert_eq!(r.kinds, vec!["demand_unsupported".to_string()], "{r:?}");
        let text = s.metrics.render();
        assert!(
            text.contains("sched_source_has_observer{source=\"backup-schedule\"} 0"),
            "{text}"
        );
        assert!(
            text.contains("sched_source_has_observer{source=\"paperless-ocr-pending\"} 1"),
            "{text}"
        );
    }
}
