//! The shared scheduler context and one tick. A tick takes the cached snapshot, runs the
//! core (planner, then sequencer), and logs the decision and why. Nothing is executed: the
//! service has no actuator, and the observers only read.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use sched_model::{ClassId, Millis, ModelId, SchedState, Step};
use sched_observe::Report;
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
    /// Milliseconds since the newest observation arrived; absent if none ever did.
    pub age_ms: Option<u64>,
    pub last_error: Option<String>,
}

impl From<&InputAge> for InputReport {
    fn from(a: &InputAge) -> Self {
        InputReport {
            source: a.source.to_string(),
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
            clock,
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
        if let Report::Failed { source, error } = &report {
            self.metrics.source_error(source.as_str());
            tracing::warn!(source = %source, "source error: {error}");
        }
        let result = self.cache().ingest(&config, report, now);
        match &result {
            Ingest::Taken { changed: true } => self.changed.notify_one(),
            Ingest::Taken { changed: false } => {}
            Ingest::UnknownSource => {
                tracing::warn!("report from a source not in the config dropped")
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
        let started = Instant::now();
        let config = self.config.current();
        let (snapshot, ages, changed) = {
            let mut cache = self.cache();
            (
                cache.snapshot(&config, now),
                cache.input_ages(&config, now),
                cache.take_dirty(),
            )
        };
        // Shadow mode: the sequencer's state is not carried between ticks, because no step
        // is ever executed and carrying it would only let the scheduler wait for effects
        // that cannot happen. Each tick answers "what would I do given what I see now".
        let result = decide(&config, &snapshot, &SchedState::default());
        let (kinds, reason, steps, allowed, error) = match result {
            Ok((intent, decision)) => (
                reason::kinds(&decision.steps)
                    .into_iter()
                    .map(String::from)
                    .collect(),
                reason::reason(&intent, &decision.steps),
                decision.steps,
                decision.allowed,
                None,
            ),
            Err(e) => (
                vec![reason::KIND_ERROR.to_string()],
                format!("no decision: {e}"),
                Vec::new(),
                Default::default(),
                Some(e.to_string()),
            ),
        };
        let report = TickReport {
            config_generation: config.generation(),
            at: now,
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
}

fn log_tick(r: &TickReport, ages: &[InputAge]) {
    let kinds = r.kinds.join(",");
    let inputs = reason::format_inputs(ages);
    if r.error.is_some() {
        tracing::error!(
            generation = r.config_generation,
            at_ms = r.at.as_u64(),
            decision = %kinds,
            reason = %r.reason,
            inputs = %inputs,
            "shadow tick failed (nothing executed)"
        );
    } else {
        tracing::info!(
            generation = r.config_generation,
            at_ms = r.at.as_u64(),
            decision = %kinds,
            reason = %r.reason,
            inputs = %inputs,
            input_changed = r.input_changed,
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
                .contains("sched_decisions_total{kind=\"allow_model\"} 2")
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
}
