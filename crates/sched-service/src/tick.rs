//! The shared scheduler context and one tick.
//!
//! A tick reads the cached snapshot and the scheduler's durable state ([`Durable`]: the
//! sequencer state, the leases, and which sources have ever reported), runs the core
//! (planner, then sequencer) and returns a [`TickOutcome`]: the decision and the state to
//! persist. The `Scheduler` object computes it inside a journaled step, stores the new
//! state, and only then calls [`Shared::publish`], which makes the decision visible: the
//! allowed set on the channel the KEDA scaler reads, logs and metrics.
//!
//! What is carried out: the allowed set is published, so allowing a model, draining it and
//! pausing a class's intake take effect once the scaler converges on it. Powering a host on
//! or off, stopping in-flight work and recovery actions have no actuator in this build;
//! those steps are listed in `not_executed` and logged as such.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use sched_model::{
    AllowedSet, ClassId, Lease, LeaseId, Millis, ModelId, PublishedAllowed, SchedState, SourceId,
    Step, class_work,
};
use sched_observe::{ObserverState, Report, observer_state};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::cache::{Ingest, InputAge, ObservationCache};
use crate::clock::Clock;
use crate::config_source::ConfigHolder;
use crate::core::decide;
use crate::leases;
use crate::metrics::Metrics;
use crate::reason::{self, DemandGaps};

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
    /// Classes whose demand is unknown although something should be reporting it: an
    /// observer runs for the source, or the source has reported before (in this process or
    /// an earlier one), and it is silent or stale. A real problem. Unknown is not zero:
    /// such a tick is never `idle`.
    pub unknown_demand: Vec<ClassId>,
    /// Classes whose demand is unknown because this build has no observer for their source
    /// kind (schedule, session, ...) and nothing has ever pushed to it. Expected for now,
    /// and kept apart so it cannot drown `unknown_demand`.
    pub unsupported_demand: Vec<ClassId>,
    /// Classes whose source is of an observable kind but configured without `params`, and
    /// has never been pushed to: nothing polls it. A config gap, still not idle.
    pub unpolled_demand: Vec<ClassId>,
    /// Distinct decision kinds (see `reason::step_kind`), `idle` for none, `error` on failure.
    pub kinds: Vec<String>,
    pub reason: String,
    pub steps: Vec<Step>,
    /// The steps nothing in this build carries out (power on/off, stopping in-flight work,
    /// recovery actions). The sequencer has recorded them as taken all the same.
    #[serde(default)]
    pub not_executed: Vec<Step>,
    pub allowed_models: Vec<ModelId>,
    pub allowed_classes: Vec<ClassId>,
    /// The leases the core was given.
    #[serde(default)]
    pub leases: Vec<Lease>,
    /// Leases held but not given to the core, because the active config no longer has
    /// their resource or model.
    #[serde(default)]
    pub ignored_leases: Vec<LeaseId>,
    /// The sequencer state after this tick (unchanged from before if the tick failed).
    #[serde(default)]
    pub state: SchedState,
    pub inputs: Vec<InputReport>,
    /// Whether observations changed since the previous tick of this process.
    pub input_changed: bool,
    pub error: Option<String>,
}

/// What a tick reads from the scheduler's durable state, kept by the `Scheduler` object in
/// Restate.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Durable {
    /// The sequencer state the previous tick left.
    #[serde(default)]
    pub state: SchedState,
    #[serde(default)]
    pub leases: Vec<Lease>,
    /// Configured sources that have reported values at some point, in this process or an
    /// earlier one.
    #[serde(default)]
    pub reported: BTreeSet<SourceId>,
}

impl Durable {
    /// Takes in what a tick says to persist.
    pub fn apply(&mut self, outcome: &TickOutcome) {
        self.state = outcome.report.state.clone();
        self.reported = outcome.reported.clone();
    }
}

/// One tick's outcome: the decision and what to persist. The `Scheduler` object journals
/// it whole, so a retried invocation reuses it instead of deciding again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickOutcome {
    pub report: TickReport,
    /// Sources that have ever reported, to persist (configured sources only).
    pub reported: BTreeSet<SourceId>,
    /// The inputs as one log field.
    pub inputs_text: String,
    /// How long deciding took, in microseconds.
    pub duration_us: u64,
}

impl TickOutcome {
    /// The allowed set to publish, or `None` if the tick failed (the previous publication
    /// then stays).
    pub fn published(&self) -> Option<PublishedAllowed> {
        let r = &self.report;
        r.error.is_none().then(|| PublishedAllowed {
            config_generation: r.config_generation,
            at: r.at,
            allowed: AllowedSet {
                models: r.allowed_models.iter().cloned().collect(),
                classes: r.allowed_classes.iter().cloned().collect(),
            },
        })
    }
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
    /// The allowed set of the latest tick this process published; `None` until then.
    allowed: watch::Sender<Option<PublishedAllowed>>,
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
            allowed: watch::channel(None).0,
        }
    }

    fn cache(&self) -> std::sync::MutexGuard<'_, ObservationCache> {
        self.cache.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A receiver of the published allowed set, for the KEDA scaler. It holds `None` until
    /// the first tick of this process has been published, then the latest tick's set.
    pub fn subscribe_allowed(&self) -> watch::Receiver<Option<PublishedAllowed>> {
        self.allowed.subscribe()
    }

    /// The allowed set currently published.
    pub fn published_allowed(&self) -> Option<PublishedAllowed> {
        self.allowed.borrow().clone()
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

    /// Decides one tick at `now` from the cache and `durable`. Publishes nothing: see
    /// [`Shared::publish`]. The config handle is taken once, so the whole tick sees one
    /// generation.
    pub fn decide(&self, now: Millis, durable: &Durable) -> TickOutcome {
        self.decide_pinned(now, durable, || {})
    }

    /// As [`Shared::decide`]; `after_pin` runs right after the config is pinned, a seam for
    /// tests to change the config mid-tick.
    pub fn decide_pinned(
        &self,
        now: Millis,
        durable: &Durable,
        after_pin: impl FnOnce(),
    ) -> TickOutcome {
        let started = Instant::now();
        let config = self.config.current();
        after_pin();
        let (leases, ignored_leases) = leases::effective(&config, &durable.leases);
        let (mut snapshot, ages, changed, now, reported) = {
            let mut cache = self.cache();
            // Which sources reported before is durable; the cache only knows this process.
            cache.seed_reported(&config, &durable.reported);
            // Never run before something already received: that observation would be
            // dated after `now` and read as not fresh.
            let now = now.max(cache.latest_receipt());
            (
                cache.snapshot(&config, now),
                cache.input_ages(&config, now),
                cache.take_dirty(),
                now,
                config
                    .sources()
                    .iter()
                    .filter(|s| cache.has_reported(&s.id))
                    .map(|s| s.id.clone())
                    .collect::<BTreeSet<_>>(),
            )
        };
        snapshot.leases = leases.clone();
        let mut gaps = DemandGaps::default();
        for c in config.classes() {
            let Some(d) = &c.demand else { continue };
            if class_work(&config, &snapshot, c).known {
                continue;
            }
            let state = config.source(&d.source).map(observer_state);
            let silent_before = reported.contains(&d.source);
            // Something that has ever reported is merely silent now, whatever feeds it.
            let list = match state {
                _ if silent_before => &mut gaps.unknown,
                Some(ObserverState::NoParams) => &mut gaps.unpolled,
                Some(ObserverState::NoImplementation) => &mut gaps.unsupported,
                _ => &mut gaps.unknown,
            };
            list.push(c.id.clone());
        }
        let (kinds, reason, steps, allowed, state, error) =
            match decide(&config, &snapshot, &durable.state) {
                Ok((intent, decision)) => (
                    reason::kinds(&decision.steps, &gaps)
                        .into_iter()
                        .map(String::from)
                        .collect(),
                    reason::reason(&intent, &decision.steps, &gaps),
                    decision.steps,
                    decision.allowed,
                    decision.state,
                    None,
                ),
                // The previous state stays, and so does the previous publication.
                Err(e) => (
                    vec![reason::KIND_ERROR.to_string()],
                    format!("no decision: {e}"),
                    Vec::new(),
                    AllowedSet::default(),
                    durable.state.clone(),
                    Some(e.to_string()),
                ),
            };
        // Input ages describe the cache as read now, not the decision, so they are set
        // here rather than on publication.
        self.metrics.set_input_ages(&ages);
        let report = TickReport {
            config_generation: config.generation(),
            at: now,
            unknown_demand: gaps.unknown,
            unsupported_demand: gaps.unsupported,
            unpolled_demand: gaps.unpolled,
            kinds,
            reason,
            not_executed: steps
                .iter()
                .filter(|s| reason::not_executed(s))
                .cloned()
                .collect(),
            steps,
            allowed_models: allowed.models.into_iter().collect(),
            allowed_classes: allowed.classes.into_iter().collect(),
            leases,
            ignored_leases,
            state,
            inputs: ages.iter().map(InputReport::from).collect(),
            input_changed: changed,
            error,
        };
        TickOutcome {
            report,
            reported,
            inputs_text: reason::format_inputs(&ages),
            duration_us: u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
        }
    }

    /// Makes a decided tick visible: publishes its allowed set (unless the tick failed),
    /// counts it, and logs it, with a warning per step that nothing carries out.
    pub fn publish(&self, outcome: &TickOutcome) {
        let r = &outcome.report;
        if let Some(p) = outcome.published() {
            self.allowed.send_replace(Some(p));
        }
        for kind in &r.kinds {
            self.metrics.decision(kind);
        }
        self.metrics.set_phases(r.at, &r.state);
        self.metrics.tick_finished_at(r.at);
        self.metrics
            .tick_duration(outcome.duration_us as f64 / 1_000_000.0);
        log_tick(r, &outcome.inputs_text);
    }

    /// Decides and publishes one tick against `durable`, then updates it as the
    /// `Scheduler` object updates its Restate state. For tests and tools; the object
    /// journals the decision in between.
    pub fn tick(&self, now: Millis, durable: &mut Durable) -> TickReport {
        let outcome = self.decide(now, durable);
        self.publish(&outcome);
        durable.apply(&outcome);
        outcome.report
    }

    /// Records that the backstop ran at `now`.
    pub fn note_backstop(&self, now: Millis) {
        self.last_backstop.store(now.as_u64(), Ordering::SeqCst);
    }
}

fn log_tick(r: &TickReport, inputs: &str) {
    let kinds = r.kinds.join(",");
    if r.error.is_some() {
        tracing::error!(
            generation = r.config_generation,
            at_ms = r.at.as_u64(),
            decision = %kinds,
            reason = %r.reason,
            inputs = %inputs,
            "tick failed; the previous state and allowed set stay"
        );
        return;
    }
    fn ids(v: &[ClassId]) -> Vec<&str> {
        v.iter().map(|c| c.as_str()).collect()
    }
    tracing::info!(
        generation = r.config_generation,
        at_ms = r.at.as_u64(),
        decision = %kinds,
        reason = %r.reason,
        allowed_models = ?r.allowed_models.iter().map(|m| m.as_str()).collect::<Vec<_>>(),
        allowed_classes = ?ids(&r.allowed_classes),
        leases = ?r.leases.iter().map(|l| l.id.as_str()).collect::<Vec<_>>(),
        inputs = %inputs,
        input_changed = r.input_changed,
        unknown_demand = ?ids(&r.unknown_demand),
        unsupported_demand = ?ids(&r.unsupported_demand),
        unpolled_demand = ?ids(&r.unpolled_demand),
        "tick"
    );
    for step in &r.not_executed {
        tracing::warn!(
            generation = r.config_generation,
            at_ms = r.at.as_u64(),
            kind = reason::step_kind(step),
            step = ?step,
            "step NOT EXECUTED: no actuator for it in this build"
        );
    }
    if !r.ignored_leases.is_empty() {
        tracing::warn!(
            leases = ?r.ignored_leases.iter().map(|l| l.as_str()).collect::<Vec<_>>(),
            "leases ignored: the active config has no such resource or model"
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
        let mut d = Durable::default();
        cluster(&s);
        ocr_demand(&s, 3);
        let r = s.tick(Millis(T0 + 1_000), &mut d);
        assert_eq!(r.error, None);
        assert!(r.kinds.contains(&"allow_model".to_string()), "{r:?}");
        assert!(
            r.reason.contains("allow mineru on cuda-central"),
            "{}",
            r.reason
        );
        assert_eq!(r.config_generation, 1);
        assert!(r.input_changed);
        // The state is carried: the next tick sees the model loading, not idle.
        let again = s.tick(Millis(T0 + 2_000), &mut d);
        assert!(!again.input_changed);
        assert!(again.kinds.contains(&"wait".to_string()), "{again:?}");
        assert!(
            !again.kinds.contains(&"allow_model".to_string()),
            "{again:?}"
        );
        assert!(again.reason.contains("still loading"), "{}", again.reason);
        let text = s.metrics.render();
        assert!(text.contains("sched_tick_decisions_total{kind=\"allow_model\"} 1"));
        assert!(text.contains("sched_tick_decisions_total{kind=\"wait\"} 1"));
    }

    #[test]
    fn a_source_that_stops_reporting_makes_decisions_wait_instead_of_looking_healthy() {
        let (s, _) = shared();
        let mut d = Durable::default();
        cluster(&s);
        ocr_demand(&s, 3);
        // 31s later nothing was refreshed: pod counts are older than safety_max_age (30s).
        let r = s.tick(Millis(T0 + 31_000), &mut d);
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
        let mut d = Durable::default();
        cluster(&s);
        ocr_demand(&s, 3);
        s.ingest(
            Report::Failed {
                source: "paperless-ocr-pending".into(),
                error: "HTTP 503".into(),
            },
            Millis(T0 + 5_000),
        );
        let r = s.tick(Millis(T0 + 6_000), &mut d);
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
        let mut d = Durable::default();
        assert_eq!(s.tick(Millis(T0), &mut d).config_generation, 1);
        s.config
            .apply_text(&EXAMPLE_CONFIG.replace("idle_grace: 10m", "idle_grace: 20m"));
        let r = s.tick(Millis(T0 + 1), &mut d);
        assert_eq!((r.config_generation, r.error), (2, None));
    }

    #[test]
    fn an_empty_world_decides_without_error() {
        let (s, _) = shared();
        let mut d = Durable::default();
        let r = s.tick(Millis(T0), &mut d);
        assert_eq!(r.error, None);
        assert!(!r.kinds.is_empty());
        println!("{r:#?}");
    }

    #[test]
    fn a_demand_source_that_never_reported_is_unknown_not_idle() {
        let (s, _) = shared();
        let mut d = Durable::default();
        cluster(&s);
        // Pods and reachability are fresh; no demand source has ever reported.
        let r = s.tick(Millis(T0 + 1_000), &mut d);
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
        let r = s.tick(Millis(T0 + 2_000), &mut d);
        assert!(r.unknown_demand.is_empty() && r.unsupported_demand.is_empty());
        assert_eq!(r.kinds, vec!["idle".to_string()], "{r:?}");
        // And stale demand is unknown again.
        let r = s.tick(Millis(T0 + 2_000 + 600_001), &mut d);
        assert!(r.kinds.contains(&"demand_unknown".to_string()), "{r:?}");
    }

    /// The durable state as Restate keeps it: written as JSON, read back in a new process.
    fn restart(d: &Durable) -> (Arc<Shared>, Durable) {
        let text = serde_json::to_string(d).unwrap();
        let (s, _) = shared();
        (s, serde_json::from_str(&text).unwrap())
    }

    fn llm_lease() -> Lease {
        Lease {
            id: "manual-model-1".into(),
            resource: "cuda-central".into(),
            kind: sched_model::LeaseKind::ManualModel {
                model: "llm".into(),
            },
            granted_at: Millis(T0),
        }
    }

    #[test]
    fn the_sequencer_state_is_carried_and_survives_a_restart_with_an_empty_cache() {
        let (s, clock) = shared();
        let mut d = Durable::default();
        cluster(&s);
        ocr_demand(&s, 3);
        let r = s.tick(Millis(T0 + 1_000), &mut d);
        assert!(r.kinds.contains(&"allow_model".to_string()), "{r:?}");
        assert!(!r.reason.contains("shadow"), "{}", r.reason);
        assert!(matches!(
            d.state.resource(&"cuda-central".into()).unwrap().mode,
            sched_model::ModePhase::Loading { .. }
        ));
        clock.set(Millis(T0 + 2_000));
        push(
            &s,
            "cluster-pods",
            vec![ObservedValue::ModelPods {
                model: "mineru".into(),
                pods: 1,
                ready_pods: 1,
            }],
        );
        let r = s.tick(Millis(T0 + 2_000), &mut d);
        assert_eq!(r.allowed_models, vec![ModelId::from("mineru")]);
        let serving_since = match &d.state.resource(&"cuda-central".into()).unwrap().mode {
            sched_model::ModePhase::Serving { since, .. } => *since,
            m => panic!("{m:?}"),
        };
        // A new process: nothing observed yet, the state comes back from Restate.
        let (s2, mut d2) = restart(&d);
        assert_eq!(d2, d);
        assert_eq!(s2.published_allowed(), None);
        let r = s2.tick(Millis(T0 + 3_000), &mut d2);
        assert_eq!(r.error, None);
        // It goes on serving mineru (since the same instant) instead of starting over.
        assert_eq!(r.allowed_models, vec![ModelId::from("mineru")], "{r:?}");
        assert!(!r.kinds.contains(&"allow_model".to_string()), "{r:?}");
        match &d2.state.resource(&"cuda-central".into()).unwrap().mode {
            sched_model::ModePhase::Serving { since, .. } => assert_eq!(*since, serving_since),
            m => panic!("{m:?}"),
        }
        assert_eq!(
            s2.published_allowed().unwrap().allowed.models,
            [ModelId::from("mineru")].into_iter().collect()
        );
    }

    #[test]
    fn the_allowed_set_is_published_only_after_a_tick_and_follows_each_one() {
        let (s, _) = shared();
        let mut d = Durable::default();
        let mut rx = s.subscribe_allowed();
        assert_eq!(*rx.borrow_and_update(), None);
        cluster(&s);
        ocr_demand(&s, 3);
        // Deciding alone publishes nothing: the object journals the decision first.
        let outcome = s.decide(Millis(T0 + 1_000), &d);
        assert!(!rx.has_changed().unwrap());
        assert_eq!(s.published_allowed(), None);
        s.publish(&outcome);
        d.apply(&outcome);
        assert!(rx.has_changed().unwrap());
        let p = rx.borrow_and_update().clone().unwrap();
        assert_eq!((p.config_generation, p.at), (1, Millis(T0 + 1_000)));
        assert_eq!(
            p.allowed.models,
            [ModelId::from("mineru")].into_iter().collect()
        );
        // NFS is up, so the NFS-only classes are allowed; OCR is not until mineru serves.
        assert!(p.allowed.classes.contains(&ClassId::from("paperless-ngx")));
        assert!(!p.allowed.classes.contains(&ClassId::from("paperless-ocr")));
        let r = s.tick(Millis(T0 + 2_000), &mut d);
        assert!(rx.has_changed().unwrap());
        assert_eq!(rx.borrow_and_update().as_ref().unwrap().at, r.at);
    }

    #[test]
    fn a_failed_tick_keeps_the_previous_state_and_publication() {
        let (s, _) = shared();
        let mut d = Durable::default();
        cluster(&s);
        ocr_demand(&s, 3);
        s.tick(Millis(T0 + 1_000), &mut d);
        let (state, published) = (d.state.clone(), s.published_allowed());
        assert!(published.is_some());
        s.config
            .apply_text(&EXAMPLE_CONFIG.replace("fifo-exhaustive", "no-such-strategy"));
        let r = s.tick(Millis(T0 + 2_000), &mut d);
        assert!(r.error.is_some(), "{r:?}");
        assert_eq!(r.kinds, vec!["error".to_string()]);
        assert_eq!(d.state, state);
        assert_eq!(s.published_allowed(), published);
    }

    #[test]
    fn sources_that_reported_before_a_restart_stay_unknown_rather_than_unsupported() {
        let (s, _) = shared();
        let mut d = Durable::default();
        cluster(&s);
        // backup-schedule has no observer in this build, but something pushed to it.
        push(
            &s,
            "backup-schedule",
            vec![ObservedValue::Demand {
                class: "backup".into(),
                ready: 0,
                in_flight: 0,
                oldest_ready_at: None,
            }],
        );
        s.tick(Millis(T0 + 1_000), &mut d);
        assert!(d.reported.contains(&SourceId::from("backup-schedule")));
        assert!(d.reported.contains(&SourceId::from("cluster-pods")));
        assert!(!d.reported.contains(&SourceId::from("llm-sessions")));
        // After a restart the pusher is dead: nothing arrives.
        let (s2, mut d2) = restart(&d);
        let r = s2.tick(Millis(T0 + 2_000), &mut d2);
        assert!(
            r.unknown_demand.iter().any(|c| c.as_str() == "backup"),
            "{r:?}"
        );
        assert!(
            !r.unsupported_demand.iter().any(|c| c.as_str() == "backup"),
            "{r:?}"
        );
        // The memory is kept across further ticks and restarts.
        assert!(d2.reported.contains(&SourceId::from("backup-schedule")));
        // Without the durable memory it would read as never reported.
        let (s3, _) = shared();
        let r = s3.tick(Millis(T0 + 2_000), &mut Durable::default());
        assert!(r.unsupported_demand.iter().any(|c| c.as_str() == "backup"));
    }

    #[test]
    fn durable_leases_reach_the_core_and_leases_the_config_lost_are_ignored() {
        let (s, _) = shared();
        let mut d = Durable {
            leases: vec![
                llm_lease(),
                Lease {
                    id: "manual-model-9".into(),
                    resource: "cuda-central".into(),
                    kind: sched_model::LeaseKind::ManualModel {
                        model: "gone".into(),
                    },
                    granted_at: Millis(T0),
                },
            ],
            ..Durable::default()
        };
        cluster(&s);
        ocr_demand(&s, 3);
        let r = s.tick(Millis(T0 + 1_000), &mut d);
        // OCR has work, but the manual lease pins the LLM.
        assert_eq!(r.allowed_models, vec![ModelId::from("llm")], "{r:?}");
        assert_eq!(r.leases, vec![llm_lease()]);
        assert_eq!(r.ignored_leases, vec![LeaseId::from("manual-model-9")]);
    }

    #[test]
    fn steps_without_an_actuator_are_reported_as_not_executed() {
        let (s, _) = shared();
        let mut d = Durable {
            leases: vec![llm_lease()],
            ..Durable::default()
        };
        // cuda-central is down; a manual model lease asks for it to be powered up.
        push(
            &s,
            "cluster-pods",
            vec![ObservedValue::ResourceUp {
                resource: "cuda-central".into(),
                up: false,
            }],
        );
        let r = s.tick(Millis(T0 + 1_000), &mut d);
        let on = Step::PowerOn {
            resource: "cuda-central".into(),
        };
        assert!(r.steps.contains(&on), "{r:?}");
        assert_eq!(r.not_executed, vec![on]);
        assert!(r.reason.contains(reason::NOT_EXECUTED), "{}", r.reason);
        // The sequencer took the step all the same: the host now reads as powering on.
        assert!(matches!(
            d.state.resource(&"cuda-central".into()).unwrap().power,
            sched_model::PowerPhase::PoweringOn { .. }
        ));
        let text = s.metrics.render();
        assert!(
            text.contains(
                "sched_resource_phase_age_seconds{axis=\"power\",phase=\"powering_on\",resource=\"cuda-central\"} 0"
            ),
            "{text}"
        );
    }

    #[test]
    fn a_tick_never_runs_before_an_observation_it_can_read() {
        let (s, _) = shared();
        let mut d = Durable::default();
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
        let r = s.tick(Millis(T0), &mut d);
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
        let mut d = Durable::default();
        let changed = EXAMPLE_CONFIG.replace("idle_grace: 10m", "idle_grace: 20m");
        let r = s
            .decide_pinned(Millis(T0), &d, || {
                assert!(matches!(
                    s.config.apply_text(&changed),
                    crate::config_source::ReloadOutcome::Applied(2)
                ));
            })
            .report;
        // The whole tick ran on generation 1, snapshot and core alike: no mismatch error.
        assert_eq!((r.config_generation, r.error), (1, None));
        assert_eq!(s.tick(Millis(T0 + 1), &mut d).config_generation, 2);
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
        let mut d = Durable::default();
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
        let r = s.tick(Millis(T0 + 1_000), &mut d);
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

    #[test]
    fn a_pushed_source_that_goes_silent_is_unknown_not_unsupported() {
        let (s, _) = shared();
        let mut d = Durable::default();
        cluster(&s);
        // backup-schedule has no observer in this build, but something pushed to it.
        push(
            &s,
            "backup-schedule",
            vec![ObservedValue::Demand {
                class: "backup".into(),
                ready: 0,
                in_flight: 0,
                oldest_ready_at: None,
            }],
        );
        let fresh = s.tick(Millis(T0 + 1_000), &mut d);
        assert!(
            !fresh
                .unsupported_demand
                .iter()
                .any(|c| c.as_str() == "backup")
        );
        assert!(!fresh.unknown_demand.iter().any(|c| c.as_str() == "backup"));
        // Ten minutes later it has gone stale: a source that reported before and has gone
        // quiet is a real problem, not the expected "nothing observes this".
        let stale = s.tick(Millis(T0 + 1_000 + 600_001), &mut d);
        assert!(
            stale.unknown_demand.iter().any(|c| c.as_str() == "backup"),
            "{stale:?}"
        );
        assert!(
            !stale
                .unsupported_demand
                .iter()
                .any(|c| c.as_str() == "backup")
        );
    }

    #[test]
    fn an_observable_source_configured_without_params_is_labelled_as_such() {
        let y = EXAMPLE_CONFIG.replace(
            "    params:\n      requests:\n        - url: http://immich-server.media.svc:2283/api/jobs\n          count: /waiting\n",
            "",
        );
        assert_ne!(y, EXAMPLE_CONFIG);
        let clock = Arc::new(ManualClock::new(Millis(T0)));
        let holder = Arc::new(ConfigHolder::new(&y).unwrap());
        let s = Shared::new(holder, Arc::new(Metrics::new()), clock);
        let mut d = Durable::default();
        cluster(&s);
        let r = s.tick(Millis(T0 + 1_000), &mut d);
        assert_eq!(r.unpolled_demand, vec![ClassId::from("immich-ml")], "{r:?}");
        assert!(!r.unknown_demand.iter().any(|c| c.as_str() == "immich-ml"));
        assert!(
            !r.unsupported_demand
                .iter()
                .any(|c| c.as_str() == "immich-ml")
        );
        assert!(r.kinds.contains(&"demand_unpolled".to_string()), "{r:?}");
        assert!(
            r.reason.contains("configured without params"),
            "{}",
            r.reason
        );
        // Nothing polls it, but a push is accepted and makes it known.
        push(
            &s,
            "immich-queues",
            vec![ObservedValue::Demand {
                class: "immich-ml".into(),
                ready: 0,
                in_flight: 0,
                oldest_ready_at: None,
            }],
        );
        assert!(
            s.tick(Millis(T0 + 2_000), &mut d)
                .unpolled_demand
                .is_empty()
        );
    }
}
