//! Prometheus metrics. Every label comes from a fixed set (decision kinds, reload
//! outcomes) or from configuration (source ids), so cardinality is bounded.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use prometheus::{
    Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
};
use sched_model::{Millis, ModePhase, PowerPhase, SchedState};

use sched_observe::ObserverState;

use crate::cache::InputAge;

pub struct Metrics {
    registry: Registry,
    decisions: IntCounterVec,
    observation_age: GaugeVec,
    source_errors: IntCounterVec,
    tick_duration: Histogram,
    config_generation: IntGauge,
    config_reloads: IntCounterVec,
    trigger_failures: IntCounter,
    last_tick_timestamp: Gauge,
    rejected: IntCounterVec,
    age_labels: Mutex<HashSet<(String, String)>>,
    has_observer: IntGaugeVec,
    observer_labels: Mutex<HashSet<String>>,
    phase_age: GaugeVec,
    state_errors: IntCounterVec,
    recovery_runs: IntCounterVec,
    recovery_requeues: IntCounterVec,
    power_requests: IntCounterVec,
    /// Per (resource, axis), the phase whose series is set and when it began.
    phases: Mutex<HashMap<PhaseKey, (&'static str, Millis)>>,
}

/// A resource id and a phase axis (`power` or `mode`).
type PhaseKey = (String, &'static str);

/// A resource's power phase as a metric label, with when it began if the state says.
fn power_phase(p: &PowerPhase) -> (&'static str, Option<Millis>) {
    match p {
        PowerPhase::Off => ("off", None),
        PowerPhase::PoweringOn { since } => ("powering_on", Some(*since)),
        PowerPhase::On { since } => ("on", Some(*since)),
        PowerPhase::ShuttingDown { since } => ("shutting_down", Some(*since)),
    }
}

/// A resource's mode phase as a metric label, with when it began if the state says. A
/// serving model whose wind-down has started is `winding_down`, measured from its start.
/// `serving` is measured from the tick the model was allowed.
fn mode_phase(m: &ModePhase) -> (&'static str, Option<Millis>) {
    match m {
        ModePhase::Idle => ("idle", None),
        ModePhase::Serving {
            wind_down: Some(w), ..
        } => ("winding_down", Some(w.since)),
        ModePhase::Serving { since, .. } => ("serving", Some(*since)),
    }
}

impl Metrics {
    pub fn new() -> Metrics {
        let registry = Registry::new();
        let decisions = IntCounterVec::new(
            Opts::new("sched_tick_decisions_total", "Ticks by decision kind, counted once per distinct kind in each tick (per tick, not per executed action)"),
            &["kind"],
        )
        .unwrap();
        let observation_age = GaugeVec::new(
            Opts::new(
                "sched_observation_age_seconds",
                "Age of the newest observation per source and declared capability at the last tick; infinite if that capability never reported",
            ),
            &["source", "capability"],
        )
        .unwrap();
        let source_errors = IntCounterVec::new(
            Opts::new(
                "sched_source_errors_total",
                "Failed reads reported by each source",
            ),
            &["source"],
        )
        .unwrap();
        let tick_duration = Histogram::with_opts(HistogramOpts::new(
            "sched_tick_duration_seconds",
            "Time to snapshot, plan, sequence and describe one tick",
        ))
        .unwrap();
        let config_generation = IntGauge::new(
            "sched_config_generation",
            "Generation of the active configuration",
        )
        .unwrap();
        let config_reloads = IntCounterVec::new(
            Opts::new(
                "sched_config_reloads_total",
                "Configuration reload attempts by outcome",
            ),
            &["outcome"],
        )
        .unwrap();
        let trigger_failures = IntCounter::new(
            "sched_tick_trigger_failures_total",
            "Failed attempts to ask Restate for a tick after a change",
        )
        .unwrap();
        let last_tick_timestamp = Gauge::new(
            "sched_last_tick_timestamp_seconds",
            "Unix time of the last tick",
        )
        .unwrap();
        let rejected = IntCounterVec::new(
            Opts::new(
                "sched_rejected_reports_total",
                "Reports and values dropped because the config has no place for them, by reason",
            ),
            &["reason"],
        )
        .unwrap();
        let has_observer = IntGaugeVec::new(
            Opts::new(
                "sched_source_has_observer",
                "1 if this build runs an observer for the source, 0 if it can only be fed by pushes (its demand then reads as not observable yet)",
            ),
            &["source"],
        )
        .unwrap();
        let phase_age = GaugeVec::new(
            Opts::new(
                "sched_resource_phase_age_seconds",
                "Seconds each resource has been in its current power phase (axis=power) and mode phase (axis=mode), at the last tick. Phases without a recorded start (off, idle) are measured from when this process first saw them",
            ),
            &["resource", "axis", "phase"],
        )
        .unwrap();
        registry.register(Box::new(phase_age.clone())).unwrap();
        let state_errors = IntCounterVec::new(
            Opts::new(
                "sched_persisted_state_errors_total",
                "Ticks (and lease requests) that could not read a persisted state key (undecodable or of an unknown version), by key; such a tick publishes nothing",
            ),
            &["key"],
        )
        .unwrap();
        let recovery_runs = IntCounterVec::new(
            Opts::new(
                "sched_recovery_runs_total",
                "Recovery action runs by class and outcome: ran, skipped_interval (min_interval has not passed), skipped_not_allowed (the class is not in the allowed set), not_wired (the class has no runnable action), list_failed, state_error",
            ),
            &["class", "outcome"],
        )
        .unwrap();
        let recovery_requeues = IntCounterVec::new(
            Opts::new(
                "sched_recovery_requeues_total",
                "Items the recovery action handled, by class and outcome: attempted (an attempt was recorded and the request sent), queued (the service accepted it), failed (the request failed or the answer said it was not queued), skipped_cap (an item listed in a run had used all its attempts; counted per item and run, so one stuck item adds one per run), skipped_not_allowed (the model was no longer allowed before the request), deferred (over max_per_run, left for a later run)",
            ),
            &["class", "outcome"],
        )
        .unwrap();
        let power_requests = IntCounterVec::new(
            Opts::new(
                "sched_power_requests_total",
                "Power actions by resource, action (on, off) and outcome: requested (the router or the host accepted the request; the host's own state follows from its reachability), failed (the request failed; it is never retried automatically), not_wired (the resource has no managed power actuator in the active config, so nothing was sent)",
            ),
            &["resource", "action", "outcome"],
        )
        .unwrap();
        registry.register(Box::new(power_requests.clone())).unwrap();
        registry.register(Box::new(recovery_runs.clone())).unwrap();
        registry
            .register(Box::new(recovery_requeues.clone()))
            .unwrap();
        registry.register(Box::new(state_errors.clone())).unwrap();
        registry.register(Box::new(has_observer.clone())).unwrap();
        registry.register(Box::new(rejected.clone())).unwrap();
        registry.register(Box::new(decisions.clone())).unwrap();
        registry
            .register(Box::new(observation_age.clone()))
            .unwrap();
        registry.register(Box::new(source_errors.clone())).unwrap();
        registry.register(Box::new(tick_duration.clone())).unwrap();
        registry
            .register(Box::new(config_generation.clone()))
            .unwrap();
        registry.register(Box::new(config_reloads.clone())).unwrap();
        registry
            .register(Box::new(trigger_failures.clone()))
            .unwrap();
        registry
            .register(Box::new(last_tick_timestamp.clone()))
            .unwrap();
        Metrics {
            registry,
            decisions,
            observation_age,
            source_errors,
            tick_duration,
            config_generation,
            config_reloads,
            trigger_failures,
            last_tick_timestamp,
            rejected,
            age_labels: Mutex::new(HashSet::new()),
            has_observer,
            observer_labels: Mutex::new(HashSet::new()),
            phase_age,
            state_errors,
            recovery_runs,
            recovery_requeues,
            power_requests,
            phases: Mutex::new(HashMap::new()),
        }
    }

    /// Sets how long each resource of `state` has been in its power and mode phase at `now`.
    /// A phase change replaces the resource's series (the old phase label is removed);
    /// resources no longer in the state lose theirs.
    pub fn set_phases(&self, now: Millis, state: &SchedState) {
        let mut phases = self.phases.lock().unwrap_or_else(|e| e.into_inner());
        let mut seen = HashSet::new();
        for (id, st) in &state.resources {
            for (axis, (phase, since)) in [
                ("power", power_phase(&st.power)),
                ("mode", mode_phase(&st.mode)),
            ] {
                let key = (id.to_string(), axis);
                let began = match (since, phases.get(&key)) {
                    (Some(t), _) => t,
                    (None, Some((p, t))) if *p == phase => *t,
                    (None, _) => now,
                };
                if let Some((old, _)) = phases.get(&key)
                    && *old != phase
                {
                    let _ = self
                        .phase_age
                        .remove_label_values(&[id.as_str(), axis, old]);
                }
                self.phase_age
                    .with_label_values(&[id.as_str(), axis, phase])
                    .set(now.saturating_sub(began).as_u64() as f64 / 1000.0);
                phases.insert(key.clone(), (phase, began));
                seen.insert(key);
            }
        }
        phases.retain(|key, (phase, _)| {
            let keep = seen.contains(key);
            if !keep {
                let _ = self
                    .phase_age
                    .remove_label_values(&[key.0.as_str(), key.1, phase]);
            }
            keep
        });
    }

    pub fn decision(&self, kind: &str) {
        self.decisions.with_label_values(&[kind]).inc();
    }

    /// Sets the per-source, per-capability ages and removes series that are no longer
    /// configured. Series are only ever set or removed, never reset first, so a scrape
    /// in between never sees them missing.
    pub fn set_input_ages(&self, ages: &[InputAge]) {
        let mut labels = self.age_labels.lock().unwrap_or_else(|e| e.into_inner());
        let mut now_set = HashSet::new();
        for a in ages {
            let cap = a.capability.map_or("none".to_string(), |c| c.to_string());
            let secs = a.age.map_or(f64::INFINITY, |Millis(ms)| ms as f64 / 1000.0);
            self.observation_age
                .with_label_values(&[a.source.as_str(), &cap])
                .set(secs);
            now_set.insert((a.source.to_string(), cap));
        }
        for (source, cap) in labels.difference(&now_set) {
            let _ = self.observation_age.remove_label_values(&[source, cap]);
        }
        *labels = now_set;
        let mut observers = self
            .observer_labels
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut sources = HashSet::new();
        for a in ages {
            self.has_observer
                .with_label_values(&[a.source.as_str()])
                .set(i64::from(a.observer == ObserverState::Running));
            sources.insert(a.source.to_string());
        }
        for gone in observers.difference(&sources) {
            let _ = self.has_observer.remove_label_values(&[gone]);
        }
        *observers = sources;
    }

    /// Counts a dropped report (`unknown_source`) or value (`unknown_subject`).
    pub fn rejected(&self, reason: &str, n: u64) {
        self.rejected.with_label_values(&[reason]).inc_by(n);
    }

    pub fn source_error(&self, source: &str) {
        self.source_errors.with_label_values(&[source]).inc();
    }

    pub fn tick_duration(&self, seconds: f64) {
        self.tick_duration.observe(seconds);
    }

    pub fn set_config_generation(&self, generation: u64) {
        self.config_generation
            .set(i64::try_from(generation).unwrap_or(i64::MAX));
    }

    pub fn config_reload(&self, outcome: &str) {
        self.config_reloads.with_label_values(&[outcome]).inc();
    }

    /// Counts a failed read of the persisted state `key` (a fixed set of keys).
    pub fn persisted_state_error(&self, key: &str) {
        self.state_errors.with_label_values(&[key]).inc();
    }

    /// Counts one run of a class's recovery action by outcome (see the metric's help).
    pub fn recovery_run(&self, class: &str, outcome: &str) {
        self.recovery_runs
            .with_label_values(&[class, outcome])
            .inc();
    }

    /// Counts `n` items of a class's recovery action by outcome (see the metric's help).
    pub fn recovery_requeue(&self, class: &str, outcome: &str, n: u64) {
        if n > 0 {
            self.recovery_requeues
                .with_label_values(&[class, outcome])
                .inc_by(n);
        }
    }

    /// Counts one power action by outcome (see the metric's help). `resource` must be a
    /// configured resource id.
    pub fn power_request(&self, resource: &str, action: &str, outcome: &str) {
        self.power_requests
            .with_label_values(&[resource, action, outcome])
            .inc();
    }

    pub fn trigger_failed(&self) {
        self.trigger_failures.inc();
    }

    pub fn tick_finished_at(&self, now: Millis) {
        self.last_tick_timestamp.set(now.as_u64() as f64 / 1000.0);
    }

    /// The text exposition format.
    pub fn render(&self) -> String {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .unwrap();
        String::from_utf8(buf).unwrap_or_default()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_every_metric_family_and_replaces_ages() {
        let m = Metrics::new();
        m.decision("wait");
        m.source_error("s1");
        m.tick_duration(0.002);
        m.set_config_generation(4);
        m.config_reload("rejected");
        m.set_input_ages(&[
            InputAge {
                source: "s1".into(),
                capability: Some(sched_model::Capability::Pods),
                observer: ObserverState::Running,
                age: Some(Millis(1500)),
                last_error: None,
            },
            InputAge {
                source: "s2".into(),
                capability: None,
                observer: ObserverState::Running,
                age: None,
                last_error: None,
            },
        ]);
        let text = m.render();
        for needle in [
            "sched_tick_decisions_total{kind=\"wait\"} 1",
            "sched_source_errors_total{source=\"s1\"} 1",
            "sched_config_generation 4",
            "sched_config_reloads_total{outcome=\"rejected\"} 1",
            "sched_observation_age_seconds{capability=\"pods\",source=\"s1\"} 1.5",
            "sched_observation_age_seconds{capability=\"none\",source=\"s2\"} inf",
            "sched_tick_duration_seconds_count 1",
        ] {
            assert!(text.contains(needle), "missing {needle}:\n{text}");
        }
        // A series that is still configured is never absent between updates.
        m.set_input_ages(&[InputAge {
            source: "s1".into(),
            capability: Some(sched_model::Capability::Pods),
            observer: ObserverState::Running,
            age: Some(Millis(2000)),
            last_error: None,
        }]);
        let text = m.render();
        assert!(
            text.contains("source=\"s1\"} 2") && !text.contains("source=\"s2\""),
            "{text}"
        );
        m.set_input_ages(&[]);
        assert!(!m.render().contains("sched_observation_age_seconds{"));
    }

    #[test]
    fn phase_ages_follow_the_state_and_a_phase_change_replaces_the_series() {
        use sched_model::{ResourceState, WindDown};
        let m = Metrics::new();
        let mut state = SchedState::default();
        let mut gpu = ResourceState::new(PowerPhase::On {
            since: Millis(1_000),
        });
        gpu.mode = ModePhase::Serving {
            model: "m1".into(),
            since: Millis(4_000),
            idle_since: None,
            wind_down: None,
        };
        state.resources.insert("gpu".into(), gpu.clone());
        state
            .resources
            .insert("nfs".into(), ResourceState::new(PowerPhase::Off));
        m.set_phases(Millis(10_000), &state);
        let text = m.render();
        for needle in [
            "sched_resource_phase_age_seconds{axis=\"power\",phase=\"on\",resource=\"gpu\"} 9",
            "sched_resource_phase_age_seconds{axis=\"mode\",phase=\"serving\",resource=\"gpu\"} 6",
            // No start recorded: measured from when it was first seen.
            "sched_resource_phase_age_seconds{axis=\"power\",phase=\"off\",resource=\"nfs\"} 0",
            "sched_resource_phase_age_seconds{axis=\"mode\",phase=\"idle\",resource=\"nfs\"} 0",
        ] {
            assert!(text.contains(needle), "missing {needle}:\n{text}");
        }
        // Later: still off (first seen kept), and the GPU now winds down.
        gpu.mode = ModePhase::Serving {
            model: "m1".into(),
            since: Millis(5_000),
            idle_since: None,
            wind_down: Some(WindDown {
                since: Millis(20_000),
                timeout: None,
                reclaim: false,
                killed: Default::default(),
            }),
        };
        state.resources.insert("gpu".into(), gpu);
        m.set_phases(Millis(25_000), &state);
        let text = m.render();
        assert!(
            text.contains("{axis=\"power\",phase=\"off\",resource=\"nfs\"} 15"),
            "{text}"
        );
        assert!(
            text.contains("{axis=\"mode\",phase=\"winding_down\",resource=\"gpu\"} 5"),
            "{text}"
        );
        assert!(!text.contains("phase=\"serving\""), "{text}");
        // A resource that leaves the state loses its series.
        state.resources.remove(&sched_model::ResourceId::new("nfs"));
        m.set_phases(Millis(26_000), &state);
        assert!(!m.render().contains("resource=\"nfs\""));
    }

    #[test]
    fn a_scrape_never_sees_the_age_series_missing_while_they_are_updated() {
        let m = std::sync::Arc::new(Metrics::new());
        let ages = vec![InputAge {
            source: "s1".into(),
            capability: Some(sched_model::Capability::Pods),
            observer: ObserverState::Running,
            age: Some(Millis(1000)),
            last_error: None,
        }];
        m.set_input_ages(&ages);
        let writer = {
            let (m, ages) = (m.clone(), ages.clone());
            std::thread::spawn(move || {
                for _ in 0..20_000 {
                    m.set_input_ages(&ages);
                }
            })
        };
        for _ in 0..5_000 {
            assert!(
                m.render()
                    .contains("sched_observation_age_seconds{capability=\"pods\",source=\"s1\"}"),
                "series missing during an update"
            );
        }
        writer.join().unwrap();
    }
}
