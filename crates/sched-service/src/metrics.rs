//! Prometheus metrics. Every label comes from a fixed set (decision kinds, reload
//! outcomes) or from configuration (source ids), so cardinality is bounded.

use std::collections::HashSet;
use std::sync::Mutex;

use prometheus::{
    Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
};
use sched_model::Millis;

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
}

impl Metrics {
    pub fn new() -> Metrics {
        let registry = Registry::new();
        let decisions = IntCounterVec::new(
            Opts::new("sched_tick_decisions_total", "Ticks by decision kind, counted once per distinct kind in each tick (per tick, not per executed action: shadow mode executes nothing and each tick starts from a fresh sequencer state)"),
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
        }
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
