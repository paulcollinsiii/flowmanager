//! Prometheus metrics. Every label comes from a fixed set (decision kinds, reload
//! outcomes) or from configuration (source ids), so cardinality is bounded.

use prometheus::{
    Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts,
    Registry, TextEncoder,
};
use sched_model::Millis;

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
}

impl Metrics {
    pub fn new() -> Metrics {
        let registry = Registry::new();
        let decisions = IntCounterVec::new(
            Opts::new("sched_decisions_total", "Ticks by decision kind, once per distinct kind in a tick (shadow mode: logged, never executed)"),
            &["kind"],
        )
        .unwrap();
        let observation_age = GaugeVec::new(
            Opts::new(
                "sched_observation_age_seconds",
                "Age of the newest observation per source at the last tick; infinite if it never reported",
            ),
            &["source"],
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
        }
    }

    pub fn decision(&self, kind: &str) {
        self.decisions.with_label_values(&[kind]).inc();
    }

    /// Replaces the per-source ages, so a source removed from the config disappears.
    pub fn set_input_ages(&self, ages: &[InputAge]) {
        self.observation_age.reset();
        for a in ages {
            let secs = a.age.map_or(f64::INFINITY, |Millis(ms)| ms as f64 / 1000.0);
            self.observation_age
                .with_label_values(&[a.source.as_str()])
                .set(secs);
        }
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
                age: Some(Millis(1500)),
                last_error: None,
            },
            InputAge {
                source: "s2".into(),
                age: None,
                last_error: None,
            },
        ]);
        let text = m.render();
        for needle in [
            "sched_decisions_total{kind=\"wait\"} 1",
            "sched_source_errors_total{source=\"s1\"} 1",
            "sched_config_generation 4",
            "sched_config_reloads_total{outcome=\"rejected\"} 1",
            "sched_observation_age_seconds{source=\"s1\"} 1.5",
            "sched_observation_age_seconds{source=\"s2\"} inf",
            "sched_tick_duration_seconds_count 1",
        ] {
            assert!(text.contains(needle), "missing {needle}:\n{text}");
        }
        m.set_input_ages(&[]);
        assert!(!m.render().contains("source=\"s1\"} 1.5"));
    }
}
