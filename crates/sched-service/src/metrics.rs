//! Prometheus metrics. Every label comes from a fixed set (decision kinds, reload
//! outcomes) or from configuration (source ids), so cardinality is bounded.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use prometheus::{
    Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
};
use sched_model::{
    Millis, ModePhase, MountsVeto, PowerPhase, PowerTransition, ResourceId, SchedState,
};

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
    power_timeouts: IntCounterVec,
    power_failed: GaugeVec,
    /// Resources with `power_failed` series, to drop those that leave the state.
    power_failed_resources: Mutex<HashSet<String>>,
    mounts_timeouts: IntCounterVec,
    mounts_veto: GaugeVec,
    min_off_remaining: GaugeVec,
    toggle_requests: IntCounterVec,
    webhook_requests: IntCounterVec,
    webhook_nudges: IntCounter,
    manual_lease_age: GaugeVec,
    /// (kind, target) pairs with `manual_lease_age` series, to drop released leases.
    manual_lease_labels: Mutex<HashSet<(String, String)>>,
    /// Resources with `min_off_remaining` series, to drop those that leave the set.
    min_off_resources: Mutex<HashSet<String>>,
    /// Resources with `mounts_veto` series, to drop those that leave the checked set.
    mounts_veto_resources: Mutex<HashSet<String>>,
    job_events: IntCounterVec,
    job_running: IntGaugeVec,
    job_last_success: GaugeVec,
    job_next_due: GaugeVec,
    /// Jobs with series, to drop those that leave the config.
    job_labels: Mutex<HashSet<String>>,
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
        PowerPhase::Failed { since, .. } => ("failed", Some(*since)),
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
        let power_timeouts = IntCounterVec::new(
            Opts::new(
                "sched_power_timeouts_total",
                "Power transitions that outlasted their configured timeout, by resource and the transition (powering_on, shutting_down). Each one put the resource in the failed state and raised the power_timeout alert; the scheduler never retries",
            ),
            &["resource", "during"],
        )
        .unwrap();
        let power_failed = GaugeVec::new(
            Opts::new(
                "sched_resource_power_failed",
                "1 while the resource is in the failed power state (its power-on or shutdown timed out; the scheduler has stopped and a person must act), else 0, by resource and the transition that timed out. Alert on this being 1",
            ),
            &["resource", "during"],
        )
        .unwrap();
        registry.register(Box::new(power_requests.clone())).unwrap();
        registry.register(Box::new(power_timeouts.clone())).unwrap();
        registry.register(Box::new(power_failed.clone())).unwrap();
        let mounts_timeouts = IntCounterVec::new(
            Opts::new(
                "sched_mounts_timeouts_total",
                "Shutdowns that waited longer than the resource's mounts_timeout for pods to stop mounting its NFS server, by resource. Each one raised the shutdown_mounts_timeout alert and the scheduler stopped: the host stays up and is not shut down by flowmanager again until a person acts or a new manual request takes it over",
            ),
            &["resource"],
        )
        .unwrap();
        let mounts_veto = GaugeVec::new(
            Opts::new(
                "sched_resource_mounts_veto",
                "1 while a shutdown of the resource is held back because pods mount its NFS server or the count is unknown (state waiting, bounded by mounts_timeout), or because that wait timed out and flowmanager stopped (state stopped; a person must act), else 0, by resource and state. Only resources whose mounts are checked have a series. Alert on stopped being 1",
            ),
            &["resource", "state"],
        )
        .unwrap();
        let job_events = IntCounterVec::new(
            Opts::new(
                "sched_job_events_total",
                "Scheduled-job events by job and event: started (a Job was created), succeeded, failed (after its retries, past its deadline or deleted; an alert), retried, missed (a run that never started, or runs dropped during downtime; an alert), skipped_overlap (a run came due or was asked for while the previous instance was pending or running; an alert), caught_up (a late run started), start_late (a catch_up run still waiting for its needs after its start window; an alert), create_failed (the Job could not be created; an alert), triggered (a person started a run)",
            ),
            &["scheduled_job", "event"],
        )
        .unwrap();
        let job_running = IntGaugeVec::new(
            Opts::new(
                "sched_job_running",
                "1 while the job has a run in progress (waiting for its needs, creating, running or in retry backoff), else 0, by configured job",
            ),
            &["scheduled_job"],
        )
        .unwrap();
        let job_last_success = GaugeVec::new(
            Opts::new(
                "sched_job_last_success_timestamp_seconds",
                "Unix time the job last succeeded, by configured job; absent until it has. Alert on time() minus this exceeding the schedule interval plus slack",
            ),
            &["scheduled_job"],
        )
        .unwrap();
        let job_next_due = GaugeVec::new(
            Opts::new(
                "sched_job_next_due_timestamp_seconds",
                "Unix time of the job's next scheduled run, by configured job",
            ),
            &["scheduled_job"],
        )
        .unwrap();
        registry.register(Box::new(job_events.clone())).unwrap();
        registry.register(Box::new(job_running.clone())).unwrap();
        registry
            .register(Box::new(job_last_success.clone()))
            .unwrap();
        registry.register(Box::new(job_next_due.clone())).unwrap();
        registry
            .register(Box::new(mounts_timeouts.clone()))
            .unwrap();
        registry.register(Box::new(mounts_veto.clone())).unwrap();
        let toggle_requests = IntCounterVec::new(
            Opts::new(
                "sched_toggle_requests_total",
                "Requests to the toggle listener (Home Assistant) by endpoint (a fixed set: list, model_get, model_on, model_off, model_set, resource_get, resource_on, resource_off, resource_set, resource_resume, reclaim_get, reclaim_on, reclaim_off, reclaim_set, unmatched) and outcome (ok, unauthorized, bad_request, not_found, method_not_allowed, conflict, error); never labelled with a model or resource name",
            ),
            &["endpoint", "outcome"],
        )
        .unwrap();
        registry
            .register(Box::new(toggle_requests.clone()))
            .unwrap();
        let webhook_requests = IntCounterVec::new(
            Opts::new(
                "sched_webhook_requests_total",
                "Requests to the webhook listener by outcome (a fixed set: accepted, unauthorized, not_found, method_not_allowed, payload_too_large, bad_request, unavailable); never labelled with a path or a source",
            ),
            &["outcome"],
        )
        .unwrap();
        registry
            .register(Box::new(webhook_requests.clone()))
            .unwrap();
        let webhook_nudges = IntCounter::new(
            "sched_webhook_nudges_total",
            "Sources asked to poll now by accepted webhooks (one request counts once per source it maps to). Nudges are merged and rate-limited by each observer, so this can exceed the polls they caused",
        )
        .unwrap();
        registry.register(Box::new(webhook_nudges.clone())).unwrap();
        let manual_lease_age = GaugeVec::new(
            Opts::new(
                "sched_manual_lease_age_seconds",
                "Seconds since each manual lease or reclaim was granted, as of the last tick: kind is model (target = the model), host (the manual host lease) or reclaim (target = the resource). An alert on this finds a lease that was forgotten",
            ),
            &["kind", "target"],
        )
        .unwrap();
        registry
            .register(Box::new(manual_lease_age.clone()))
            .unwrap();
        let min_off_remaining = GaugeVec::new(
            Opts::new(
                "sched_resource_min_off_remaining_seconds",
                "Seconds left of the resource's min_off, the minimum time flowmanager keeps a host off after a shutdown it started before it powers the host on again by itself; 0 when it is not in that window (the host is up, was found down, or the time has passed). A want that arrives while this is above 0 waits (wait reason min_off); a manual lease does not. Only managed resources with min_off above zero have a series",
            ),
            &["resource"],
        )
        .unwrap();
        registry
            .register(Box::new(min_off_remaining.clone()))
            .unwrap();
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
            power_timeouts,
            power_failed,
            power_failed_resources: Mutex::new(HashSet::new()),
            mounts_timeouts,
            mounts_veto,
            min_off_remaining,
            toggle_requests,
            webhook_requests,
            webhook_nudges,
            manual_lease_age,
            manual_lease_labels: Mutex::new(HashSet::new()),
            min_off_resources: Mutex::new(HashSet::new()),
            mounts_veto_resources: Mutex::new(HashSet::new()),
            phases: Mutex::new(HashMap::new()),
            job_events,
            job_running,
            job_last_success,
            job_next_due,
            job_labels: Mutex::new(HashSet::new()),
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

    /// Counts one scheduled-job event (`event` is `JobEventKind::counter`). Labels are the
    /// configured job ids and a fixed set of events.
    pub fn job_event(&self, job: &str, event: &str) {
        self.job_events.with_label_values(&[job, event]).inc();
    }

    /// Sets the per-job gauges from the summaries; jobs that left the config lose their series.
    pub fn set_jobs(&self, jobs: &[crate::jobs::JobSummary]) {
        let mut known = self.job_labels.lock().unwrap_or_else(|e| e.into_inner());
        let now: HashSet<String> = jobs.iter().map(|j| j.id.to_string()).collect();
        for gone in known.difference(&now) {
            let _ = self.job_running.remove_label_values(&[gone.as_str()]);
            let _ = self.job_last_success.remove_label_values(&[gone.as_str()]);
            let _ = self.job_next_due.remove_label_values(&[gone.as_str()]);
        }
        for j in jobs {
            let id = j.id.as_str();
            self.job_running
                .with_label_values(&[id])
                .set(i64::from(j.run.is_some()));
            match j.last_success_at {
                Some(t) => self
                    .job_last_success
                    .with_label_values(&[id])
                    .set(t.as_u64() as f64 / 1000.0),
                None => {
                    let _ = self.job_last_success.remove_label_values(&[id]);
                }
            }
            match j.next_due {
                Some(t) => self
                    .job_next_due
                    .with_label_values(&[id])
                    .set(t.as_u64() as f64 / 1000.0),
                None => {
                    let _ = self.job_next_due.remove_label_values(&[id]);
                }
            }
        }
        *known = now;
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

    /// Counts a power transition that timed out (see the metric's help).
    pub fn power_timeout(&self, resource: &str, during: &str) {
        self.power_timeouts
            .with_label_values(&[resource, during])
            .inc();
    }

    /// Sets `sched_resource_power_failed` from the state: 1 for the transition a resource
    /// is failed in, 0 for the other; resources that left the state lose their series. Only
    /// resources for which `can_fail` holds (those with a power section) get a series.
    pub fn set_power_failed(&self, state: &SchedState, can_fail: impl Fn(&ResourceId) -> bool) {
        let mut known = self
            .power_failed_resources
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for (id, st) in state.resources.iter().filter(|(id, _)| can_fail(id)) {
            for t in [PowerTransition::PoweringOn, PowerTransition::ShuttingDown] {
                let failed = matches!(st.power, PowerPhase::Failed { during, .. } if during == t);
                self.power_failed
                    .with_label_values(&[id.as_str(), t.as_str()])
                    .set(if failed { 1.0 } else { 0.0 });
            }
        }
        known.retain(|id| {
            let keep = state
                .resources
                .keys()
                .any(|k| k.as_str() == id && can_fail(k));
            if !keep {
                for t in [PowerTransition::PoweringOn, PowerTransition::ShuttingDown] {
                    let _ = self
                        .power_failed
                        .remove_label_values(&[id.as_str(), t.as_str()]);
                }
            }
            keep
        });
        known.extend(
            state
                .resources
                .keys()
                .filter(|k| can_fail(k))
                .map(|k| k.to_string()),
        );
    }

    /// Counts a shutdown whose wait for unmounting pods timed out (see the metric's help).
    pub fn mounts_timeout(&self, resource: &str) {
        self.mounts_timeouts.with_label_values(&[resource]).inc();
    }

    /// Sets `sched_resource_mounts_veto` from the state: per resource for which `checked`
    /// holds (its mounts are checked), 1 for the state it is in (waiting or stopped) and 0
    /// for the other; resources that are no longer checked lose their series.
    pub fn set_mounts_veto(&self, state: &SchedState, checked: impl Fn(&ResourceId) -> bool) {
        let mut known = self
            .mounts_veto_resources
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for (id, st) in state.resources.iter().filter(|(id, _)| checked(id)) {
            let waiting = matches!(st.mounts_veto, Some(MountsVeto::Waiting { .. }));
            let stopped = matches!(st.mounts_veto, Some(MountsVeto::Stopped { .. }));
            for (name, on) in [("waiting", waiting), ("stopped", stopped)] {
                self.mounts_veto
                    .with_label_values(&[id.as_str(), name])
                    .set(if on { 1.0 } else { 0.0 });
            }
        }
        known.retain(|id| {
            let keep = state
                .resources
                .keys()
                .any(|k| k.as_str() == id && checked(k));
            if !keep {
                for name in ["waiting", "stopped"] {
                    let _ = self.mounts_veto.remove_label_values(&[id.as_str(), name]);
                }
            }
            keep
        });
        known.extend(
            state
                .resources
                .keys()
                .filter(|k| checked(k))
                .map(|k| k.to_string()),
        );
    }

    /// Sets `sched_resource_min_off_remaining_seconds` from the state at `now`: per resource
    /// for which `min_off` gives a minimum (managed, above zero), the seconds left of it;
    /// resources without one lose their series.
    pub fn set_min_off_remaining(
        &self,
        state: &SchedState,
        now: Millis,
        min_off: impl Fn(&ResourceId) -> Option<Millis>,
    ) {
        let mut known = self
            .min_off_resources
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut now_known = HashSet::new();
        for (id, st) in &state.resources {
            let Some(m) = min_off(id) else { continue };
            let left = st.min_off_remaining(m, now);
            self.min_off_remaining
                .with_label_values(&[id.as_str()])
                .set(left.as_u64() as f64 / 1000.0);
            now_known.insert(id.to_string());
        }
        for gone in known.difference(&now_known) {
            let _ = self.min_off_remaining.remove_label_values(&[gone.as_str()]);
        }
        *known = now_known;
    }

    /// Counts one toggle-listener request; `endpoint` and `outcome` are from the fixed sets
    /// in the metric's help.
    pub fn toggle_request(&self, endpoint: &str, outcome: &str) {
        self.toggle_requests
            .with_label_values(&[endpoint, outcome])
            .inc();
    }

    /// Counts one webhook-listener request; `outcome` is from the fixed set in the
    /// metric's help.
    pub fn webhook_request(&self, outcome: &str) {
        self.webhook_requests.with_label_values(&[outcome]).inc();
    }

    /// Counts sources nudged by an accepted webhook.
    pub fn webhook_nudged(&self, sources: u64) {
        self.webhook_nudges.inc_by(sources);
    }

    /// Sets `sched_manual_lease_age_seconds` from the leases the tick gave the core: one
    /// series per manual model lease (target the model) and manual host lease (target the
    /// resource); a released lease loses its series.
    pub fn set_manual_lease_ages(&self, now: Millis, leases: &[sched_model::Lease]) {
        use sched_model::LeaseKind;
        let mut known = self
            .manual_lease_labels
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut now_known = HashSet::new();
        for l in leases {
            let (kind, target) = match &l.kind {
                LeaseKind::ManualModel { model } => ("model", model.to_string()),
                LeaseKind::ManualHost => ("host", l.resource.to_string()),
                LeaseKind::Reclaim => ("reclaim", l.resource.to_string()),
                _ => continue,
            };
            let age = now.as_u64().saturating_sub(l.granted_at.as_u64()) as f64 / 1000.0;
            self.manual_lease_age
                .with_label_values(&[kind, &target])
                .set(age);
            now_known.insert((kind.to_string(), target));
        }
        for (kind, target) in known.difference(&now_known) {
            let _ = self
                .manual_lease_age
                .remove_label_values(&[kind.as_str(), target.as_str()]);
        }
        *known = now_known;
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
    fn manual_lease_ages_count_up_and_a_released_lease_loses_its_series() {
        use sched_model::{Lease, LeaseKind};
        let m = Metrics::new();
        let lease = |id: &str, resource: &str, kind: LeaseKind| Lease {
            id: id.into(),
            resource: resource.into(),
            kind,
            granted_at: Millis(1_000),
        };
        let leases = vec![
            lease(
                "a",
                "gpu",
                LeaseKind::ManualModel {
                    model: "llm".into(),
                },
            ),
            lease("b", "nfs", LeaseKind::ManualHost),
            lease("c", "gpu", LeaseKind::Reclaim),
        ];
        m.set_manual_lease_ages(Millis(61_000), &leases);
        let text = m.render();
        assert!(
            text.contains("sched_manual_lease_age_seconds{kind=\"model\",target=\"llm\"} 60"),
            "{text}"
        );
        assert!(
            text.contains("sched_manual_lease_age_seconds{kind=\"host\",target=\"nfs\"} 60"),
            "{text}"
        );
        assert!(
            text.contains("sched_manual_lease_age_seconds{kind=\"reclaim\",target=\"gpu\"} 60"),
            "{text}"
        );
        m.set_manual_lease_ages(Millis(62_000), &leases[1..]);
        let text = m.render();
        assert!(!text.contains("target=\"llm\""), "{text}");
        assert!(text.contains("target=\"nfs\"} 61"), "{text}");
        m.toggle_request("model_on", "ok");
        assert!(
            m.render()
                .contains("sched_toggle_requests_total{endpoint=\"model_on\",outcome=\"ok\"} 1")
        );
    }

    #[test]
    fn the_min_off_gauge_counts_down_to_zero_and_drops_resources_without_a_minimum() {
        use sched_model::{PowerPhase, ResourceState};
        let m = Metrics::new();
        let mut off = ResourceState::new(PowerPhase::Off);
        off.shutdown_confirmed_at = Some(Millis(1_000));
        let mut s = SchedState::default();
        s.resources.insert("a".into(), off);
        s.resources
            .insert("b".into(), ResourceState::new(PowerPhase::Off));
        let min = |id: &ResourceId| (id.as_str() != "c").then_some(Millis(61_000));
        m.set_min_off_remaining(&s, Millis(11_000), min);
        let text = m.render();
        assert!(
            text.contains("sched_resource_min_off_remaining_seconds{resource=\"a\"} 51"),
            "{text}"
        );
        assert!(
            text.contains("sched_resource_min_off_remaining_seconds{resource=\"b\"} 0"),
            "{text}"
        );
        m.set_min_off_remaining(&s, Millis(70_000), min);
        assert!(
            m.render()
                .contains("sched_resource_min_off_remaining_seconds{resource=\"a\"} 0")
        );
        // Without a minimum (disabled, or no longer managed) the series goes away.
        m.set_min_off_remaining(&s, Millis(70_000), |_| None);
        assert!(
            !m.render()
                .contains("sched_resource_min_off_remaining_seconds{")
        );
    }

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

    #[test]
    fn the_power_failed_gauge_covers_only_resources_that_can_fail() {
        let m = Metrics::new();
        let mut st = SchedState::default();
        for id in ["host", "plain"] {
            st.resources.insert(
                ResourceId::new(id),
                sched_model::ResourceState::new(PowerPhase::On { since: Millis(0) }),
            );
        }
        m.set_power_failed(&st, |id| id.as_str() == "host");
        let text = m.render();
        let lines = |r: &str| {
            text.lines()
                .filter(|l| {
                    l.starts_with("sched_resource_power_failed{")
                        && l.contains(&format!("resource=\"{r}\""))
                })
                .count()
        };
        assert_eq!(lines("host"), 2, "{text}");
        assert_eq!(lines("plain"), 0, "{text}");
    }
}
