//! The shared scheduler context and one tick.
//!
//! A tick reads the cached snapshot and the scheduler's durable state ([`Durable`]: the
//! sequencer state, the leases, and which sources have ever reported), runs the core
//! (planner, then sequencer) and returns a [`TickOutcome`]: the decision and the state to
//! persist. The `Scheduler` object computes it inside a journaled step, stores the new
//! state, and only then calls [`Shared::publish`], which makes the decision visible: the
//! allowed set on the channel the KEDA scaler reads, logs and metrics.
//!
//! What is carried out: the allowed set is published, so allowing a model and withdrawing
//! it take effect once the scaler converges on it. A paused class leaves the set too, but
//! nothing scales on a demand class (consumers are not gated for GPU contention), so
//! `PauseIntake` stops nobody. `KillInFlight` has no actuator either: the work is cut off
//! because the same tick withdraws its model. These two are listed in `not_executed` and
//! logged as such. Power on and off are carried out, in the resource's `Power` object (see
//! `power`), and so is a recovery action, but not here: after publishing, the `tick` handler
//! sends the resource's `Power` or the class's `Recovery` object a one-way request (see
//! `recovery`), so a slow or failing service can never delay a tick.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use sched_model::{
    AllowedSet, Capability, ClassId, CreateJob, JobBook, JobEvent, Lease, LeaseId, Millis, ModelId,
    PowerPhase, PowerTransition, PublishedAllowed, ResourceId, SchedState, SourceId, Step,
    class_work,
};
use sched_observe::{ObserverState, Report, observer_state};
use sched_sequencer::status::{
    self, Decided, HostEvent, HostStatus, blockers_label, readings_label,
};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::cache::{Ingest, InputAge, ObservationCache};
use crate::clock::Clock;
use crate::config_source::ConfigHolder;
use crate::core::decide;
use crate::leases;
use crate::metrics::Metrics;
use crate::power::PowerAction;
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
    /// Whether something observes the source: `running`, `no_params` (an observable kind
    /// configured without params) or `no_observer` (no observer for the kind).
    #[serde(default)]
    pub observer: String,
}

fn observer_label(o: ObserverState) -> &'static str {
    match o {
        ObserverState::Running => "running",
        ObserverState::NoParams => "no_params",
        ObserverState::NoImplementation => "no_observer",
    }
}

impl From<&InputAge> for InputReport {
    fn from(a: &InputAge) -> Self {
        InputReport {
            source: a.source.to_string(),
            capability: a.capability.map(|c| c.to_string()),
            age_ms: a.age.map(Millis::as_u64),
            last_error: a.last_error.clone(),
            observer: observer_label(a.observer).to_string(),
        }
    }
}

impl InputReport {
    /// Back to the form metrics and log lines are made from.
    pub fn to_input_age(&self) -> InputAge {
        InputAge {
            source: self.source.as_str().into(),
            capability: match self.capability.as_deref() {
                Some("pods") => Some(Capability::Pods),
                Some("reachability") => Some(Capability::Reachability),
                Some("demand") => Some(Capability::Demand),
                Some("switch") => Some(Capability::Switch),
                _ => None,
            },
            observer: match self.observer.as_str() {
                "no_params" => ObserverState::NoParams,
                "no_observer" => ObserverState::NoImplementation,
                _ => ObserverState::Running,
            },
            age: self.age_ms.map(Millis),
            last_error: self.last_error.clone(),
        }
    }
}

/// A power request whose call failed, as the `Power` object reported it to this process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PowerRequestFailure {
    pub resource: ResourceId,
    pub action: PowerAction,
    pub at: Millis,
    pub error: String,
}
/// One class's queue counts in a [`TickReport`], as the planner read them in this tick.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueReport {
    pub class: ClassId,
    /// Items whose upstream steps are done. Zero when `known` is false.
    pub ready: u64,
    pub in_flight: u64,
    /// Items waiting for the class's recovery action to re-queue them.
    pub failed: u64,
    /// How long the oldest ready item has been ready; absent if nothing is ready.
    pub oldest_ready_age_ms: Option<u64>,
    /// False when the reading is missing or stale (the counts are then zero, not measured).
    pub known: bool,
    /// The reading is missing or stale but a host the class requires is down: no work is
    /// possible, so `known` stays true.
    #[serde(default)]
    pub offline: bool,
}

/// What a tick decided, for logs and for the `last_decision` handler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickReport {
    pub config_generation: u64,
    pub at: Millis,
    /// Classes whose demand is unknown although something should be reporting it: an
    /// observer runs for the source, or the source has reported before (in this process or
    /// an earlier one), and it is silent or stale with no host the class requires seen down
    /// (that is `offline_demand`). A real problem. Unknown is not zero: such a tick is never
    /// `idle`.
    pub unknown_demand: Vec<ClassId>,
    /// Classes with no usable reading while a host they require is seen down: their work
    /// cannot run and their source is expected to be silent. Counted as no demand.
    #[serde(default)]
    pub offline_demand: Vec<ClassId>,
    /// Classes whose demand cannot be observed because this build has no observer for their
    /// source kind (schedule, session, ...) and the source has never reported. Nothing can
    /// push to it either (`observe` is ingress-private), so such a class counts as having
    /// no demand: idle, not busy.
    pub unsupported_demand: Vec<ClassId>,
    /// Classes whose source is of an observable kind but configured without `params`, and
    /// has never reported: nothing polls it. A config gap; counted as no demand, like
    /// `unsupported_demand`.
    pub unpolled_demand: Vec<ClassId>,
    /// Exclusive resources where the intent is forced: a manual model lease (or a lockout)
    /// cuts batch work short, so intake is paused and in-flight work gets a timeout instead
    /// of being waited for.
    #[serde(default)]
    pub forced: Vec<ResourceId>,
    /// Distinct decision kinds (see `reason::step_kind`), `idle` for none, `error` on failure.
    pub kinds: Vec<String>,
    pub reason: String,
    pub steps: Vec<Step>,
    /// The steps nothing in this build carries out (stopping in-flight work, pausing a
    /// class's intake). `PowerOn`, `PowerOff` and `RunRecovery` are carried out by the
    /// `Power` and `Recovery` objects. The sequencer has recorded them as taken all the same.
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
    /// The status of every resource after this tick (see `sched_sequencer::status`): the one
    /// derived answer to what each host is doing, which the reason, the metrics, the logs
    /// and the toggle listener read. Empty when the tick failed.
    #[serde(default)]
    pub hosts: Vec<HostStatus>,
    /// What became true in this very tick and is alerted once: a power transition or a veto
    /// wait timing out, a host adopted, an adoption ended. Empty when the tick failed. A
    /// replayed tick carries the same events and does not alert or count them again.
    #[serde(default)]
    pub events: Vec<HostEvent>,
    /// Power requests that failed (the call to the router or the host), for resources still
    /// powering on, shutting down or failed. A failed request is not retried; the host is
    /// followed from its reachability and the transition times out.
    #[serde(default)]
    pub power_request_failures: Vec<PowerRequestFailure>,
    /// What the scheduled jobs did in this tick (alerts among them).
    #[serde(default)]
    pub job_events: Vec<JobEvent>,
    /// Every configured job and its state after this tick.
    #[serde(default)]
    pub jobs: Vec<crate::jobs::JobSummary>,
    pub inputs: Vec<InputReport>,
    /// The queue counts of every class that has a demand source.
    #[serde(default)]
    pub queues: Vec<QueueReport>,
    /// Whether observations changed since the previous tick of this process.
    pub input_changed: bool,
    pub error: Option<String>,
}

impl TickReport {
    /// The status of `resource` after this tick, if the tick decided and the resource exists.
    pub fn host(&self, resource: &ResourceId) -> Option<&HostStatus> {
        self.hosts.iter().find(|h| &h.resource == resource)
    }
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
    /// The scheduled jobs' state.
    #[serde(default)]
    pub jobs: JobBook,
    /// The stored job book could not be read: the job step is skipped (jobs and their holds
    /// stay as they are, the job book is not written) while scheduling the GPU and NFS goes on.
    /// Adoption still runs and the lease book is still stored, since adoption changes it.
    #[serde(default)]
    pub jobs_off: bool,
}

impl Durable {
    /// Takes in what a tick says to persist.
    pub fn apply(&mut self, outcome: &TickOutcome) {
        self.state = outcome.report.state.clone();
        self.reported = outcome.reported.clone();
        self.leases = outcome.leases.clone();
        if !self.jobs_off {
            self.jobs = outcome.jobs.clone();
        }
    }
}

/// One tick's outcome: the decision and what to persist. The `Scheduler` object journals
/// it whole, so a retried invocation reuses it instead of deciding again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickOutcome {
    pub report: TickReport,
    /// Sources that have ever reported, to persist (configured sources only).
    pub reported: BTreeSet<SourceId>,
    /// The scheduled jobs' state after this tick, to persist.
    pub jobs: JobBook,
    /// The lease book after this tick: the stored leases with the jobs' holds reconciled.
    pub leases: Vec<Lease>,
    /// Jobs to create now (the object does it, journaled, after storing this outcome).
    pub creates: Vec<CreateJob>,
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
    /// The latest tick report this process published (decided or failed); what
    /// `last_decision` returns. Kept in memory so the report is journaled once per tick.
    last_report: Mutex<Option<TickReport>>,
    /// The latest failed power request per resource, as the `Power` object reported it. In
    /// memory: the durable record is the `Power` object's `last_action`.
    power_failures: Mutex<BTreeMap<ResourceId, PowerRequestFailure>>,
    /// Creates the Jobs of scheduled jobs; `None` until the binary sets one (jobs then fail to
    /// start, visibly, rather than being skipped).
    job_backend: std::sync::RwLock<Option<Arc<dyn sched_actuate::JobBackend>>>,
    /// Test seam: makes the core refuse to decide, which cannot otherwise happen here.
    #[cfg(test)]
    refuse_to_decide: std::sync::atomic::AtomicBool,
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
            last_report: Mutex::new(None),
            power_failures: Mutex::new(BTreeMap::new()),
            job_backend: std::sync::RwLock::new(None),
            #[cfg(test)]
            refuse_to_decide: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Sets the backend that creates scheduled jobs' Jobs.
    pub fn set_job_backend(&self, backend: Arc<dyn sched_actuate::JobBackend>) {
        *self.job_backend.write().unwrap_or_else(|e| e.into_inner()) = Some(backend);
    }

    /// The backend that creates scheduled jobs' Jobs, if one was set.
    pub fn job_backend(&self) -> Option<Arc<dyn sched_actuate::JobBackend>> {
        self.job_backend
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// The latest tick report this process published, if any.
    pub fn last_report(&self) -> Option<TickReport> {
        self.last_report
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Records the outcome of a power request for the next tick's report: a failure is kept
    /// (it makes the transition's report say so), a success clears the resource's entry.
    pub fn note_power_request(
        &self,
        resource: &ResourceId,
        action: PowerAction,
        at: Millis,
        error: Option<&str>,
    ) {
        let mut m = self
            .power_failures
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        match error {
            Some(e) => {
                m.insert(
                    resource.clone(),
                    PowerRequestFailure {
                        resource: resource.clone(),
                        action,
                        at,
                        error: e.to_string(),
                    },
                );
            }
            None => {
                m.remove(resource);
            }
        }
    }

    /// The failed power requests that still matter for `state`: one that belongs to the
    /// transition the resource is in now (the same action, and made at or after the current
    /// transition began; a failed resource has no start to compare, so only the action). An
    /// entry of another action or an earlier cycle is dropped, so a line never shows against
    /// a different transition than the one that produced it.
    fn power_request_failures(&self, state: &SchedState) -> Vec<PowerRequestFailure> {
        let mut m = self
            .power_failures
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        m.retain(|id, f| {
            state.resource(id).is_some_and(|st| match st.power {
                PowerPhase::PoweringOn { since } => f.action == PowerAction::On && f.at >= since,
                PowerPhase::ShuttingDown { since } => f.action == PowerAction::Off && f.at >= since,
                PowerPhase::Failed { during, .. } => {
                    f.action
                        == match during {
                            PowerTransition::PoweringOn => PowerAction::On,
                            PowerTransition::ShuttingDown => PowerAction::Off,
                        }
                }
                _ => false,
            })
        });
        m.values().cloned().collect()
    }

    /// Remembers in the cache which sources reported before, as the durable state says.
    /// Idempotent; done before the journaled decision so a replay does it too.
    pub fn seed_reported(&self, reported: &BTreeSet<SourceId>) {
        let config = self.config.current();
        self.cache().seed_reported(&config, reported);
    }

    /// A tick that cannot decide because the persisted state could not be read: reported,
    /// logged at error level and counted per key, and nothing is published, so the scaler
    /// keeps what it had. `problems` are `(key, problem)`.
    pub fn state_error(&self, now: Millis, problems: &[(String, String)]) -> TickReport {
        let text = problems
            .iter()
            .map(|(k, p)| format!("{k}: {p}"))
            .collect::<Vec<_>>()
            .join("; ");
        for (key, _) in problems {
            self.metrics.persisted_state_error(key);
        }
        let report = TickReport {
            config_generation: self.config.current().generation(),
            at: now,
            unknown_demand: vec![],
            offline_demand: vec![],
            unsupported_demand: vec![],
            unpolled_demand: vec![],
            forced: vec![],
            kinds: vec![reason::KIND_ERROR.to_string()],
            reason: format!(
                "no decision: the persisted state cannot be read ({text}); nothing is published and the stored state is left as it is (see docs/operations.md, 'Reading and changing state')"
            ),
            steps: vec![],
            not_executed: vec![],
            allowed_models: vec![],
            allowed_classes: vec![],
            leases: vec![],
            ignored_leases: vec![],
            state: SchedState::default(),
            hosts: vec![],
            events: vec![],
            power_request_failures: vec![],
            job_events: vec![],
            jobs: vec![],
            inputs: vec![],
            queues: vec![],
            input_changed: false,
            error: Some(format!("persisted state cannot be read: {text}")),
        };
        tracing::error!(
            at_ms = now.as_u64(),
            problems = %text,
            "tick failed: the persisted state cannot be read; nothing published, stored state left untouched"
        );
        *self.last_report.lock().unwrap_or_else(|e| e.into_inner()) = Some(report.clone());
        report
    }

    fn cache(&self) -> std::sync::MutexGuard<'_, ObservationCache> {
        self.cache.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A receiver of the published allowed set, for the KEDA scaler. It holds `None` until
    /// the first tick of this process has been published, then the latest tick's set.
    pub fn subscribe_allowed(&self) -> watch::Receiver<Option<PublishedAllowed>> {
        self.allowed.subscribe()
    }

    /// Whether the allowed set this process published contains `class`: its model is
    /// serving and not winding down, its hosts are up, no lockout. False until a tick has
    /// been published, so nothing runs on a guess.
    pub fn class_is_allowed(&self, class: &ClassId) -> bool {
        self.allowed
            .borrow()
            .as_ref()
            .is_some_and(|p| p.allowed.classes.contains(class))
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

    /// The core: plan, then sequence. A test can make it refuse.
    fn core(
        &self,
        config: &sched_model::Config,
        snapshot: &sched_model::Snapshot,
        state: &SchedState,
    ) -> Result<(sched_model::Intent, sched_sequencer::Decision), crate::core::CoreError> {
        #[cfg(test)]
        if self.refuse_to_decide.load(Ordering::SeqCst) {
            return Err(crate::core::CoreError::Tick(
                sched_sequencer::TickError::GenerationMismatch {
                    config: 0,
                    snapshot: 1,
                },
            ));
        }
        decide(config, snapshot, state)
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
        let (mut snapshot, ages, changed, now, reported) = {
            let mut cache = self.cache();
            // Which sources reported before is durable and was seeded into the cache before
            // this step (`seed_reported`); the cache itself only knows this process.
            // Never run before something already received: that observation would be
            // dated after `now` and read as not fresh.
            let now = now.max(cache.latest_receipt());
            (
                cache.snapshot(&config, now),
                cache.input_ages(&config, now),
                // The dirty flag is taken inside the journaled step: on a replay it stays
                // set, so the next tick reports `input_changed`, which is harmless.
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
        // The scheduled jobs move first: their holds enter the lease book before the planner
        // looks at it, so a job that comes due is seen by the very tick that notices it. If
        // the stored job book was unreadable the step is skipped and nothing about jobs moves.
        let (jobs, job_leases, job_events, creates) = if durable.jobs_off {
            (
                durable.jobs.clone(),
                durable.leases.clone(),
                Vec::new(),
                Vec::new(),
            )
        } else {
            let s = crate::jobs::step(
                &config,
                &snapshot,
                &durable.state,
                &durable.jobs,
                &durable.leases,
                now,
            );
            (s.book, s.leases, s.events, s.creates)
        };
        // Ownership (`docs/architecture/power.md`, *Ownership and adoption*): the sequencer decides which hosts found
        // running are adopted (a manual host lease with origin `adopted`, before the planner
        // looks, so this very tick sees it) and which adopted leases end because the host was
        // seen down, and gives back the lease book and the previous state to use. This applies
        // it; it edits nothing itself. It runs with an unreadable job book too: the lease book
        // is stored either way (see `object::tick`).
        let sched_sequencer::adopt::Adoption {
            leases: all_leases,
            prev: prev_state,
            adopted,
            ended: adoptions_ended,
        } = sched_sequencer::adopt::adopt(&config, &snapshot, &durable.state, &job_leases, now);
        let (leases, ignored_leases) = leases::effective(&config, &all_leases);
        snapshot.leases = leases.clone();
        // Sources that can never report: nothing observes them in this build and, `observe`
        // being ingress-private, nothing outside Restate can push to them. If they have
        // never reported, their classes have no demand (see `Snapshot::unobservable`). One
        // that has reported before (in any process) is merely silent: unknown, as before.
        snapshot.unobservable = config
            .sources()
            .iter()
            .filter(|s| observer_state(s) != ObserverState::Running)
            .filter(|s| !reported.contains(&s.id))
            .map(|s| s.id.clone())
            .collect();
        let mut gaps = DemandGaps::default();
        let mut queues = Vec::new();
        for c in config.classes() {
            let Some(d) = &c.demand else { continue };
            let work = class_work(&config, &snapshot, &prev_state, c);
            queues.push(QueueReport {
                class: c.id.clone(),
                ready: work.ready + work.withheld,
                in_flight: work.in_flight,
                failed: work.failed,
                oldest_ready_age_ms: work
                    .oldest_ready_at
                    .map(|at| now.saturating_sub(at).as_u64()),
                known: work.known,
                offline: work.offline,
            });
            if snapshot.unobservable.contains(&d.source) {
                match config.source(&d.source).map(observer_state) {
                    Some(ObserverState::NoParams) => gaps.unpolled.push(c.id.clone()),
                    _ => gaps.unsupported.push(c.id.clone()),
                }
            } else if work.switched_off {
                gaps.switched_off.push(c.id.clone());
            } else if work.offline {
                gaps.offline.push(c.id.clone());
            } else if !work.known {
                gaps.unknown.push(c.id.clone());
            }
        }
        let (
            kinds,
            reason,
            steps,
            allowed,
            state,
            forced,
            hosts,
            events,
            power_request_failures,
            error,
        ) = match self.core(&config, &snapshot, &prev_state) {
            Ok((intent, decision)) => {
                // Power wishes of hosts under manual control are never acted on; the
                // reason does not list them.
                let mut shown = intent.clone();
                shown.power_up.retain(|r, _| config.power_managed(r));
                let (hosts, events) = status::derive(&Decided {
                    config: &config,
                    snapshot: &snapshot,
                    prev: &durable.state,
                    next: &decision.state,
                    steps: &decision.steps,
                    now,
                    adopted: &adopted,
                    ended: &adoptions_ended,
                });
                let kinds =
                    reason::kinds(&decision.steps, &gaps, !reason::pinned(&intent).is_empty())
                        .into_iter()
                        .map(String::from)
                        .collect();
                let power_request_failures = self.power_request_failures(&decision.state);
                let (kinds, reason) = reason::with_hosts(
                    kinds,
                    reason::reason(&shown, &decision.steps, &gaps),
                    &hosts,
                    &events,
                    &power_request_failures,
                );
                let reason = reason
                    + &reason::switch_text(&reason::switch_gaps(
                        &config,
                        &snapshot,
                        &prev_state,
                        &decision.allowed,
                    ));
                let reason = reason
                    + &reason::idle_text(&reason::idle_classes(
                        &config,
                        &decision.state,
                        &decision.allowed,
                    ));
                (
                    kinds,
                    reason,
                    decision.steps,
                    decision.allowed,
                    decision.state,
                    intent.forced.iter().cloned().collect(),
                    hosts,
                    events,
                    power_request_failures,
                    None,
                )
            }
            // The previous state stays, and so does the previous publication. Nothing was
            // decided: the statuses describe the carried state at this tick's time, with no
            // wait (no steps) and no events (nothing became true).
            Err(e) => {
                let (hosts, _) = status::derive(&Decided {
                    config: &config,
                    snapshot: &snapshot,
                    prev: &prev_state,
                    next: &prev_state,
                    steps: &[],
                    now,
                    adopted: &[],
                    ended: &[],
                });
                (
                    vec![reason::KIND_ERROR.to_string()],
                    format!("no decision: {e}"),
                    Vec::new(),
                    AllowedSet::default(),
                    prev_state.clone(),
                    Vec::new(),
                    hosts,
                    Vec::new(),
                    self.power_request_failures(&prev_state),
                    Some(e.to_string()),
                )
            }
        };
        let report = TickReport {
            config_generation: config.generation(),
            at: now,
            unknown_demand: gaps.unknown,
            // Offline for either cause; the reason text tells them apart.
            offline_demand: gaps.offline.into_iter().chain(gaps.switched_off).collect(),
            unsupported_demand: gaps.unsupported,
            unpolled_demand: gaps.unpolled,
            forced,
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
            hosts,
            events,
            power_request_failures,
            inputs: ages.iter().map(InputReport::from).collect(),
            queues,
            job_events,
            jobs: crate::jobs::summaries(&config, &jobs),
            input_changed: changed,
            error,
        };
        TickOutcome {
            report,
            reported,
            jobs,
            leases: all_leases,
            creates,
            duration_us: u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
        }
    }

    /// Makes a decided tick visible: publishes its allowed set (unless the tick failed),
    /// counts it, and logs it, with a warning per step that nothing carries out.
    ///
    /// `replayed` says the decision came back from the journal (the step did not run in
    /// this attempt): it is published and logged again, since this process may be a new
    /// one, but not counted again in `sched_tick_decisions_total` or the tick duration.
    pub fn publish(&self, outcome: &TickOutcome, replayed: bool) {
        let r = &outcome.report;
        if let Some(p) = outcome.published() {
            self.allowed.send_replace(Some(p));
        }
        if !replayed {
            for kind in &r.kinds {
                self.metrics.decision(kind);
            }
            self.metrics
                .tick_duration(outcome.duration_us as f64 / 1_000_000.0);
        }
        let ages: Vec<InputAge> = r.inputs.iter().map(InputReport::to_input_age).collect();
        self.metrics.set_input_ages(&ages);
        self.metrics.set_phases(r.at, &r.state);
        self.metrics.set_manual_lease_ages(r.at, &r.leases);
        // The per-host gauges come from the statuses (a failed tick's are those of the carried
        // state).
        self.metrics.set_host_gauges(&r.hosts);
        if !replayed {
            for event in &r.events {
                match event {
                    HostEvent::VetoTimeout {
                        resource,
                        readings,
                        blocking,
                        ..
                    } => {
                        // One count per reading that was still blocking (all that apply if
                        // the snapshot shows none, which a timeout never does).
                        let labels: Vec<&str> = if blocking.is_empty() {
                            readings.iter().map(|r| r.as_str()).collect()
                        } else {
                            blocking.iter().map(|b| b.reading.as_str()).collect()
                        };
                        for reading in labels {
                            self.metrics.veto_timeout(resource.as_str(), reading);
                        }
                    }
                    HostEvent::PowerTimeout {
                        resource, during, ..
                    } => self
                        .metrics
                        .power_timeout(resource.as_str(), during.as_str()),
                    HostEvent::Adopted { .. } | HostEvent::AdoptionEnded { .. } => {}
                }
            }
        }
        self.metrics.set_jobs(&r.jobs);
        if !replayed {
            for e in &r.job_events {
                self.metrics.job_event(e.job.as_str(), e.kind.counter());
            }
        }
        self.metrics.tick_finished_at(r.at);
        log_tick(r, &reason::format_inputs(&ages), replayed);
        *self.last_report.lock().unwrap_or_else(|e| e.into_inner()) = Some(r.clone());
    }

    /// Decides and publishes one tick against `durable`, then updates it as the
    /// `Scheduler` object updates its Restate state. For tests and tools; the object
    /// journals the decision in between.
    pub fn tick(&self, now: Millis, durable: &mut Durable) -> TickReport {
        self.seed_reported(&durable.reported);
        let outcome = self.decide(now, durable);
        self.publish(&outcome, false);
        durable.apply(&outcome);
        outcome.report
    }

    /// Records that the backstop ran at `now`.
    pub fn note_backstop(&self, now: Millis) {
        self.last_backstop.store(now.as_u64(), Ordering::SeqCst);
    }
}

/// Logs one scheduled-job event. The alert-grade ones are errors under a stable `event` name
/// (`job_failed`, `job_missed`, `job_skipped_overlap`, `job_start_late`, `job_create_failed`)
/// for the tick that decided them; a replay logs the same fact at info level under the name
/// plus `_replayed`, so a log-based alert fires once. The others are info.
fn log_job_event(e: &sched_model::JobEvent, replayed: bool) {
    let name = format!("job_{}", e.kind.counter());
    let detail = serde_json::to_string(&e.kind).unwrap_or_default();
    let (job, at, scheduled) = (e.job.as_str(), e.at.as_u64(), e.scheduled_at.as_u64());
    if !e.kind.is_alert() {
        tracing::info!(event = %name, job, at_ms = at, scheduled_at_ms = scheduled, detail = %detail, "scheduled job {job}: {name}");
    } else if replayed {
        tracing::info!(event = %format!("{name}_replayed"), job, at_ms = at, scheduled_at_ms = scheduled, detail = %detail, "replayed tick: scheduled job {job}: {name} (alerted when first decided)");
    } else {
        tracing::error!(event = %name, job, at_ms = at, scheduled_at_ms = scheduled, detail = %detail, "ALERT: scheduled job {job}: {name}: {detail}");
    }
}

/// Logs one host event. The alert-grade ones (`power_timeout`, `shutdown_veto_timeout`) are
/// errors for the tick that decided them. A replay (the decision came back from the journal)
/// logs the same fact at info level under the name plus `_replayed`, so a log-based alert
/// fires once.
fn log_host_event(e: &HostEvent, at: Millis, replayed: bool) {
    match e {
        HostEvent::Adopted { resource: h } => tracing::info!(
            event = "host_adopted",
            resource = %h,
            at_ms = at.as_u64(),
            replayed,
            "{h} was found running and flowmanager did not power it on: it is held by the user (a manual host lease was minted; switching the host off releases it)"
        ),
        HostEvent::AdoptionEnded { resource: h } => tracing::info!(
            event = "host_adoption_ended",
            resource = %h,
            at_ms = at.as_u64(),
            replayed,
            "{h} was seen down: its adopted lease is dropped, so it is not woken again"
        ),
        HostEvent::PowerTimeout {
            resource,
            during,
            started_at,
            at,
        } => {
            let wanted = match during {
                PowerTransition::PoweringOn => "up",
                PowerTransition::ShuttingDown => "down",
            };
            if replayed {
                tracing::info!(
                    event = "power_timeout_replayed",
                    resource = %resource,
                    during = during.as_str(),
                    started_at_ms = ?started_at.map(Millis::as_u64),
                    at_ms = at.as_u64(),
                    "replayed tick: {} of {} timed out (alerted when first decided); it is not retried",
                    during.as_str(),
                    resource,
                );
            } else {
                tracing::error!(
                    event = "power_timeout",
                    resource = %resource,
                    during = during.as_str(),
                    started_at_ms = ?started_at.map(Millis::as_u64),
                    at_ms = at.as_u64(),
                    "ALERT: {} of {} did not complete in time; flowmanager stops here and will not retry any power action for it: nothing is allowed on it until it is observed {} or a person acts",
                    during.as_str(),
                    resource,
                    wanted,
                );
            }
        }
        HostEvent::VetoTimeout {
            resource,
            readings,
            blocking,
            waiting_since,
            at,
        } => {
            if replayed {
                tracing::info!(
                    event = "shutdown_veto_timeout_replayed",
                    resource = %resource,
                    readings = %readings_label(readings),
                    blocking = %blockers_label(blocking),
                    waiting_since_ms = ?waiting_since.map(Millis::as_u64),
                    at_ms = at.as_u64(),
                    "replayed tick: the shutdown of {} timed out waiting for the veto to clear (alerted when first decided); it is not retried",
                    resource,
                );
            } else {
                tracing::error!(
                    event = "shutdown_veto_timeout",
                    resource = %resource,
                    readings = %readings_label(readings),
                    blocking = %blockers_label(blocking),
                    waiting_since_ms = ?waiting_since.map(Millis::as_u64),
                    at_ms = at.as_u64(),
                    "ALERT: {} was still in use (or its use was unknown) after veto_timeout; flowmanager did NOT shut it down and stops here: it will not retry by itself, the host stays up until a person acts (or a new manual request takes it over)",
                    resource,
                );
            }
        }
    }
}

fn log_tick(r: &TickReport, inputs: &str, replayed: bool) {
    let kinds = r.kinds.join(",");
    if r.error.is_some() {
        tracing::error!(
            generation = r.config_generation,
            at_ms = r.at.as_u64(),
            decision = %kinds,
            reason = %r.reason,
            inputs = %inputs,
            replayed,
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
        forced = ?r.forced.iter().map(|x| x.as_str()).collect::<Vec<_>>(),
        inputs = %inputs,
        replayed,
        input_changed = r.input_changed,
        unknown_demand = ?ids(&r.unknown_demand),
        offline_demand = ?ids(&r.offline_demand),
        unsupported_demand = ?ids(&r.unsupported_demand),
        unpolled_demand = ?ids(&r.unpolled_demand),
        drains = ?r
            .hosts
            .iter()
            .filter_map(|h| {
                let d = h.drain()?;
                Some(format!("{}:{}:{}:{}", h.resource, d.cause.as_str(), d.mode.as_str(), if d.scaled_down { "scaled_down" } else { "running" }))
            })
            .collect::<Vec<_>>(),
        "tick"
    );
    for h in &r.hosts {
        let Some(d) = h.drain() else { continue };
        tracing::info!(
            event = "drain",
            resource = %h.resource,
            mode = d.mode.as_str(),
            cause = d.cause.as_str(),
            since_ms = d.since.as_u64(),
            scaled_down = d.scaled_down,
            replayed,
            "{} drain of {} ({}): {}",
            d.cause.as_str(),
            h.resource,
            d.mode.as_str(),
            if d.scaled_down { "consumers scaled down, shutting down once nothing holds the host" } else { "in-flight work is still given its drain_timeout" }
        );
    }
    // One line per class, so a log query can graph each queue on its own. Not on a replay:
    // the same tick would be counted twice.
    if !replayed {
        for q in &r.queues {
            tracing::info!(
                event = "queue",
                class = %q.class,
                ready = q.ready,
                in_flight = q.in_flight,
                failed = q.failed,
                oldest_ready_age_ms = ?q.oldest_ready_age_ms,
                known = q.known,
                offline = q.offline,
                at_ms = r.at.as_u64(),
                "queue {}: {} ready, {} in flight, {} failed",
                q.class,
                q.ready,
                q.in_flight,
                q.failed,
            );
        }
    }
    for e in &r.events {
        log_host_event(e, r.at, replayed);
    }
    for e in &r.job_events {
        log_job_event(e, replayed);
    }
    for step in &r.not_executed {
        tracing::warn!(
            generation = r.config_generation,
            at_ms = r.at.as_u64(),
            kind = reason::step_kind(step),
            step = ?step,
            why = reason::not_executed_note(step).unwrap_or_default().trim(),
            "step NOT EXECUTED"
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
    use sched_model::{Fidelity, ObservedValue, PowerTransition, VetoReading};
    use sched_sequencer::status::{BlockerState, VetoBlocker, VetoStatus};

    const T0: u64 = 10_000_000;

    impl TickReport {
        pub fn adopted(&self) -> Vec<ResourceId> {
            self.events
                .iter()
                .filter_map(|e| match e {
                    HostEvent::Adopted { resource } => Some(resource.clone()),
                    _ => None,
                })
                .collect()
        }

        pub fn adoptions_ended(&self) -> Vec<ResourceId> {
            self.events
                .iter()
                .filter_map(|e| match e {
                    HostEvent::AdoptionEnded { resource } => Some(resource.clone()),
                    _ => None,
                })
                .collect()
        }

        pub fn power_timeouts(&self) -> Vec<&HostEvent> {
            self.events
                .iter()
                .filter(|e| matches!(e, HostEvent::PowerTimeout { .. }))
                .collect()
        }

        pub fn veto_timeouts(&self) -> Vec<&HostEvent> {
            self.events
                .iter()
                .filter(|e| matches!(e, HostEvent::VetoTimeout { .. }))
                .collect()
        }

        pub fn power_failed(&self) -> Vec<&HostStatus> {
            self.hosts
                .iter()
                .filter(|h| matches!(h.power, PowerPhase::Failed { .. }))
                .collect()
        }

        /// Hosts whose shutdown the veto holds back or stopped.
        pub fn veto_held(&self) -> Vec<&HostStatus> {
            self.hosts.iter().filter(|h| h.veto().is_some()).collect()
        }

        /// Hosts whose automatic power-on waits for `min_off`.
        pub fn min_off_waits(&self) -> Vec<&HostStatus> {
            self.hosts
                .iter()
                .filter(|h| h.wait == Some(sched_model::WaitReason::MinOff) && h.min_off.is_some())
                .collect()
        }

        /// The drains under way, with their host.
        pub fn drains(&self) -> Vec<(&HostStatus, status::Drain)> {
            self.hosts
                .iter()
                .filter_map(|h| h.drain().map(|d| (h, d)))
                .collect()
        }
    }

    fn shared() -> (Arc<Shared>, Arc<ManualClock>) {
        shared_with(EXAMPLE_CONFIG)
    }

    fn shared_with(yaml: &str) -> (Arc<Shared>, Arc<ManualClock>) {
        let clock = Arc::new(ManualClock::new(Millis(T0)));
        let holder = Arc::new(ConfigHolder::new(yaml).unwrap());
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
                failed: 0,
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
        // The state is carried: the next tick sees the model serving (from the tick it was
        // allowed, whatever its pods do) and has nothing to do.
        let again = s.tick(Millis(T0 + 2_000), &mut d);
        assert!(!again.input_changed);
        assert!(again.steps.is_empty(), "{again:?}");
        assert!(
            !again.kinds.contains(&"allow_model".to_string()),
            "{again:?}"
        );
        assert!(!again.reason.contains("pods"), "{}", again.reason);
        let text = s.metrics.render();
        assert!(text.contains("sched_tick_decisions_total{kind=\"allow_model\"} 1"));
        assert!(!text.contains("sched_tick_decisions_total{kind=\"wait\"}"));
    }

    #[test]
    fn every_tick_reports_each_classs_queue_counts() {
        let (s, _) = shared();
        let mut d = Durable::default();
        cluster(&s);
        push(
            &s,
            "paperless-ocr-pending",
            vec![ObservedValue::Demand {
                class: "paperless-ocr".into(),
                ready: 7,
                in_flight: 2,
                failed: 1,
                oldest_ready_at: None,
            }],
        );
        let r = s.tick(Millis(T0 + 1_000), &mut d);
        let ocr = r
            .queues
            .iter()
            .find(|q| q.class.as_str() == "paperless-ocr")
            .unwrap_or_else(|| panic!("no queue report for paperless-ocr: {:?}", r.queues));
        assert_eq!(
            (ocr.ready, ocr.in_flight, ocr.failed, ocr.known),
            (7, 2, 1, true)
        );
        // The cache stamps when it first saw the queue non-empty (T0, the test clock), not the
        // reported time, so the age is the time since then.
        assert_eq!(ocr.oldest_ready_age_ms, Some(1_000));
        // A class whose source has not reported is listed too, marked unknown rather than
        // read as an empty queue.
        let silent = r.queues.iter().find(|q| !q.known);
        assert!(
            silent.is_some_and(|q| q.ready == 0 && q.oldest_ready_age_ms.is_none()),
            "{:?}",
            r.queues
        );
    }

    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Captured {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
        type Writer = Captured;
        fn make_writer(&'a self) -> Captured {
            self.clone()
        }
    }

    #[test]
    fn queue_counts_are_logged_one_line_per_class_and_not_again_on_replay() {
        let (s, _) = shared();
        let mut d = Durable::default();
        cluster(&s);
        ocr_demand(&s, 3);
        let r = s.tick(Millis(T0 + 1_000), &mut d);
        let out = Captured::default();
        let sub = tracing_subscriber::fmt()
            .with_writer(out.clone())
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(sub, || {
            // Tests run in parallel and log with no subscriber, and a callsite first
            // registered while another thread installs a dispatcher can be left cached
            // as uninteresting. Register every callsite first, then rebuild the cache
            // (which does reach registered callsites) and start the capture clean.
            log_tick(&r, "", false);
            tracing::callsite::rebuild_interest_cache();
            out.0.lock().unwrap().clear();
            log_tick(&r, "", false);
            log_tick(&r, "", true);
        });
        let text = String::from_utf8(out.0.lock().unwrap().clone()).unwrap();
        let lines: Vec<&str> = text
            .lines()
            .filter(|l| l.contains("event=\"queue\""))
            .collect();
        assert_eq!(lines.len(), r.queues.len(), "{text}");
        let ocr = lines
            .iter()
            .find(|l| l.contains("class=paperless-ocr"))
            .unwrap_or_else(|| panic!("{text}"));
        assert!(
            ocr.contains("ready=3") && ocr.contains("in_flight=0") && ocr.contains("failed=0"),
            "{ocr}"
        );
    }

    #[test]
    fn a_source_that_stops_reporting_makes_decisions_wait_instead_of_looking_healthy() {
        let (s, _) = shared();
        let mut d = Durable::default();
        cluster(&s);
        ocr_demand(&s, 3);
        // 31s later nothing was refreshed: the host's reachability is older than
        // host_reading_stale_after (30s), so no model is allowed on it.
        let r = s.tick(Millis(T0 + 31_000), &mut d);
        assert!(!r.kinds.contains(&"allow_model".to_string()), "{r:?}");
        assert!(
            r.steps.contains(&Step::Wait {
                resource: "cuda-central".into(),
                reason: sched_model::WaitReason::StaleObservation
            }),
            "{r:?}"
        );
        assert!(
            r.reason
                .contains("reachability observation of the host is missing or stale"),
            "{}",
            r.reason
        );
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
            ("llm-sessions", "interactive-llm"),
        ] {
            push(
                &s,
                src,
                vec![ObservedValue::Demand {
                    failed: 0,
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
            origin: sched_model::LeaseOrigin::Requested,
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
            sched_model::ModePhase::Serving { .. }
        ));
        clock.set(Millis(T0 + 2_000));
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
        s.publish(&outcome, false);
        d.apply(&outcome);
        assert!(rx.has_changed().unwrap());
        let p = rx.borrow_and_update().clone().unwrap();
        assert_eq!((p.config_generation, p.at), (1, Millis(T0 + 1_000)));
        assert_eq!(
            p.allowed.models,
            [ModelId::from("mineru")].into_iter().collect()
        );
        // NFS is up, so the NFS-only classes are allowed, and so is the gate class (the GPU
        // host is up too); OCR is allowed with mineru, which serves from this tick.
        assert!(p.allowed.classes.contains(&ClassId::from("paperless-ngx")));
        assert!(p.allowed.classes.contains(&ClassId::from("paperless-gpt")));
        assert!(p.allowed.classes.contains(&ClassId::from("paperless-ocr")));
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
        // llm-sessions has no observer in this build, but something pushed to it.
        push(
            &s,
            "llm-sessions",
            vec![ObservedValue::Demand {
                failed: 0,
                class: "interactive-llm".into(),
                ready: 0,
                in_flight: 0,
                oldest_ready_at: None,
            }],
        );
        s.tick(Millis(T0 + 1_000), &mut d);
        assert!(d.reported.contains(&SourceId::from("llm-sessions")));
        assert!(d.reported.contains(&SourceId::from("cluster-pods")));
        assert!(
            !d.reported
                .contains(&SourceId::from("paperless-ocr-pending"))
        );
        // After a restart the pusher is dead: nothing arrives.
        let (s2, mut d2) = restart(&d);
        let r = s2.tick(Millis(T0 + 2_000), &mut d2);
        assert!(
            r.unknown_demand
                .iter()
                .any(|c| c.as_str() == "interactive-llm"),
            "{r:?}"
        );
        assert!(
            !r.unsupported_demand
                .iter()
                .any(|c| c.as_str() == "interactive-llm"),
            "{r:?}"
        );
        // The memory is kept across further ticks and restarts.
        assert!(d2.reported.contains(&SourceId::from("llm-sessions")));
        // Without the durable memory it would read as never reported.
        let (s3, _) = shared();
        let r = s3.tick(Millis(T0 + 2_000), &mut Durable::default());
        assert!(
            r.unsupported_demand
                .iter()
                .any(|c| c.as_str() == "interactive-llm")
        );
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
                    origin: sched_model::LeaseOrigin::Requested,
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
    fn a_power_step_is_carried_out_not_reported_as_not_executed() {
        // Managed power control (M4), the only way a power step can appear.
        let (s, _) = shared_with(&sched_model::test_support::example_yaml_managed());
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
        assert!(r.not_executed.is_empty(), "{:?}", r.not_executed);
        assert!(!r.reason.contains("NOT EXECUTED"), "{}", r.reason);
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
    fn under_manual_power_control_a_lease_on_a_host_that_is_off_waits_for_a_person() {
        let (s, _) = shared();
        let mut d = Durable {
            leases: vec![llm_lease()],
            ..Durable::default()
        };
        push(
            &s,
            "cluster-pods",
            vec![ObservedValue::ResourceUp {
                resource: "cuda-central".into(),
                up: false,
            }],
        );
        let r = s.tick(Millis(T0 + 1_000), &mut d);
        assert!(r.not_executed.is_empty(), "{r:?}");
        assert!(
            r.steps.contains(&Step::Wait {
                resource: "cuda-central".into(),
                reason: sched_model::WaitReason::HostNotUp
            }),
            "{r:?}"
        );
        let gpu = d.state.resource(&"cuda-central".into()).unwrap();
        assert_eq!(gpu.power, sched_model::PowerPhase::Off { ours: false });
        assert!(!gpu.ours());
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
                    failed: 0,
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
                    failed: 0,
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
    fn demand_of_sources_without_an_observer_is_reported_apart_and_counts_as_idle() {
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
                    failed: 0,
                    class: class.into(),
                    ready: 0,
                    in_flight: 0,
                    oldest_ready_at: None,
                }],
            );
        }
        // Everything that has an observer reports; only the session source is
        // silent, and nothing is built to observe it.
        let r = s.tick(Millis(T0 + 1_000), &mut d);
        assert!(r.unknown_demand.is_empty(), "{:?}", r.unknown_demand);
        assert_eq!(r.unsupported_demand, vec![ClassId::from("interactive-llm")]);
        // Nothing can ever report it, so it is no demand: the tick is idle.
        assert_eq!(
            r.kinds,
            vec!["demand_unsupported".to_string(), "idle".to_string()],
            "{r:?}"
        );
        assert!(r.reason.contains("counted as no demand"), "{}", r.reason);
        let text = s.metrics.render();
        assert!(
            text.contains("sched_source_has_observer{source=\"llm-sessions\"} 0"),
            "{text}"
        );
        assert!(
            text.contains("sched_source_has_observer{source=\"paperless-ocr-pending\"} 1"),
            "{text}"
        );
    }

    #[test]
    fn a_model_pinned_by_a_manual_lease_does_not_read_as_idle() {
        let (s, _) = shared();
        let mut d = Durable {
            leases: vec![llm_lease()],
            ..Durable::default()
        };
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
                    failed: 0,
                    class: class.into(),
                    ready: 0,
                    in_flight: 0,
                    oldest_ready_at: None,
                }],
            );
        }
        let first = s.tick(Millis(T0 + 1_000), &mut d);
        assert!(
            first.kinds.contains(&"allow_model".to_string()),
            "{first:?}"
        );
        // Pinned and settled: no step, but the LLM is held on purpose, not idle.
        let r = s.tick(Millis(T0 + 2_000), &mut d);
        assert!(r.steps.is_empty(), "{r:?}");
        assert_eq!(
            r.kinds,
            vec!["demand_unsupported".to_string(), "pinned".to_string()],
            "{r:?}"
        );
        assert!(
            r.reason
                .contains("pinned by a manual model lease [cuda-central=llm]"),
            "{}",
            r.reason
        );
        assert!(
            s.metrics
                .render()
                .contains("sched_tick_decisions_total{kind=\"pinned\"} 2")
        );
        // Released: idle again.
        d.leases.clear();
        let r = s.tick(Millis(T0 + 3_000), &mut d);
        assert!(!r.kinds.contains(&"pinned".to_string()), "{r:?}");
    }

    #[test]
    fn a_pushed_source_that_goes_silent_is_unknown_not_unsupported() {
        let (s, _) = shared();
        let mut d = Durable::default();
        cluster(&s);
        // llm-sessions has no observer in this build, but something pushed to it.
        push(
            &s,
            "llm-sessions",
            vec![ObservedValue::Demand {
                failed: 0,
                class: "interactive-llm".into(),
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
                .any(|c| c.as_str() == "interactive-llm")
        );
        assert!(
            !fresh
                .unknown_demand
                .iter()
                .any(|c| c.as_str() == "interactive-llm")
        );
        // Ten minutes later it has gone stale: a source that reported before and has gone
        // quiet is a real problem, not the expected "nothing observes this".
        let stale = s.tick(Millis(T0 + 1_000 + 600_001), &mut d);
        assert!(
            stale
                .unknown_demand
                .iter()
                .any(|c| c.as_str() == "interactive-llm"),
            "{stale:?}"
        );
        assert!(
            !stale
                .unsupported_demand
                .iter()
                .any(|c| c.as_str() == "interactive-llm")
        );
    }

    #[test]
    fn an_observable_source_configured_without_params_is_labelled_as_such() {
        // Drop the params block of immich-queues (it ends at the blank line after it).
        let start = EXAMPLE_CONFIG.find("  - id: immich-queues\n").unwrap();
        let params = start + EXAMPLE_CONFIG[start..].find("    params:\n").unwrap();
        let end = params + EXAMPLE_CONFIG[params..].find("\n\n").unwrap() + 1;
        let y = format!("{}{}", &EXAMPLE_CONFIG[..params], &EXAMPLE_CONFIG[end..]);
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
                failed: 0,
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

    fn state_in(id: &str, power: PowerPhase) -> SchedState {
        let mut st = SchedState::default();
        st.resources
            .insert(ResourceId::new(id), sched_model::ResourceState::new(power));
        st
    }

    /// The managed example with basestar up, managed and idle since long ago: the shutdown
    /// is due as soon as the grace period is checked.
    fn due_nfs() -> (Arc<Shared>, Arc<ManualClock>, Durable) {
        let (s, clock) = shared_with(&sched_model::test_support::example_yaml_managed());
        let mut d = Durable::default();
        for (id, managed) in [("cuda-central", false), ("basestar-nfs", true)] {
            let mut st = sched_model::ResourceState::new(PowerPhase::On {
                since: Millis(1),
                ours: false,
            });
            st.set_ours(managed);
            st.idle_since = managed.then_some(Millis(1));
            d.state.resources.insert(ResourceId::new(id), st);
        }
        (s, clock, d)
    }

    /// Every source reports again at the clock's time: both hosts up, nothing queued, and
    /// `mounts` pods mounting basestar (no reading at all if `None`).
    fn world(s: &Shared, mounts: Option<u32>) {
        cluster(s);
        world_without_cluster(s, mounts);
    }

    /// `world` without the cluster-pods and probe readings.
    fn world_without_cluster(s: &Shared, mounts: Option<u32>) {
        for (class, source) in [
            ("paperless-ocr", "paperless-ocr-pending"),
            ("paperless-tagging", "paperless-post-ocr"),
            ("speakr-transcription", "speakr-pending"),
            ("speakr-summarization", "speakr-summaries"),
            ("immich-ml", "immich-queues"),
        ] {
            push(
                s,
                source,
                vec![ObservedValue::Demand {
                    class: class.into(),
                    ready: 0,
                    in_flight: 0,
                    failed: 0,
                    oldest_ready_at: None,
                }],
            );
        }
        if let Some(pods) = mounts {
            push(
                s,
                "basestar-mounts",
                vec![ObservedValue::ResourceMounts {
                    resource: "basestar-nfs".into(),
                    pods,
                }],
            );
        }
    }

    fn veto_gauge(s: &Shared, state: &str) -> String {
        s.metrics
            .render()
            .lines()
            .find(|l| {
                l.starts_with("sched_resource_veto{")
                    && l.contains("resource=\"basestar-nfs\"")
                    && l.contains(&format!("state=\"{state}\""))
            })
            .and_then(|l| l.rsplit(' ').next())
            .unwrap_or("absent")
            .to_string()
    }

    #[test]
    fn a_shutdown_held_by_mounting_pods_is_reported_alerts_once_on_timeout_and_is_not_retried() {
        let (s, clock, mut d) = due_nfs();
        let wait = |reason| Step::Wait {
            resource: "basestar-nfs".into(),
            reason,
        };
        // Due and mounted: held back, visible in the report, the reason and the gauge.
        world(&s, Some(2));
        let r = s.tick(Millis(T0 + 1_000), &mut d);
        assert_eq!(r.error, None);
        assert_eq!(r.steps, vec![wait(sched_model::WaitReason::VetoInUse)]);
        assert_eq!(r.veto_held().len(), 1, "{r:?}");
        let h = r.host(&"basestar-nfs".into()).unwrap();
        assert_eq!(h.veto_readings, vec![VetoReading::Mounts]);
        assert_eq!(
            h.veto(),
            Some(&VetoStatus::Waiting {
                since: Millis(T0 + 1_000),
                blocking: vec![VetoBlocker {
                    reading: VetoReading::Mounts,
                    state: BlockerState::InUse
                }],
            })
        );
        assert!(r.veto_timeouts().is_empty());
        assert!(r.reason.contains("held back"), "{}", r.reason);
        assert_eq!(veto_gauge(&s, "waiting"), "1");
        assert_eq!(veto_gauge(&s, "stopped"), "0");

        // veto_timeout is 30 minutes: the tick that reaches it alerts, once.
        let at = T0 + 1_000 + 30 * 60_000;
        clock.set(Millis(at));
        world(&s, Some(2));
        let r = s.tick(Millis(at), &mut d);
        assert_eq!(
            r.steps,
            vec![wait(sched_model::WaitReason::ShutdownStopped)]
        );
        assert_eq!(r.veto_timeouts().len(), 1, "{r:?}");
        assert!(
            matches!(
                r.veto_timeouts()[0],
                HostEvent::VetoTimeout { waiting_since, .. }
                    if *waiting_since == Some(Millis(T0 + 1_000))
            ),
            "{r:?}"
        );
        assert!(r.kinds.contains(&"shutdown_stopped".to_string()), "{r:?}");
        assert!(r.reason.contains("ALERT"), "{}", r.reason);
        assert!(r.reason.contains("NOT shut down"), "{}", r.reason);
        assert_eq!(veto_gauge(&s, "stopped"), "1");
        assert_eq!(veto_gauge(&s, "waiting"), "0");
        assert!(
            s.metrics.render().contains(
                "sched_veto_timeouts_total{reading=\"mounts\",resource=\"basestar-nfs\"} 1"
            )
        );
        // Still allowed: the host is up.
        assert!(r.allowed_classes.contains(&"paperless-ngx".into()), "{r:?}");

        // The pods are gone: no power-off, no new alert, no second count.
        clock.set(Millis(at + 10_000));
        world(&s, Some(0));
        let r = s.tick(Millis(at + 10_000), &mut d);
        assert_eq!(
            r.steps,
            vec![wait(sched_model::WaitReason::ShutdownStopped)]
        );
        assert!(r.veto_timeouts().is_empty(), "{r:?}");
        assert!(r.veto_held()[0].stopped());
        assert!(
            s.metrics.render().contains(
                "sched_veto_timeouts_total{reading=\"mounts\",resource=\"basestar-nfs\"} 1"
            )
        );
    }

    /// The managed example with cuda-central up, managed and idle since long ago (its shutdown
    /// is due as soon as the grace period is checked) and basestar up but held by someone else.
    fn due_gpu() -> (Arc<Shared>, Arc<ManualClock>, Durable) {
        let (s, clock) = shared_with(&sched_model::test_support::example_yaml_managed());
        let mut d = Durable::default();
        for (id, managed) in [("cuda-central", true), ("basestar-nfs", false)] {
            let mut st = sched_model::ResourceState::new(PowerPhase::On {
                since: Millis(1),
                ours: false,
            });
            st.set_ours(managed);
            st.idle_since = managed.then_some(Millis(1));
            d.state.resources.insert(ResourceId::new(id), st);
        }
        (s, clock, d)
    }

    /// Every source reports again at the clock's time with `llm` having `llm_pods` pods (no
    /// reading of its pods at all if `None`; the other models have none).
    fn gpu_world(s: &Shared, llm_pods: Option<u32>) {
        world_without_cluster(s, Some(0));
        push(
            s,
            "basestar-probe",
            vec![ObservedValue::ResourceUp {
                resource: "basestar-nfs".into(),
                up: true,
            }],
        );
        let mut v: Vec<ObservedValue> = ["mineru", "whisperx", "immich-ml"]
            .iter()
            .map(|m| ObservedValue::ModelPods {
                model: (*m).into(),
                pods: 0,
                ready_pods: 0,
            })
            .collect();
        v.extend(llm_pods.map(|pods| ObservedValue::ModelPods {
            model: "llm".into(),
            pods,
            ready_pods: pods,
        }));
        v.push(ObservedValue::ResourceUp {
            resource: "cuda-central".into(),
            up: true,
        });
        push(s, "cluster-pods", v);
    }

    fn gpu_veto_gauge(s: &Shared, state: &str) -> String {
        s.metrics
            .render()
            .lines()
            .find(|l| {
                l.starts_with("sched_resource_veto{")
                    && l.contains("resource=\"cuda-central\"")
                    && l.contains("reading=\"pods\"")
                    && l.contains(&format!("state=\"{state}\""))
            })
            .and_then(|l| l.rsplit(' ').next())
            .unwrap_or("absent")
            .to_string()
    }

    #[test]
    fn a_gpu_host_whose_models_still_have_pods_is_held_back_alerts_once_on_timeout_and_is_not_retried()
     {
        let (s, clock, mut d) = due_gpu();
        let wait = |reason| Step::Wait {
            resource: "cuda-central".into(),
            reason,
        };
        // Due, with a pod of a model running: held back, with the reading named.
        gpu_world(&s, Some(1));
        let r = s.tick(Millis(T0 + 1_000), &mut d);
        assert_eq!(r.error, None);
        assert!(
            r.steps.contains(&wait(sched_model::WaitReason::VetoInUse)),
            "{:?}",
            r.steps
        );
        assert_eq!(r.veto_held().len(), 1, "{r:?}");
        let h = r.host(&"cuda-central".into()).unwrap();
        assert_eq!(h.veto_readings, vec![VetoReading::Pods]);
        assert_eq!(
            h.veto(),
            Some(&VetoStatus::Waiting {
                since: Millis(T0 + 1_000),
                blocking: vec![VetoBlocker {
                    reading: VetoReading::Pods,
                    state: BlockerState::InUse
                }],
            })
        );
        assert_eq!(gpu_veto_gauge(&s, "waiting"), "1");
        assert_eq!(gpu_veto_gauge(&s, "stopped"), "0");
        // veto_timeout is 30 minutes: the tick that reaches it alerts once, with the reading.
        let at = T0 + 1_000 + 30 * 60_000;
        clock.set(Millis(at));
        gpu_world(&s, Some(1));
        let r = s.tick(Millis(at), &mut d);
        assert!(
            r.steps
                .contains(&wait(sched_model::WaitReason::ShutdownStopped)),
            "{:?}",
            r.steps
        );
        assert_eq!(r.veto_timeouts().len(), 1, "{r:?}");
        assert!(
            matches!(
                r.veto_timeouts()[0],
                HostEvent::VetoTimeout { readings, .. } if *readings == [VetoReading::Pods]
            ),
            "{r:?}"
        );
        assert_eq!(gpu_veto_gauge(&s, "stopped"), "1");
        assert!(
            s.metrics.render().contains(
                "sched_veto_timeouts_total{reading=\"pods\",resource=\"cuda-central\"} 1"
            )
        );
        // The pods are gone: the host stays up, no new alert, no second count.
        clock.set(Millis(at + 10_000));
        gpu_world(&s, Some(0));
        let r = s.tick(Millis(at + 10_000), &mut d);
        assert!(
            !r.steps.iter().any(|s| matches!(s, Step::PowerOff { .. })),
            "{:?}",
            r.steps
        );
        assert!(r.veto_timeouts().is_empty(), "{r:?}");
        assert!(r.veto_held()[0].stopped());
    }

    #[test]
    fn a_gpu_host_whose_model_pods_were_never_read_is_not_shut_down_until_they_are_read_as_none() {
        let (s, _, mut d) = due_gpu();
        gpu_world(&s, None);
        let r = s.tick(Millis(T0 + 1_000), &mut d);
        assert!(
            r.steps.contains(&Step::Wait {
                resource: "cuda-central".into(),
                reason: sched_model::WaitReason::VetoUnknown
            }),
            "{:?}",
            r.steps
        );
        assert!(!r.steps.iter().any(|s| matches!(s, Step::PowerOff { .. })));
        assert_eq!(
            r.veto_held()[0].blocking(),
            [VetoBlocker {
                reading: VetoReading::Pods,
                state: BlockerState::Unknown
            }]
        );
        assert!(r.reason.contains("pods:unknown"), "{}", r.reason);
        gpu_world(&s, Some(0));
        let r = s.tick(Millis(T0 + 2_000), &mut d);
        assert!(
            r.steps.contains(&Step::PowerOff {
                resource: "cuda-central".into()
            }),
            "{:?}",
            r.steps
        );
        assert!(r.veto_held().is_empty());
        assert_eq!(gpu_veto_gauge(&s, "waiting"), "0");
    }

    #[test]
    fn a_replayed_tick_does_not_count_a_shutdown_wait_timeout_again() {
        let (s, clock, mut d) = due_nfs();
        world(&s, Some(2));
        s.tick(Millis(T0 + 1_000), &mut d);
        let at = T0 + 1_000 + 30 * 60_000;
        clock.set(Millis(at));
        world(&s, Some(2));
        s.seed_reported(&d.reported);
        let outcome = s.decide(Millis(at), &d);
        assert_eq!(
            outcome.report.veto_timeouts().len(),
            1,
            "{:?}",
            outcome.report
        );
        let counted = || {
            s.metrics
                .render()
                .contains("sched_veto_timeouts_total{reading=\"mounts\",resource=\"basestar-nfs\"}")
        };
        // The journal gave the decision back: it is published again, not counted again.
        s.publish(&outcome, true);
        assert!(!counted(), "{}", s.metrics.render());
        s.publish(&outcome, false);
        assert!(
            s.metrics.render().contains(
                "sched_veto_timeouts_total{reading=\"mounts\",resource=\"basestar-nfs\"} 1"
            )
        );
    }

    /// A shared host whose power-on has outlasted its timeout and that reads down.
    fn nfs_powering_on_too_long() -> (Arc<Shared>, Arc<ManualClock>, Durable) {
        let (s, clock, mut d) = due_nfs();
        let mut st = sched_model::ResourceState::new(PowerPhase::PoweringOn {
            since: Millis(T0 - 20 * 60_000),
        });
        st.idle_since = None;
        d.state
            .resources
            .insert(ResourceId::new("basestar-nfs"), st);
        clock.set(Millis(T0));
        world(&s, Some(0));
        push(
            &s,
            "basestar-probe",
            vec![ObservedValue::ResourceUp {
                resource: "basestar-nfs".into(),
                up: false,
            }],
        );
        (s, clock, d)
    }

    fn timeouts_counted(s: &Shared) -> bool {
        s.metrics
            .render()
            .lines()
            .any(|l| l.starts_with("sched_power_timeouts_total{") && l.ends_with(" 1"))
    }

    #[test]
    fn a_power_timeout_is_one_event_and_a_replayed_tick_does_not_count_it_again() {
        let (s, _, mut d) = nfs_powering_on_too_long();
        s.seed_reported(&d.reported);
        let outcome = s.decide(Millis(T0), &d);
        let r = &outcome.report;
        assert_eq!(r.error, None, "{r:?}");
        // (The GPU host of this setup is found running and adopted in the same tick.)
        assert_eq!(r.adopted(), vec![ResourceId::new("cuda-central")]);
        assert!(
            matches!(
                r.power_timeouts()[..],
                [HostEvent::PowerTimeout { resource, during: PowerTransition::PoweringOn, started_at: Some(Millis(started)), .. }]
                    if resource.as_str() == "basestar-nfs" && *started == T0 - 20 * 60_000
            ),
            "{:?}",
            r.events
        );
        let nfs: ResourceId = "basestar-nfs".into();
        assert!(r.host(&nfs).unwrap().attention);
        assert!(
            r.kinds.contains(&"power_failed".to_string()),
            "{:?}",
            r.kinds
        );
        assert!(
            r.reason
                .contains("ALERT: powering_on of basestar-nfs timed out in this tick")
        );
        // The journal gave the decision back: published again, not counted again.
        s.publish(&outcome, true);
        assert!(!timeouts_counted(&s), "{}", s.metrics.render());
        s.publish(&outcome, false);
        assert!(timeouts_counted(&s), "{}", s.metrics.render());
        // The tick after it: still failed, no new event, so nothing more to alert.
        d.apply(&outcome);
        clock_tick(&s, &mut d, T0 + 1_000);
    }

    /// One more tick against `d` at `at`, after which the host must still read failed with
    /// no event.
    fn clock_tick(s: &Shared, d: &mut Durable, at: u64) {
        let r = s.tick(Millis(at), d);
        assert!(r.events.is_empty(), "{:?}", r.events);
        let h = r.host(&"basestar-nfs".into()).unwrap();
        assert_eq!((h.phase(), h.attention), ("failed", true));
        assert!(!r.reason.contains("timed out in this tick"), "{}", r.reason);
        assert!(r.reason.contains("FAILED power state"), "{}", r.reason);
    }

    #[test]
    fn a_report_carries_one_status_per_resource_and_survives_the_journal() {
        let (s, _, mut d) = due_nfs();
        world(&s, Some(2));
        let r = s.tick(Millis(T0 + 1_000), &mut d);
        let kinds: Vec<_> = r
            .hosts
            .iter()
            .map(|h| (h.resource.as_str(), h.kind))
            .collect();
        assert_eq!(
            kinds,
            vec![
                ("basestar-nfs", sched_model::ResourceKind::Shared),
                // The example's blade-runner has no `power:` section but still has a status.
                ("blade-runner", sched_model::ResourceKind::Shared),
                ("cuda-central", sched_model::ResourceKind::Exclusive)
            ]
        );
        let nfs = r.host(&"basestar-nfs".into()).unwrap();
        assert!(nfs.ours && nfs.managed && nfs.is_up());
        assert_eq!(nfs.wait, Some(sched_model::WaitReason::VetoInUse));
        // Nothing ticked twice: the whole report round-trips as the journal stores it.
        let json = serde_json::to_string(&r).unwrap();
        let back: TickReport = serde_json::from_str(&json).unwrap();
        assert_eq!(back, r);
    }

    /// A shared and an exclusive host in the given carried states, the example config with a
    /// `min_off` of 10 minutes, and a core that refuses to decide.
    fn failing_core(
        nfs: sched_model::ResourceState,
        gpu: sched_model::ResourceState,
    ) -> (Arc<Shared>, Arc<ManualClock>, Durable) {
        let yaml = sched_model::test_support::example_yaml_managed()
            .replace("      # min_off: 10m", "      min_off: 10m");
        let (s, clock) = shared_with(&yaml);
        let mut d = Durable::default();
        d.state
            .resources
            .insert(ResourceId::new("basestar-nfs"), nfs);
        d.state
            .resources
            .insert(ResourceId::new("cuda-central"), gpu);
        clock.set(Millis(T0));
        world(&s, Some(0));
        s.refuse_to_decide.store(true, Ordering::SeqCst);
        (s, clock, d)
    }

    #[test]
    fn a_failed_tick_still_says_what_a_person_must_attend_to() {
        use crate::toggle::{lockout_state, model_state, resource_state};
        use sched_sequencer::switch::HostRequest;
        let mut nfs = sched_model::ResourceState::new(PowerPhase::On {
            since: Millis(1),
            ours: true,
        });
        nfs.shutdown = Some(
            sched_model::Shutdown::idle_without_consumers(Millis(2))
                .vetoing(Millis(3))
                .stopped(Millis(4)),
        );
        let gpu = sched_model::ResourceState::new(PowerPhase::Failed {
            since: Millis(5),
            during: PowerTransition::ShuttingDown,
        });
        let (s, _, mut d) = failing_core(nfs, gpu);
        let r = s.tick(Millis(T0 + 1_000), &mut d);
        assert!(r.error.is_some(), "{r:?}");
        // Nothing became true: no events, and nothing counted.
        assert!(r.events.is_empty(), "{:?}", r.events);
        let text = s.metrics.render();
        assert!(!text.contains("sched_power_timeouts_total{"), "{text}");
        assert!(!text.contains("sched_veto_timeouts_total{"), "{text}");
        assert_eq!(r.hosts.len(), 2);
        let (nfs, gpu): (ResourceId, ResourceId) = ("basestar-nfs".into(), "cuda-central".into());
        let config = s.config.current();
        let now = Millis(T0 + 1_000);
        let v = resource_state(
            &config,
            &nfs,
            None,
            false,
            HostRequest::Read,
            &Some(r.clone()),
            now,
        );
        assert_eq!(v["needs_attention"], true, "{v}");
        assert_eq!(v["power"]["phase"], "on", "{v}");
        assert_eq!(v["wait"][0]["reason"], "shutdown_held_by_veto", "{v}");
        assert_eq!(v["wait"][0]["stopped"], true, "{v}");
        let v = resource_state(
            &config,
            &gpu,
            None,
            false,
            HostRequest::Read,
            &Some(r.clone()),
            now,
        );
        assert_eq!(v["needs_attention"], true, "{v}");
        assert_eq!(v["power"]["phase"], "failed", "{v}");
        // The status words of a switch read the carried state, not "no state yet".
        let lease = |id: &str, kind| sched_model::Lease {
            id: id.into(),
            resource: "cuda-central".into(),
            kind,
            granted_at: Millis(1),
            origin: sched_model::LeaseOrigin::Requested,
        };
        let model = lease(
            "m",
            sched_model::LeaseKind::ManualModel {
                model: "llm".into(),
            },
        );
        let mut rep = r.clone();
        rep.leases.push(model.clone());
        let v = model_state(
            &"llm".into(),
            &gpu,
            Some(&model),
            Some(false),
            &Some(rep),
            now,
        );
        assert_eq!(v["status"], "host_failed", "{v}");
        // A host that is powering on waits; one that is off is powered off under a lockout.
        let waiting = sched_model::ResourceState::new(PowerPhase::PoweringOn { since: Millis(1) });
        let off = sched_model::ResourceState::new(PowerPhase::Off { ours: false });
        let (s, _, mut d) = failing_core(off, waiting);
        let r = s.tick(Millis(T0 + 1_000), &mut d);
        let mut rep = r.clone();
        rep.leases.push(model.clone());
        let v = model_state(
            &"llm".into(),
            &gpu,
            Some(&model),
            Some(false),
            &Some(rep),
            now,
        );
        assert_eq!(v["status"], "waiting_for_host", "{v}");
        let lock = lease("l", sched_model::LeaseKind::Lockout);
        let mut lock_nfs = lock.clone();
        lock_nfs.resource = nfs.clone();
        let mut rep = r;
        rep.leases.push(lock_nfs.clone());
        let v = lockout_state(&config, &nfs, Some(&lock_nfs), &Some(rep), now);
        assert_eq!(v["status"], "powered_off", "{v}");
    }

    #[test]
    fn a_failed_tick_keeps_the_min_off_gauge_counting_down() {
        let mut nfs = sched_model::ResourceState::new(PowerPhase::Off { ours: false });
        nfs.off_since = Some(Millis(T0 - 4 * 60_000));
        // Managed and with a minimum: counted. The GPU host has none configured.
        let gpu = sched_model::ResourceState::new(PowerPhase::Off { ours: false });
        let (s, _, mut d) = failing_core(nfs, gpu);
        let gauge = |s: &Shared| {
            s.metrics
                .render()
                .lines()
                .find(|l| l.starts_with("sched_resource_min_off_remaining_seconds{"))
                .map(|l| l.to_string())
        };
        let r = s.tick(Millis(T0), &mut d);
        assert!(r.error.is_some());
        assert_eq!(
            gauge(&s).as_deref(),
            Some("sched_resource_min_off_remaining_seconds{resource=\"basestar-nfs\"} 360")
        );
        let r = s.tick(Millis(T0 + 60_000), &mut d);
        assert!(r.error.is_some());
        assert_eq!(
            gauge(&s).as_deref(),
            Some("sched_resource_min_off_remaining_seconds{resource=\"basestar-nfs\"} 300")
        );
        // No wait from a step (there are none), though the gauge is a level.
        assert!(r.host(&"basestar-nfs".into()).unwrap().wait.is_none());
    }

    #[test]
    fn a_failed_tick_recomputes_the_veto_gauge_from_the_carried_state_and_this_snapshot() {
        let (s, clock, mut d) = due_nfs();
        world(&s, Some(2));
        s.tick(Millis(T0 + 1_000), &mut d);
        assert_eq!(veto_gauge(&s, "waiting"), "1");
        // The pods are gone, but the core refuses: the carried shutdown still waits, and no
        // reading blocks it any more.
        clock.set(Millis(T0 + 2_000));
        world(&s, Some(0));
        s.refuse_to_decide.store(true, Ordering::SeqCst);
        let r = s.tick(Millis(T0 + 2_000), &mut d);
        assert!(r.error.is_some());
        assert_eq!(veto_gauge(&s, "waiting"), "0");
        assert!(r.events.is_empty());
    }

    #[test]
    fn a_report_without_a_state_has_no_statuses() {
        let (s, _) = shared();
        let r = s.state_error(
            Millis(T0),
            &[("scheduler_state".to_string(), "bad".to_string())],
        );
        assert!(r.error.is_some());
        assert!(r.hosts.is_empty() && r.events.is_empty(), "{r:?}");
        assert!(r.power_request_failures.is_empty());
    }

    #[test]
    fn a_host_whose_mounts_were_never_read_is_not_shut_down_and_the_reason_says_unknown() {
        let (s, _, mut d) = due_nfs();
        world(&s, None);
        let r = s.tick(Millis(T0 + 1_000), &mut d);
        assert_eq!(
            r.steps,
            vec![Step::Wait {
                resource: "basestar-nfs".into(),
                reason: sched_model::WaitReason::VetoUnknown
            }]
        );
        assert!(
            r.reason.contains("unknown counts as in use"),
            "{}",
            r.reason
        );
        assert!(!r.steps.iter().any(|s| matches!(s, Step::PowerOff { .. })));
        // Once the pods are read as none, the shutdown goes ahead.
        world(&s, Some(0));
        let r = s.tick(Millis(T0 + 2_000), &mut d);
        assert_eq!(
            r.steps,
            vec![Step::PowerOff {
                resource: "basestar-nfs".into()
            }]
        );
        assert!(r.veto_held().is_empty());
        assert_eq!(veto_gauge(&s, "waiting"), "0");
    }

    #[test]
    fn a_failed_shutdown_request_from_an_earlier_cycle_is_not_shown_against_a_later_shutdown() {
        let (s, _) = shared();
        let id = ResourceId::new("cuda-central");
        let shown = |s: &Shared, since: u64| {
            s.power_request_failures(&state_in(
                "cuda-central",
                PowerPhase::ShuttingDown {
                    since: Millis(since),
                },
            ))
        };
        let fail = |at| s.note_power_request(&id, PowerAction::Off, Millis(at), Some("refused"));

        // The request failed before the current shutdown began (an earlier cycle): dropped.
        fail(8_000);
        assert!(shown(&s, 9_000).is_empty());
        // One made when the current shutdown began, or after, is this transition's.
        fail(9_000);
        assert_eq!(shown(&s, 9_000).len(), 1);
        fail(9_500);
        assert_eq!(shown(&s, 9_000)[0].at, Millis(9_500));
    }

    #[test]
    fn a_failed_request_shows_only_against_the_transition_that_produced_it() {
        let (s, _) = shared();
        let id = ResourceId::new("cuda-central");
        let shown =
            |s: &Shared, p: PowerPhase| s.power_request_failures(&state_in("cuda-central", p));
        let fail = |action, at| {
            s.note_power_request(&id, action, Millis(at), Some("refused"));
        };

        // A failed shutdown request, then the host is observed down and a power-on starts in
        // the same tick: the old line is dropped, not shown against the new transition.
        fail(PowerAction::Off, 1_000);
        assert_eq!(
            shown(
                &s,
                PowerPhase::Failed {
                    since: Millis(5_000),
                    during: PowerTransition::ShuttingDown
                }
            )
            .len(),
            1
        );
        assert!(
            shown(
                &s,
                PowerPhase::PoweringOn {
                    since: Millis(9_000)
                }
            )
            .is_empty()
        );
        // And it does not come back if the resource later shuts down again.
        assert!(
            shown(
                &s,
                PowerPhase::ShuttingDown {
                    since: Millis(9_500)
                }
            )
            .is_empty()
        );

        // The same action from an earlier cycle is dropped too; one from this cycle is kept.
        fail(PowerAction::On, 8_000);
        assert!(
            shown(
                &s,
                PowerPhase::PoweringOn {
                    since: Millis(9_000)
                }
            )
            .is_empty()
        );
        fail(PowerAction::On, 9_000);
        assert_eq!(
            shown(
                &s,
                PowerPhase::PoweringOn {
                    since: Millis(9_000)
                }
            )
            .len(),
            1
        );
        // A failed power-on does not show against a failed shutdown.
        assert!(
            shown(
                &s,
                PowerPhase::Failed {
                    since: Millis(20_000),
                    during: PowerTransition::ShuttingDown
                }
            )
            .is_empty()
        );
    }

    // ---- adoption of a host found running, and the drain ----

    fn managed_world() -> (Arc<Shared>, Arc<ManualClock>) {
        shared_with(&sched_model::test_support::example_yaml_managed_nfs_only())
    }

    fn nfs_probe(s: &Shared, up: bool) {
        push(
            s,
            "basestar-probe",
            vec![ObservedValue::ResourceUp {
                resource: "basestar-nfs".into(),
                up,
            }],
        );
    }

    fn nfs() -> ResourceId {
        "basestar-nfs".into()
    }

    /// A new process (managed config, empty cache, its own clock at the same time) with the
    /// persisted state.
    fn restart_managed(d: &Durable) -> (Arc<Shared>, Arc<ManualClock>, Durable) {
        let text = serde_json::to_string(d).unwrap();
        let (s, clock) = managed_world();
        (s, clock, serde_json::from_str(&text).unwrap())
    }

    #[test]
    fn a_host_found_running_is_adopted_once_and_adopted_again_after_it_went_down_and_came_back() {
        let (s, clock) = managed_world();
        let mut d = Durable::default();
        world(&s, Some(0));
        let r = s.tick(clock.now(), &mut d);
        assert_eq!(r.adopted(), vec![nfs()], "{r:?}");
        let adopted: Vec<_> = d.leases.iter().filter(|l| l.is_adopted_host()).collect();
        assert_eq!(adopted.len(), 1, "{:?}", d.leases);
        assert_eq!(adopted[0].resource, nfs());
        assert!(
            r.leases.iter().any(|l| l.is_adopted_host()),
            "the planner saw it"
        );
        let st = d.state.resource(&nfs()).unwrap();
        assert!(st.ours() && st.is_on(), "{st:?}");
        // The GPU host is `control: manual` in this fixture: never adopted.
        assert!(
            d.state
                .resource(&"cuda-central".into())
                .is_some_and(|s| !s.ours())
        );
        // Once only.
        clock.advance(Millis(60_000));
        world(&s, Some(0));
        let r = s.tick(clock.now(), &mut d);
        assert!(
            r.adopted().is_empty() && r.adoptions_ended().is_empty(),
            "{r:?}"
        );
        assert_eq!(d.leases.len(), 1);
        // The person switches the host off by hand: the claim ends and it is not woken.
        clock.advance(Millis(60_000));
        world(&s, Some(0));
        nfs_probe(&s, false);
        let r = s.tick(clock.now(), &mut d);
        assert_eq!(r.adoptions_ended(), vec![nfs()], "{r:?}");
        assert!(d.leases.is_empty(), "{:?}", d.leases);
        assert!(
            !r.steps.iter().any(|s| matches!(s, Step::PowerOn { .. })),
            "{r:?}"
        );
        assert_eq!(
            d.state.resource(&nfs()).unwrap().power,
            PowerPhase::Off { ours: false }
        );
        // Brought back: adopted afresh.
        clock.advance(Millis(60_000));
        world(&s, Some(0));
        let r = s.tick(clock.now(), &mut d);
        assert_eq!(r.adopted(), vec![nfs()], "{r:?}");
        assert!(d.state.resource(&nfs()).unwrap().ours());
    }

    #[test]
    fn a_host_flowmanager_powered_on_or_found_again_after_a_restart_is_not_adopted() {
        let (s, clock) = managed_world();
        let mut d = Durable::default();
        let mut st = sched_model::ResourceState::new(PowerPhase::PoweringOn { since: Millis(1) });
        st.set_ours(true);
        d.state.resources.insert(nfs(), st);
        world(&s, Some(0));
        let r = s.tick(clock.now(), &mut d);
        assert!(r.adopted().is_empty(), "{r:?}");
        assert!(d.leases.is_empty());
        assert!(d.state.resource(&nfs()).unwrap().ours());
        // A restart: a new process with the persisted state and an empty cache.
        let (s2, clock2, d2) = restart_managed(&d);
        clock2.set(clock.now());
        let mut d2 = d2;
        world(&s2, Some(0));
        let r = s2.tick(clock2.now(), &mut d2);
        assert!(r.adopted().is_empty(), "{r:?}");
        assert!(d2.leases.is_empty());
    }

    // ---- a managed exclusive host: adoption, and only a host lease wakes it ----

    /// The example with both hosts under managed control.
    fn managed_world_all() -> (Arc<Shared>, Arc<ManualClock>) {
        shared_with(&sched_model::test_support::example_yaml_managed())
    }

    fn gpu() -> ResourceId {
        "cuda-central".into()
    }

    /// The pods source's reading of the GPU host (and the models, with no pods).
    fn gpu_reading(s: &Shared, up: bool) {
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
            up,
        });
        push(s, "cluster-pods", v);
    }

    #[test]
    fn an_exclusive_managed_host_found_running_is_adopted_once_and_not_again_after_a_blip() {
        let (s, clock) = managed_world_all();
        let mut d = Durable::default();
        world(&s, Some(0));
        let r = s.tick(clock.now(), &mut d);
        assert!(
            r.adopted().contains(&gpu()) && r.adopted().contains(&nfs()),
            "{r:?}"
        );
        let host_lease = |d: &Durable| {
            d.leases
                .iter()
                .filter(|l| l.resource == gpu() && l.is_adopted_host())
                .count()
        };
        assert_eq!(host_lease(&d), 1, "{:?}", d.leases);
        let st = d.state.resource(&gpu()).unwrap();
        assert!(st.ours() && st.is_on(), "{st:?}");
        // Once only.
        clock.advance(Millis(60_000));
        world(&s, Some(0));
        let r = s.tick(clock.now(), &mut d);
        assert!(
            r.adopted().is_empty() && r.adoptions_ended().is_empty(),
            "{r:?}"
        );
        // Seen down by hand: the claim ends and the host is not woken.
        clock.advance(Millis(60_000));
        world(&s, Some(0));
        gpu_reading(&s, false);
        let r = s.tick(clock.now(), &mut d);
        assert_eq!(r.adoptions_ended(), vec![gpu()], "{r:?}");
        assert_eq!(host_lease(&d), 0);
        assert!(
            !r.steps.iter().any(|s| matches!(s, Step::PowerOn { .. })),
            "{r:?}"
        );
        // Back up: found running again, adopted afresh.
        clock.advance(Millis(60_000));
        world(&s, Some(0));
        let r = s.tick(clock.now(), &mut d);
        assert!(r.adopted().contains(&gpu()), "{r:?}");
        // A host flowmanager powered on that is read down for a moment and then up again
        // is not adopted (it is still ours).
        d.leases.retain(|l| l.resource != gpu());
        let st = d.state.resources.get_mut(&gpu()).unwrap();
        st.power = PowerPhase::Off { ours: false };
        st.set_ours(true);
        clock.advance(Millis(60_000));
        world(&s, Some(0));
        let r = s.tick(clock.now(), &mut d);
        assert!(!r.adopted().contains(&gpu()), "{r:?}");
        assert_eq!(host_lease(&d), 0);
    }

    #[test]
    fn a_lockout_shuts_either_kind_of_host_down_once_adopts_nothing_and_clearing_it_finds_a_running_host()
     {
        let lockout = |res: &ResourceId| sched_model::Lease {
            id: sched_model::LeaseId::new(format!("lockout-{res}")),
            resource: res.clone(),
            kind: sched_model::LeaseKind::Lockout,
            granted_at: Millis(0),
            origin: sched_model::LeaseOrigin::Requested,
        };
        let (s, clock) = managed_world_all();
        let mut d = Durable::default();
        world(&s, Some(0));
        let r = s.tick(clock.now(), &mut d);
        assert!(
            r.adopted().contains(&gpu()) && r.adopted().contains(&nfs()),
            "{r:?}"
        );
        // Locked out while both are up and adopted (a manual lease each): the lease is no
        // excuse. The exclusive host has no model to wind down; the shared host withdraws.
        d.leases.push(lockout(&gpu()));
        d.leases.push(lockout(&nfs()));
        let mut off_steps = 0;
        for _ in 0..3 {
            clock.advance(Millis(5_000));
            world(&s, Some(0));
            let r = s.tick(clock.now(), &mut d);
            off_steps += r
                .steps
                .iter()
                .filter(|s| matches!(s, Step::PowerOff { .. }))
                .count();
        }
        assert_eq!(off_steps, 2, "each host is shut down once");
        // Both are seen down: settled under the lockout.
        clock.advance(Millis(5_000));
        world(&s, Some(0));
        gpu_reading(&s, false);
        nfs_probe(&s, false);
        let r = s.tick(clock.now(), &mut d);
        assert!(
            d.state.resource(&gpu()).unwrap().locked_down
                && d.state.resource(&nfs()).unwrap().locked_down,
            "{r:?}"
        );
        // Something else boots them: no power-off, no power-on, nothing adopted, no work.
        for _ in 0..2 {
            clock.advance(Millis(60_000));
            world(&s, Some(2));
            let r = s.tick(clock.now(), &mut d);
            assert!(r.adopted().is_empty(), "{r:?}");
            assert!(
                !r.steps
                    .iter()
                    .any(|s| matches!(s, Step::PowerOff { .. } | Step::PowerOn { .. })),
                "{r:?}"
            );
            assert!(
                d.leases.iter().all(|l| !l.is_adopted_host()),
                "{:?}",
                d.leases
            );
            assert!(d.state.resource(&gpu()).unwrap().is_on());
            assert!(d.state.resource(&nfs()).unwrap().is_on());
        }
        // Cleared while both are up: found running, adopted like any other host.
        d.leases.retain(|l| !l.is_lockout());
        clock.advance(Millis(60_000));
        world(&s, Some(0));
        let r = s.tick(clock.now(), &mut d);
        assert!(
            r.adopted().contains(&gpu()) && r.adopted().contains(&nfs()),
            "{r:?}"
        );
        assert!(!d.state.resource(&gpu()).unwrap().locked_down);
        assert!(!d.state.resource(&nfs()).unwrap().locked_down);
    }

    #[test]
    fn only_a_host_lease_powers_an_exclusive_managed_host_on_and_the_queues_still_choose_the_model()
    {
        let (s, clock) = managed_world_all();
        let mut d = Durable::default();
        // The GPU host is down: queued work, a failed item and an interactive session do not
        // wake it.
        let off = || {
            let mut st = sched_model::ResourceState::new(PowerPhase::Off { ours: false });
            st.set_ours(false);
            st
        };
        d.state.resources.insert(gpu(), off());
        let down = |s: &Shared| {
            world(s, Some(0));
            gpu_reading(s, false);
            ocr_demand(s, 5);
        };
        down(&s);
        d.leases.push(Lease {
            id: "interactive-1".into(),
            resource: gpu(),
            kind: sched_model::LeaseKind::Interactive {
                model: "llm".into(),
                expires_at: Millis(clock.now().as_u64() + 3_600_000),
            },
            granted_at: clock.now(),
            origin: sched_model::LeaseOrigin::Requested,
        });
        let r = s.tick(clock.now(), &mut d);
        assert!(
            !r.steps
                .iter()
                .any(|s| matches!(s, Step::PowerOn { resource } if *resource == gpu())),
            "{r:?}"
        );
        assert!(r.allowed_models.is_empty(), "{r:?}");
        d.leases.retain(|l| l.resource != gpu());
        // A host lease: the host is powered on, and then the queued work (ocr: mineru) picks
        // the model; the lease pins nothing.
        d.leases.push(Lease {
            id: "manual-host-1".into(),
            resource: gpu(),
            kind: sched_model::LeaseKind::ManualHost,
            granted_at: clock.now(),
            origin: sched_model::LeaseOrigin::Requested,
        });
        clock.advance(Millis(10_000));
        down(&s);
        let r = s.tick(clock.now(), &mut d);
        assert!(
            r.steps.contains(&Step::PowerOn { resource: gpu() }),
            "{r:?}"
        );
        clock.advance(Millis(10_000));
        world(&s, Some(0));
        ocr_demand(&s, 5);
        let r = s.tick(clock.now(), &mut d);
        assert!(r.forced.is_empty(), "{r:?}");
        assert_eq!(r.allowed_models, vec!["mineru".into()], "{r:?}");
    }

    #[test]
    fn under_manual_power_control_nothing_is_adopted() {
        let (s, clock) = shared();
        let mut d = Durable::default();
        world(&s, Some(0));
        let r = s.tick(clock.now(), &mut d);
        assert!(r.adopted().is_empty() && d.leases.is_empty(), "{r:?}");
    }

    /// The managed example, basestar adopted and up, with 5 paperless-ocr items queued and
    /// no mounting pods; `request` is applied to the stored state as the release handler does.
    fn adopted_with_work() -> (Arc<Shared>, Arc<ManualClock>, Durable) {
        let (s, clock) = managed_world();
        let mut d = Durable::default();
        world(&s, Some(0));
        s.tick(clock.now(), &mut d);
        // The person releases the lease (as the handler would) with the work still queued.
        d.leases.clear();
        clock.advance(Millis(60_000));
        world(&s, Some(0));
        ocr_demand(&s, 5);
        (s, clock, d)
    }

    #[test]
    fn a_drain_shuts_the_host_down_despite_queued_work_and_is_reported_and_logged() {
        // The same world without the drain: the queued work keeps the host up.
        let (s, clock, mut d) = adopted_with_work();
        let r = s.tick(clock.now(), &mut d);
        assert!(
            !r.steps.iter().any(|s| matches!(s, Step::PowerOff { .. })),
            "{r:?}"
        );
        assert!(r.drains().is_empty());

        let (s, clock, mut d) = adopted_with_work();
        let config = s.config.current();
        let rec = sched_sequencer::drain::request(
            &mut d.state,
            &config,
            &nfs(),
            sched_model::DrainMode::Immediate,
            clock.now(),
            &[],
        );
        assert_eq!(rec, sched_sequencer::drain::Requested::Recorded);
        let r = s.tick(clock.now(), &mut d);
        assert!(
            r.steps.contains(&Step::PowerOff { resource: nfs() }),
            "{r:?}"
        );
        assert_eq!(r.drains().len(), 1, "{r:?}");
        assert!(r.drains()[0].1.scaled_down && r.drains()[0].1.mode.as_str() == "immediate");
        assert_eq!(r.drains()[0].1.cause.as_str(), "requested");
        let nfs_consumers: Vec<_> = r
            .allowed_classes
            .iter()
            .filter(|c| {
                config
                    .class(c)
                    .is_some_and(|c| c.requires.iter().any(|q| q.resource == nfs()))
            })
            .collect();
        assert!(nfs_consumers.is_empty(), "{nfs_consumers:?}");
        // The drain survives a restart (it is in the persisted state) until the host is off.
        let (s2, clock2, d2) = restart_managed(&d);
        clock2.set(clock.now());
        let mut d2 = d2;
        assert!(d2.state.resource(&nfs()).unwrap().shutdown.is_some());
        clock2.advance(Millis(60_000));
        world(&s2, Some(0));
        nfs_probe(&s2, false);
        let r = s2.tick(clock2.now(), &mut d2);
        assert_eq!(
            d2.state.resource(&nfs()).unwrap().power,
            PowerPhase::Off { ours: false }
        );
        assert!(r.drains().is_empty(), "{r:?}");
        assert_eq!(d2.state.resource(&nfs()).unwrap().shutdown, None);
    }

    #[test]
    fn a_manual_lease_taken_during_a_drain_cancels_it() {
        let (s, clock, mut d) = adopted_with_work();
        let config = s.config.current();
        sched_sequencer::drain::request(
            &mut d.state,
            &config,
            &nfs(),
            sched_model::DrainMode::Graceful,
            clock.now(),
            &[],
        );
        // Work is queued, nothing in flight: the graceful drain would scale down at once,
        // but a manual lease arrives first.
        d.leases.push(Lease {
            id: "manual-host-1".into(),
            resource: nfs(),
            kind: sched_model::LeaseKind::ManualHost,
            granted_at: clock.now(),
            origin: sched_model::LeaseOrigin::Requested,
        });
        let r = s.tick(clock.now(), &mut d);
        assert_eq!(d.state.resource(&nfs()).unwrap().shutdown, None);
        assert!(r.drains().is_empty());
        assert!(
            !r.steps.iter().any(|s| matches!(s, Step::PowerOff { .. })),
            "{r:?}"
        );
    }

    #[test]
    fn a_host_that_is_up_and_not_ours_is_adopted_once_unless_ours_or_stopped_by_the_veto() {
        let stored = |shutdown: Option<sched_model::Shutdown>, ours: bool| {
            let mut st = sched_model::ResourceState::new(PowerPhase::On {
                since: Millis(1),
                ours: false,
            });
            st.set_ours(ours);
            st.shutdown = shutdown;
            let mut d = Durable::default();
            d.state.resources.insert(nfs(), st);
            d
        };
        // Up and not ours (a manual-to-managed reload, say), no lease: adopted, once.
        let (s, clock) = managed_world();
        let mut d = stored(None, false);
        world(&s, Some(0));
        let r = s.tick(clock.now(), &mut d);
        assert_eq!(r.adopted(), vec![nfs()], "{r:?}");
        assert!(d.state.resource(&nfs()).unwrap().ours());
        clock.advance(Millis(60_000));
        world(&s, Some(0));
        assert!(s.tick(clock.now(), &mut d).adopted().is_empty());
        assert_eq!(d.leases.len(), 1);
        // A host we powered on: ours, not adopted.
        let (s, clock) = managed_world();
        let mut d = stored(None, true);
        world(&s, Some(0));
        assert!(s.tick(clock.now(), &mut d).adopted().is_empty());
        assert!(d.leases.is_empty());
        // A shutdown stopped by mounting pods: left as it is.
        let (s, clock) = managed_world();
        let mut d = stored(
            Some(
                sched_model::Shutdown::idle_withdrawn(Millis(1))
                    .vetoing(Millis(1))
                    .stopped(Millis(2)),
            ),
            false,
        );
        world(&s, Some(1));
        assert!(s.tick(clock.now(), &mut d).adopted().is_empty());
        assert!(d.leases.is_empty());
    }

    #[test]
    fn adoption_runs_with_an_unreadable_job_book_and_its_lease_is_kept() {
        let (s, clock) = managed_world();
        let mut d = Durable {
            jobs_off: true,
            ..Durable::default()
        };
        world(&s, Some(0));
        let r = s.tick(clock.now(), &mut d);
        assert_eq!(r.adopted(), vec![nfs()], "{r:?}");
        // The lease is part of what the tick says to keep, and `apply` takes it in.
        assert_eq!(d.leases.len(), 1);
        assert!(d.leases[0].is_adopted_host());
        assert!(d.state.resource(&nfs()).unwrap().ours());
        // The next tick, still without a job book, finds the lease and adopts nothing more.
        clock.advance(Millis(60_000));
        world(&s, Some(0));
        let r = s.tick(clock.now(), &mut d);
        assert!(r.adopted().is_empty(), "{r:?}");
        assert_eq!(d.leases.len(), 1);
        // The host goes down: the adopted lease ends, jobs still off.
        clock.advance(Millis(60_000));
        world(&s, Some(0));
        nfs_probe(&s, false);
        let r = s.tick(clock.now(), &mut d);
        assert_eq!(r.adoptions_ended(), vec![nfs()], "{r:?}");
        assert!(d.leases.is_empty());
        assert!(!d.state.resource(&nfs()).unwrap().ours());
    }

    #[test]
    fn a_host_flowmanager_powered_on_that_blips_down_and_back_is_not_adopted_and_still_shuts_down()
    {
        for with_drain in [false, true] {
            let (s, clock) = managed_world();
            let mut d = Durable::default();
            let mut st = sched_model::ResourceState::new(PowerPhase::On {
                since: Millis(1),
                ours: false,
            });
            st.set_ours(true);
            if with_drain {
                st.shutdown = Some(sched_model::Shutdown::requested(
                    sched_model::DrainMode::Graceful,
                    clock.now(),
                ));
            }
            d.state.resources.insert(nfs(), st);
            world(&s, Some(0));
            if with_drain {
                // Work in flight keeps a graceful drain waiting, so the host is still up and
                // draining when the blip comes.
                push(
                    &s,
                    "paperless-ocr-pending",
                    vec![ObservedValue::Demand {
                        failed: 0,
                        class: "paperless-ocr".into(),
                        ready: 0,
                        in_flight: 1,
                        oldest_ready_at: None,
                    }],
                );
            }
            let r = s.tick(clock.now(), &mut d);
            assert!(r.adopted().is_empty(), "{r:?}");
            // Down for one tick, then up again.
            clock.advance(Millis(10_000));
            world(&s, Some(0));
            nfs_probe(&s, false);
            let r = s.tick(clock.now(), &mut d);
            assert_eq!(
                d.state.resource(&nfs()).unwrap().power,
                PowerPhase::Off { ours: true },
                "{r:?}"
            );
            assert!(d.state.resource(&nfs()).unwrap().ours());
            clock.advance(Millis(10_000));
            world(&s, Some(0));
            let r = s.tick(clock.now(), &mut d);
            assert!(
                r.adopted().is_empty() && d.leases.is_empty(),
                "{with_drain}: {r:?}"
            );
            assert!(d.state.resource(&nfs()).unwrap().ours());
            // The shutdown proceeds as before: after idle_grace (10m in the example), none
            // earlier.
            clock.advance(Millis(9 * 60_000));
            world(&s, Some(0));
            let r = s.tick(clock.now(), &mut d);
            assert!(
                !r.steps.iter().any(|s| matches!(s, Step::PowerOff { .. })),
                "{r:?}"
            );
            clock.advance(Millis(2 * 60_000));
            world(&s, Some(0));
            let r = s.tick(clock.now(), &mut d);
            assert!(
                r.steps.contains(&Step::PowerOff { resource: nfs() }),
                "{with_drain}: {r:?}"
            );
            assert!(r.adopted().is_empty());
        }
    }

    fn nfs_consumers_allowed(s: &Shared, r: &TickReport) -> bool {
        let config = s.config.current();
        r.allowed_classes.iter().any(|c| {
            config
                .class(c)
                .is_some_and(|c| c.requires.iter().any(|q| q.resource == nfs()))
        })
    }

    /// The managed example with basestar off and a backup job's hold on it (the hold is a
    /// shared hold of a holder that is not a job record, so the job step leaves it alone).
    fn woken_by_a_hold() -> (Arc<Shared>, Arc<ManualClock>, Durable) {
        let (s, clock) = managed_world();
        let mut d = Durable::default();
        d.leases.push(Lease {
            id: "hold-backup".into(),
            resource: nfs(),
            kind: sched_model::LeaseKind::SharedHold {
                holder: "backup".into(),
            },
            granted_at: clock.now(),
            origin: sched_model::LeaseOrigin::Requested,
        });
        world(&s, Some(0));
        nfs_probe(&s, false);
        let r = s.tick(clock.now(), &mut d);
        assert!(
            r.steps.contains(&Step::PowerOn { resource: nfs() }),
            "{r:?}"
        );
        // Seen up: powered on by us, so it is ours and not adopted.
        clock.advance(Millis(60_000));
        world(&s, Some(0));
        let r = s.tick(clock.now(), &mut d);
        assert!(
            r.adopted().is_empty() && nfs_consumers_allowed(&s, &r),
            "{r:?}"
        );
        (s, clock, d)
    }

    #[test]
    fn a_host_a_job_woke_scales_its_consumers_down_when_the_last_hold_ends_then_shuts_down() {
        let (s, clock, mut d) = woken_by_a_hold();
        // The hold keeps it up, however long.
        clock.advance(Millis(30 * 60_000));
        world(&s, Some(0));
        let r = s.tick(clock.now(), &mut d);
        assert!(
            r.drains().is_empty() && nfs_consumers_allowed(&s, &r),
            "{r:?}"
        );
        // The job ends: the idle clock starts; the consumers run until idle_grace (10m).
        d.leases.clear();
        clock.advance(Millis(60_000));
        world(&s, Some(0));
        let r = s.tick(clock.now(), &mut d);
        assert!(
            r.drains().is_empty() && nfs_consumers_allowed(&s, &r),
            "{r:?}"
        );
        // Grace over with pods still mounting: the idle drain starts, the consumers leave the
        // allowed set, the shutdown waits for the pods, and the report says whose drain it is.
        clock.advance(Millis(10 * 60_000));
        world(&s, Some(2));
        let r = s.tick(clock.now(), &mut d);
        assert_eq!(r.drains().len(), 1, "{r:?}");
        assert_eq!(
            (
                r.drains()[0].1.cause.as_str(),
                r.drains()[0].1.mode.as_str()
            ),
            ("idle", "immediate")
        );
        assert!(!nfs_consumers_allowed(&s, &r), "{:?}", r.allowed_classes);
        assert!(
            !r.steps.iter().any(|s| matches!(s, Step::PowerOff { .. })),
            "{r:?}"
        );
        // A restart in the middle of it: the drain is in the persisted state and goes on.
        let (s2, clock2, d2) = restart_managed(&d);
        let mut d2 = d2;
        clock2.set(Millis(clock.now().as_u64() + 30_000));
        world(&s2, Some(2));
        let r = s2.tick(clock2.now(), &mut d2);
        assert_eq!(r.drains().len(), 1, "{r:?}");
        assert!(!nfs_consumers_allowed(&s2, &r));
        // The pods are gone: shut down.
        clock2.advance(Millis(30_000));
        world(&s2, Some(0));
        let r = s2.tick(clock2.now(), &mut d2);
        assert!(
            r.steps.contains(&Step::PowerOff { resource: nfs() }),
            "{r:?}"
        );
        // Seen down: the drain ends.
        clock2.advance(Millis(30_000));
        world(&s2, Some(0));
        nfs_probe(&s2, false);
        let r = s2.tick(clock2.now(), &mut d2);
        assert!(r.drains().is_empty(), "{r:?}");
        assert_eq!(d2.state.resource(&nfs()).unwrap().shutdown, None);
    }

    #[test]
    fn a_new_job_lease_or_a_person_or_work_cancels_an_idle_drain_on_a_job_woken_host() {
        for how in ["job", "manual", "work"] {
            let (s, clock, mut d) = woken_by_a_hold();
            d.leases.clear();
            clock.advance(Millis(60_000));
            world(&s, Some(2));
            s.tick(clock.now(), &mut d);
            clock.advance(Millis(10 * 60_000));
            world(&s, Some(2));
            let r = s.tick(clock.now(), &mut d);
            assert_eq!(r.drains().len(), 1, "{how}: {r:?}");
            clock.advance(Millis(30_000));
            world(&s, Some(2));
            match how {
                "job" => d.leases.push(Lease {
                    id: "hold-backup2".into(),
                    resource: nfs(),
                    kind: sched_model::LeaseKind::SharedHold {
                        holder: "backup".into(),
                    },
                    granted_at: clock.now(),
                    origin: sched_model::LeaseOrigin::Requested,
                }),
                "manual" => d.leases.push(Lease {
                    id: "manual-host-1".into(),
                    resource: nfs(),
                    kind: sched_model::LeaseKind::ManualHost,
                    granted_at: clock.now(),
                    origin: sched_model::LeaseOrigin::Requested,
                }),
                _ => ocr_demand(&s, 4),
            }
            let r = s.tick(clock.now(), &mut d);
            assert!(r.drains().is_empty(), "{how}: {r:?}");
            assert_eq!(d.state.resource(&nfs()).unwrap().shutdown, None);
            assert!(nfs_consumers_allowed(&s, &r), "{how}: consumers are back");
            assert_eq!(d.state.resource(&nfs()).unwrap().idle_since, None);
        }
    }

    #[test]
    fn a_host_a_person_powered_on_never_idles_out() {
        let (s, clock) = managed_world();
        let mut d = Durable::default();
        for _ in 0..6 {
            world(&s, Some(0));
            let r = s.tick(clock.now(), &mut d);
            assert!(r.drains().is_empty(), "{r:?}");
            assert!(
                !r.steps.iter().any(|s| matches!(s, Step::PowerOff { .. })),
                "{r:?}"
            );
            assert!(nfs_consumers_allowed(&s, &r));
            clock.advance(Millis(20 * 60_000));
        }
        assert_eq!(d.leases.len(), 1, "the adopted lease holds it");
    }
}
