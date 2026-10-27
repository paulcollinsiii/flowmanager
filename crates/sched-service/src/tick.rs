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
    AllowedSet, Capability, ClassId, Config, CreateJob, JobBook, JobEvent, Lease, LeaseId, Millis,
    ModelId, MountsVeto, PowerPhase, PowerTransition, PublishedAllowed, ResourceId, SchedState,
    SourceId, Step, WaitReason, class_work,
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

/// A resource in the failed power state, in a [`TickReport`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PowerFailed {
    pub resource: ResourceId,
    /// The transition that timed out.
    pub during: PowerTransition,
    /// When the timeout expired.
    pub since: Millis,
}

/// A power transition that timed out in this tick: the alert (see the `power_timeout` log
/// event and `sched_power_timeouts_total`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PowerTimeout {
    pub resource: ResourceId,
    pub during: PowerTransition,
    /// When the transition began, if the previous state said.
    pub started_at: Option<Millis>,
    pub at: Millis,
}

/// A power request whose call failed, as the `Power` object reported it to this process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PowerRequestFailure {
    pub resource: ResourceId,
    pub action: PowerAction,
    pub at: Millis,
    pub error: String,
}

/// The resources failed in `next`, and those that entered the failed state since `prev`.
fn power_report(prev: &SchedState, next: &SchedState) -> (Vec<PowerFailed>, Vec<PowerTimeout>) {
    let mut failed = Vec::new();
    let mut timeouts = Vec::new();
    for (id, st) in &next.resources {
        let PowerPhase::Failed { since, during } = st.power else {
            continue;
        };
        failed.push(PowerFailed {
            resource: id.clone(),
            during,
            since,
        });
        let before = prev.resource(id).map(|p| p.power);
        if !matches!(before, Some(PowerPhase::Failed { .. })) {
            timeouts.push(PowerTimeout {
                resource: id.clone(),
                during,
                started_at: match before {
                    Some(PowerPhase::PoweringOn { since } | PowerPhase::ShuttingDown { since }) => {
                        Some(since)
                    }
                    _ => None,
                },
                at: since,
            });
        }
    }
    (failed, timeouts)
}

/// A resource whose shutdown is held back because pods mount it (or may), in a
/// [`TickReport`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MountsHeld {
    pub resource: ResourceId,
    /// When the wait began (`stopped` false), or when flowmanager gave up (`stopped` true).
    pub since: Millis,
    /// The wait outlasted `mounts_timeout`: flowmanager stopped, the host stays up, and a
    /// person must act.
    pub stopped: bool,
}

/// A shutdown wait that timed out in this tick: the alert (see the `shutdown_mounts_timeout`
/// log event and `sched_mounts_timeouts_total`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MountsTimeout {
    pub resource: ResourceId,
    /// When the wait began, if the previous state said.
    pub waiting_since: Option<Millis>,
    pub at: Millis,
}

/// The resources whose shutdown is held back in `next`, and those that were stopped in this
/// tick (they were not in `prev`).
fn mounts_report(prev: &SchedState, next: &SchedState) -> (Vec<MountsHeld>, Vec<MountsTimeout>) {
    let mut held = Vec::new();
    let mut timeouts = Vec::new();
    for (id, st) in &next.resources {
        match st.mounts_veto {
            Some(MountsVeto::Waiting { since }) => held.push(MountsHeld {
                resource: id.clone(),
                since,
                stopped: false,
            }),
            Some(MountsVeto::Stopped { since }) => {
                held.push(MountsHeld {
                    resource: id.clone(),
                    since,
                    stopped: true,
                });
                let before = prev.resource(id).and_then(|p| p.mounts_veto);
                if !matches!(before, Some(MountsVeto::Stopped { .. })) {
                    timeouts.push(MountsTimeout {
                        resource: id.clone(),
                        waiting_since: match before {
                            Some(MountsVeto::Waiting { since }) => Some(since),
                            _ => None,
                        },
                        at: since,
                    });
                }
            }
            None => {}
        }
    }
    (held, timeouts)
}

/// A resource whose automatic power-on waits for `min_off`, in a [`TickReport`]: flowmanager
/// shut the host down and saw it down, and a want arrived before the minimum off time passed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MinOffWait {
    pub resource: ResourceId,
    /// When the host was seen down after flowmanager shut it down.
    pub since: Millis,
    /// The instant the wait ends (the earliest a power-on can be decided).
    pub until: Millis,
    /// How long is left at the tick's time.
    pub remaining: Millis,
}

/// The `min_off` waits of a decision: one per resource whose step is that wait.
fn min_off_report(
    config: &Config,
    state: &SchedState,
    steps: &[Step],
    now: Millis,
) -> Vec<MinOffWait> {
    steps
        .iter()
        .filter_map(|s| match s {
            Step::Wait {
                resource,
                reason: WaitReason::MinOff,
            } => {
                let st = state.resource(resource)?;
                let min_off = config.resource(resource)?.power.as_ref()?.min_off();
                let remaining = st.min_off_remaining(min_off, now);
                Some(MinOffWait {
                    resource: resource.clone(),
                    since: st.shutdown_confirmed_at?,
                    until: now.saturating_add(remaining),
                    remaining,
                })
            }
            _ => None,
        })
        .collect()
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
    /// Classes whose demand cannot be observed because this build has no observer for their
    /// source kind (schedule, session, ...) and the source has never reported. Nothing can
    /// push to it either (`observe` is ingress-private), so such a class counts as having
    /// no demand: idle, not busy.
    pub unsupported_demand: Vec<ClassId>,
    /// Classes whose source is of an observable kind but configured without `params`, and
    /// has never reported: nothing polls it. A config gap; counted as no demand, like
    /// `unsupported_demand`.
    pub unpolled_demand: Vec<ClassId>,
    /// Exclusive resources where the intent is forced: a manual model lease (or a reclaim)
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
    /// Resources in the failed power state: a power-on or shutdown outlasted its timeout and
    /// flowmanager stopped (it never retries; a person must act). Allowed: nothing on them.
    #[serde(default)]
    pub power_failed: Vec<PowerFailed>,
    /// Power transitions that timed out in this very tick (the alert fires once, then the
    /// resource stays in `power_failed`).
    #[serde(default)]
    pub power_timeouts: Vec<PowerTimeout>,
    /// Power requests that failed (the call to the router or the host), for resources still
    /// powering on, shutting down or failed. A failed request is not retried; the host is
    /// followed from its reachability and the transition times out.
    #[serde(default)]
    pub power_request_failures: Vec<PowerRequestFailure>,
    /// Resources whose shutdown is held back because pods mount their NFS server (or the
    /// count is unknown): waiting, bounded by `mounts_timeout`, or stopped after it ran out
    /// (the host stays up; flowmanager does not retry; a person must act).
    #[serde(default)]
    pub mounts_held: Vec<MountsHeld>,
    /// Shutdown waits that timed out in this very tick (the alert fires once, then the
    /// resource stays in `mounts_held` as stopped).
    #[serde(default)]
    pub mounts_timeouts: Vec<MountsTimeout>,
    /// Resources whose automatic power-on is held back for `min_off` after a shutdown
    /// flowmanager started: the time left per resource. A manual lease never waits.
    #[serde(default)]
    pub min_off_waits: Vec<MinOffWait>,
    /// What the scheduled jobs did in this tick (alerts among them).
    #[serde(default)]
    pub job_events: Vec<JobEvent>,
    /// Every configured job and its state after this tick.
    #[serde(default)]
    pub jobs: Vec<crate::jobs::JobSummary>,
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
    /// The scheduled jobs' state.
    #[serde(default)]
    pub jobs: JobBook,
    /// The stored job book could not be read: the job step is skipped (jobs and their holds
    /// stay as they are) while scheduling the GPU and NFS goes on.
    #[serde(default)]
    pub jobs_off: bool,
}

impl Durable {
    /// Takes in what a tick says to persist.
    pub fn apply(&mut self, outcome: &TickOutcome) {
        self.state = outcome.report.state.clone();
        self.reported = outcome.reported.clone();
        if !self.jobs_off {
            self.jobs = outcome.jobs.clone();
            self.leases = outcome.leases.clone();
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
            unsupported_demand: vec![],
            unpolled_demand: vec![],
            forced: vec![],
            kinds: vec![reason::KIND_ERROR.to_string()],
            reason: format!(
                "no decision: the persisted state cannot be read ({text}); nothing is published and the stored state is left as it is (see the README section 'Versions, and recovering the persisted state')"
            ),
            steps: vec![],
            not_executed: vec![],
            allowed_models: vec![],
            allowed_classes: vec![],
            leases: vec![],
            ignored_leases: vec![],
            state: SchedState::default(),
            power_failed: vec![],
            power_timeouts: vec![],
            power_request_failures: vec![],
            mounts_held: vec![],
            mounts_timeouts: vec![],
            min_off_waits: vec![],
            job_events: vec![],
            jobs: vec![],
            inputs: vec![],
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
    /// serving and not winding down, its hosts are up, no reclaim. False until a tick has
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
        let (jobs, all_leases, job_events, creates) = if durable.jobs_off {
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
        for c in config.classes() {
            let Some(d) = &c.demand else { continue };
            if snapshot.unobservable.contains(&d.source) {
                match config.source(&d.source).map(observer_state) {
                    Some(ObserverState::NoParams) => gaps.unpolled.push(c.id.clone()),
                    _ => gaps.unsupported.push(c.id.clone()),
                }
            } else if !class_work(&config, &snapshot, c).known {
                gaps.unknown.push(c.id.clone());
            }
        }
        let (kinds, reason, steps, allowed, state, forced, error) =
            match decide(&config, &snapshot, &durable.state) {
                Ok((intent, decision)) => {
                    // Power wishes of hosts under manual control are never acted on; the
                    // reason does not list them.
                    let mut shown = intent.clone();
                    shown.power_up.retain(|r, _| config.power_managed(r));
                    (
                        reason::kinds(&decision.steps, &gaps, !reason::pinned(&intent).is_empty())
                            .into_iter()
                            .map(String::from)
                            .collect(),
                        reason::reason(&shown, &decision.steps, &gaps),
                        decision.steps,
                        decision.allowed,
                        decision.state,
                        intent.forced.iter().cloned().collect(),
                        None,
                    )
                }
                // The previous state stays, and so does the previous publication.
                Err(e) => (
                    vec![reason::KIND_ERROR.to_string()],
                    format!("no decision: {e}"),
                    Vec::new(),
                    AllowedSet::default(),
                    durable.state.clone(),
                    Vec::new(),
                    Some(e.to_string()),
                ),
            };
        let (power_failed, power_timeouts) = power_report(&durable.state, &state);
        let power_request_failures = self.power_request_failures(&state);
        let (mounts_held, mounts_timeouts) = mounts_report(&durable.state, &state);
        let (kinds, reason) = if error.is_none() {
            reason::with_power(
                kinds,
                reason,
                &power_failed,
                &power_timeouts,
                &power_request_failures,
            )
        } else {
            (kinds, reason)
        };
        let (kinds, reason) = if error.is_none() {
            reason::with_mounts(kinds, reason, &mounts_held, &mounts_timeouts)
        } else {
            (kinds, reason)
        };
        let min_off_waits = if error.is_none() {
            min_off_report(&config, &state, &steps, now)
        } else {
            Vec::new()
        };
        let reason = if min_off_waits.is_empty() {
            reason
        } else {
            reason::with_min_off(reason, &min_off_waits)
        };
        let report = TickReport {
            config_generation: config.generation(),
            at: now,
            unknown_demand: gaps.unknown,
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
            power_failed,
            power_timeouts,
            power_request_failures,
            mounts_held,
            mounts_timeouts,
            min_off_waits,
            inputs: ages.iter().map(InputReport::from).collect(),
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
        let config = self.config.current();
        self.metrics.set_power_failed(&r.state, |id| {
            config.resource(id).is_some_and(|res| res.power.is_some())
        });
        self.metrics
            .set_mounts_veto(&r.state, |id| config.mounts_checked(id));
        self.metrics.set_min_off_remaining(&r.state, r.at, |id| {
            config
                .resource(id)
                .filter(|_| config.power_managed(id))
                .and_then(|res| res.power.as_ref())
                .map(|p| p.min_off())
                .filter(|m| *m > Millis::ZERO)
        });
        if !replayed {
            for t in &r.mounts_timeouts {
                self.metrics.mounts_timeout(t.resource.as_str());
            }
        }
        if !replayed {
            for t in &r.power_timeouts {
                self.metrics
                    .power_timeout(t.resource.as_str(), t.during.as_str());
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
        unsupported_demand = ?ids(&r.unsupported_demand),
        unpolled_demand = ?ids(&r.unpolled_demand),
        "tick"
    );
    for t in &r.power_timeouts {
        let wanted = match t.during {
            PowerTransition::PoweringOn => "up",
            PowerTransition::ShuttingDown => "down",
        };
        // The alert-grade event is for the tick that decided it. A replay (the decision came
        // back from the journal) logs the same fact at info level under another event name,
        // so a log-based alert fires once.
        if replayed {
            tracing::info!(
                event = "power_timeout_replayed",
                resource = %t.resource,
                during = t.during.as_str(),
                started_at_ms = ?t.started_at.map(Millis::as_u64),
                at_ms = t.at.as_u64(),
                "replayed tick: {} of {} timed out (alerted when first decided); it is not retried",
                t.during.as_str(),
                t.resource,
            );
        } else {
            tracing::error!(
                event = "power_timeout",
                resource = %t.resource,
                during = t.during.as_str(),
                started_at_ms = ?t.started_at.map(Millis::as_u64),
                at_ms = t.at.as_u64(),
                "ALERT: {} of {} did not complete in time; flowmanager stops here and will not retry any power action for it: nothing is allowed on it until it is observed {} or a person acts",
                t.during.as_str(),
                t.resource,
                wanted,
            );
        }
    }
    for t in &r.mounts_timeouts {
        // As for power timeouts: the alert-grade event is for the tick that decided it; a
        // replay logs the same fact at info level under another event name.
        if replayed {
            tracing::info!(
                event = "shutdown_mounts_timeout_replayed",
                resource = %t.resource,
                waiting_since_ms = ?t.waiting_since.map(Millis::as_u64),
                at_ms = t.at.as_u64(),
                "replayed tick: the shutdown of {} timed out waiting for pods to unmount (alerted when first decided); it is not retried",
                t.resource,
            );
        } else {
            tracing::error!(
                event = "shutdown_mounts_timeout",
                resource = %t.resource,
                waiting_since_ms = ?t.waiting_since.map(Millis::as_u64),
                at_ms = t.at.as_u64(),
                "ALERT: pods still mount (or may mount) {} after mounts_timeout; flowmanager did NOT shut it down and stops here: it will not retry by itself, the host stays up until a person acts (or a new manual request takes it over)",
                t.resource,
            );
        }
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
    use sched_model::{Fidelity, ObservedValue};

    const T0: u64 = 10_000_000;

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
        assert_eq!(gpu.power, sched_model::PowerPhase::Off);
        assert!(!gpu.managed);
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
            let mut st = sched_model::ResourceState::new(PowerPhase::On { since: Millis(1) });
            st.managed = managed;
            st.host_idle_since = managed.then_some(Millis(1));
            d.state.resources.insert(ResourceId::new(id), st);
        }
        (s, clock, d)
    }

    /// Every source reports again at the clock's time: both hosts up, nothing queued, and
    /// `mounts` pods mounting basestar (no reading at all if `None`).
    fn world(s: &Shared, mounts: Option<u32>) {
        cluster(s);
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
                l.starts_with("sched_resource_mounts_veto{")
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
        assert_eq!(r.steps, vec![wait(sched_model::WaitReason::PodsMounted)]);
        assert_eq!(
            r.mounts_held,
            vec![MountsHeld {
                resource: "basestar-nfs".into(),
                since: Millis(T0 + 1_000),
                stopped: false
            }]
        );
        assert!(r.mounts_timeouts.is_empty());
        assert!(r.reason.contains("held back"), "{}", r.reason);
        assert_eq!(veto_gauge(&s, "waiting"), "1");
        assert_eq!(veto_gauge(&s, "stopped"), "0");

        // mounts_timeout is 30 minutes: the tick that reaches it alerts, once.
        let at = T0 + 1_000 + 30 * 60_000;
        clock.set(Millis(at));
        world(&s, Some(2));
        let r = s.tick(Millis(at), &mut d);
        assert_eq!(
            r.steps,
            vec![wait(sched_model::WaitReason::ShutdownStopped)]
        );
        assert_eq!(r.mounts_timeouts.len(), 1, "{r:?}");
        assert_eq!(r.mounts_timeouts[0].waiting_since, Some(Millis(T0 + 1_000)));
        assert!(r.kinds.contains(&"shutdown_stopped".to_string()), "{r:?}");
        assert!(r.reason.contains("ALERT"), "{}", r.reason);
        assert!(r.reason.contains("NOT shut down"), "{}", r.reason);
        assert_eq!(veto_gauge(&s, "stopped"), "1");
        assert_eq!(veto_gauge(&s, "waiting"), "0");
        assert!(
            s.metrics
                .render()
                .contains("sched_mounts_timeouts_total{resource=\"basestar-nfs\"} 1")
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
        assert!(r.mounts_timeouts.is_empty(), "{r:?}");
        assert!(r.mounts_held[0].stopped);
        assert!(
            s.metrics
                .render()
                .contains("sched_mounts_timeouts_total{resource=\"basestar-nfs\"} 1")
        );
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
            outcome.report.mounts_timeouts.len(),
            1,
            "{:?}",
            outcome.report
        );
        let counted = || {
            s.metrics
                .render()
                .contains("sched_mounts_timeouts_total{resource=\"basestar-nfs\"}")
        };
        // The journal gave the decision back: it is published again, not counted again.
        s.publish(&outcome, true);
        assert!(!counted(), "{}", s.metrics.render());
        s.publish(&outcome, false);
        assert!(
            s.metrics
                .render()
                .contains("sched_mounts_timeouts_total{resource=\"basestar-nfs\"} 1")
        );
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
                reason: sched_model::WaitReason::MountsUnknown
            }]
        );
        assert!(
            r.reason.contains("unknown counts as mounted"),
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
        assert!(r.mounts_held.is_empty());
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
}
