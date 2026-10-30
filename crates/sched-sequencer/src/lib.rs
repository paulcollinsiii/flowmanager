//! Per-resource state machines and safety rules that turn an intent and the current state
//! into the next safe steps, the next state, and the set KEDA is allowed to run.
//!
//! Pure: depends only on `sched-model`; no I/O, no async runtime, no wall-clock access.
//! Deterministic: the same inputs always produce the same decision. The sequencer applies
//! no scheduling policy beyond hysteresis guards (minimum dwell, idle grace); which model
//! to prefer is the planner's job.
//!
//! Safety rules enforced here, regardless of the intent it is given:
//! 1. At most one model of an exclusive resource is in the allowed set at a time. A switch
//!    withdraws the old model and allows the new one in the same tick.
//! 2. No decision is gated on pod state (but see 7: a shutdown veto, and recovery, which waits
//!    for a ready pod). GPU exclusivity is enforced by Kubernetes (every
//!    GPU workload requests the one GPU the node exposes), so the new model's pod stays
//!    pending until the old one has released the GPU. A model is allowed only on a host
//!    that is up, and a model the config does not have is never allowed (one removed while
//!    allowed leaves the set at the next tick).
//! 3. A host is powered down only when no lease holds it and no queued or in-flight work
//!    needs it, after `idle_grace`; the overrides are a reclaim and a drain. A drain (a person
//!    released the host's manual lease asking for the host to go down) ignores queued and
//!    in-flight work, never a lease: its consumers leave the allowed set at once (`immediate`)
//!    or when their in-flight work is done or `drain_timeout` has passed (`graceful`), and the
//!    host is shut down once no lease holds it and the mount veto (7) is clear.
//! 4. Power changes come only from the intent's `power_up` (manual requests, or classes
//!    that may change power), and a host is powered down only if the scheduler took
//!    responsibility for it. A host under `power.control: manual` never gets a power step
//!    and the scheduler never takes responsibility for it: its phase follows observation.
//!    A reclaim lease applies only to an exclusive resource with a power section; on any
//!    other resource it is ignored and reported as a wait.
//! 5. Safety-critical steps (allowing a model, powering a host off) need a fresh "up"
//!    reading of the host; otherwise the sequencer waits.
//! 6. A power transition (powering on, shutting down) is bounded by the resource's
//!    `power_on_timeout` / `shutdown_timeout`. On expiry the resource enters the failed phase:
//!    nothing is allowed on it, no power step is ever emitted for it again, and it leaves that
//!    phase only when a fresh reading shows the state the transition was after (or its
//!    control is manual). Other resources are unaffected.
//! 7. A host whose NFS mounts are checked (a `mounts` source names it) is powered off only
//!    on a fresh reading of zero mounting pods. A reading that is missing, stale or failed is
//!    unknown and counts as mounted: the shutdown waits, bounded by `power.mounts_timeout`,
//!    and on expiry the scheduler stops (the host stays up, the managed flag is dropped, no
//!    retry) until the host is seen down, a person resets it, or a new manual request takes it
//!    over. This is the only place a pod reading holds anything back, and it only ever vetoes.
//! 8. Early switching happens only away from classes that declare it safe, or when forced,
//!    and forced switches still give in-flight work its timeout. In-flight work is stopped
//!    only on a forced switch (reclaim or manual model lease), class by class, each when
//!    its own timeout has run out. In-flight work is what the queues report, not pods.
//! 9. After a shutdown flowmanager started and saw confirmed (the host observed down), an
//!    automatic power-on (a workload, a scheduled job's hold) waits for `power.min_off`
//!    (default 0: no wait); a manual lease does not wait. A host found down, or gone down by
//!    itself, is not penalised. The wait only delays a fresh decision; it is never a retry.

pub mod adopt;
mod exclusive;
pub mod jobs;
mod power;

use sched_model::{
    AllowedSet, ClassSpec, Config, Intent, ModePhase, ResourceSpec, ResourceState, SchedState,
    Snapshot, Step, WaitReason,
};

/// The outcome of one tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    /// At most one resource-scoped step per resource (a switch is the one pair: the
    /// `DrainModel` of the old model and the `AllowModel` of the new one), plus class-scoped
    /// steps (`PauseIntake`, `KillInFlight`, `RunRecovery`).
    pub steps: Vec<Step>,
    pub allowed: AllowedSet,
    pub state: SchedState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TickError {
    /// The snapshot was taken against a different config generation than the one given.
    GenerationMismatch { config: u64, snapshot: u64 },
}

impl std::fmt::Display for TickError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TickError::GenerationMismatch { config, snapshot } => write!(
                f,
                "snapshot is pinned to config generation {snapshot} but the config is generation {config}"
            ),
        }
    }
}

impl std::error::Error for TickError {}

/// Runs every resource's state machine once.
pub fn tick(
    config: &Config,
    snapshot: &Snapshot,
    state: &SchedState,
    intent: &Intent,
) -> Result<Decision, TickError> {
    if snapshot.config_generation != config.generation() {
        return Err(TickError::GenerationMismatch {
            config: config.generation(),
            snapshot: snapshot.config_generation,
        });
    }
    let mut next = SchedState::default();
    let mut steps = Vec::new();
    for res in config.resources() {
        let cx = Cx {
            config,
            snapshot,
            intent,
            res,
        };
        if let Some(st) = resource_tick(&cx, state.resource(&res.id), &mut steps) {
            next.resources.insert(res.id.clone(), st);
        }
    }
    let allowed = allowed_set(config, &next, intent);
    recovery_steps(config, snapshot, &allowed, &mut steps);
    Ok(Decision {
        steps,
        allowed,
        state: next,
    })
}

/// Everything one resource's machine reads.
pub(crate) struct Cx<'a> {
    pub config: &'a Config,
    pub snapshot: &'a Snapshot,
    pub intent: &'a Intent,
    pub res: &'a ResourceSpec,
}

impl Cx<'_> {
    pub fn now(&self) -> sched_model::Millis {
        self.snapshot.now
    }

    /// Whether this resource is reclaimed. A reclaim only exists for an exclusive resource
    /// with a power section; the intent cannot make it apply anywhere else.
    pub fn reclaimed(&self) -> bool {
        self.intent.reclaim.contains(&self.res.id) && self.config.supports_reclaim(&self.res.id)
    }
}

fn resource_tick(
    cx: &Cx,
    prev: Option<&ResourceState>,
    steps: &mut Vec<Step>,
) -> Option<ResourceState> {
    // A reclaim lease on a resource that cannot be reclaimed is ignored, visibly.
    if !cx.config.supports_reclaim(&cx.res.id)
        && cx
            .snapshot
            .leases
            .iter()
            .any(|l| l.resource == cx.res.id && l.is_reclaim() && l.is_active(cx.now()))
    {
        steps.push(Step::Wait {
            resource: cx.res.id.clone(),
            reason: WaitReason::ReclaimUnsupported,
        });
    }
    let mut st = match prev {
        Some(st) => st.clone(),
        None => power::initial(cx, steps)?,
    };
    power::reconcile(cx, &mut st);
    power::drain_step(cx, &mut st);
    let mut acted = false;
    if cx.res.kind == sched_model::ResourceKind::Exclusive && st.is_on() {
        acted = exclusive::step(cx, &mut st, steps);
    }
    power::step(cx, &mut st, steps, acted);
    Some(st)
}

/// `RunRecovery` for every class whose recovery action needs its model up, while the class is
/// in the allowed set (its model serving and not winding down, its hosts up, no reclaim) and
/// the snapshot reports failed items for it. Emitted on every such tick: the action is
/// idempotent and rate-limited by itself, and must re-check the allowed set before it acts.
/// Never for a class that is not allowed, so recovery never runs for a model that is not.
/// Held back while a fresh pods reading says one of the class's models has no ready pod:
/// allowed only means KEDA may start it, and a request sent before the pod serves is lost.
/// An unknown reading (no pods source, or a stale one) does not hold it back.
fn recovery_steps(
    config: &Config,
    snapshot: &Snapshot,
    allowed: &sched_model::AllowedSet,
    steps: &mut Vec<Step>,
) {
    for class in config.classes() {
        let needs_model = class
            .recovery
            .as_ref()
            .is_some_and(|r| r.needs_serving_model());
        if needs_model
            && allowed.classes.contains(&class.id)
            && sched_model::class_work(config, snapshot, class).failed > 0
            && !model_known_not_ready(config, snapshot, class)
        {
            steps.push(Step::RunRecovery {
                class: class.id.clone(),
            });
        }
    }
}

/// Whether a fresh pods reading shows a model the class requires with no ready pod.
fn model_known_not_ready(config: &Config, snapshot: &Snapshot, class: &ClassSpec) -> bool {
    let max_age = config.policy().host_reading_stale_after;
    class
        .requires
        .iter()
        .filter_map(|r| r.model.as_ref())
        .any(|m| {
            snapshot
                .fresh_model_pods(config, m, max_age)
                .is_some_and(|(_, ready)| ready == 0)
        })
}

/// What KEDA may run, derived from the new state alone.
fn allowed_set(config: &Config, state: &SchedState, intent: &Intent) -> AllowedSet {
    let mut set = AllowedSet::default();
    for (id, st) in &state.resources {
        if st.is_on()
            && let Some(m) = st.mode.allowed_model()
            && config.model(m).is_some_and(|spec| &spec.resource == id)
        {
            set.models.insert(m.clone());
        }
    }
    for class in config.classes() {
        if class_allowed(config, class, state, intent) {
            set.classes.insert(class.id.clone());
        }
    }
    set
}

fn class_allowed(config: &Config, class: &ClassSpec, state: &SchedState, intent: &Intent) -> bool {
    class.requires.iter().all(|req| {
        let Some(st) = state.resource(&req.resource) else {
            return false;
        };
        if !st.is_on()
            // A drain has scaled the consumers of this host down.
            || st.shutdown.is_some_and(|s| s.consumers_withdrawn())
            || (intent.reclaim.contains(&req.resource) && config.supports_reclaim(&req.resource))
        {
            return false;
        }
        match &req.model {
            // A shared resource, or a gate on an exclusive host: the host is up.
            None => true,
            Some(model) => matches!(
                &st.mode,
                ModePhase::Serving { model: m, wind_down: None, .. } if m == model
            ),
        }
    })
}
