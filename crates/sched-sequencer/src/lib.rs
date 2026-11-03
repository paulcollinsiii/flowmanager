//! Per-resource state machines and safety rules that turn an intent and the current state
//! into the next safe steps, the next state, and the set KEDA is allowed to run.
//!
//! Pure: depends only on `sched-model`; no I/O, no async runtime, no wall-clock access.
//! Deterministic: the same inputs always produce the same decision. The sequencer applies
//! no scheduling policy beyond hysteresis guards (minimum dwell, idle grace); which model
//! to prefer is the planner's job.
//!
//! Safety rules enforced here, regardless of the intent it is given: the numbered invariants
//! of `planning/architecture.md` (*Safety invariants*), the one canonical list. The host
//! power ones follow the model in `planning/power-model.md`. The checker that tests them
//! against every decision is `sched-service/src/invariants.rs`; its messages carry the same
//! numbers.

pub mod adopt;
pub mod drain;
mod exclusive;
pub mod jobs;
mod power;
pub mod status;
pub mod switch;

use sched_model::{
    AllowedSet, ClassSpec, Config, Intent, ModePhase, ResourceSpec, ResourceState, SchedState,
    Snapshot, Step, WaitReason,
};

/// The outcome of one tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    /// At most one resource-scoped step per resource (a switch is the one pair: the
    /// `WithdrawModel` of the old model and the `AllowModel` of the new one), plus class-scoped
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

    /// Whether this resource is locked out. A lockout exists for any resource with a power
    /// section (shared or exclusive); the intent cannot make it apply anywhere else.
    pub fn locked_out(&self) -> bool {
        self.intent.lockout.contains(&self.res.id) && self.config.supports_lockout(&self.res.id)
    }
}

fn resource_tick(
    cx: &Cx,
    prev: Option<&ResourceState>,
    steps: &mut Vec<Step>,
) -> Option<ResourceState> {
    // A lockout lease on a resource that cannot be locked out is ignored, visibly.
    if !cx.config.supports_lockout(&cx.res.id)
        && sched_model::lockout_lease(&cx.snapshot.leases, &cx.res.id, cx.now()).is_some()
    {
        steps.push(Step::Wait {
            resource: cx.res.id.clone(),
            reason: WaitReason::LockoutUnsupported,
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
        power::withdrawal_step(cx, &mut st);
    }
    power::step(cx, &mut st, steps, acted);
    Some(st)
}

/// `RunRecovery` for every class whose recovery action needs its model up, while the class is
/// in the allowed set (its model serving and not winding down, its hosts up, no lockout) and
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

/// Whether a locked-out shared host is still giving its consumers' in-flight work its
/// `lockout_timeout` (the withdrawal of its lockout shutdown is pending): until then they
/// stay allowed, as for a graceful drain. Never true for an exclusive host, whose classes
/// leave at once while the mode machine winds the model down.
fn lockout_withdrawal_pending(
    config: &Config,
    st: &ResourceState,
    id: &sched_model::ResourceId,
) -> bool {
    config
        .resource(id)
        .is_some_and(|r| r.kind == sched_model::ResourceKind::Shared)
        && st
            .shutdown
            .is_some_and(|s| s.is_lockout() && s.withdrawal() == sched_model::Withdrawal::Pending)
}

fn class_allowed(config: &Config, class: &ClassSpec, state: &SchedState, intent: &Intent) -> bool {
    class.requires.iter().all(|req| {
        let Some(st) = state.resource(&req.resource) else {
            return false;
        };
        if !st.is_on()
            // A drain has scaled the consumers of this host down.
            || st.shutdown.is_some_and(|s| s.consumers_withdrawn())
            || (intent.lockout.contains(&req.resource)
                && config.supports_lockout(&req.resource)
                && !lockout_withdrawal_pending(config, st, &req.resource))
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
