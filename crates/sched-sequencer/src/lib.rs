//! Per-resource state machines and safety rules that turn an intent and the current state
//! into the next safe steps, the next state, and the set KEDA is allowed to run.
//!
//! Pure: depends only on `sched-model`; no I/O, no async runtime, no wall-clock access.
//! Deterministic: the same inputs always produce the same decision. The sequencer applies
//! no scheduling policy beyond hysteresis guards (minimum dwell, idle grace); which model
//! to prefer is the planner's job.
//!
//! Safety rules enforced here, regardless of the intent it is given:
//! 1. At most one model of an exclusive resource is allowed at a time.
//! 2. A model is allowed only after every other model of the resource is observed with
//!    zero pods, by an observation no older than `safety_max_age`.
//! 3. A host is powered down only when no lease holds it and no queued or in-flight work
//!    needs it, after `idle_grace`; the one override is a reclaim.
//! 4. Power changes come only from the intent's `power_up` (manual requests, or classes
//!    that may change power), and a host is powered down only if the scheduler took
//!    responsibility for it.
//! 5. Safety-critical steps need fresh observations; otherwise the sequencer waits.
//! 8. Early switching happens only away from classes that declare it safe, or when forced,
//!    and forced switches still give in-flight work its timeout.

mod exclusive;
mod power;

use sched_model::{
    AllowedSet, ClassSpec, Config, Intent, ModePhase, ModelId, ResourceSpec, ResourceState,
    SchedState, Snapshot, Step, WaitReason,
};

/// The outcome of one tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    /// At most one resource-scoped step per resource, plus class-scoped steps
    /// (`PauseIntake`, `KillInFlight`, `RunRecovery`).
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

    pub fn reclaimed(&self) -> bool {
        self.intent.reclaim.contains(&self.res.id)
    }
}

fn resource_tick(
    cx: &Cx,
    prev: Option<&ResourceState>,
    steps: &mut Vec<Step>,
) -> Option<ResourceState> {
    let mut st = match prev {
        Some(st) => st.clone(),
        None => power::initial(cx, steps)?,
    };
    power::reconcile(cx, &mut st);
    let mut acted = false;
    if cx.res.kind == sched_model::ResourceKind::Exclusive && st.is_on() {
        acted = exclusive::step(cx, &mut st, steps);
    }
    power::step(cx, &mut st, steps, acted);
    Some(st)
}

/// What KEDA may run, derived from the new state alone.
fn allowed_set(config: &Config, state: &SchedState, intent: &Intent) -> AllowedSet {
    let mut set = AllowedSet::default();
    for st in state.resources.values() {
        if st.is_on()
            && let Some(m) = st.mode.allowed_model()
        {
            set.models.insert(m.clone());
        }
    }
    for class in config.classes() {
        if class_allowed(class, state, intent) {
            set.classes.insert(class.id.clone());
        }
    }
    set
}

fn class_allowed(class: &ClassSpec, state: &SchedState, intent: &Intent) -> bool {
    class.requires.iter().all(|req| {
        let Some(st) = state.resource(&req.resource) else {
            return false;
        };
        if !st.is_on() || intent.reclaim.contains(&req.resource) {
            return false;
        }
        match &req.model {
            None => true,
            Some(model) => matches!(
                &st.mode,
                ModePhase::Serving { model: m, wind_down: None, .. } if m == model
            ),
        }
    })
}

/// Every model of the resource except `except` must be observed with zero pods, freshly.
pub(crate) fn others_zero(cx: &Cx, except: Option<&ModelId>) -> Result<(), WaitReason> {
    let max_age = cx.config.policy().safety_max_age;
    for m in cx
        .config
        .models_of(&cx.res.id)
        .filter(|m| Some(&m.id) != except)
    {
        match cx.snapshot.fresh_model_pods(&m.id, max_age) {
            None => return Err(WaitReason::StaleObservation),
            Some((0, _)) => {}
            Some(_) => return Err(WaitReason::OtherModelPresent),
        }
    }
    Ok(())
}
