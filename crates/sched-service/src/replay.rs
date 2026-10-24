//! Replay harness: feeds a sequence of frames through the planner and sequencer, threading
//! the state from tick to tick, and records every decision.
//!
//! A scenario describes the world as an observer would push it. Observations persist from
//! frame to frame until replaced (keyed by source and subject), so they age naturally; the
//! lease set persists until a frame supplies a new one. Expectations are optional per frame.

use std::collections::BTreeMap;

use sched_model::{
    AllowedSet, Config, Fidelity, Intent, Lease, Millis, Observation, ObservedValue, SchedState,
    Snapshot, SourceId, Step,
};
use sched_sequencer::Decision;
use serde::Deserialize;

use crate::core::{CoreError, decide};
use crate::invariants;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub description: String,
    #[serde(default)]
    pub initial_state: SchedState,
    /// Overrides `power.control` of every powered resource of the config the scenario is
    /// replayed against (scenarios of the power machine use `managed`).
    #[serde(default)]
    pub power_control: Option<sched_model::PowerControl>,
    pub frames: Vec<Frame>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Frame {
    pub at: Millis,
    /// Observations pushed at this frame. They stay in the snapshot until replaced.
    #[serde(default)]
    pub observe: Vec<Observe>,
    /// Re-stamps every observation held so far to this frame's time, as a healthy set of
    /// observers re-reporting would. Observations pushed in this frame are not affected.
    #[serde(default)]
    pub refresh: bool,
    /// Replaces the lease set when present.
    #[serde(default)]
    pub leases: Option<Vec<Lease>>,
    #[serde(default)]
    pub expect: Option<Expect>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observe {
    pub source: SourceId,
    /// Defaults to the frame time.
    #[serde(default)]
    pub observed_at: Option<Millis>,
    #[serde(default = "event")]
    pub fidelity: Fidelity,
    pub value: ObservedValue,
}

fn event() -> Fidelity {
    Fidelity::Event
}

/// What a frame's decision must be. Steps are compared as a set, ignoring order.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expect {
    #[serde(default)]
    pub steps: Option<Vec<Step>>,
    #[serde(default)]
    pub allowed: Option<AllowedSet>,
}

/// One recorded tick.
#[derive(Debug, Clone)]
pub struct FrameResult {
    pub at: Millis,
    pub snapshot: Snapshot,
    pub intent: Intent,
    pub decision: Decision,
    /// Violated safety invariants; always empty for a correct sequencer.
    pub violations: Vec<String>,
}

impl Scenario {
    pub fn from_yaml(text: &str) -> Result<Scenario, serde_yaml_ng::Error> {
        serde_yaml_ng::from_str(text)
    }
}

fn subject(v: &ObservedValue) -> String {
    match v {
        ObservedValue::Demand { class, .. } => format!("demand:{class}"),
        ObservedValue::ModelPods { model, .. } => format!("model_pods:{model}"),
        ObservedValue::ResourceUp { resource, .. } => format!("resource_up:{resource}"),
    }
}

/// Runs every frame and records the decisions. Does not check expectations.
pub fn run(config: &Config, scenario: &Scenario) -> Result<Vec<FrameResult>, CoreError> {
    let mut world: BTreeMap<(SourceId, String), Observation> = BTreeMap::new();
    let mut leases: Vec<Lease> = Vec::new();
    let mut state = scenario.initial_state.clone();
    let mut results = Vec::new();
    for frame in &scenario.frames {
        if frame.refresh {
            for o in world.values_mut() {
                o.observed_at = frame.at;
            }
        }
        for o in &frame.observe {
            let obs = Observation {
                source: o.source.clone(),
                observed_at: o.observed_at.unwrap_or(frame.at),
                fidelity: o.fidelity,
                value: o.value.clone(),
            };
            world.insert((o.source.clone(), subject(&o.value)), obs);
        }
        if let Some(l) = &frame.leases {
            leases = l.clone();
        }
        let snapshot = Snapshot {
            config_generation: config.generation(),
            now: frame.at,
            observations: world.values().cloned().collect(),
            leases: leases.clone(),
            unobservable: Default::default(),
        };
        let (intent, decision) = decide(config, &snapshot, &state)?;
        let violations = invariants::check(config, &snapshot, &state, &intent, &decision);
        state = decision.state.clone();
        results.push(FrameResult {
            at: frame.at,
            snapshot,
            intent,
            decision,
            violations,
        });
    }
    Ok(results)
}

/// Runs the scenario and compares each frame to its expectations. Returns every mismatch
/// and invariant violation, or the recorded results if there are none.
pub fn run_checked(config: &Config, scenario: &Scenario) -> Result<Vec<FrameResult>, Vec<String>> {
    let results = run(config, scenario).map_err(|e| vec![e.to_string()])?;
    let mut problems = Vec::new();
    for (i, (frame, res)) in scenario.frames.iter().zip(&results).enumerate() {
        for v in &res.violations {
            problems.push(format!("frame {i} (at {}): {v}", frame.at));
        }
        let Some(expect) = &frame.expect else {
            continue;
        };
        if let Some(want) = &expect.steps {
            let mut want = want.clone();
            let mut got = res.decision.steps.clone();
            want.sort();
            got.sort();
            if want != got {
                problems.push(format!(
                    "frame {i} (at {}): steps differ\n    expected: {want:?}\n    actual:   {got:?}",
                    frame.at
                ));
            }
        }
        if let Some(want) = &expect.allowed
            && want != &res.decision.allowed
        {
            problems.push(format!(
                "frame {i} (at {}): allowed set differs\n    expected: {want:?}\n    actual:   {:?}",
                frame.at, res.decision.allowed
            ));
        }
    }
    if problems.is_empty() {
        Ok(results)
    } else {
        Err(problems)
    }
}
