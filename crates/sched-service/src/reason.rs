//! Turning a decision into words: a kind for metrics and a reason for logs. Pure.

use std::collections::BTreeSet;

use sched_model::{Intent, Millis, PowerWant, Step, WaitReason};

use crate::cache::InputAge;

/// The metric label of a step. A fixed set, so label cardinality is bounded.
pub fn step_kind(step: &Step) -> &'static str {
    match step {
        Step::Wait { .. } => "wait",
        Step::AllowModel { .. } => "allow_model",
        Step::DrainModel { .. } => "drain_model",
        Step::PowerOn { .. } => "power_on",
        Step::PowerOff { .. } => "power_off",
        Step::PauseIntake { .. } => "pause_intake",
        Step::KillInFlight { .. } => "kill_in_flight",
        Step::RunRecovery { .. } => "run_recovery",
    }
}

/// Metric label for a tick that produced no step at all.
pub const KIND_IDLE: &str = "idle";
/// Metric label for a tick that failed to produce a decision.
pub const KIND_ERROR: &str = "error";

/// Metric label for demand that is not known (its source never reported or is stale). A
/// tick with no step but unknown demand is this, never [`KIND_IDLE`]: unknown is not zero.
pub const KIND_DEMAND_UNKNOWN: &str = "demand_unknown";

/// Metric label for demand that is unknown only because this build has no observer for the
/// source kind. Expected for now; kept apart from [`KIND_DEMAND_UNKNOWN`].
pub const KIND_DEMAND_UNSUPPORTED: &str = "demand_unsupported";

/// The distinct kinds in a decision, sorted; [`KIND_IDLE`] when there are no steps and all
/// demand is known, [`KIND_DEMAND_UNKNOWN`] when the demand of a class that has an observer
/// is unknown, [`KIND_DEMAND_UNSUPPORTED`] when the only unknown demand is of a class whose
/// source has no observer in this build.
pub fn kinds(steps: &[Step], unknown_demand: bool, unsupported_demand: bool) -> Vec<&'static str> {
    let mut set: BTreeSet<&'static str> = steps.iter().map(step_kind).collect();
    if unknown_demand {
        set.insert(KIND_DEMAND_UNKNOWN);
    }
    if unsupported_demand {
        set.insert(KIND_DEMAND_UNSUPPORTED);
    }
    if !unknown_demand && !unsupported_demand && set.is_empty() {
        set.insert(KIND_IDLE);
    }
    set.into_iter().collect()
}

/// What every shadow reason starts with, so a log line cannot be mistaken for the state of
/// a live scheduler: the sequencer starts each tick from nothing.
pub const SHADOW_PREFIX: &str = "[shadow: next step from a fresh sequencer state] ";

fn wait_text(reason: WaitReason) -> &'static str {
    match reason {
        WaitReason::StaleObservation => "a pod or reachability observation is missing or stale",
        WaitReason::StaleDemand => {
            "a demand observation needed to prove no work is in flight is missing or stale"
        }
        WaitReason::Reclaimed => "the resource is reclaimed",
        WaitReason::ReclaimUnsupported => "a reclaim names a resource that cannot be reclaimed",
        WaitReason::HostNotUp => "the host is not up",
        WaitReason::PoweringOn => "the host is powering on",
        WaitReason::ShuttingDown => "the host is shutting down",
        WaitReason::MinDwell => "the loaded model has not served its minimum dwell",
        WaitReason::NotPreemptible => "the loaded model's active work is not preemptible",
        WaitReason::InFlightWork => "in-flight work is still running",
        WaitReason::OtherModelPresent => "another model still has pods",
        WaitReason::Loading => "the model is still loading",
        WaitReason::Draining => "the model is still draining",
    }
}

/// Why one step is taken, from the intent that produced it.
pub fn step_reason(step: &Step, intent: &Intent) -> String {
    let wants = |r: &sched_model::ResourceId| match intent.models.get(r) {
        Some(Some(m)) => format!("the planner wants {m} on {r}"),
        _ => format!("the planner wants no model on {r}"),
    };
    match step {
        Step::Wait { resource, reason } => {
            format!("wait on {resource}: {}", wait_text(*reason))
        }
        Step::AllowModel { resource, model } => {
            format!("allow {model} on {resource}: {}", wants(resource))
        }
        Step::DrainModel { resource, model } => {
            format!("drain {model} on {resource}: {}", wants(resource))
        }
        Step::PowerOn { resource } => {
            let why = match intent.power_up.get(resource) {
                Some(PowerWant::Manual) => "a manual lease holds it up",
                Some(PowerWant::Workload) => "a workload that may change power needs it",
                None => "the sequencer needs it up",
            };
            format!("power on {resource}: {why}")
        }
        Step::PowerOff { resource } => {
            format!("power off {resource}: nothing holds it and its idle grace has passed")
        }
        Step::PauseIntake { class } => {
            format!("pause intake for {class}: its model is being wound down")
        }
        Step::KillInFlight { class } => {
            format!("stop in-flight work of {class}: its timeout ran out")
        }
        Step::RunRecovery { class } => {
            format!("run recovery for {class}: its in-flight work was stopped")
        }
    }
}

/// The whole reason of a decision: what the planner wants, then why each step.
pub fn reason(
    intent: &Intent,
    steps: &[Step],
    unknown_demand: &[sched_model::ClassId],
    unsupported_demand: &[sched_model::ClassId],
) -> String {
    let wants: Vec<String> = intent
        .models
        .iter()
        .map(|(r, m)| match m {
            Some(m) => format!("{r}={m}"),
            None => format!("{r}=none"),
        })
        .collect();
    let mut out = format!("{SHADOW_PREFIX}intent [{}]", wants.join(", "));
    if !intent.power_up.is_empty() {
        let up: Vec<_> = intent.power_up.keys().map(|r| r.to_string()).collect();
        out.push_str(&format!(", power up [{}]", up.join(", ")));
    }
    if !intent.reclaim.is_empty() {
        let rc: Vec<_> = intent.reclaim.iter().map(|r| r.to_string()).collect();
        out.push_str(&format!(", reclaim [{}]", rc.join(", ")));
    }
    if steps.is_empty() {
        out.push_str("; no step needed");
    }
    for s in steps {
        out.push_str("; ");
        out.push_str(&step_reason(s, intent));
    }
    if !unknown_demand.is_empty() {
        let ids: Vec<_> = unknown_demand.iter().map(|c| c.to_string()).collect();
        out.push_str(&format!(
            "; demand UNKNOWN for [{}] (source never reported or stale): this is not idle",
            ids.join(", ")
        ));
    }
    if !unsupported_demand.is_empty() {
        let ids: Vec<_> = unsupported_demand.iter().map(|c| c.to_string()).collect();
        out.push_str(&format!(
            "; demand not observable yet for [{}] (no observer for the source kind): not idle either",
            ids.join(", ")
        ));
    }
    out
}

/// `source=age` pairs for a log line; `none` for a source that never reported, with its
/// last failure in parentheses.
pub fn format_inputs(ages: &[InputAge]) -> String {
    ages.iter()
        .map(|a| {
            let name = match a.capability {
                Some(c) => format!("{}/{c}", a.source),
                None => a.source.to_string(),
            };
            let age = match a.age {
                Some(Millis(ms)) => format!("{ms}ms"),
                None => "none".to_string(),
            };
            let note = if a.observer { "" } else { " (no observer)" };
            match &a.last_error {
                Some(e) => format!("{name}={age}{note} (error: {e})"),
                None => format!("{name}={age}{note}"),
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn intent() -> Intent {
        let mut i = Intent::default();
        i.models
            .insert("cuda-central".into(), Some("mineru".into()));
        i
    }

    #[test]
    fn kinds_are_distinct_sorted_and_idle_when_empty() {
        assert_eq!(kinds(&[], false, false), vec![KIND_IDLE]);
        assert_eq!(kinds(&[], true, false), vec![KIND_DEMAND_UNKNOWN]);
        assert_eq!(kinds(&[], false, true), vec![KIND_DEMAND_UNSUPPORTED]);
        assert_eq!(
            kinds(&[], true, true),
            vec![KIND_DEMAND_UNKNOWN, KIND_DEMAND_UNSUPPORTED]
        );
        let steps = vec![
            Step::Wait {
                resource: "a".into(),
                reason: WaitReason::Loading,
            },
            Step::AllowModel {
                resource: "b".into(),
                model: "m".into(),
            },
            Step::Wait {
                resource: "c".into(),
                reason: WaitReason::MinDwell,
            },
        ];
        assert_eq!(kinds(&steps, false, false), vec!["allow_model", "wait"]);
        assert_eq!(
            kinds(&steps, true, false),
            vec!["allow_model", "demand_unknown", "wait"]
        );
    }

    #[test]
    fn reasons_name_the_intent_and_the_blocker() {
        let steps = vec![
            Step::AllowModel {
                resource: "cuda-central".into(),
                model: "mineru".into(),
            },
            Step::Wait {
                resource: "basestar-nfs".into(),
                reason: WaitReason::HostNotUp,
            },
        ];
        let r = reason(&intent(), &steps, &[], &[]);
        assert!(
            r.starts_with(SHADOW_PREFIX) && r.contains("intent [cuda-central=mineru]"),
            "{r}"
        );
        assert!(
            r.contains("allow mineru on cuda-central: the planner wants mineru on cuda-central"),
            "{r}"
        );
        assert!(
            r.contains("wait on basestar-nfs: the host is not up"),
            "{r}"
        );
        assert!(reason(&Intent::default(), &[], &[], &[]).contains("no step needed"));
    }

    #[test]
    fn inputs_show_ages_missing_sources_and_errors() {
        let ages = vec![
            InputAge {
                source: "a".into(),
                capability: Some(sched_model::Capability::Pods),
                observer: true,
                age: Some(Millis(1200)),
                last_error: None,
            },
            InputAge {
                source: "b".into(),
                capability: None,
                observer: true,
                age: None,
                last_error: Some("HTTP 500".into()),
            },
        ];
        assert_eq!(
            format_inputs(&ages),
            "a/pods=1200ms, b=none (error: HTTP 500)"
        );
    }
}
