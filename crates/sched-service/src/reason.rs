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

/// The distinct kinds in a decision, sorted; [`KIND_IDLE`] when there are no steps.
pub fn kinds(steps: &[Step]) -> Vec<&'static str> {
    let set: BTreeSet<&'static str> = steps.iter().map(step_kind).collect();
    if set.is_empty() {
        vec![KIND_IDLE]
    } else {
        set.into_iter().collect()
    }
}

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
pub fn reason(intent: &Intent, steps: &[Step]) -> String {
    let wants: Vec<String> = intent
        .models
        .iter()
        .map(|(r, m)| match m {
            Some(m) => format!("{r}={m}"),
            None => format!("{r}=none"),
        })
        .collect();
    let mut out = format!("intent [{}]", wants.join(", "));
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
    out
}

/// `source=age` pairs for a log line; `none` for a source that never reported, with its
/// last failure in parentheses.
pub fn format_inputs(ages: &[InputAge]) -> String {
    ages.iter()
        .map(|a| {
            let age = match a.age {
                Some(Millis(ms)) => format!("{ms}ms"),
                None => "none".to_string(),
            };
            match &a.last_error {
                Some(e) => format!("{}={age} (error: {e})", a.source),
                None => format!("{}={age}", a.source),
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
        assert_eq!(kinds(&[]), vec![KIND_IDLE]);
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
        assert_eq!(kinds(&steps), vec!["allow_model", "wait"]);
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
        let r = reason(&intent(), &steps);
        assert!(r.starts_with("intent [cuda-central=mineru]"), "{r}");
        assert!(
            r.contains("allow mineru on cuda-central: the planner wants mineru on cuda-central"),
            "{r}"
        );
        assert!(
            r.contains("wait on basestar-nfs: the host is not up"),
            "{r}"
        );
        assert!(reason(&Intent::default(), &[]).contains("no step needed"));
    }

    #[test]
    fn inputs_show_ages_missing_sources_and_errors() {
        let ages = vec![
            InputAge {
                source: "a".into(),
                age: Some(Millis(1200)),
                last_error: None,
            },
            InputAge {
                source: "b".into(),
                age: None,
                last_error: Some("HTTP 500".into()),
            },
        ];
        assert_eq!(format_inputs(&ages), "a=1200ms, b=none (error: HTTP 500)");
    }
}
