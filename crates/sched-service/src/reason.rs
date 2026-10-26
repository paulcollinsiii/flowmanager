//! Turning a decision into words: a kind for metrics and a reason for logs. Pure.

use std::collections::BTreeSet;

use sched_model::{Intent, Millis, ModelId, PowerWant, ResourceId, Step, WaitReason};
use sched_observe::ObserverState;

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

/// Metric label for a tick that produced no step at all, with no class's demand unknown
/// and no model pinned by a manual lease.
pub const KIND_IDLE: &str = "idle";
/// Metric label for a tick that failed to produce a decision.
pub const KIND_ERROR: &str = "error";
/// Metric label for a tick in which a manual model lease pins a model on a resource (the
/// intent is forced to that model). Such a tick is never [`KIND_IDLE`]: the model is held
/// on purpose however little work it has.
pub const KIND_PINNED: &str = "pinned";

/// Metric label for demand that is not known although something should be reporting it:
/// an observer runs for the source (or the source has reported before) and it is silent or
/// stale. A tick with no step but unknown demand is this, never [`KIND_IDLE`]: unknown is
/// not zero.
pub const KIND_DEMAND_UNKNOWN: &str = "demand_unknown";

/// Metric label for demand of a source whose kind has no observer in this build and that
/// has never reported. It counts as no demand; kept apart from [`KIND_DEMAND_UNKNOWN`].
pub const KIND_DEMAND_UNSUPPORTED: &str = "demand_unsupported";

/// Metric label for demand of a source of an observable kind that was configured without
/// `params`, so nothing polls it, and that has never reported. A config gap; it counts as
/// no demand.
pub const KIND_DEMAND_UNPOLLED: &str = "demand_unpolled";

/// Classes whose demand is unknown, by why. None of them is "zero demand".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DemandGaps {
    pub unknown: Vec<sched_model::ClassId>,
    pub unsupported: Vec<sched_model::ClassId>,
    pub unpolled: Vec<sched_model::ClassId>,
}

impl DemandGaps {
    pub fn is_empty(&self) -> bool {
        self.unknown.is_empty() && self.unsupported.is_empty() && self.unpolled.is_empty()
    }
}

/// The resources a manual model lease pins, with their model: forced to a model (a reclaim
/// is forced to none).
pub fn pinned(intent: &Intent) -> Vec<(ResourceId, ModelId)> {
    intent
        .forced
        .iter()
        .filter(|r| !intent.reclaim.contains(*r))
        .filter_map(|r| match intent.models.get(r) {
            Some(Some(m)) => Some((r.clone(), m.clone())),
            _ => None,
        })
        .collect()
}

/// The distinct kinds in a decision, sorted; [`KIND_PINNED`] when a manual model lease pins
/// a model; [`KIND_IDLE`] when there are no steps, no class's demand is unknown and nothing
/// is pinned (unobservable demand counts as none, so it does not prevent `idle`, but its
/// kind is still listed).
pub fn kinds(steps: &[Step], gaps: &DemandGaps, pinned: bool) -> Vec<&'static str> {
    let mut set: BTreeSet<&'static str> = steps.iter().map(step_kind).collect();
    if !gaps.unknown.is_empty() {
        set.insert(KIND_DEMAND_UNKNOWN);
    }
    if !gaps.unsupported.is_empty() {
        set.insert(KIND_DEMAND_UNSUPPORTED);
    }
    if !gaps.unpolled.is_empty() {
        set.insert(KIND_DEMAND_UNPOLLED);
    }
    if pinned {
        set.insert(KIND_PINNED);
    }
    if gaps.unknown.is_empty() && steps.is_empty() && !pinned {
        set.insert(KIND_IDLE);
    }
    set.into_iter().collect()
}

/// Why nothing carries `step` out, if nothing does. Allowing and withdrawing a model, and a
/// class leaving or entering the allowed set, are published (KEDA converges on the set).
/// But nothing scales on a demand class: consumers are not gated for GPU contention (only
/// gate classes, whose requirements are hosts, gate a consumer), so a `PauseIntake` does not
/// stop anyone taking new work. `KillInFlight` has no actuator of its own either: the work
/// is cut off because the same tick withdraws its model (KEDA scales the workload to 0).
/// Powering a host on or off and running a recovery action have no actuator yet.
pub fn not_executed_note(step: &Step) -> Option<&'static str> {
    match step {
        Step::PowerOn { .. } | Step::PowerOff { .. } | Step::RunRecovery { .. } => {
            Some(NOT_EXECUTED)
        }
        Step::KillInFlight { .. } => Some(NOT_EXECUTED_KILL),
        Step::PauseIntake { .. } => Some(NOT_EXECUTED_PAUSE),
        Step::Wait { .. } | Step::AllowModel { .. } | Step::DrainModel { .. } => None,
    }
}

/// Whether nothing in this build carries `step` out (see [`not_executed_note`]).
pub fn not_executed(step: &Step) -> bool {
    not_executed_note(step).is_some()
}

/// Appended to the reason of a step that has no actuator in this build.
pub const NOT_EXECUTED: &str = " [NOT EXECUTED: no actuator for this step in this build]";
/// Appended to the reason of a `KillInFlight` step.
pub const NOT_EXECUTED_KILL: &str = " [NOT EXECUTED: nothing stops the work directly; it ends because the model is withdrawn (its workload is scaled to 0), and the class's recovery, if any, is not run either]";
/// Appended to the reason of a `PauseIntake` step.
pub const NOT_EXECUTED_PAUSE: &str = " [NOT EXECUTED: consumers are not gated on their demand class, so nothing stops them taking new work; the class only leaves the allowed set]";

fn wait_text(reason: WaitReason) -> &'static str {
    match reason {
        WaitReason::StaleObservation => {
            "a reachability observation of the host is missing or stale"
        }
        WaitReason::StaleDemand => {
            "a demand observation needed to prove no work is in flight is missing or stale"
        }
        WaitReason::Reclaimed => "the resource is reclaimed",
        WaitReason::ReclaimUnsupported => "a reclaim names a resource that cannot be reclaimed",
        WaitReason::HostNotUp => "the host is not up",
        WaitReason::PoweringOn => "the host is powering on",
        WaitReason::ShuttingDown => "the host is shutting down",
        WaitReason::MinDwell => "the allowed model has not been allowed for its minimum dwell",
        WaitReason::NotPreemptible => "the allowed model's active work is not preemptible",
        WaitReason::InFlightWork => "in-flight work reported by its queue is still running",
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
            format!(
                "withdraw {model} on {resource}: {} (it leaves the allowed set now; nothing waits for its pods)",
                wants(resource)
            )
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
            format!(
                "pause intake for {class}: its model is being wound down, so the class leaves the allowed set"
            )
        }
        Step::KillInFlight { class } => {
            format!(
                "cut off in-flight work of {class}: its timeout ran out, so the model is withdrawn with the work still running"
            )
        }
        Step::RunRecovery { class } => {
            format!("run recovery for {class}: its in-flight work was stopped")
        }
    }
}

/// The whole reason of a decision: what the planner wants, then why each step.
pub fn reason(intent: &Intent, steps: &[Step], gaps: &DemandGaps) -> String {
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
    if !intent.forced.is_empty() {
        let f: Vec<_> = intent.forced.iter().map(|r| r.to_string()).collect();
        out.push_str(&format!(
            ", forced [{}] (a manual model lease or a reclaim cuts batch work short: intake paused, in-flight work gets a timeout)",
            f.join(", ")
        ));
    }
    if !intent.reclaim.is_empty() {
        let rc: Vec<_> = intent.reclaim.iter().map(|r| r.to_string()).collect();
        out.push_str(&format!(", reclaim [{}]", rc.join(", ")));
    }
    let pins = pinned(intent);
    if !pins.is_empty() {
        let p: Vec<_> = pins.iter().map(|(r, m)| format!("{r}={m}")).collect();
        out.push_str(&format!(
            ", pinned by a manual model lease [{}] (held until the lease is released, however little work it has)",
            p.join(", ")
        ));
    }
    if steps.is_empty() {
        out.push_str("; no step needed");
    }
    for s in steps {
        out.push_str("; ");
        out.push_str(&step_reason(s, intent));
        if let Some(note) = not_executed_note(s) {
            out.push_str(note);
        }
    }
    let ids = |v: &[sched_model::ClassId]| {
        v.iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    };
    if !gaps.unknown.is_empty() {
        out.push_str(&format!(
            "; demand UNKNOWN for [{}] (its source is silent or stale): this is not idle",
            ids(&gaps.unknown)
        ));
    }
    if !gaps.unpolled.is_empty() {
        out.push_str(&format!(
            "; demand not polled for [{}] (the source is configured without params and has never reported): counted as no demand",
            ids(&gaps.unpolled)
        ));
    }
    if !gaps.unsupported.is_empty() {
        out.push_str(&format!(
            "; demand not observable for [{}] (no observer for the source kind in this build, and it has never reported): counted as no demand",
            ids(&gaps.unsupported)
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
            let note = match a.observer {
                ObserverState::Running => "",
                ObserverState::NoParams => " (no params: not polled)",
                ObserverState::NoImplementation => " (no observer)",
            };
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
        let c = |n: &str| vec![sched_model::ClassId::from(n)];
        let gap = |u: &[&str], s: &[&str], p: &[&str]| DemandGaps {
            unknown: u.iter().map(|n| (*n).into()).collect(),
            unsupported: s.iter().map(|n| (*n).into()).collect(),
            unpolled: p.iter().map(|n| (*n).into()).collect(),
        };
        let _ = c;
        assert_eq!(kinds(&[], &DemandGaps::default(), false), vec![KIND_IDLE]);
        assert_eq!(
            kinds(&[], &gap(&["a"], &[], &[]), false),
            vec![KIND_DEMAND_UNKNOWN]
        );
        // Unobservable demand counts as none: still idle, and still labelled.
        assert_eq!(
            kinds(&[], &gap(&[], &["a"], &[]), false),
            vec![KIND_DEMAND_UNSUPPORTED, KIND_IDLE]
        );
        assert_eq!(
            kinds(&[], &gap(&[], &[], &["a"]), false),
            vec![KIND_DEMAND_UNPOLLED, KIND_IDLE]
        );
        // A model pinned by a manual lease is held on purpose: never idle.
        assert_eq!(
            kinds(&[], &gap(&[], &["a"], &[]), true),
            vec![KIND_DEMAND_UNSUPPORTED, KIND_PINNED]
        );
        assert_eq!(kinds(&[], &DemandGaps::default(), true), vec![KIND_PINNED]);
        assert_eq!(
            kinds(&[], &gap(&["a"], &["b"], &["c"]), false),
            vec![
                KIND_DEMAND_UNKNOWN,
                KIND_DEMAND_UNPOLLED,
                KIND_DEMAND_UNSUPPORTED
            ]
        );
        let steps = vec![
            Step::Wait {
                resource: "a".into(),
                reason: WaitReason::InFlightWork,
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
        assert_eq!(
            kinds(&steps, &DemandGaps::default(), false),
            vec!["allow_model", "wait"]
        );
        assert_eq!(
            kinds(
                &steps,
                &DemandGaps {
                    unknown: vec!["x".into()],
                    ..Default::default()
                },
                false
            ),
            vec!["allow_model", "demand_unknown", "wait"]
        );
    }

    #[test]
    fn only_a_forced_intent_with_a_model_is_pinned() {
        let mut i = intent();
        assert!(pinned(&i).is_empty(), "unforced");
        i.forced.insert("cuda-central".into());
        assert_eq!(
            pinned(&i),
            vec![(ResourceId::from("cuda-central"), ModelId::from("mineru"))]
        );
        let r = reason(&i, &[], &DemandGaps::default());
        assert!(
            r.contains("pinned by a manual model lease [cuda-central=mineru]"),
            "{r}"
        );
        // A reclaim is forced to no model: not a pin.
        let mut rc = Intent::default();
        rc.models.insert("cuda-central".into(), None);
        rc.forced.insert("cuda-central".into());
        rc.reclaim.insert("cuda-central".into());
        assert!(pinned(&rc).is_empty());
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
        let r = reason(&intent(), &steps, &DemandGaps::default());
        assert!(r.starts_with("intent [cuda-central=mineru]"), "{r}");
        assert!(
            r.contains("allow mineru on cuda-central: the planner wants mineru on cuda-central"),
            "{r}"
        );
        assert!(
            r.contains("wait on basestar-nfs: the host is not up"),
            "{r}"
        );
        assert!(reason(&Intent::default(), &[], &DemandGaps::default()).contains("no step needed"));
    }

    #[test]
    fn steps_without_an_actuator_are_marked_as_not_executed() {
        let steps = vec![
            Step::PowerOn {
                resource: "cuda-central".into(),
            },
            Step::KillInFlight {
                class: "paperless-ocr".into(),
            },
            Step::DrainModel {
                resource: "cuda-central".into(),
                model: "mineru".into(),
            },
        ];
        let r = reason(&intent(), &steps, &DemandGaps::default());
        assert_eq!(r.matches(NOT_EXECUTED).count(), 1, "{r}");
        assert_eq!(r.matches(NOT_EXECUTED_KILL).count(), 1, "{r}");
        assert!(
            r.contains(&format!(
                "cut off in-flight work of paperless-ocr: its timeout ran out, so the model is withdrawn with the work still running{NOT_EXECUTED_KILL}"
            )),
            "{r}"
        );
        assert!(
            r.contains(&format!(
                "power on cuda-central: the sequencer needs it up{NOT_EXECUTED}"
            )),
            "{r}"
        );
        // Withdrawing a model is carried out (published), so its reason carries no note.
        let withdraw = reason(
            &intent(),
            &[Step::DrainModel {
                resource: "cuda-central".into(),
                model: "mineru".into(),
            }],
            &DemandGaps::default(),
        );
        assert!(
            withdraw.contains("withdraw mineru on cuda-central: the planner wants "),
            "{withdraw}"
        );
        assert!(!withdraw.contains("NOT EXECUTED"), "{withdraw}");
        for (step, want) in [
            (
                Step::PowerOff {
                    resource: "r".into(),
                },
                true,
            ),
            (Step::RunRecovery { class: "c".into() }, true),
            (Step::KillInFlight { class: "c".into() }, true),
            (Step::PauseIntake { class: "c".into() }, true),
            (
                Step::AllowModel {
                    resource: "r".into(),
                    model: "m".into(),
                },
                false,
            ),
            (
                Step::Wait {
                    resource: "r".into(),
                    reason: WaitReason::InFlightWork,
                },
                false,
            ),
        ] {
            assert_eq!(not_executed(&step), want, "{step:?}");
        }
    }

    #[test]
    fn inputs_show_ages_missing_sources_and_errors() {
        let ages = vec![
            InputAge {
                source: "a".into(),
                capability: Some(sched_model::Capability::Pods),
                observer: ObserverState::Running,
                age: Some(Millis(1200)),
                last_error: None,
            },
            InputAge {
                source: "b".into(),
                capability: None,
                observer: ObserverState::Running,
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
