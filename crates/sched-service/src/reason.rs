//! Turning a decision into words: a kind for metrics and a reason for logs. Pure.

use std::collections::BTreeSet;

use sched_model::{
    Intent, Millis, ModelId, PowerTransition, PowerWant, ResourceId, Step, WaitReason,
};
use sched_observe::ObserverState;

use crate::cache::InputAge;
use crate::tick::{
    MinOffWait, PowerFailed, PowerRequestFailure, PowerTimeout, VetoHeld, VetoTimeout,
};

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

/// Metric label for a tick in which a resource is in the failed power state: its power-on
/// or shutdown timed out and flowmanager stopped. Never [`KIND_IDLE`].
pub const KIND_POWER_FAILED: &str = "power_failed";

/// Adds the power failures to a decision's kinds and reason: each resource in the failed
/// state (with what a person must do), each timeout raised in this tick, and each power
/// request whose call failed (so a failed request is never read as a success).
pub fn with_power(
    mut kinds: Vec<String>,
    mut reason: String,
    failed: &[PowerFailed],
    timeouts: &[PowerTimeout],
    request_failures: &[PowerRequestFailure],
) -> (Vec<String>, String) {
    if !failed.is_empty() {
        kinds.push(KIND_POWER_FAILED.to_string());
        kinds.sort();
        kinds.dedup();
    }
    for f in request_failures {
        reason.push_str(&format!(
            "; power {} of {} FAILED at {}ms: {} (not retried; the host's state is followed from its reachability and the transition times out)",
            f.action.as_str(),
            f.resource,
            f.at.as_u64(),
            f.error
        ));
    }
    for t in timeouts {
        reason.push_str(&format!(
            "; ALERT: {} of {} timed out in this tick",
            t.during.as_str(),
            t.resource
        ));
    }
    for f in failed {
        let wanted = match f.during {
            PowerTransition::PoweringOn => "up",
            PowerTransition::ShuttingDown => "down",
        };
        reason.push_str(&format!(
            "; {} is in the FAILED power state since {}ms ({} timed out): nothing is allowed on it, no power action will be taken for it by flowmanager, and it clears when the host is observed {wanted} or a person acts (control: manual also clears it)",
            f.resource,
            f.since.as_u64(),
            f.during.as_str()
        ));
    }
    (kinds, reason)
}

/// Metric label for a tick in which a resource's shutdown wait for unmounting pods timed out
/// and flowmanager stopped: the host stays up and a person must act.
pub const KIND_SHUTDOWN_STOPPED: &str = "shutdown_stopped";

/// Adds the held-back shutdowns to a decision's kinds and reason: each resource waiting for
/// the veto to clear (a pod still using the host, or a reading of it missing), each wait that
/// timed out in this tick, and each resource where flowmanager stopped.
pub fn with_veto(
    mut kinds: Vec<String>,
    mut reason: String,
    held: &[VetoHeld],
    timeouts: &[VetoTimeout],
) -> (Vec<String>, String) {
    if held.iter().any(|h| h.stopped) {
        kinds.push(KIND_SHUTDOWN_STOPPED.to_string());
        kinds.sort();
        kinds.dedup();
    }
    for t in timeouts {
        reason.push_str(&format!(
            "; ALERT: the shutdown of {} waited for the veto ({}) to clear until veto_timeout ran out in this tick",
            t.resource,
            crate::tick::blockers_label(&t.blocking)
        ));
    }
    for h in held {
        if h.stopped {
            reason.push_str(&format!(
                "; {} was NOT shut down: it was still in use (or its use was unknown; veto {}) when veto_timeout ran out at {}ms; flowmanager stopped, the host stays up, and it will not shut it down by itself again until a person acts (resume), it is seen down, or a new manual request takes it over",
                h.resource,
                h.readings.join("+"),
                h.since.as_u64()
            ));
        } else {
            reason.push_str(&format!(
                "; the shutdown of {} is held back since {}ms by the veto ({}): a pod mounts its NFS server or runs its models, or the reading is unknown (unknown counts as in use)",
                h.resource,
                h.since.as_u64(),
                crate::tick::blockers_label(&h.blocking)
            ));
        }
    }
    (kinds, reason)
}

/// Adds the power-ons held back by `min_off` to a decision's reason: how long each has left.
pub fn with_min_off(mut reason: String, waits: &[MinOffWait]) -> String {
    for w in waits {
        reason.push_str(&format!(
            "; {} is wanted but was shut down by flowmanager and seen down at {}ms: waiting for min_off, {}s left (power-on decided again at {}ms; a manual lease would not wait)",
            w.resource,
            w.since.as_u64(),
            w.remaining.as_u64().div_ceil(1000),
            w.until.as_u64()
        ));
    }
    reason
}

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

/// The resources a manual model lease pins, with their model: forced to a model (a lockout
/// is forced to none).
pub fn pinned(intent: &Intent) -> Vec<(ResourceId, ModelId)> {
    intent
        .forced
        .iter()
        .filter(|r| !intent.lockout.contains(*r))
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
/// is cut off because its model is withdrawn (KEDA scales the workload to 0): in the same
/// tick, or when the last blocking class of the model is cut off or finishes.
/// `PowerOn` and `PowerOff` (the sequencer emits them only for hosts under managed control)
/// are carried out: the tick sends the resource's `Power` object a request, which makes the
/// call once. `RunRecovery` is carried out: the tick sends the class's `Recovery` object a
/// request, which runs the action (config load refuses an action this build cannot run, and
/// the sequencer emits the step only for such a class).
pub fn not_executed_note(step: &Step) -> Option<&'static str> {
    match step {
        Step::KillInFlight { .. } => Some(NOT_EXECUTED_KILL),
        Step::PauseIntake { .. } => Some(NOT_EXECUTED_PAUSE),
        Step::Wait { .. }
        | Step::AllowModel { .. }
        | Step::DrainModel { .. }
        | Step::PowerOn { .. }
        | Step::PowerOff { .. }
        | Step::RunRecovery { .. } => None,
    }
}

/// Whether nothing in this build carries `step` out (see [`not_executed_note`]).
pub fn not_executed(step: &Step) -> bool {
    not_executed_note(step).is_some()
}

/// Appended to the reason of a `KillInFlight` step.
pub const NOT_EXECUTED_KILL: &str = " [NOT EXECUTED: nothing stops the work directly; it ends when its model is withdrawn (its workload is scaled to 0), in the same tick or, while other classes of the model are still blocking or the switch itself is waiting (a stale host reading, say), later; the class's recovery, which re-queues failed items, runs while its model is allowed, not after the cut-off]";
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
        WaitReason::LockedOut => "the resource is locked out",
        WaitReason::LockoutUnsupported => "a lockout names a resource that cannot be locked out",
        WaitReason::HostNotUp => "the host is not up",
        WaitReason::PoweringOn => "the host is powering on",
        WaitReason::ShuttingDown => "the host is shutting down",
        WaitReason::PowerFailed => {
            "the host's power transition timed out and flowmanager stopped (a person must act; it is not retried)"
        }
        WaitReason::VetoInUse => {
            "the host is due to be powered off but pods still use it (mounting its NFS server or running its models); waiting for them to go"
        }
        WaitReason::VetoUnknown => {
            "the host is due to be powered off but whether pods still use it is unknown (no fresh reading; unknown counts as in use)"
        }
        WaitReason::ShutdownStopped => {
            "the host was still in use when veto_timeout ran out and flowmanager stopped (the host stays up; a person must act; it is not retried)"
        }
        WaitReason::MinOff => {
            "the host was shut down by flowmanager and its minimum off time (min_off) has not passed yet"
        }
        WaitReason::MinDwell => "the allowed model has not been allowed for its minimum dwell",
        WaitReason::NotPreemptible => "the allowed model's active work is not preemptible",
        WaitReason::InFlightWork => "in-flight work reported by its queue is still running",
        WaitReason::Draining => {
            "a drain of the host is under way: its consumers are still allowed while in-flight work gets its drain_timeout"
        }
        WaitReason::DrainHeld => {
            "a drain of the host is under way and its consumers are scaled down, but a lease still holds the host (a drain overrides no lease); the shutdown waits for it"
        }
    }
}

/// What happens to the model of a class whose in-flight work is cut off.
#[derive(Clone, Copy)]
enum KillOutcome {
    /// The same tick withdraws the model.
    Withdrawn,
    /// Other classes of the model are still blocking.
    OthersBlock,
    /// Nothing blocks, but the switch itself waits for this reason (a stale host reading, say).
    SwitchWaits(WaitReason),
    /// The tick's other steps are not known.
    Unknown,
}

/// Each class is cut off when its own timeout runs out, but the model is withdrawn only when
/// no class of it is still blocking and the switch can go ahead. The tick's steps say which:
/// a withdrawal, a wait on in-flight work (other classes block), or any other wait (the
/// switch itself waits).
fn kill_outcome(steps: &[Step]) -> KillOutcome {
    if steps.iter().any(|s| matches!(s, Step::DrainModel { .. })) {
        return KillOutcome::Withdrawn;
    }
    for s in steps {
        if let Step::Wait { reason, .. } = s {
            return match reason {
                WaitReason::InFlightWork | WaitReason::StaleDemand | WaitReason::NotPreemptible => {
                    KillOutcome::OthersBlock
                }
                other => KillOutcome::SwitchWaits(*other),
            };
        }
    }
    KillOutcome::Unknown
}

/// Why `class`'s in-flight work is cut off.
fn kill_reason(class: &sched_model::ClassId, outcome: KillOutcome) -> String {
    let rest = match outcome {
        KillOutcome::Withdrawn => {
            "so the model is withdrawn in this same tick, with the work still running".to_string()
        }
        KillOutcome::OthersBlock => "but other classes of its model are still blocking, so the model stays allowed and the work keeps running until the last of them is cut off at its own timeout or finishes".to_string(),
        KillOutcome::SwitchWaits(reason) => format!(
            "but the model stays allowed and the work keeps running because the switch itself is waiting: {}",
            wait_text(reason)
        ),
        KillOutcome::Unknown => "its model is withdrawn in the same tick if no other class of it is still blocking and the switch can go ahead, otherwise the work keeps running until the last blocking class is cut off at its own timeout or finishes, or the switch can go ahead".to_string(),
    };
    format!("cut off in-flight work of {class}: its timeout ran out, {rest}")
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
        Step::KillInFlight { class } => kill_reason(class, KillOutcome::Unknown),
        Step::RunRecovery { class } => {
            format!(
                "run recovery for {class}: items failed while its model is allowed; the Recovery object re-queues them, bounded per item and per run (see sched_recovery_* metrics)"
            )
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
            ", forced [{}] (a manual model lease or a lockout cuts batch work short: intake paused, in-flight work gets a timeout)",
            f.join(", ")
        ));
    }
    if !intent.lockout.is_empty() {
        let rc: Vec<_> = intent.lockout.iter().map(|r| r.to_string()).collect();
        out.push_str(&format!(", lockout [{}]", rc.join(", ")));
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
    let outcome = kill_outcome(steps);
    for s in steps {
        out.push_str("; ");
        match s {
            Step::KillInFlight { class } => out.push_str(&kill_reason(class, outcome)),
            _ => out.push_str(&step_reason(s, intent)),
        }
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
        // A lockout is forced to no model: not a pin.
        let mut rc = Intent::default();
        rc.models.insert("cuda-central".into(), None);
        rc.forced.insert("cuda-central".into());
        rc.lockout.insert("cuda-central".into());
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
            Step::PauseIntake {
                class: "paperless-ocr".into(),
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
        assert_eq!(r.matches(NOT_EXECUTED_PAUSE).count(), 1, "{r}");
        assert_eq!(r.matches(NOT_EXECUTED_KILL).count(), 1, "{r}");
        assert!(
            r.contains(&format!(
                "cut off in-flight work of paperless-ocr: its timeout ran out, so the model is withdrawn in this same tick, with the work still running{NOT_EXECUTED_KILL}"
            )),
            "{r}"
        );
        assert!(
            r.contains(&format!(
                "pause intake for paperless-ocr: its model is being wound down, so the class leaves the allowed set{NOT_EXECUTED_PAUSE}"
            )),
            "{r}"
        );
        // The model is withdrawn in the same tick only when the tick says so: with no
        // withdrawal, other classes of the model are still blocking.
        let wait = |reason| Step::Wait {
            resource: "cuda-central".into(),
            reason,
        };
        let kill = steps[1].clone();
        let blocked = reason(
            &intent(),
            &[kill.clone(), wait(WaitReason::InFlightWork)],
            &DemandGaps::default(),
        );
        assert!(
            blocked.contains("its timeout ran out, but other classes of its model are still blocking, so the model stays allowed"),
            "{blocked}"
        );
        assert!(
            !blocked.contains("withdrawn in this same tick"),
            "{blocked}"
        );
        // Nothing else blocks but the switch itself waits (a stale host reading).
        let waiting = reason(
            &intent(),
            &[kill, wait(WaitReason::StaleObservation)],
            &DemandGaps::default(),
        );
        assert!(
            waiting.contains("the model stays allowed and the work keeps running because the switch itself is waiting: a reachability observation of the host is missing or stale"),
            "{waiting}"
        );
        assert!(!waiting.contains("other classes of its model"), "{waiting}");
        assert!(
            !waiting.contains("withdrawn in this same tick"),
            "{waiting}"
        );
        // Without the tick's other steps the text covers both cases.
        let both = step_reason(&steps[1], &intent());
        assert!(
            both.contains("in the same tick if no other class of it is still blocking"),
            "{both}"
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
        // Running a recovery is carried out too, and its reason says what happens.
        let rec = reason(
            &intent(),
            &[Step::RunRecovery {
                class: "speakr-transcription".into(),
            }],
            &DemandGaps::default(),
        );
        assert!(
            rec.contains(
                "run recovery for speakr-transcription: items failed while its model is allowed"
            ),
            "{rec}"
        );
        assert!(!rec.contains("NOT EXECUTED"), "{rec}");
        for (step, want) in [
            (
                Step::PowerOff {
                    resource: "r".into(),
                },
                false,
            ),
            // Carried out by the resource's Power object (above) and the class's Recovery object.
            (Step::RunRecovery { class: "c".into() }, false),
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
