//! The design's safety invariants, checked against one decision. Used by replay and property
//! tests; it re-derives each rule from the inputs rather than trusting the sequencer.

use sched_model::{
    Config, Intent, ModePhase, ResourceKind, SchedState, Snapshot, Step, class_work,
    classes_of_model,
};
use sched_sequencer::Decision;

/// Every violated invariant, as human-readable text. Empty means none.
pub fn check(
    config: &Config,
    snapshot: &Snapshot,
    prev: &SchedState,
    intent: &Intent,
    decision: &Decision,
) -> Vec<String> {
    let mut out = Vec::new();
    let policy = config.policy();
    let now = snapshot.now;

    // 1. At most one model allowed per exclusive resource.
    for res in config
        .resources()
        .iter()
        .filter(|r| r.kind == ResourceKind::Exclusive)
    {
        let n = config
            .models_of(&res.id)
            .filter(|m| decision.allowed.models.contains(&m.id))
            .count();
        if n > 1 {
            out.push(format!("invariant 1: {n} models allowed on {}", res.id));
        }
    }

    let mut resource_steps = std::collections::BTreeMap::new();
    for step in &decision.steps {
        if let Some(r) = step.resource()
            && !matches!(step, Step::Wait { .. })
            && resource_steps.insert(r.clone(), step).is_some()
        {
            out.push(format!("more than one acting step for {r}"));
        }
        match step {
            // 2. A model is allowed only when every other model is observed at zero pods.
            Step::AllowModel { resource, model } => {
                for other in config.models_of(resource).filter(|m| &m.id != model) {
                    if snapshot
                        .fresh_model_pods(&other.id, policy.safety_max_age)
                        .map(|p| p.0)
                        != Some(0)
                    {
                        out.push(format!(
                            "invariant 2: allowed {model} without fresh zero pods of {}",
                            other.id
                        ));
                    }
                }
            }
            // 3 and 4. Power changes need a reason, and powering down needs no holds.
            Step::PowerOn { resource } => {
                if !intent.power_up.contains_key(resource) {
                    out.push(format!(
                        "invariant 4: powered on {resource} without a manual or workload reason"
                    ));
                }
            }
            Step::PowerOff { resource } => {
                let reclaim = intent.reclaim.contains(resource);
                let before = prev.resource(resource);
                if !reclaim {
                    if !before.is_some_and(|s| s.managed) {
                        out.push(format!(
                            "invariant 4: powered off unmanaged host {resource}"
                        ));
                    }
                    let held = snapshot
                        .leases
                        .iter()
                        .any(|l| &l.resource == resource && l.holds_host(now));
                    if held {
                        out.push(format!(
                            "invariant 3: powered off {resource} while a lease holds it"
                        ));
                    }
                    for class in config
                        .classes()
                        .iter()
                        .filter(|c| c.requires.iter().any(|r| &r.resource == resource))
                    {
                        let w = class_work(config, snapshot, class);
                        if class.demand.is_some() && (w.busy() || !w.known) {
                            out.push(format!(
                                "invariant 3: powered off {resource} while {} has or may have work",
                                class.id
                            ));
                        }
                    }
                    let grace_ok = policy.idle_grace == sched_model::Millis::ZERO
                        || before
                            .and_then(|s| s.host_idle_since)
                            .is_some_and(|t| now.saturating_sub(t) >= policy.idle_grace);
                    if !grace_ok {
                        out.push(format!(
                            "invariant 3: powered off {resource} before idle grace ended"
                        ));
                    }
                }
                if snapshot
                    .fresh_resource_up(resource, policy.safety_max_age)
                    .is_none()
                {
                    out.push(format!(
                        "invariant 5: powered off {resource} on a stale observation"
                    ));
                }
                for m in config.models_of(resource) {
                    if snapshot
                        .fresh_model_pods(&m.id, policy.safety_max_age)
                        .map(|p| p.0)
                        != Some(0)
                    {
                        out.push(format!(
                            "invariant 5: powered off {resource} without fresh zero pods of {}",
                            m.id
                        ));
                    }
                }
            }
            // 8. Draining a serving model early needs a declared-safe class or a forced switch.
            Step::DrainModel { resource, model } => {
                let Some(ModePhase::Serving { since, .. }) =
                    prev.resource(resource).map(|s| &s.mode)
                else {
                    continue;
                };
                let forced = intent.forced.contains(resource) || intent.reclaim.contains(resource);
                let target_is_model =
                    intent.models.get(resource).cloned().flatten().as_ref() == Some(model);
                if target_is_model && !forced {
                    out.push(format!("drained {model} though it is the intended model"));
                }
                if !forced && now.saturating_sub(*since) < policy.min_dwell {
                    out.push(format!("drained {model} before min_dwell"));
                }
                for class in classes_of_model(config, model) {
                    let w = class_work(config, snapshot, class);
                    let killed = decision.steps.contains(&Step::KillInFlight {
                        class: class.id.clone(),
                    });
                    let unsure = class.demand.is_some() && (w.in_flight > 0 || !w.known);
                    if unsure && !killed {
                        out.push(format!(
                            "invariant 8: drained {model} with possible in-flight work of {}",
                            class.id
                        ));
                    }
                    if !forced
                        && w.busy()
                        && class.preemptibility == sched_model::Preemptibility::None
                    {
                        out.push(format!(
                            "invariant 8: switched away early from unpreemptible {}",
                            class.id
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    out
}
