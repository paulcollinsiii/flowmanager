//! The design's safety invariants, checked against one decision.
//!
//! The checker is deliberately independent of the sequencer: it reads observations and
//! leases straight from the snapshot with its own small readers, derives "reclaimed",
//! "forced" and "held" from the leases rather than from the intent, and shares only the
//! data types and config accessors with the code it checks. It re-uses none of
//! `sched_model`'s query helpers (`class_work`, `fresh_*`, `holds_host`, ...).

use std::collections::{BTreeMap, BTreeSet};

use sched_model::{
    Capability, ClassId, ClassSpec, Config, Intent, LeaseKind, Millis, ModePhase, ModelId,
    Observation, ObservedValue, ResourceId, ResourceKind, SchedState, Snapshot, SourceId, Step,
};
use sched_sequencer::Decision;

/// Readings matching `pred` from sources declaring `cap`, reduced per source and then
/// across sources, restated here independently of the core: each declaring source
/// contributes only its own newest timestamp, and if the newest reading of any source that
/// has reported is stale or future-dated the result is `None`; otherwise every contributed
/// reading is returned, so callers can require all of them to be safe. The filter by
/// subject and by declared source comes before any newest timestamp is chosen, and the
/// declaration is read from the config's `observe` list, never from the source kind.
fn newest<'a>(
    config: &Config,
    cap: Capability,
    snap: &'a Snapshot,
    max_age: Millis,
    pred: impl Fn(&ObservedValue) -> bool,
) -> Option<Vec<&'a ObservedValue>> {
    let mut per_source: BTreeMap<&SourceId, Vec<&Observation>> = BTreeMap::new();
    for o in &snap.observations {
        let declares = config
            .source(&o.source)
            .is_some_and(|s| s.observe.contains(&cap));
        if declares && pred(&o.value) {
            per_source.entry(&o.source).or_default().push(o);
        }
    }
    if per_source.is_empty() {
        return None;
    }
    let mut out = Vec::new();
    for all in per_source.values() {
        let latest = all.iter().map(|o| o.observed_at.as_u64()).max()?;
        if latest > snap.now.as_u64() || snap.now.as_u64() - latest > max_age.as_u64() {
            return None;
        }
        out.extend(
            all.iter()
                .filter(|o| o.observed_at.as_u64() == latest)
                .map(|o| &o.value),
        );
    }
    Some(out)
}

fn pods_zero(config: &Config, snap: &Snapshot, model: &ModelId, max_age: Millis) -> bool {
    newest(
        config,
        Capability::Pods,
        snap,
        max_age,
        |v| matches!(v, ObservedValue::ModelPods { model: m, .. } if m == model),
    )
    .is_some_and(|g| {
        g.iter()
            .all(|v| matches!(v, ObservedValue::ModelPods { pods: 0, .. }))
    })
}

fn host_up(config: &Config, snap: &Snapshot, res: &ResourceId, max_age: Millis) -> bool {
    newest(
        config,
        Capability::Reachability,
        snap,
        max_age,
        |v| matches!(v, ObservedValue::ResourceUp { resource, .. } if resource == res),
    )
    .is_some_and(|g| {
        g.iter()
            .all(|v| matches!(v, ObservedValue::ResourceUp { up: true, .. }))
    })
}

/// `Some((busy, in_flight))` for a class, `None` when its demand is unknown (missing, stale
/// or future-dated). Conservative over ties: the busiest reading.
fn demand(config: &Config, snap: &Snapshot, class: &ClassSpec) -> Option<(bool, bool)> {
    let src = &class.demand.as_ref()?.source;
    let seen: Vec<_> = snap
        .observations
        .iter()
        .filter(|o| &o.source == src)
        .filter_map(|o| match &o.value {
            ObservedValue::Demand {
                class: c,
                ready,
                in_flight,
                ..
            } if c == &class.id => Some((o.observed_at.as_u64(), *ready, *in_flight)),
            _ => None,
        })
        .collect();
    let latest = seen.iter().map(|x| x.0).max()?;
    let (now, max_age) = (snap.now.as_u64(), config.policy().demand_max_age.as_u64());
    if latest > now || now - latest > max_age {
        return None;
    }
    let at: Vec<_> = seen.iter().filter(|x| x.0 == latest).collect();
    Some((
        at.iter().any(|x| x.1 > 0 || x.2 > 0),
        at.iter().any(|x| x.2 > 0),
    ))
}

fn active(kind: &LeaseKind, now: Millis) -> bool {
    !matches!(kind, LeaseKind::Interactive { expires_at, .. } if now >= *expires_at)
}

fn reclaimed(config: &Config, snap: &Snapshot, res: &ResourceId) -> bool {
    let spec = config.resource(res);
    spec.is_some_and(|r| r.kind == ResourceKind::Exclusive && r.power.is_some())
        && snap.leases.iter().any(|l| {
            &l.resource == res && matches!(l.kind, LeaseKind::Reclaim) && active(&l.kind, snap.now)
        })
}

fn manual_model(snap: &Snapshot, res: &ResourceId) -> bool {
    snap.leases.iter().any(|l| {
        &l.resource == res
            && matches!(l.kind, LeaseKind::ManualModel { .. })
            && active(&l.kind, snap.now)
    })
}

/// Whether the scheduler controls `res`'s power, read from the config spec directly.
fn managed_power(config: &Config, res: &ResourceId) -> bool {
    config
        .resource(res)
        .and_then(|r| r.power.as_ref())
        .is_some_and(|p| p.control == sched_model::PowerControl::Managed)
}

fn needs(class: &ClassSpec, res: &ResourceId) -> bool {
    class.requires.iter().any(|r| &r.resource == res)
}

fn on_model<'a>(config: &'a Config, model: &'a ModelId) -> impl Iterator<Item = &'a ClassSpec> {
    config
        .classes()
        .iter()
        .filter(move |c| c.requires.iter().any(|r| r.model.as_ref() == Some(model)))
}

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
    let safety = policy.safety_max_age;
    let _ = intent;

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

    let mut acting = BTreeSet::new();
    for step in &decision.steps {
        if let Some(r) = step.resource()
            && !matches!(step, Step::Wait { .. })
            && !acting.insert(r.clone())
        {
            out.push(format!("more than one acting step for {r}"));
        }
        match step {
            // 2. A model is allowed only when every other model is observed at zero pods.
            Step::AllowModel { resource, model } => {
                for other in config.models_of(resource).filter(|m| &m.id != model) {
                    if !pods_zero(config, snapshot, &other.id, safety) {
                        out.push(format!(
                            "invariant 2: allowed {model} without fresh zero pods of {}",
                            other.id
                        ));
                    }
                }
            }
            // 4. Power-on needs a manual request or a power-changing class with work.
            Step::PowerOn { resource } => {
                if !managed_power(config, resource) {
                    out.push(format!(
                        "invariant 4: powered on {resource}, whose power is under manual control"
                    ));
                }
                let manual = snapshot.leases.iter().any(|l| {
                    &l.resource == resource
                        && matches!(
                            l.kind,
                            LeaseKind::ManualHost | LeaseKind::ManualModel { .. }
                        )
                        && active(&l.kind, now)
                });
                let workload = config.classes().iter().any(|c| {
                    c.may_change_power
                        && needs(c, resource)
                        && demand(config, snapshot, c).is_some_and(|d| d.0)
                });
                if !manual && !workload {
                    out.push(format!(
                        "invariant 4: powered on {resource} without a manual or workload reason"
                    ));
                }
                if reclaimed(config, snapshot, resource) {
                    out.push(format!("powered on reclaimed {resource}"));
                }
            }
            // 3, 4, 5. Power-off needs no holds (a reclaim of an exclusive powered resource
            // is the only exception), idle grace, management, and fresh observations.
            Step::PowerOff { resource } => {
                if !managed_power(config, resource) {
                    out.push(format!(
                        "invariant 4: powered off {resource}, whose power is under manual control"
                    ));
                }
                let before = prev.resource(resource);
                if !reclaimed(config, snapshot, resource) {
                    if !before.is_some_and(|s| s.managed) {
                        out.push(format!(
                            "invariant 4: powered off unmanaged host {resource}"
                        ));
                    }
                    let held = snapshot.leases.iter().any(|l| {
                        &l.resource == resource
                            && !matches!(l.kind, LeaseKind::Reclaim)
                            && active(&l.kind, now)
                    });
                    if held {
                        out.push(format!(
                            "invariant 3: powered off {resource} while a lease holds it"
                        ));
                    }
                    for class in config
                        .classes()
                        .iter()
                        .filter(|c| c.demand.is_some() && needs(c, resource))
                    {
                        if demand(config, snapshot, class).is_none_or(|d| d.0) {
                            out.push(format!(
                                "invariant 3: powered off {resource} while {} has or may have work",
                                class.id
                            ));
                        }
                    }
                    let grace_ok = policy.idle_grace == Millis::ZERO
                        || before.and_then(|s| s.host_idle_since).is_some_and(|t| {
                            now.as_u64() - t.as_u64().min(now.as_u64())
                                >= policy.idle_grace.as_u64()
                        });
                    if !grace_ok {
                        out.push(format!(
                            "invariant 3: powered off {resource} before idle grace ended"
                        ));
                    }
                }
                if !host_up(config, snapshot, resource, safety) {
                    out.push(format!(
                        "invariant 5: powered off {resource} without a fresh up observation"
                    ));
                }
                for m in config.models_of(resource) {
                    if !pods_zero(config, snapshot, &m.id, safety) {
                        out.push(format!(
                            "invariant 5: powered off {resource} without fresh zero pods of {}",
                            m.id
                        ));
                    }
                }
            }
            // 8. Stopping in-flight work is allowed only on a forced switch, per class, after
            // that class's own timeout has run out.
            Step::KillInFlight { class } => {
                let Some(spec) = config.class(class) else {
                    continue;
                };
                let res_forcing: Vec<&ResourceId> = spec
                    .requires
                    .iter()
                    .filter(|r| r.model.is_some())
                    .map(|r| &r.resource)
                    .filter(|r| reclaimed(config, snapshot, r) || manual_model(snapshot, r))
                    .collect();
                let Some(res) = res_forcing.first() else {
                    out.push(format!("invariant 8: stopped in-flight work of {class} without a reclaim or manual model lease"));
                    continue;
                };
                if demand(config, snapshot, spec).is_some_and(|d| !d.1) {
                    out.push(format!("stopped {class} though nothing of it is in flight"));
                }
                let reclaim = reclaimed(config, snapshot, res);
                let base = if reclaim {
                    policy.reclaim_timeout()
                } else {
                    policy.drain_timeout
                };
                match prev.resource(res).map(|s| &s.mode) {
                    Some(ModePhase::Serving {
                        wind_down: Some(wd),
                        ..
                    }) if wd.reclaim == reclaim && wd.timeout.is_some() => {
                        let due = wd
                            .since
                            .as_u64()
                            .saturating_add(spec.kill_timeout.unwrap_or(base).as_u64());
                        if now.as_u64() < due {
                            out.push(format!(
                                "invariant 8: stopped {class} {}ms early",
                                due - now.as_u64()
                            ));
                        }
                    }
                    _ => out.push(format!(
                        "invariant 8: stopped {class} without a running forced wind-down"
                    )),
                }
            }
            // 8. Draining a serving model needs a settled model or a declared-safe class.
            Step::DrainModel { resource, model } => {
                let Some(ModePhase::Serving {
                    since, wind_down, ..
                }) = prev.resource(resource).map(|s| &s.mode)
                else {
                    continue;
                };
                let forced =
                    reclaimed(config, snapshot, resource) || manual_model(snapshot, resource);
                if !forced
                    && now.as_u64() - since.as_u64().min(now.as_u64()) < policy.min_dwell.as_u64()
                {
                    out.push(format!("drained {model} before min_dwell"));
                }
                let killed: BTreeSet<ClassId> = wind_down
                    .iter()
                    .flat_map(|w| w.killed.iter().cloned())
                    .chain(decision.steps.iter().filter_map(|s| match s {
                        Step::KillInFlight { class } => Some(class.clone()),
                        _ => None,
                    }))
                    .collect();
                for class in on_model(config, model) {
                    let d = demand(config, snapshot, class);
                    let unsure = class.demand.is_some() && d.is_none_or(|d| d.1);
                    if unsure && !killed.contains(&class.id) {
                        out.push(format!(
                            "invariant 8: drained {model} with possible in-flight work of {}",
                            class.id
                        ));
                    }
                    let busy = d.is_some_and(|d| d.0);
                    if !forced && busy && class.preemptibility == sched_model::Preemptibility::None
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
