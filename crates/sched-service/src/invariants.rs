//! The design's safety invariants, checked against one decision.
//!
//! No invariant is about pods: GPU exclusivity is enforced by Kubernetes, and the scheduler
//! guarantees only what it publishes (at most one model per exclusive resource, the old one
//! leaving in the same decision that admits the new one, never on a host that is not up).
//! That no decision depends on pod observations at all is checked by the property tests,
//! which compare decisions with and without them.
//!
//! The checker is deliberately independent of the sequencer: it reads observations and
//! leases straight from the snapshot with its own small readers, derives "locked_out",
//! "forced" and "held" from the leases rather than from the intent, and shares only the
//! data types and config accessors with the code it checks. It re-uses none of
//! `sched_model`'s query helpers (`class_work`, `fresh_*`, `holds_host`, ...).

use std::collections::{BTreeMap, BTreeSet};

use sched_model::{
    Capability, ClassId, ClassSpec, Config, Intent, LeaseKind, Millis, ModePhase, ModelId,
    Observation, ObservedValue, PowerPhase, PowerTransition, ResourceId, ResourceKind, SchedState,
    Snapshot, SourceId, Step,
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

/// Whether a fresh reading of a declaring source says `res` is down (or disagrees about it
/// at that source's newest instant). Nothing may be allowed on such a host.
fn host_seen_down(config: &Config, snap: &Snapshot, res: &ResourceId, max_age: Millis) -> bool {
    newest(
        config,
        Capability::Reachability,
        snap,
        max_age,
        |v| matches!(v, ObservedValue::ResourceUp { resource, .. } if resource == res),
    )
    .is_some_and(|g| {
        g.iter()
            .any(|v| matches!(v, ObservedValue::ResourceUp { up: false, .. }))
    })
}

/// Failed items fresh readings report for a class: `None` when its demand is unknown (as
/// for `demand`), else the most any reading at the newest timestamp reports.
fn failed_items(config: &Config, snap: &Snapshot, class: &ClassSpec) -> Option<u64> {
    let src = &class.demand.as_ref()?.source;
    let seen: Vec<(u64, u64)> = snap
        .observations
        .iter()
        .filter(|o| &o.source == src)
        .filter_map(|o| match &o.value {
            ObservedValue::Demand {
                class: c, failed, ..
            } if c == &class.id => Some((o.observed_at.as_u64(), *failed)),
            _ => None,
        })
        .collect();
    let latest = seen.iter().map(|x| x.0).max()?;
    let (now, max_age) = (
        snap.now.as_u64(),
        config.policy().queue_reading_stale_after.as_u64(),
    );
    if latest > now || now - latest > max_age {
        return None;
    }
    seen.iter().filter(|x| x.0 == latest).map(|x| x.1).max()
}

/// Whether `model` is a configured model of `res`, read from the config's model list.
fn model_of(config: &Config, res: &ResourceId, model: &ModelId) -> bool {
    config
        .models()
        .iter()
        .any(|m| &m.id == model && &m.resource == res)
}

/// `Some((busy, in_flight))` for a class, `None` when its demand is unknown (missing, stale
/// or future-dated). Conservative over ties: the busiest reading. A source listed as
/// unobservable that has no reading is no demand.
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
    let Some(latest) = seen.iter().map(|x| x.0).max() else {
        // A source that can never report and never has: no demand.
        return snap.unobservable.contains(src).then_some((false, false));
    };
    let (now, max_age) = (
        snap.now.as_u64(),
        config.policy().queue_reading_stale_after.as_u64(),
    );
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

fn locked_out(config: &Config, snap: &Snapshot, res: &ResourceId) -> bool {
    // A lockout applies to any resource with a power section, shared or exclusive.
    let spec = config.resource(res);
    spec.is_some_and(|r| r.power.is_some())
        && snap.leases.iter().any(|l| {
            &l.resource == res && matches!(l.kind, LeaseKind::Lockout) && active(&l.kind, snap.now)
        })
}

fn manual_model(snap: &Snapshot, res: &ResourceId) -> bool {
    snap.leases.iter().any(|l| {
        &l.resource == res
            && matches!(l.kind, LeaseKind::ManualModel { .. })
            && active(&l.kind, snap.now)
    })
}

/// The shutdown a person asked for that is in force on `res` at this tick, with its mode and
/// the instant it was requested: stored in the previous state, and not cancelled by a manual
/// lease taking the host over (the sequencer drops it before the mode step then). On an
/// exclusive host it makes the wind-down of the model forced (see `exclusive::step`).
fn requested_shutdown(
    prev: &SchedState,
    snap: &Snapshot,
    res: &ResourceId,
) -> Option<(sched_model::DrainMode, Millis)> {
    let s = prev.resource(res)?.shutdown?;
    let sched_model::ShutdownCause::Requested { mode } = s.cause() else {
        return None;
    };
    let cancelled = snap.leases.iter().any(|l| {
        &l.resource == res
            && matches!(
                l.kind,
                LeaseKind::ManualHost | LeaseKind::ManualModel { .. }
            )
            && active(&l.kind, snap.now)
    });
    (!cancelled).then_some((mode, s.since()))
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
    let safety = policy.host_reading_stale_after;
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

    // 3. Only a configured model, on a host that is up and not locked out, is allowed; one
    // that was not allowed before needs an allow step in this decision.
    let before_allowed: BTreeSet<&ModelId> = prev
        .resources
        .values()
        .filter_map(|r| r.mode.allowed_model())
        .collect();
    for model in &decision.allowed.models {
        let Some(spec) = config.models().iter().find(|m| &m.id == model) else {
            out.push(format!(
                "invariant 3: allowed {model}, which is not configured"
            ));
            continue;
        };
        let res = &spec.resource;
        if host_seen_down(config, snapshot, res, safety) {
            out.push(format!(
                "invariant 3: {model} allowed on {res}, which is observed down"
            ));
        }
        if !decision.state.resource(res).is_some_and(|s| s.is_on()) {
            out.push(format!(
                "invariant 3: {model} allowed on {res}, which is not on"
            ));
        }
        let allowed_now = decision.steps.iter().any(
            |s| matches!(s, Step::AllowModel { resource, model: m } if resource == res && m == model),
        );
        // A lockout withdraws the model once its in-flight work is done or timed out; until
        // then it stays allowed, winding down. It is never newly allowed.
        let winding_down = matches!(
            decision.state.resource(res).map(|s| &s.mode),
            Some(ModePhase::Serving { model: m, wind_down: Some(_), .. }) if m == model
        );
        if locked_out(config, snapshot, res) && (allowed_now || !winding_down) {
            out.push(format!(
                "invariant 3: {model} allowed on locked out {res} without winding down"
            ));
        }
        if !before_allowed.contains(model) && !allowed_now {
            out.push(format!(
                "invariant 3: {model} entered the allowed set without an allow step"
            ));
        }
    }

    // Classes: every requirement holds. A model requirement needs that model allowed and
    // not being wound down in this decision; any requirement needs its host not observed
    // down and not locked out (a gate class, on an exclusive host without a model, included).
    for class in &decision.allowed.classes {
        let Some(spec) = config.class(class) else {
            out.push(format!(
                "invariant 3: allowed class {class}, which is not configured"
            ));
            continue;
        };
        for req in &spec.requires {
            let r = &req.resource;
            if host_seen_down(config, snapshot, r, safety)
                || !decision.state.resource(r).is_some_and(|s| s.is_on())
            {
                out.push(format!(
                    "invariant 3: allowed class {class} while {r} is not up"
                ));
            }
            // A shared host's consumers keep running while its lockout gives in-flight work
            // its timeout (the withdrawal is pending); an exclusive host's classes never do.
            let withdrawing = config
                .resource(r)
                .is_some_and(|s| s.kind == ResourceKind::Shared)
                && decision.state.resource(r).is_some_and(|s| {
                    s.shutdown.is_some_and(|x| {
                        x.cause() == sched_model::ShutdownCause::Lockout
                            && x.withdrawal() == sched_model::Withdrawal::Pending
                    })
                });
            if locked_out(config, snapshot, r) && !withdrawing {
                out.push(format!(
                    "invariant 3: allowed class {class} on locked out {r}"
                ));
            }
            if let Some(m) = &req.model {
                if !decision.allowed.models.contains(m) {
                    out.push(format!(
                        "invariant 3: allowed class {class} while {m} is not allowed"
                    ));
                }
                let winding = matches!(
                    decision.state.resource(r).map(|s| &s.mode),
                    Some(ModePhase::Serving {
                        wind_down: Some(_),
                        ..
                    })
                );
                if winding {
                    out.push(format!(
                        "invariant 3: allowed class {class} while {m} is being wound down"
                    ));
                }
            }
        }
        if decision
            .steps
            .iter()
            .any(|s| matches!(s, Step::PauseIntake { class: c } if c == class))
        {
            out.push(format!(
                "invariant 3: allowed class {class} whose intake is paused"
            ));
        }
    }

    // 12. An idle drain (the scheduler's own) ends the moment anything holds the host: a lease
    // that holds it, or work for a class that needs it. Unknown demand is not work.
    for (id, st) in &decision.state.resources {
        if !st
            .shutdown
            .is_some_and(|s| s.is_drain() && s.cause() == sched_model::ShutdownCause::Idle)
        {
            continue;
        }
        let leased = snapshot.leases.iter().any(|l| {
            &l.resource == id
                && !matches!(l.kind, LeaseKind::Lockout)
                && active(&l.kind, snapshot.now)
        });
        let busy = config
            .classes()
            .iter()
            .filter(|c| c.demand.is_some() && needs(c, id))
            .any(|c| demand(config, snapshot, c).is_some_and(|d| d.0));
        if leased || busy {
            out.push(format!(
                "invariant 12: idle drain of {id} still set while something holds the host"
            ));
        }
    }

    // 9. A resource whose power transition timed out is never acted on again by the
    // scheduler for that reason: unless a fresh reading shows what the transition was after
    // (up after a power-on, down after a shutdown) or its control is manual (a person is in
    // charge), it stays failed, with no power step and nothing allowed on it.
    for (id, before) in &prev.resources {
        let PowerPhase::Failed { during, .. } = before.power else {
            continue;
        };
        let wanted_seen = match during {
            PowerTransition::PoweringOn => host_up(config, snapshot, id, safety),
            PowerTransition::ShuttingDown => host_seen_down(config, snapshot, id, safety),
        };
        if wanted_seen || !managed_power(config, id) {
            continue;
        }
        if let Some(step) = decision.steps.iter().find(|s| {
            matches!(s, Step::PowerOn { resource } | Step::PowerOff { resource } if resource == id)
        }) {
            out.push(format!(
                "invariant 9: {step:?} for {id}, which is in the failed power state"
            ));
        }
        if let Some(after) = decision.state.resource(id)
            && !matches!(after.power, PowerPhase::Failed { .. })
        {
            out.push(format!(
                "invariant 9: {id} left the failed power state ({during:?}) to {:?} without what the transition was after",
                after.power
            ));
        }
    }

    // One acting step per resource, except a switch: the withdrawal of the previously
    // allowed model and the allowance of the next one, together.
    let mut acting: BTreeMap<&ResourceId, Vec<&Step>> = BTreeMap::new();
    for step in &decision.steps {
        if let Some(r) = step.resource()
            && !matches!(step, Step::Wait { .. })
        {
            acting.entry(r).or_default().push(step);
        }
    }
    for (r, list) in &acting {
        let switch = matches!(
            list.as_slice(),
            [Step::WithdrawModel { model: a, .. }, Step::AllowModel { model: b, .. }]
                | [Step::AllowModel { model: b, .. }, Step::WithdrawModel { model: a, .. }]
                if a != b
        );
        if list.len() > 1 && !switch {
            out.push(format!(
                "invariant 4: more than one acting step for {r}: {list:?}"
            ));
        }
    }

    for step in &decision.steps {
        match step {
            // 1, 3, 5. Allowing a model: configured on the resource, on a host freshly observed
            // up, and the model allowed before (if another) leaves in the same decision.
            Step::AllowModel { resource, model } => {
                if !model_of(config, resource, model) {
                    out.push(format!(
                        "invariant 3: allowed {model}, which is not a configured model of {resource}"
                    ));
                }
                let powered = config.resource(resource).is_some_and(|r| r.power.is_some());
                if powered && !host_up(config, snapshot, resource, safety) {
                    out.push(format!(
                        "invariant 5: allowed {model} on {resource} without a fresh up observation"
                    ));
                }
                if let Some(old) = prev.resource(resource).and_then(|s| s.mode.allowed_model())
                    && old != model
                    && !decision.steps.iter().any(|s| {
                        matches!(s, Step::WithdrawModel { resource: r, model: m } if r == resource && m == old)
                    })
                {
                    out.push(format!(
                        "invariant 1: allowed {model} on {resource} while {old} stays allowed"
                    ));
                }
            }
            // 6. Power-on needs a manual request, a power-changing class with work or a job's hold.
            Step::PowerOn { resource } => {
                if !managed_power(config, resource) {
                    out.push(format!(
                        "invariant 6: powered on {resource}, whose power is under manual control"
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
                // A shared hold is a scheduled job's: jobs are workloads allowed to change
                // power (a hold only ever exists for a running or waiting job).
                let held = snapshot.leases.iter().any(|l| {
                    &l.resource == resource
                        && matches!(l.kind, LeaseKind::SharedHold { .. })
                        && active(&l.kind, now)
                });
                if !manual && !workload && !held {
                    out.push(format!(
                        "invariant 6: powered on {resource} without a manual or workload reason"
                    ));
                }
                // 8. No automatic power-on within `min_off` of a shutdown flowmanager
                // started and saw confirmed; a manual lease is the only exception. The
                // confirmation is the stamp the previous state carries, or a shutdown that
                // this snapshot shows complete (the host freshly read down).
                let min_off = config
                    .resource(resource)
                    .and_then(|r| r.power.as_ref())
                    .and_then(|p| p.min_off)
                    .unwrap_or(Millis::ZERO);
                if !manual && min_off > Millis::ZERO {
                    let before = prev.resource(resource);
                    let confirmed = match before.map(|s| (s.power, s.off_since)) {
                        Some((PowerPhase::Off { .. }, Some(at))) => Some(at),
                        Some((
                            PowerPhase::ShuttingDown { .. }
                            | PowerPhase::Failed {
                                during: PowerTransition::ShuttingDown,
                                ..
                            },
                            _,
                        )) if host_seen_down(config, snapshot, resource, safety) => Some(now),
                        _ => None,
                    };
                    if let Some(at) = confirmed
                        && now.saturating_sub(at) < min_off
                    {
                        out.push(format!(
                            "invariant 8: powered on {resource} {}ms after a confirmed shutdown, within its min_off of {min_off}, with no manual lease",
                            now.saturating_sub(at).as_u64()
                        ));
                    }
                }
                if locked_out(config, snapshot, resource) {
                    out.push(format!("invariant 14: powered on locked out {resource}"));
                }
            }
            // 5, 6, 7, 10, 11, 13, 14. Power-off needs no holds (a lockout of a host with a
            // power section is the only exception), idle grace, management, and fresh observations.
            Step::PowerOff { resource } => {
                if !managed_power(config, resource) {
                    out.push(format!(
                        "invariant 6: powered off {resource}, whose power is under manual control"
                    ));
                }
                let before = prev.resource(resource);
                if !locked_out(config, snapshot, resource) {
                    if !before.is_some_and(|s| s.ours()) {
                        out.push(format!(
                            "invariant 7: powered off {resource}, which flowmanager did not start (not ours)"
                        ));
                    }
                    // 11. A shared host's consumers are scaled down before it is shut down: by a
                    // drain a person asked for or, when it merely went idle, by its own idle drain.
                    if config
                        .resource(resource)
                        .is_some_and(|r| r.kind == ResourceKind::Shared)
                        && !decision
                            .state
                            .resource(resource)
                            .is_some_and(|s| s.shutdown.is_some_and(|x| x.is_drain()))
                    {
                        out.push(format!(
                            "invariant 11: powered off shared {resource} with no drain scaling its consumers down first"
                        ));
                    }
                    let held = snapshot.leases.iter().any(|l| {
                        &l.resource == resource
                            && !matches!(l.kind, LeaseKind::Lockout)
                            && active(&l.kind, now)
                    });
                    if held {
                        out.push(format!(
                            "invariant 10: powered off {resource} while a lease holds it"
                        ));
                    }
                    // A drain (a person asked for the host to go down) ignores queued work, never
                    // a lease, and only once its consumers are scaled down: `immediate`, or the
                    // scale-down already happened, or `drain_timeout` has passed, or every class
                    // that needs the host is known to have nothing in flight.
                    // Only a requested drain waives work and grace: an idle drain ignores nothing.
                    let drain = before.and_then(|s| s.shutdown).filter(|s| s.is_requested());
                    let draining = drain.is_some();
                    if let Some(d) = drain {
                        let scaled = matches!(
                            d.cause(),
                            sched_model::ShutdownCause::Requested {
                                mode: sched_model::DrainMode::Immediate
                            }
                        ) || d.consumers_withdrawn()
                            || now.saturating_sub(d.since()) >= policy.drain_timeout
                            || config
                                .classes()
                                .iter()
                                .filter(|c| c.demand.is_some() && needs(c, resource))
                                .all(|c| demand(config, snapshot, c).is_some_and(|x| !x.1));
                        if !scaled {
                            out.push(format!(
                                "invariant 11: powered off {resource} on a graceful drain before its consumers were scaled down"
                            ));
                        }
                    }
                    for class in config
                        .classes()
                        .iter()
                        .filter(|c| !draining && c.demand.is_some() && needs(c, resource))
                    {
                        if demand(config, snapshot, class).is_none_or(|d| d.0) {
                            out.push(format!(
                                "invariant 10: powered off {resource} while {} has or may have work",
                                class.id
                            ));
                        }
                    }
                    let grace_ok = policy.idle_grace == Millis::ZERO
                        || draining
                        || before.and_then(|s| s.idle_since).is_some_and(|t| {
                            now.as_u64() - t.as_u64().min(now.as_u64())
                                >= policy.idle_grace.as_u64()
                        });
                    if !grace_ok {
                        out.push(format!(
                            "invariant 10: powered off {resource} before idle grace ended"
                        ));
                    }
                } else {
                    // 14. A lockout overrides holds, leases, work and grace, but shuts the host
                    // down once: never again after it was seen off under the lockout (a later
                    // boot is left alone).
                    if before.is_some_and(|s| s.locked_down) {
                        out.push(format!(
                            "invariant 14: powered off locked out {resource} after it was seen off under the lockout"
                        ));
                    }
                    // 11. A shared host's consumers are withdrawn first, and in-flight work was
                    // given `lockout_timeout` (or none was in flight).
                    if config
                        .resource(resource)
                        .is_some_and(|r| r.kind == ResourceKind::Shared)
                    {
                        let withdrawn = decision
                            .state
                            .resource(resource)
                            .and_then(|s| s.shutdown)
                            .filter(|s| s.is_drain() && s.consumers_withdrawn());
                        match withdrawn {
                            None => out.push(format!(
                                "invariant 11: powered off locked out shared {resource} with no withdrawal of its consumers first"
                            )),
                            Some(d) => {
                                // A withdrawal already done before this tick (a drain the
                                // lockout took over) stays done and needs no new wait.
                                let was_done = before
                                    .and_then(|s| s.shutdown)
                                    .is_some_and(|x| x.consumers_withdrawn());
                                let waited = was_done
                                    || d.cause() != sched_model::ShutdownCause::Lockout
                                    || now.saturating_sub(d.since()) >= policy.lockout_timeout()
                                    || config
                                        .classes()
                                        .iter()
                                        .filter(|c| c.demand.is_some() && needs(c, resource))
                                        .all(|c| demand(config, snapshot, c).is_some_and(|x| !x.1));
                                if !waited {
                                    out.push(format!(
                                        "invariant 11: powered off locked out shared {resource} before its in-flight work had lockout_timeout"
                                    ));
                                }
                            }
                        }
                    }
                }
                if !host_up(config, snapshot, resource, safety) {
                    out.push(format!(
                        "invariant 5: powered off {resource} without a fresh up observation"
                    ));
                }
                // 13. A host is powered off only on fresh readings that say nothing still uses
                // it, for every reading that applies to it: no pod mounts its NFS server (where
                // a mounts source names it), and no pod of any of its models exists (where a
                // pods source covers them). An unknown count is as good as a used one.
                if config.pods_checked(resource) {
                    for m in config.models_of(resource) {
                        let clear = newest(config, Capability::Pods, snapshot, safety, |v| {
                            matches!(v, ObservedValue::ModelPods { model, .. } if model == &m.id)
                        })
                        .is_some_and(|g| {
                            g.iter()
                                .all(|v| matches!(v, ObservedValue::ModelPods { pods: 0, .. }))
                        });
                        if !clear {
                            out.push(format!(
                                "invariant 13: powered off {resource} without a fresh reading of no pods of model {}",
                                m.id
                            ));
                        }
                    }
                }
                if config.mounts_checked(resource) {
                    let clear = newest(config, Capability::Mounts, snapshot, safety, |v| {
                        matches!(v, ObservedValue::ResourceMounts { resource: r, .. } if r == resource)
                    })
                    .is_some_and(|g| {
                        g.iter()
                            .all(|v| matches!(v, ObservedValue::ResourceMounts { pods: 0, .. }))
                    });
                    if !clear {
                        out.push(format!(
                            "invariant 13: powered off {resource} without a fresh reading of no mounting pods"
                        ));
                    }
                }
                for m in config.models_of(resource) {
                    if decision.allowed.models.contains(&m.id) {
                        out.push(format!(
                            "invariant 10: powered off {resource} while {} is allowed on it",
                            m.id
                        ));
                    }
                }
            }
            // 16. Stopping in-flight work is allowed only per class, after that class's own
            // timeout (`kill_timeout`, else the cause's: `lockout_timeout` under a lockout,
            // `drain_timeout` otherwise) has run out within a running wind-down.
            Step::KillInFlight { class } => {
                let Some(spec) = config.class(class) else {
                    continue;
                };
                let Some(res) = spec
                    .requires
                    .iter()
                    .find(|r| r.model.is_some())
                    .map(|r| &r.resource)
                else {
                    out.push(format!(
                        "invariant 16: stopped in-flight work of {class}, which requires no model"
                    ));
                    continue;
                };
                if demand(config, snapshot, spec).is_some_and(|d| !d.1) {
                    out.push(format!(
                        "invariant 16: stopped {class} though nothing of it is in flight"
                    ));
                }
                let lockout = locked_out(config, snapshot, res);
                let base = if lockout {
                    policy.lockout_timeout()
                } else {
                    policy.drain_timeout
                };
                // A shutdown a person asked for on this host: `immediate` cuts work off at
                // once, `graceful` (unless a lockout holds the host) after exactly
                // `drain_timeout` from the request; neither waits for a running wind-down
                // nor for a class's `kill_timeout`.
                match (requested_shutdown(prev, snapshot, res), lockout) {
                    (Some((sched_model::DrainMode::Immediate, _)), _) => continue,
                    (Some((sched_model::DrainMode::Graceful, asked)), false) => {
                        let due = asked.as_u64().saturating_add(policy.drain_timeout.as_u64());
                        if now.as_u64() < due {
                            out.push(format!(
                                "invariant 16: stopped {class} {}ms before the graceful shutdown's drain_timeout",
                                due - now.as_u64()
                            ));
                        }
                        continue;
                    }
                    _ => {}
                }
                match prev.resource(res).map(|s| &s.mode) {
                    Some(ModePhase::Serving {
                        wind_down: Some(wd),
                        ..
                    }) if wd.lockout == lockout && wd.timeout.is_some() => {
                        let due = wd
                            .since
                            .as_u64()
                            .saturating_add(spec.kill_timeout.unwrap_or(base).as_u64());
                        if now.as_u64() < due {
                            out.push(format!(
                                "invariant 16: stopped {class} {}ms early",
                                due - now.as_u64()
                            ));
                        }
                    }
                    _ => out.push(format!(
                        "invariant 16: stopped {class} without a running wind-down on the current cause"
                    )),
                }
            }
            // 16. Withdrawing a serving model needs a settled model or a declared-safe class.
            // A model the config no longer has on the resource is withdrawn at once: none of
            // its work is known any more.
            Step::WithdrawModel { resource, model } => {
                if !model_of(config, resource, model) {
                    continue;
                }
                let Some(ModePhase::Serving {
                    since, wind_down, ..
                }) = prev
                    .resource(resource)
                    .map(|s| &s.mode)
                    .filter(|m| m.allowed_model() == Some(model))
                else {
                    out.push(format!(
                        "invariant 1: withdrew {model} on {resource}, which was not allowed"
                    ));
                    continue;
                };
                let forced = locked_out(config, snapshot, resource)
                    || manual_model(snapshot, resource)
                    || requested_shutdown(prev, snapshot, resource).is_some();
                if !forced
                    && now.as_u64() - since.as_u64().min(now.as_u64()) < policy.min_dwell.as_u64()
                {
                    out.push(format!("invariant 16: drained {model} before min_dwell"));
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
                            "invariant 16: drained {model} with possible in-flight work of {}",
                            class.id
                        ));
                    }
                    let busy = d.is_some_and(|d| d.0);
                    if !forced && busy && class.preemptibility == sched_model::Preemptibility::None
                    {
                        out.push(format!(
                            "invariant 16: switched away early from unpreemptible {}",
                            class.id
                        ));
                    }
                }
            }
            // 17. A recovery runs only for a class in the allowed set (its model serving, not
            // winding down) that has a recovery action, and only for failed items that a
            // fresh reading reports. Recovery never runs for a model that is not allowed.
            Step::RunRecovery { class } => {
                let Some(spec) = config.class(class) else {
                    continue;
                };
                if spec.recovery.is_none() {
                    out.push(format!(
                        "invariant 17: ran recovery for {class}, which declares none"
                    ));
                }
                if !decision.allowed.classes.contains(class) {
                    out.push(format!(
                        "invariant 17: ran recovery for {class}, which is not in the allowed set"
                    ));
                }
                if failed_items(config, snapshot, spec).is_none_or(|n| n == 0) {
                    out.push(format!(
                        "invariant 17: ran recovery for {class} without fresh failed items"
                    ));
                }
            }
            _ => {}
        }
    }
    out
}
