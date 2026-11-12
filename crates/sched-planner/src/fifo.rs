//! First-in-first-out, exhaustive service with bounds.
//!
//! Model selection:
//! 1. A model's *priority* is the highest priority among its classes that have runnable
//!    work: ready items, or failed ones (see below). Higher priority tiers win.
//! 2. A model with a ready queue older than `age_threshold` is *starved* and ranks above
//!    every tier; among starved models the usual order applies.
//! 3. Within a tier, the model whose oldest ready item has waited longest wins, then the
//!    lower `load_time` (switch cost), then the lower id.
//!
//! *Failed* items (a separate demand bucket, waiting for the class's recovery action to
//! re-queue them) make their model wanted, at the class's normal priority, so the model
//! is brought in to re-queue them. They are not ready work: they have no age (a model
//! wanted only for failed items ranks as if its oldest item arrived this instant, so it
//! never starves and loses ties on age to real work of the same priority), and failed
//! items never hold a model: for the model currently serving they count as nothing, so
//! they neither keep it from going idle (`idle_grace` counts as for any idle model) nor
//! make it the pick when it is released. A failed-only model that was just loaded and
//! has re-queued its items is therefore released after the usual `min_dwell` and
//! `idle_grace`, not longer. Failed items stay listed only for the source's `since`
//! window, which bounds how long they can bring a model in.
//!
//! A loaded model keeps serving while it has work (exhaustive). It yields to the best
//! *other* candidate only when it has been allowed for at least `min_dwell` (counted from
//! the tick it was allowed, so load time is included) and either
//! `max_dwell` has passed or another queue has become starved, and only when none of its
//! busy classes is unpreemptible. An idle model is released only after `idle_grace`.
//! Leases override this: a manual model lease forces its model, an interactive lease pins
//! its model against batch eviction, and a lockout clears the resource.

use std::cmp::Reverse;
use std::collections::BTreeSet;

use sched_model::{
    ClassSpec, Config, Intent, Lease, LeaseKind, Millis, ModePhase, ModelId, ModelSpec, PowerWant,
    Preemptibility, ResourceId, ResourceKind, SchedState, Snapshot, class_work, classes_of_model,
    host_is_up,
};

use crate::Strategy;

pub struct FifoExhaustive;

impl Strategy for FifoExhaustive {
    fn name(&self) -> &'static str {
        "fifo-exhaustive"
    }

    fn plan(&self, config: &Config, snapshot: &Snapshot, state: &SchedState) -> Intent {
        let mut intent = Intent::default();
        let now = snapshot.now;
        let active = |l: &&Lease| l.is_active(now);

        intent.lockout = config
            .resources()
            .iter()
            .filter(|r| config.locked_out(&snapshot.leases, &r.id, now))
            .map(|r| r.id.clone())
            .collect();

        for l in snapshot.leases.iter().filter(active) {
            if matches!(
                l.kind,
                LeaseKind::ManualHost | LeaseKind::ManualModel { .. }
            ) {
                intent
                    .power_up
                    .insert(l.resource.clone(), PowerWant::Manual);
            }
        }
        // A shared hold (a scheduled job's NFS hold, say) wants its host up like a workload
        // does: a managed host that is off is woken for it. It never takes the host over
        // (only a manual request does), so a host found running stays not ours to shut down.
        for l in snapshot.leases.iter().filter(active) {
            if matches!(l.kind, LeaseKind::SharedHold { .. })
                && config
                    .resource(&l.resource)
                    .is_some_and(|r| r.power.is_some())
            {
                intent
                    .power_up
                    .entry(l.resource.clone())
                    .or_insert(PowerWant::Workload);
            }
        }
        for class in config.classes().iter().filter(|c| c.may_change_power) {
            // A switched class has no demand: it wants its hosts up while its switch is on.
            let wants_hosts = match sched_model::class_switch(config, snapshot, class) {
                Some(switch) => switch.on(),
                None => class_work(config, snapshot, state, class).busy(),
            };
            if !wants_hosts {
                continue;
            }
            for req in &class.requires {
                let powered = config
                    .resource(&req.resource)
                    .is_some_and(|r| r.power.is_some());
                if powered {
                    intent
                        .power_up
                        .entry(req.resource.clone())
                        .or_insert(PowerWant::Workload);
                }
            }
        }
        intent.power_up.retain(|r, _| !intent.lockout.contains(r));

        for res in config
            .resources()
            .iter()
            .filter(|r| r.kind == ResourceKind::Exclusive)
        {
            let (target, forced) = target_for(config, snapshot, state, &res.id, &intent.lockout);
            intent.models.insert(res.id.clone(), target);
            if forced {
                intent.forced.insert(res.id.clone());
            }
        }
        intent
    }
}

/// The model a resource should serve, and whether getting there is forced.
fn target_for(
    config: &Config,
    snap: &Snapshot,
    state: &SchedState,
    resource: &ResourceId,
    locked_out: &BTreeSet<ResourceId>,
) -> (Option<ModelId>, bool) {
    if locked_out.contains(resource) {
        return (None, true);
    }
    let now = snap.now;
    let leases = || {
        snap.leases
            .iter()
            .filter(|l| l.resource == *resource && l.is_active(now))
    };

    // The most recent manual request wins (then the lowest lease); interactive sessions are
    // first come. The lease's own ordering breaks every tie, so snapshot order is moot.
    let manual = leases()
        .filter(|l| matches!(l.kind, LeaseKind::ManualModel { .. }))
        .max_by(|a, b| a.granted_at.cmp(&b.granted_at).then_with(|| b.cmp(a)));
    if let Some(l) = manual {
        return (l.pinned_model(now).cloned(), true);
    }
    let interactive = leases()
        .filter(|l| matches!(l.kind, LeaseKind::Interactive { .. }))
        .min_by(|a, b| a.granted_at.cmp(&b.granted_at).then_with(|| a.cmp(b)));
    if let Some(l) = interactive {
        return (l.pinned_model(now).cloned(), false);
    }

    let policy = config.policy();
    let mode = state.resource(resource).map(|s| &s.mode);
    // Failed items bring a model in but never keep or re-pick the one serving.
    let serving = match mode {
        Some(ModePhase::Serving { model, .. }) => Some(model),
        _ => None,
    };
    let candidates = candidates(config, snap, state, resource, serving);
    // A model the config no longer has on this resource is not loaded as far as the policy
    // is concerned: it gets no dwell and no grace, and the best candidate is chosen.
    let configured = |m: &ModelId| config.model(m).is_some_and(|s| &s.resource == resource);
    match mode {
        Some(ModePhase::Serving {
            model,
            since,
            idle_since,
            ..
        }) if configured(model) => {
            let served = now.saturating_sub(*since);
            let (busy, preemptible) = cur_status(config, snap, state, model);
            if busy {
                let others: Vec<&Candidate> =
                    candidates.iter().filter(|c| &c.model != model).collect();
                let starved_other = others.iter().any(|c| c.starved);
                let may_yield = served >= policy.min_dwell
                    && (served > policy.max_dwell || starved_other)
                    && preemptible;
                match best(&others) {
                    Some(next) if may_yield => (Some(next.model.clone()), false),
                    _ => (Some(model.clone()), false),
                }
            } else {
                let idle_for = now.saturating_sub(idle_since.unwrap_or(now));
                if idle_for >= policy.idle_grace && served >= policy.min_dwell {
                    let all: Vec<&Candidate> = candidates.iter().collect();
                    (best(&all).map(|c| c.model.clone()), false)
                } else {
                    (Some(model.clone()), false)
                }
            }
        }
        _ => {
            let all: Vec<&Candidate> = candidates.iter().collect();
            (best(&all).map(|c| c.model.clone()), false)
        }
    }
}

/// Whether the loaded model has work (or unknown demand, assumed busy), and whether all
/// of that work may be interrupted by yielding.
fn cur_status(
    config: &Config,
    snap: &Snapshot,
    state: &SchedState,
    model: &ModelId,
) -> (bool, bool) {
    let mut busy = false;
    let mut preemptible = true;
    for class in classes_of_model(config, model) {
        let w = class_work(config, snap, state, class);
        let active = w.busy() || (class.demand.is_some() && !w.known);
        busy |= active;
        if active && class.preemptibility == Preemptibility::None {
            preemptible = false;
        }
        if class.demand.is_some() && !w.known {
            preemptible = false;
        }
    }
    (busy, preemptible)
}

struct Candidate {
    model: ModelId,
    /// `u64::MAX` for a starved model, otherwise the highest priority of its ready classes.
    tier: u64,
    starved: bool,
    oldest_at: Millis,
    load_time: Millis,
}

fn candidates(
    config: &Config,
    snap: &Snapshot,
    state: &SchedState,
    resource: &ResourceId,
    serving: Option<&ModelId>,
) -> Vec<Candidate> {
    config
        .models_of(resource)
        .filter_map(|m| candidate(config, snap, state, m, serving != Some(&m.id)))
        .collect()
}

fn candidate(
    config: &Config,
    snap: &Snapshot,
    state: &SchedState,
    m: &ModelSpec,
    count_failed: bool,
) -> Option<Candidate> {
    let now = snap.now;
    let mut top: Option<u64> = None;
    let mut oldest: Option<Millis> = None;
    let mut starved = false;
    for class in classes_of_model(config, &m.id) {
        let w = class_work(config, snap, state, class);
        let wanted = w.ready > 0 || (count_failed && w.failed > 0);
        if !wanted || !runnable(config, snap, state, class) {
            continue;
        }
        top = top.max(Some(u64::from(class.priority)));
        if w.ready > 0 {
            let at = w.oldest_ready_at.unwrap_or(now);
            oldest = Some(oldest.map_or(at, |o| o.min(at)));
            starved |= now.saturating_sub(at) > config.policy().age_threshold;
        } else {
            // Failed items only: no age, so no starvation, and last in a tie of ages.
            oldest = Some(oldest.map_or(now, |o| o.min(now)));
        }
    }
    Some(Candidate {
        model: m.id.clone(),
        tier: if starved { u64::MAX } else { top? },
        starved,
        oldest_at: oldest?,
        load_time: m.load_time,
    })
}

/// A class can make use of its model only if every other host it needs is up.
fn runnable(config: &Config, snap: &Snapshot, state: &SchedState, class: &ClassSpec) -> bool {
    class
        .requires
        .iter()
        .all(|r| host_is_up_or_self(config, snap, state, &r.resource, class))
}

fn host_is_up_or_self(
    config: &Config,
    snap: &Snapshot,
    state: &SchedState,
    resource: &ResourceId,
    class: &ClassSpec,
) -> bool {
    // The resource the model lives on may still be coming up; that is the sequencer's wait.
    let hosts_model = class
        .requires
        .iter()
        .any(|r| &r.resource == resource && r.model.is_some());
    hosts_model || host_is_up(config, snap, state, resource)
}

fn best<'a>(cands: &[&'a Candidate]) -> Option<&'a Candidate> {
    cands
        .iter()
        .copied()
        .min_by_key(|c| (Reverse(c.tier), c.oldest_at, c.load_time, &c.model))
}
