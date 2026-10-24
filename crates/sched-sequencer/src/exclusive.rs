//! The mode machine of an exclusive resource: idle, or serving one model (possibly winding
//! down). Decided from queues, leases and policy only, never from pod state: GPU
//! exclusivity is enforced by Kubernetes, so a switch withdraws the old model and allows the
//! new one in the same tick, and the new pod waits (pending) until the old one has released
//! the GPU.

use sched_model::{
    ClassSpec, Millis, ModePhase, ModelId, Preemptibility, ResourceState, Step, WaitReason,
    WindDown, class_work, classes_of_model,
};

use crate::Cx;
use crate::power::wait;

/// Advances the mode machine. Returns whether a resource-scoped step was emitted.
pub(crate) fn step(cx: &Cx, st: &mut ResourceState, steps: &mut Vec<Step>) -> bool {
    let target = if cx.reclaimed() {
        None
    } else {
        cx.intent
            .models
            .get(&cx.res.id)
            .cloned()
            .flatten()
            // Never a model the config does not have on this resource, whatever the intent.
            .filter(|m| configured(cx, m))
    };
    let forced = cx.reclaimed() || cx.intent.forced.contains(&cx.res.id);
    match st.mode.clone() {
        ModePhase::Idle => {
            // Left by a state from before the queue-only change (a drain in progress).
            recover(st, steps);
            match &target {
                Some(m) => allow(cx, st, steps, m),
                None => false,
            }
        }
        ModePhase::Serving { model, .. } if !configured(cx, &model) => {
            // Removed from the config (or moved to another resource): it leaves the allowed
            // set now, with no dwell, grace or wind-down; nothing of it can be waited for.
            switch(cx, st, steps, &model, target.as_ref())
        }
        ModePhase::Serving { .. } => serving(cx, st, steps, target.as_ref(), forced),
    }
}

/// Whether `model` is configured on this resource.
fn configured(cx: &Cx, model: &ModelId) -> bool {
    cx.config
        .model(model)
        .is_some_and(|m| m.resource == cx.res.id)
}

fn waiting(cx: &Cx, steps: &mut Vec<Step>, reason: WaitReason) -> bool {
    wait(cx, steps, reason);
    true
}

/// Whether the host may be given a new model: a resource without a power section always,
/// otherwise only on a fresh "up" reading (safety-critical, held to `safety_max_age`).
fn fresh_up(cx: &Cx) -> bool {
    cx.res.power.is_none()
        || cx.snapshot.fresh_resource_up(
            cx.config,
            &cx.res.id,
            cx.config.policy().safety_max_age,
        ) == Some(true)
}

/// Runs the recovery actions of classes whose work was stopped, once their model is gone.
fn recover(st: &mut ResourceState, steps: &mut Vec<Step>) {
    for class in std::mem::take(&mut st.pending_recovery) {
        steps.push(Step::RunRecovery { class });
    }
}

/// Allows `model` on an idle resource. It is serving from this tick on.
fn allow(cx: &Cx, st: &mut ResourceState, steps: &mut Vec<Step>, model: &ModelId) -> bool {
    if !fresh_up(cx) {
        return waiting(cx, steps, WaitReason::StaleObservation);
    }
    steps.push(Step::AllowModel {
        resource: cx.res.id.clone(),
        model: model.clone(),
    });
    st.mode = ModePhase::Serving {
        model: model.clone(),
        since: cx.now(),
        idle_since: None,
        wind_down: None,
    };
    true
}

/// Replaces `old` by `next` in one tick: `old` leaves the allowed set and `next` (if any)
/// enters it. Withdrawing needs nothing; allowing `next` needs a fresh "up" reading, and
/// without one the switch waits as a whole (the old model stays) rather than leaving the
/// resource empty.
fn switch(
    cx: &Cx,
    st: &mut ResourceState,
    steps: &mut Vec<Step>,
    old: &ModelId,
    next: Option<&ModelId>,
) -> bool {
    if next.is_some() && !fresh_up(cx) {
        return waiting(cx, steps, WaitReason::StaleObservation);
    }
    steps.push(Step::DrainModel {
        resource: cx.res.id.clone(),
        model: old.clone(),
    });
    st.mode = ModePhase::Idle;
    recover(st, steps);
    if let Some(m) = next {
        allow(cx, st, steps, m);
    }
    true
}

fn serving(
    cx: &Cx,
    st: &mut ResourceState,
    steps: &mut Vec<Step>,
    target: Option<&ModelId>,
    forced: bool,
) -> bool {
    let ModePhase::Serving {
        model,
        since,
        idle_since,
        wind_down,
    } = st.mode.clone()
    else {
        return false;
    };
    let now = cx.now();
    let policy = cx.config.policy();
    let classes: Vec<&ClassSpec> = classes_of_model(cx.config, &model).collect();
    let works: Vec<_> = classes
        .iter()
        .map(|c| class_work(cx.config, cx.snapshot, c))
        .collect();
    let busy = classes
        .iter()
        .zip(&works)
        .any(|(c, w)| w.busy() || (c.demand.is_some() && !w.known));

    if target == Some(&model) {
        // Staying: resume intake if a switch was abandoned, and track idleness.
        st.mode = ModePhase::Serving {
            model,
            since,
            idle_since: if busy { None } else { idle_since.or(Some(now)) },
            wind_down: None,
        };
        return false;
    }

    if !forced && now.saturating_sub(since) < policy.min_dwell {
        return waiting(cx, steps, WaitReason::MinDwell);
    }
    if !forced {
        let unknown = classes
            .iter()
            .zip(&works)
            .any(|(c, w)| c.demand.is_some() && !w.known);
        if unknown {
            return waiting(cx, steps, WaitReason::StaleDemand);
        }
        let blocked = classes
            .iter()
            .zip(&works)
            .any(|(c, w)| w.busy() && c.preemptibility == Preemptibility::None);
        if blocked {
            return waiting(cx, steps, WaitReason::NotPreemptible);
        }
    }

    let base: Option<Millis> = forced.then(|| {
        if cx.reclaimed() {
            policy.reclaim_timeout()
        } else {
            policy.drain_timeout
        }
    });
    let started = wind_down.is_none();
    // The wind-down follows its cause. A different cause (a reclaim arriving during a
    // manual-lease wind-down, or the reverse) restarts the clock with the new cause's
    // timeout; losing the forcing cause (the lease was released) removes the timeout so
    // nothing can be stopped any more. Classes already stopped stay recorded.
    let wd = match (wind_down, base) {
        (Some(w), Some(t)) if w.timeout.is_some() && w.reclaim == cx.reclaimed() => WindDown {
            timeout: Some(t),
            ..w
        },
        (Some(w), Some(t)) => WindDown {
            since: now,
            timeout: Some(t),
            reclaim: cx.reclaimed(),
            killed: w.killed,
        },
        (Some(w), None) => WindDown {
            since: w.since,
            timeout: None,
            reclaim: false,
            killed: w.killed,
        },
        (None, t) => WindDown {
            since: now,
            timeout: t,
            reclaim: cx.reclaimed(),
            killed: Default::default(),
        },
    };
    if started {
        for c in &classes {
            steps.push(Step::PauseIntake {
                class: c.id.clone(),
            });
        }
    }
    st.mode = ModePhase::Serving {
        model: model.clone(),
        since,
        idle_since,
        wind_down: Some(wd.clone()),
    };

    // Work that may still be running: in flight, or unknown, and not already stopped.
    let blockers: Vec<(&ClassSpec, bool)> = classes
        .iter()
        .zip(&works)
        .filter(|(c, w)| c.demand.is_some() && (w.in_flight > 0 || !w.known))
        .filter(|(c, _)| !wd.killed.contains(&c.id))
        .map(|(c, w)| (*c, w.known))
        .collect();
    // Each class is stopped when its own timeout runs out, not when the last one does.
    if let Some(timeout) = wd.timeout {
        let mut wd = wd.clone();
        let mut stopped = Vec::new();
        for (c, _) in &blockers {
            if now >= wd.since.saturating_add(c.kill_timeout.unwrap_or(timeout)) {
                steps.push(Step::KillInFlight {
                    class: c.id.clone(),
                });
                if c.recovery.is_some() {
                    st.pending_recovery.insert(c.id.clone());
                }
                wd.killed.insert(c.id.clone());
                stopped.push(c.id.clone());
            }
        }
        if !stopped.is_empty() {
            st.mode = ModePhase::Serving {
                model: model.clone(),
                since,
                idle_since,
                wind_down: Some(wd),
            };
        }
        let remaining: Vec<_> = blockers
            .iter()
            .filter(|(c, _)| !stopped.contains(&c.id))
            .collect();
        if remaining.is_empty() {
            return switch(cx, st, steps, &model, target);
        }
        return waiting(
            cx,
            steps,
            blocked_reason(blockers.iter().filter(|(c, _)| !stopped.contains(&c.id))),
        );
    }
    if blockers.is_empty() {
        return switch(cx, st, steps, &model, target);
    }
    waiting(cx, steps, blocked_reason(blockers.iter()))
}

fn blocked_reason<'a>(mut blockers: impl Iterator<Item = &'a (&'a ClassSpec, bool)>) -> WaitReason {
    if blockers.all(|(_, known)| !known) {
        WaitReason::StaleDemand
    } else {
        WaitReason::InFlightWork
    }
}
