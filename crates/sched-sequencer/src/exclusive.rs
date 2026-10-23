//! The mode machine of an exclusive resource: idle, loading, serving (possibly winding
//! down), draining.

use sched_model::{
    ClassSpec, Millis, ModePhase, ModelId, Preemptibility, ResourceState, Step, WaitReason,
    WindDown, class_work, classes_of_model,
};

use crate::power::wait;
use crate::{Cx, others_zero};

/// Advances the mode machine. Returns whether a resource-scoped step was emitted.
pub(crate) fn step(cx: &Cx, st: &mut ResourceState, steps: &mut Vec<Step>) -> bool {
    let target = if cx.reclaimed() {
        None
    } else {
        cx.intent.models.get(&cx.res.id).cloned().flatten()
    };
    let forced = cx.reclaimed() || cx.intent.forced.contains(&cx.res.id);
    // A draining model that reaches zero pods lets the next model be allowed in the same tick.
    for _ in 0..2 {
        match st.mode.clone() {
            ModePhase::Draining { model, .. } => match zero_pods(cx, &model) {
                Ok(true) => {
                    st.mode = ModePhase::Idle;
                    recover(st, steps);
                }
                Ok(false) => return waiting(cx, steps, WaitReason::Draining),
                Err(r) => return waiting(cx, steps, r),
            },
            ModePhase::Idle => return idle(cx, st, steps, target.as_ref()),
            ModePhase::Loading { model, .. } => {
                return loading(cx, st, steps, &model, target.as_ref());
            }
            ModePhase::Serving { .. } => return serving(cx, st, steps, target.as_ref(), forced),
        }
    }
    false
}

fn waiting(cx: &Cx, steps: &mut Vec<Step>, reason: WaitReason) -> bool {
    wait(cx, steps, reason);
    true
}

fn zero_pods(cx: &Cx, model: &ModelId) -> Result<bool, WaitReason> {
    match cx
        .snapshot
        .fresh_model_pods(model, cx.config.policy().safety_max_age)
    {
        Some((pods, _)) => Ok(pods == 0),
        None => Err(WaitReason::StaleObservation),
    }
}

/// Runs the recovery actions of classes whose work was stopped, once their model is gone.
fn recover(st: &mut ResourceState, steps: &mut Vec<Step>) {
    for class in std::mem::take(&mut st.pending_recovery) {
        steps.push(Step::RunRecovery { class });
    }
}

fn idle(cx: &Cx, st: &mut ResourceState, steps: &mut Vec<Step>, target: Option<&ModelId>) -> bool {
    let Some(model) = target else {
        return false;
    };
    if let Err(reason) = others_zero(cx, Some(model)) {
        return waiting(cx, steps, reason);
    }
    steps.push(Step::AllowModel {
        resource: cx.res.id.clone(),
        model: model.clone(),
    });
    st.mode = ModePhase::Loading {
        model: model.clone(),
        since: cx.now(),
    };
    true
}

fn loading(
    cx: &Cx,
    st: &mut ResourceState,
    steps: &mut Vec<Step>,
    model: &ModelId,
    target: Option<&ModelId>,
) -> bool {
    if target != Some(model) {
        return drain(cx, st, steps, model);
    }
    match cx
        .snapshot
        .fresh_model_pods(model, cx.config.policy().safety_max_age)
    {
        Some((_, ready)) if ready >= 1 => {
            st.mode = ModePhase::Serving {
                model: model.clone(),
                since: cx.now(),
                idle_since: None,
                wind_down: None,
            };
            false
        }
        Some(_) => waiting(cx, steps, WaitReason::Loading),
        None => waiting(cx, steps, WaitReason::StaleObservation),
    }
}

fn drain(cx: &Cx, st: &mut ResourceState, steps: &mut Vec<Step>, model: &ModelId) -> bool {
    steps.push(Step::DrainModel {
        resource: cx.res.id.clone(),
        model: model.clone(),
    });
    st.mode = ModePhase::Draining {
        model: model.clone(),
        since: cx.now(),
    };
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
    let wd = match wind_down {
        // Switching from an unforced to a forced wind-down restarts the clock.
        Some(w) if w.timeout.is_some() || base.is_none() => w,
        _ => WindDown {
            since: now,
            timeout: base,
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

    let blockers: Vec<(&ClassSpec, bool)> = classes
        .iter()
        .zip(&works)
        .filter(|(c, w)| c.demand.is_some() && (w.in_flight > 0 || !w.known))
        .map(|(c, w)| (*c, w.known))
        .collect();
    if blockers.is_empty() {
        return drain(cx, st, steps, &model);
    }
    if let Some(timeout) = wd.timeout {
        let expired =
            |c: &ClassSpec| now >= wd.since.saturating_add(c.kill_timeout.unwrap_or(timeout));
        if blockers.iter().all(|(c, _)| expired(c)) {
            for (c, _) in &blockers {
                steps.push(Step::KillInFlight {
                    class: c.id.clone(),
                });
                if c.recovery.is_some() {
                    st.pending_recovery.insert(c.id.clone());
                }
            }
            return drain(cx, st, steps, &model);
        }
    }
    let reason = if blockers.iter().all(|(_, known)| !known) {
        WaitReason::StaleDemand
    } else {
        WaitReason::InFlightWork
    };
    waiting(cx, steps, reason)
}
