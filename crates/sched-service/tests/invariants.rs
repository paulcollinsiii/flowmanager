//! The invariant checker must itself catch what it claims to: each test hands it a decision
//! no correct sequencer would make and expects a violation.

// Power-off invariants are about managed control (M4); manual control is checked apart.
use sched_model::test_support::example_config_managed as example_config;
use sched_model::{
    ClassId, Fixture, Intent, Lease, LeaseId, LeaseKind, Millis, ModePhase, ModelId, ResourceId,
    SchedState, Snapshot, Step, WindDown,
};
use sched_sequencer::Decision;
use sched_service::invariants::check;

const NOW: u64 = 90_000_000;
const MIN: u64 = 60_000;

fn lease(id: &str, res: &str, kind: LeaseKind) -> Lease {
    Lease {
        id: LeaseId::new(id),
        resource: ResourceId::new(res),
        kind,
        granted_at: Millis(0),
    }
}

fn zero_pods_and_up(res: &[&str]) -> Vec<sched_model::Observation> {
    use sched_model::{Fidelity, Observation, ObservedValue};
    let mk = |value| Observation {
        source: "cluster-pods".into(),
        observed_at: Millis(NOW),
        fidelity: Fidelity::Event,
        value,
    };
    let mut o: Vec<_> = ["mineru", "llm", "whisperx", "immich-ml"]
        .iter()
        .map(|m| {
            mk(ObservedValue::ModelPods {
                model: ModelId::new(*m),
                pods: 0,
                ready_pods: 0,
            })
        })
        .collect();
    o.extend(res.iter().map(|r| {
        mk(ObservedValue::ResourceUp {
            resource: ResourceId::new(*r),
            up: true,
        })
    }));
    o
}

fn decision(steps: Vec<Step>) -> Decision {
    Decision {
        steps,
        allowed: Default::default(),
        state: SchedState::default(),
    }
}

fn prev(yaml: &str) -> SchedState {
    let y = format!("description: s\nsnapshot: {{ now: 0 }}\nstate:\n  resources:\n{yaml}");
    Fixture::from_yaml(&y).unwrap().state
}

fn run(snap: &Snapshot, prev: &SchedState, steps: Vec<Step>) -> Vec<String> {
    check(
        &example_config(),
        snap,
        prev,
        &Intent::default(),
        &decision(steps),
    )
}

fn snap(observations: Vec<sched_model::Observation>, leases: Vec<Lease>) -> Snapshot {
    Snapshot {
        config_generation: 0,
        now: Millis(NOW),
        observations,
        leases,
        unobservable: Default::default(),
    }
}

fn power_off(res: &str) -> Step {
    Step::PowerOff {
        resource: ResourceId::new(res),
    }
}

const MANAGED_NFS: &str = "    basestar-nfs: { power: { phase: \"on\", since: 0 }, mode: { phase: idle }, managed: true, idle_since: 1 }\n";

/// The NFS host shutting down under a lockout whose consumers were withdrawn `done` or still
/// `pending`, started at `since`.
fn nfs_lockout_shutdown(withdrawal: &str, since: u64) -> SchedState {
    prev(&format!(
        "    basestar-nfs: {{ power: {{ phase: \"on\", since: 0 }}, mode: {{ phase: idle }}, managed: true, shutdown: {{ cause: {{ kind: lockout }}, since: {since}, withdrawal: {withdrawal}, veto: {{ phase: none }} }} }}\n"
    ))
}

fn nfs_mounts_zero() -> sched_model::Observation {
    sched_model::Observation {
        source: "basestar-mounts".into(),
        observed_at: Millis(NOW),
        fidelity: sched_model::Fidelity::Polled,
        value: sched_model::ObservedValue::ResourceMounts {
            resource: "basestar-nfs".into(),
            pods: 0,
        },
    }
}

fn nfs_leases_under_lockout() -> Vec<Lease> {
    vec![
        lease("r", "basestar-nfs", LeaseKind::Lockout),
        lease(
            "h",
            "basestar-nfs",
            LeaseKind::SharedHold { holder: "x".into() },
        ),
        lease("m", "basestar-nfs", LeaseKind::ManualHost),
    ]
}

fn nfs_world(leases: Vec<Lease>) -> Snapshot {
    let mut obs = zero_pods_and_up(&["basestar-nfs"]);
    obs.extend(idle_demand());
    obs.push(nfs_mounts_zero());
    snap(obs, leases)
}

#[test]
fn a_lockout_on_a_shared_host_excuses_holds_and_leases_once_its_consumers_are_withdrawn() {
    let s = nfs_world(nfs_leases_under_lockout());
    let v = check_with(
        &s,
        &nfs_lockout_shutdown("pending", 1),
        &Decision {
            steps: vec![power_off("basestar-nfs")],
            allowed: Default::default(),
            state: nfs_lockout_shutdown("done", 1),
        },
    );
    assert!(v.is_empty(), "{v:?}");
}

#[test]
fn powering_off_a_locked_out_shared_host_needs_its_withdrawal_and_the_lockout_timeout() {
    // No withdrawal in the decision's state.
    let v = check_with(
        &nfs_world(nfs_leases_under_lockout()),
        &prev(MANAGED_NFS),
        &decision(vec![power_off("basestar-nfs")]),
    );
    assert!(
        v.iter()
            .any(|m| m.contains("invariant 11") && m.contains("withdrawal")),
        "{v:?}"
    );
    // Withdrawn, but work is in flight and the lockout is younger than lockout_timeout.
    let mut obs = zero_pods_and_up(&["basestar-nfs"]);
    obs.extend(idle_demand().into_iter().filter(|o| {
        !matches!(&o.value, sched_model::ObservedValue::Demand { class, .. } if class.as_str() == "paperless-ocr")
    }));
    obs.push(busy("paperless-ocr", "paperless-ocr-pending"));
    obs.push(nfs_mounts_zero());
    let v = check_with(
        &snap(obs, nfs_leases_under_lockout()),
        &nfs_lockout_shutdown("pending", NOW - 1000),
        &Decision {
            steps: vec![power_off("basestar-nfs")],
            allowed: Default::default(),
            state: nfs_lockout_shutdown("done", NOW - 1000),
        },
    );
    assert!(v.iter().any(|m| m.contains("lockout_timeout")), "{v:?}");
}

#[test]
fn a_withdrawal_already_done_before_the_tick_needs_no_new_lockout_wait() {
    // Work in flight, the lockout (re)started a second ago, and the withdrawal was already
    // done in the previous state (a drain the lockout took over): the power-off is fine. The
    // same decision from a withdrawal still pending is the violation above.
    let mut obs = zero_pods_and_up(&["basestar-nfs"]);
    obs.extend(idle_demand().into_iter().filter(|o| {
        !matches!(&o.value, sched_model::ObservedValue::Demand { class, .. } if class.as_str() == "paperless-ocr")
    }));
    obs.push(busy("paperless-ocr", "paperless-ocr-pending"));
    obs.push(nfs_mounts_zero());
    let s = snap(obs, nfs_leases_under_lockout());
    let decision = || Decision {
        steps: vec![power_off("basestar-nfs")],
        allowed: Default::default(),
        state: nfs_lockout_shutdown("done", NOW - 1000),
    };
    let v = check_with(&s, &nfs_lockout_shutdown("done", NOW - 5000), &decision());
    assert!(v.is_empty(), "{v:?}");
    let v = check_with(
        &s,
        &nfs_lockout_shutdown("pending", NOW - 5000),
        &decision(),
    );
    assert!(v.iter().any(|m| m.contains("lockout_timeout")), "{v:?}");
}

#[test]
fn a_locked_out_host_seen_off_under_the_lockout_is_never_powered_on_or_off_again() {
    for (res, state) in [
        (
            "basestar-nfs",
            "    basestar-nfs: { power: { phase: \"on\", since: 0 }, mode: { phase: idle }, locked_down: true }\n",
        ),
        (
            "cuda-central",
            "    cuda-central: { power: { phase: \"on\", since: 0 }, mode: { phase: idle }, locked_down: true }\n",
        ),
    ] {
        let leases = vec![lease("r", res, LeaseKind::Lockout)];
        let v = run(
            &snap(zero_pods_and_up(&[res]), leases.clone()),
            &prev(state),
            vec![power_off(res)],
        );
        assert!(
            v.iter()
                .any(|m| m.contains("invariant 14") && m.contains("seen off")),
            "{res}: {v:?}"
        );
        // And never powered on while locked out, whatever kind of host it is.
        let v = run(
            &snap(vec![], leases),
            &prev(
                &state
                    .replace("\"on\", since: 0", "\"off\"")
                    .replace("phase: \"off\"", "phase: off"),
            ),
            vec![power_on(res)],
        );
        assert!(
            v.iter()
                .any(|m| m.contains("invariant 14") && m.contains("powered on locked out")),
            "{res}: {v:?}"
        );
    }
}

#[test]
fn a_lockout_does_excuse_powering_off_an_exclusive_host() {
    let leases = vec![
        lease("r", "cuda-central", LeaseKind::Lockout),
        lease(
            "h",
            "cuda-central",
            LeaseKind::SharedHold { holder: "x".into() },
        ),
    ];
    let v = run(
        &snap(zero_pods_and_up(&["cuda-central"]), leases),
        &SchedState::default(),
        vec![power_off("cuda-central")],
    );
    assert!(v.is_empty(), "{v:?}");
}

const SERVING_OCR: &str = "    cuda-central: { power: { phase: \"on\", since: 0 }, mode: { phase: serving, model: mineru, since: 1, wind_down: { since: 90000000, timeout: 600000, lockout: false } } }\n";

fn in_flight_ocr() -> sched_model::Observation {
    sched_model::Observation {
        source: "paperless-ocr-pending".into(),
        observed_at: Millis(NOW),
        fidelity: sched_model::Fidelity::Polled,
        value: sched_model::ObservedValue::Demand {
            failed: 0,
            class: ClassId::new("paperless-ocr"),
            ready: 1,
            in_flight: 1,
            oldest_ready_at: Some(Millis(NOW - 5)),
        },
    }
}

fn kill() -> Step {
    Step::KillInFlight {
        class: ClassId::new("paperless-ocr"),
    }
}

fn with_wind_down(since: u64, lockout: bool) -> SchedState {
    let mut s = prev(SERVING_OCR);
    if let Some(ModePhase::Serving { wind_down, .. }) =
        s.resources.values_mut().next().map(|r| &mut r.mode)
    {
        *wind_down = Some(WindDown {
            since: Millis(since),
            timeout: Some(Millis(10 * MIN)),
            lockout,
            killed: Default::default(),
        });
    }
    s
}

#[test]
fn an_unforced_kill_is_allowed_only_after_drain_timeout() {
    // No lease at all: the wind-down is unforced, and still bounded by drain_timeout.
    let mut obs = zero_pods_and_up(&["cuda-central"]);
    obs.push(in_flight_ocr());
    let v = run(
        &snap(obs.clone(), vec![]),
        &with_wind_down(NOW - 9 * MIN, false),
        vec![kill()],
    );
    assert!(v.iter().any(|m| m.contains("early")), "{v:?}");
    let v = run(
        &snap(obs.clone(), vec![]),
        &with_wind_down(NOW - 10 * MIN, false),
        vec![kill()],
    );
    assert!(v.is_empty(), "{v:?}");
    // An interactive lease is not a forcing lease, and changes nothing about the timeout.
    let l = lease(
        "i",
        "cuda-central",
        LeaseKind::Interactive {
            model: ModelId::new("llm"),
            expires_at: Millis(NOW + 1),
        },
    );
    let v = run(
        &snap(obs, vec![l]),
        &with_wind_down(NOW - 9 * MIN, false),
        vec![kill()],
    );
    assert!(v.iter().any(|m| m.contains("early")), "{v:?}");
}

#[test]
fn killing_before_the_class_timeout_is_a_violation() {
    let mut obs = zero_pods_and_up(&["cuda-central"]);
    obs.push(in_flight_ocr());
    let l = lease(
        "m",
        "cuda-central",
        LeaseKind::ManualModel {
            model: ModelId::new("llm"),
        },
    );
    let v = run(
        &snap(obs.clone(), vec![l.clone()]),
        &with_wind_down(NOW - 9 * MIN, false),
        vec![kill()],
    );
    assert!(v.iter().any(|m| m.contains("early")), "{v:?}");
    let v = run(
        &snap(obs, vec![l]),
        &with_wind_down(NOW - 10 * MIN, false),
        vec![kill()],
    );
    assert!(v.is_empty(), "{v:?}");
}

#[test]
fn killing_on_a_clock_that_should_have_restarted_is_a_violation() {
    let mut obs = zero_pods_and_up(&["cuda-central"]);
    obs.push(in_flight_ocr());
    // The wind-down began under a manual lease; a lockout now holds, so the clock restarts.
    let l = lease("r", "cuda-central", LeaseKind::Lockout);
    let v = run(
        &snap(obs, vec![l]),
        &with_wind_down(NOW - 20 * MIN, false),
        vec![kill()],
    );
    assert!(
        v.iter().any(|m| m.contains("without a running wind-down")),
        "{v:?}"
    );
}

#[test]
fn killing_a_class_with_nothing_in_flight_is_a_violation() {
    let mut obs = zero_pods_and_up(&["cuda-central"]);
    let mut idle = in_flight_ocr();
    idle.value = sched_model::ObservedValue::Demand {
        failed: 0,
        class: ClassId::new("paperless-ocr"),
        ready: 1,
        in_flight: 0,
        oldest_ready_at: None,
    };
    obs.push(idle);
    let l = lease(
        "m",
        "cuda-central",
        LeaseKind::ManualModel {
            model: ModelId::new("llm"),
        },
    );
    let v = run(
        &snap(obs, vec![l]),
        &with_wind_down(NOW - 20 * MIN, false),
        vec![kill()],
    );
    assert!(
        v.iter().any(|m| m.contains("nothing of it is in flight")),
        "{v:?}"
    );
}

fn allow(model: &str) -> Step {
    Step::AllowModel {
        resource: ResourceId::new("cuda-central"),
        model: ModelId::new(model),
    }
}

fn withdraw(model: &str) -> Step {
    Step::DrainModel {
        resource: ResourceId::new("cuda-central"),
        model: ModelId::new(model),
    }
}

const GPU_ON_IDLE: &str = "    cuda-central: { power: { phase: \"on\", since: 0 }, mode: { phase: idle } }\n    basestar-nfs: { power: { phase: \"on\", since: 0 }, mode: { phase: idle } }\n";
const GPU_SERVING_OCR: &str = "    cuda-central: { power: { phase: \"on\", since: 0 }, mode: { phase: serving, model: mineru, since: 1 } }\n    basestar-nfs: { power: { phase: \"on\", since: 0 }, mode: { phase: idle } }\n";

/// A decision with these steps and this allowed set, the new state `next`.
fn decided(steps: Vec<Step>, models: &[&str], classes: &[&str], next: &str) -> Decision {
    Decision {
        steps,
        allowed: sched_model::AllowedSet {
            models: models.iter().map(|m| ModelId::new(*m)).collect(),
            classes: classes.iter().map(|c| ClassId::new(*c)).collect(),
        },
        state: prev(next),
    }
}

fn idle_demand() -> Vec<sched_model::Observation> {
    [
        ("paperless-ocr", "paperless-ocr-pending"),
        ("paperless-tagging", "paperless-post-ocr"),
        ("speakr-transcription", "speakr-pending"),
        ("speakr-summarization", "speakr-summaries"),
        ("immich-ml", "immich-queues"),
        ("interactive-llm", "llm-sessions"),
    ]
    .iter()
    .map(|(c, src)| sched_model::Observation {
        source: (*src).into(),
        observed_at: Millis(NOW),
        fidelity: sched_model::Fidelity::Polled,
        value: sched_model::ObservedValue::Demand {
            failed: 0,
            class: ClassId::new(*c),
            ready: 0,
            in_flight: 0,
            oldest_ready_at: None,
        },
    })
    .collect()
}

fn check_with(snap: &Snapshot, prev_state: &SchedState, d: &Decision) -> Vec<String> {
    check(&example_config(), snap, prev_state, &Intent::default(), d)
}

#[test]
fn a_switch_must_withdraw_the_old_model_in_the_same_decision() {
    let mut obs = zero_pods_and_up(&["cuda-central", "basestar-nfs"]);
    obs.extend(idle_demand());
    let s = snap(obs, vec![]);
    let before = prev(GPU_SERVING_OCR);
    let after = GPU_SERVING_OCR.replace("model: mineru", "model: llm");
    // The correct switch: withdraw and allow together, only the new model in the set.
    let ok = decided(
        vec![withdraw("mineru"), allow("llm")],
        &["llm"],
        &[],
        &after,
    );
    assert!(
        check_with(&s, &before, &ok).is_empty(),
        "{:?}",
        check_with(&s, &before, &ok)
    );
    // Both in the set.
    let both = decided(
        vec![withdraw("mineru"), allow("llm")],
        &["mineru", "llm"],
        &[],
        &after,
    );
    let v = check_with(&s, &before, &both);
    assert!(v.iter().any(|m| m.contains("invariant 1")), "{v:?}");
    // Allowed without withdrawing the old one (even if the set were right).
    let v = check_with(
        &s,
        &before,
        &decided(vec![allow("llm")], &["llm"], &[], &after),
    );
    assert!(
        v.iter()
            .any(|m| m.contains("invariant 1") && m.contains("mineru stays allowed")),
        "{v:?}"
    );
    // Two allows on one resource.
    let v = check_with(
        &s,
        &prev(GPU_ON_IDLE),
        &decided(vec![allow("llm"), allow("mineru")], &["llm"], &[], &after),
    );
    assert!(
        v.iter().any(|m| m.contains("more than one acting step")),
        "{v:?}"
    );
}

#[test]
fn allowing_on_a_host_that_is_not_freshly_up_is_a_violation() {
    let next = GPU_SERVING_OCR;
    let up_at = |at: u64, up: bool| sched_model::Observation {
        source: "cluster-pods".into(),
        observed_at: Millis(at),
        fidelity: sched_model::Fidelity::Event,
        value: sched_model::ObservedValue::ResourceUp {
            resource: ResourceId::new("cuda-central"),
            up,
        },
    };
    let d = decided(vec![allow("mineru")], &["mineru"], &[], next);
    // Fresh and up: fine.
    let v = check_with(
        &snap(vec![up_at(NOW, true)], vec![]),
        &prev(GPU_ON_IDLE),
        &d,
    );
    assert!(v.is_empty(), "{v:?}");
    // Stale, or missing: invariant 5.
    for obs in [vec![up_at(NOW - 31_000, true)], vec![]] {
        let v = check_with(&snap(obs, vec![]), &prev(GPU_ON_IDLE), &d);
        assert!(v.iter().any(|m| m.contains("invariant 5")), "{v:?}");
    }
    // Observed down: invariant 3 (nothing is allowed on a host observed off) and 5.
    let v = check_with(
        &snap(vec![up_at(NOW, false)], vec![]),
        &prev(GPU_ON_IDLE),
        &d,
    );
    assert!(
        v.iter()
            .any(|m| m.contains("invariant 3") && m.contains("observed down")),
        "{v:?}"
    );
    // Pods are not part of it: any pod reading leaves the verdict as it was.
    let mut obs = vec![up_at(NOW, true)];
    obs.extend(zero_pods_and_up(&[]).into_iter().map(|mut o| {
        if let sched_model::ObservedValue::ModelPods {
            pods, ready_pods, ..
        } = &mut o.value
        {
            *pods = 3;
            *ready_pods = 1;
        }
        o
    }));
    assert!(check_with(&snap(obs, vec![]), &prev(GPU_ON_IDLE), &d).is_empty());
}

#[test]
fn allowing_an_unconfigured_model_or_one_without_an_allow_step_is_a_violation() {
    let obs = zero_pods_and_up(&["cuda-central", "basestar-nfs"]);
    let s = snap(obs, vec![]);
    let next = GPU_SERVING_OCR.replace("model: mineru", "model: retired");
    let v = check_with(
        &s,
        &prev(GPU_ON_IDLE),
        &decided(vec![allow("retired")], &["retired"], &[], &next),
    );
    assert!(v.iter().any(|m| m.contains("not configured")), "{v:?}");
    // mineru appears in the set with no allow step and was not allowed before.
    let v = check_with(
        &s,
        &prev(GPU_ON_IDLE),
        &decided(vec![], &["mineru"], &[], GPU_SERVING_OCR),
    );
    assert!(
        v.iter().any(|m| m.contains("without an allow step")),
        "{v:?}"
    );
    // On a locked out host: kept without winding down, or newly allowed.
    let s = snap(
        zero_pods_and_up(&["cuda-central", "basestar-nfs"]),
        vec![lease("r", "cuda-central", LeaseKind::Lockout)],
    );
    let v = check_with(
        &s,
        &prev(GPU_SERVING_OCR),
        &decided(vec![], &["mineru"], &[], GPU_SERVING_OCR),
    );
    assert!(v.iter().any(|m| m.contains("locked out")), "{v:?}");
    let v = check_with(
        &s,
        &prev(GPU_ON_IDLE),
        &decided(vec![allow("mineru")], &["mineru"], &[], SERVING_OCR),
    );
    assert!(v.iter().any(|m| m.contains("locked out")), "{v:?}");
    // Kept while it winds down: fine.
    let v = check_with(
        &s,
        &prev(SERVING_OCR),
        &decided(vec![], &["mineru"], &[], SERVING_OCR),
    );
    assert!(!v.iter().any(|m| m.contains("locked out")), "{v:?}");
}

#[test]
fn allowing_a_class_whose_requirements_do_not_hold_is_a_violation() {
    let s = snap(zero_pods_and_up(&["cuda-central", "basestar-nfs"]), vec![]);
    let before = prev(GPU_SERVING_OCR);
    // OCR allowed while its model is not.
    let v = check_with(
        &s,
        &before,
        &decided(vec![], &[], &["paperless-ocr"], GPU_ON_IDLE),
    );
    assert!(
        v.iter().any(|m| m.contains("mineru is not allowed")),
        "{v:?}"
    );
    // OCR allowed while its intake is paused.
    let v = check_with(
        &s,
        &before,
        &decided(
            vec![Step::PauseIntake {
                class: ClassId::new("paperless-ocr"),
            }],
            &["mineru"],
            &["paperless-ocr"],
            GPU_SERVING_OCR,
        ),
    );
    assert!(v.iter().any(|m| m.contains("intake is paused")), "{v:?}");
    // The gate class while the GPU host is observed down.
    let mut obs = zero_pods_and_up(&["basestar-nfs"]);
    obs.push(sched_model::Observation {
        source: "cluster-pods".into(),
        observed_at: Millis(NOW),
        fidelity: sched_model::Fidelity::Event,
        value: sched_model::ObservedValue::ResourceUp {
            resource: ResourceId::new("cuda-central"),
            up: false,
        },
    });
    let v = check_with(
        &snap(obs, vec![]),
        &prev(GPU_ON_IDLE),
        &decided(vec![], &[], &["paperless-gpt"], GPU_ON_IDLE),
    );
    assert!(
        v.iter()
            .any(|m| m.contains("paperless-gpt") && m.contains("cuda-central is not up")),
        "{v:?}"
    );
    // And open while both hosts are up: fine.
    let v = check_with(
        &s,
        &prev(GPU_ON_IDLE),
        &decided(vec![], &[], &["paperless-gpt"], GPU_ON_IDLE),
    );
    assert!(v.is_empty(), "{v:?}");
}

#[test]
fn powering_off_a_host_with_a_model_allowed_is_a_violation() {
    let leases = vec![lease("r", "cuda-central", LeaseKind::Lockout)];
    let v = check_with(
        &snap(zero_pods_and_up(&["cuda-central"]), leases),
        &prev(GPU_SERVING_OCR),
        &decided(
            vec![power_off("cuda-central")],
            &["mineru"],
            &[],
            GPU_SERVING_OCR,
        ),
    );
    assert!(
        v.iter()
            .any(|m| m.contains("invariant 10") && m.contains("mineru is allowed")),
        "{v:?}"
    );
}

#[test]
fn tied_reachability_readings_count_as_up_only_when_every_reading_is_up() {
    let off = || Step::PowerOff {
        resource: ResourceId::new("cuda-central"),
    };
    let lockout = || vec![lease("r", "cuda-central", LeaseKind::Lockout)];
    let up = |v: bool| sched_model::Observation {
        source: "cluster-pods".into(),
        observed_at: Millis(NOW),
        fidelity: sched_model::Fidelity::Event,
        value: sched_model::ObservedValue::ResourceUp {
            resource: ResourceId::new("cuda-central"),
            up: v,
        },
    };
    let base = || {
        let mut o = zero_pods_and_up(&[]);
        o.push(up(true));
        o
    };
    let mut o = base();
    o.push(up(false));
    let v = run(&snap(o, lockout()), &SchedState::default(), vec![off()]);
    assert!(v.iter().any(|m| m.contains("invariant 5")), "{v:?}");
    let v = run(
        &snap(base(), lockout()),
        &SchedState::default(),
        vec![off()],
    );
    assert!(v.is_empty(), "{v:?}");
}

fn failed_transcriptions(n: u64, at: u64) -> sched_model::Observation {
    sched_model::Observation {
        source: "speakr-pending".into(),
        observed_at: Millis(at),
        fidelity: sched_model::Fidelity::Polled,
        value: sched_model::ObservedValue::Demand {
            class: ClassId::new("speakr-transcription"),
            ready: 0,
            in_flight: 0,
            failed: n,
            oldest_ready_at: None,
        },
    }
}

#[test]
fn recovery_for_a_class_that_is_not_allowed_or_has_no_failed_items_is_a_violation() {
    let rec = || {
        vec![Step::RunRecovery {
            class: ClassId::new("speakr-transcription"),
        }]
    };
    let with = |obs: sched_model::Observation, allowed: bool| {
        let mut o = zero_pods_and_up(&["cuda-central"]);
        o.push(obs);
        let mut d = decision(rec());
        if allowed {
            d.allowed
                .classes
                .insert(ClassId::new("speakr-transcription"));
        }
        // Only this invariant's messages: the allowed set is hand-made here.
        check(
            &example_config(),
            &snap(o, vec![]),
            &SchedState::default(),
            &Intent::default(),
            &d,
        )
        .into_iter()
        .filter(|m| m.contains("invariant 17"))
        .collect::<Vec<_>>()
    };
    // The class is in the allowed set and failed items are reported: no violation.
    let v = with(failed_transcriptions(2, NOW), true);
    assert!(v.is_empty(), "{v:?}");
    // Not in the allowed set.
    let v = with(failed_transcriptions(2, NOW), false);
    assert!(
        v.iter()
            .any(|m| m.contains("invariant 17") && m.contains("not in the allowed set")),
        "{v:?}"
    );
    // No failed items.
    let v = with(failed_transcriptions(0, NOW), true);
    assert!(
        v.iter().any(|m| m.contains("without fresh failed items")),
        "{v:?}"
    );
    // Stale failed items are unknown, not failed.
    let v = with(failed_transcriptions(2, NOW - 11 * MIN), true);
    assert!(
        v.iter().any(|m| m.contains("without fresh failed items")),
        "{v:?}"
    );
    // A class that declares no recovery.
    let mut d = decision(vec![Step::RunRecovery {
        class: ClassId::new("paperless-ocr"),
    }]);
    d.allowed.classes.insert(ClassId::new("paperless-ocr"));
    let v = check(
        &example_config(),
        &snap(zero_pods_and_up(&["cuda-central"]), vec![]),
        &SchedState::default(),
        &Intent::default(),
        &d,
    );
    assert!(
        v.iter()
            .any(|m| m.contains("invariant 17") && m.contains("declares none")),
        "{v:?}"
    );
}

#[test]
fn a_power_step_or_a_silent_exit_from_the_failed_power_state_is_a_violation() {
    let failed = prev(&format!(
        "    cuda-central: {{ power: {{ phase: failed, since: {}, during: powering_on }}, mode: {{ phase: idle }}, managed: true }}\n",
        NOW - MIN
    ));
    // The host is observed down: nothing it was waiting for. A new power-on is a violation,
    // and so is leaving the failed state.
    let down = {
        let mut o = zero_pods_and_up(&[]);
        o.push(sched_model::Observation {
            source: "cluster-pods".into(),
            observed_at: Millis(NOW),
            fidelity: sched_model::Fidelity::Event,
            value: sched_model::ObservedValue::ResourceUp {
                resource: "cuda-central".into(),
                up: false,
            },
        });
        snap(o, vec![])
    };
    let v = run(
        &down,
        &failed,
        vec![Step::PowerOn {
            resource: "cuda-central".into(),
        }],
    );
    assert!(
        v.iter()
            .any(|m| m.contains("invariant 9") && m.contains("failed power state")),
        "{v:?}"
    );
    let mut d = decision(vec![]);
    d.state = prev(&format!(
        "    cuda-central: {{ power: {{ phase: on, since: {NOW} }}, mode: {{ phase: idle }} }}\n"
    ));
    let v = check_with(&down, &failed, &d);
    assert!(
        v.iter()
            .any(|m| m.contains("invariant 9") && m.contains("left the failed power state")),
        "{v:?}"
    );
    // Observed up (what a power-on was after), leaving it is fine.
    let up = snap(zero_pods_and_up(&["cuda-central"]), vec![]);
    let v = check_with(&up, &failed, &d);
    assert!(!v.iter().any(|m| m.contains("invariant 9")), "{v:?}");
}

// Invariant 8: no automatic power-on within `min_off` of a confirmed shutdown.

fn min_off_config() -> sched_model::Config {
    let y = sched_model::test_support::example_yaml_managed().replacen(
        "power_on_timeout:",
        "min_off: 10m\n      power_on_timeout:",
        2,
    );
    sched_model::Config::from_yaml(&y).unwrap()
}

fn power_on(res: &str) -> Step {
    Step::PowerOn {
        resource: ResourceId::new(res),
    }
}

fn nfs_down() -> Vec<sched_model::Observation> {
    use sched_model::{Fidelity, Observation, ObservedValue};
    vec![Observation {
        source: "cluster-pods".into(),
        observed_at: Millis(NOW),
        fidelity: Fidelity::Event,
        value: ObservedValue::ResourceUp {
            resource: "basestar-nfs".into(),
            up: false,
        },
    }]
}

fn job_hold() -> Vec<Lease> {
    vec![lease(
        "h",
        "basestar-nfs",
        LeaseKind::SharedHold {
            holder: "job:backup".into(),
        },
    )]
}

fn off_since(ago: u64) -> SchedState {
    prev(&format!(
        "    basestar-nfs: {{ power: {{ phase: \"off\" }}, mode: {{ phase: idle }}, off_since: {} }}\n",
        NOW - ago
    ))
}

fn run_min_off(snap: &Snapshot, prev: &SchedState, steps: Vec<Step>) -> Vec<String> {
    check(
        &min_off_config(),
        snap,
        prev,
        &Intent::default(),
        &decision(steps),
    )
}

#[test]
fn powering_on_within_min_off_of_a_confirmed_shutdown_is_a_violation() {
    let s = snap(nfs_down(), job_hold());
    let v = run_min_off(&s, &off_since(9 * MIN), vec![power_on("basestar-nfs")]);
    assert!(v.iter().any(|m| m.contains("invariant 8")), "{v:?}");
    // The same tick that sees the shutdown finish counts as confirmed now.
    let shutting = prev(
        "    basestar-nfs: { power: { phase: shutting_down, since: 1 }, mode: { phase: idle } }\n",
    );
    let v = run_min_off(&s, &shutting, vec![power_on("basestar-nfs")]);
    assert!(v.iter().any(|m| m.contains("invariant 8")), "{v:?}");
    // A stamp in the future is within the wait too.
    let v = run_min_off(&s, &off_since(0), vec![power_on("basestar-nfs")]);
    assert!(v.iter().any(|m| m.contains("invariant 8")), "{v:?}");
}

#[test]
fn powering_on_after_min_off_with_a_manual_lease_or_without_a_stamp_is_fine() {
    let s = snap(nfs_down(), job_hold());
    let v = run_min_off(&s, &off_since(10 * MIN), vec![power_on("basestar-nfs")]);
    assert!(v.is_empty(), "{v:?}");
    let manual = snap(
        nfs_down(),
        vec![lease("m", "basestar-nfs", LeaseKind::ManualHost)],
    );
    let v = run_min_off(&manual, &off_since(MIN), vec![power_on("basestar-nfs")]);
    assert!(v.is_empty(), "{v:?}");
    let found = prev("    basestar-nfs: { power: { phase: \"off\" }, mode: { phase: idle } }\n");
    let v = run_min_off(&s, &found, vec![power_on("basestar-nfs")]);
    assert!(v.is_empty(), "{v:?}");
    // The default of 0 never fires.
    let v = run(&s, &off_since(1), vec![power_on("basestar-nfs")]);
    assert!(v.is_empty(), "{v:?}");
}

// ---- a requested shutdown of an exclusive host is a forced wind-down: the checker agrees ----

fn busy(class: &str, source: &str) -> sched_model::Observation {
    sched_model::Observation {
        source: source.into(),
        observed_at: Millis(NOW),
        fidelity: sched_model::Fidelity::Polled,
        value: sched_model::ObservedValue::Demand {
            failed: 0,
            class: ClassId::new(class),
            ready: 1,
            in_flight: 1,
            oldest_ready_at: Some(Millis(NOW - 5)),
        },
    }
}

/// A snapshot with all demand idle but `busy_class`, the host up and every model at zero pods.
fn world_with(busy_class: sched_model::Observation) -> Snapshot {
    let mut obs = zero_pods_and_up(&["cuda-central", "basestar-nfs"]);
    obs.extend(
        idle_demand()
            .into_iter()
            .filter(|o| match (&o.value, &busy_class.value) {
                (
                    sched_model::ObservedValue::Demand { class: a, .. },
                    sched_model::ObservedValue::Demand { class: b, .. },
                ) => a != b,
                _ => true,
            }),
    );
    obs.push(busy_class);
    snap(obs, vec![])
}

/// cuda-central serving `model` (since long ago, with an optional wind-down started at
/// `wind_down`), asked to shut down `ago` ms ago in `mode`.
fn serving_requested(model: &str, mode: &str, ago: u64, wind_down: Option<u64>) -> SchedState {
    let wd = wind_down.map_or(String::new(), |s| {
        format!(", wind_down: {{ since: {s}, timeout: 600000, lockout: false }}")
    });
    prev(&format!(
        "    cuda-central: {{ power: {{ phase: \"on\", since: 0 }}, mode: {{ phase: serving, model: {model}, since: 1{wd} }}, managed: true, shutdown: {{ cause: {{ kind: requested, mode: {mode} }}, since: {}, withdrawal: pending, veto: {{ phase: none }} }} }}\n    basestar-nfs: {{ power: {{ phase: \"on\", since: 0 }}, mode: {{ phase: idle }} }}\n",
        NOW - ago
    ))
}

/// What the sequencer decides, and what the checker says about it.
fn sequencer_and_checker(snap: &Snapshot, before: &SchedState) -> (Decision, Vec<String>) {
    let config = example_config();
    let intent = Intent::default();
    let d = sched_sequencer::tick(&config, snap, before, &intent).unwrap();
    let v = check(&config, snap, before, &intent, &d);
    (d, v)
}

#[test]
fn a_graceful_requested_shutdown_of_an_exclusive_host_winds_down_without_a_violation() {
    // interactive-llm is unpreemptible and busy: a requested shutdown forces past it, but only
    // after drain_timeout (10 minutes) from the request.
    let world = world_with(busy("interactive-llm", "llm-sessions"));
    let (d, v) =
        sequencer_and_checker(&world, &serving_requested("llm", "graceful", 9 * MIN, None));
    assert!(v.is_empty(), "{v:?} {:?}", d.steps);
    assert!(!d.steps.iter().any(|s| matches!(s, Step::DrainModel { .. })));
    let (d, v) = sequencer_and_checker(
        &world,
        &serving_requested("llm", "graceful", 10 * MIN, None),
    );
    assert!(v.is_empty(), "{v:?} {:?}", d.steps);
    assert!(
        d.steps.iter().any(|s| matches!(s, Step::DrainModel { .. })),
        "{:?}",
        d.steps
    );
    assert!(
        d.steps
            .iter()
            .any(|s| matches!(s, Step::KillInFlight { .. })),
        "{:?}",
        d.steps
    );
}

#[test]
fn an_immediate_requested_shutdown_of_an_exclusive_host_cuts_off_without_a_violation() {
    let world = world_with(in_flight_ocr());
    // No wind-down running yet: it starts and cuts off in the same tick.
    let (d, v) = sequencer_and_checker(&world, &serving_requested("mineru", "immediate", 0, None));
    assert!(v.is_empty(), "{v:?} {:?}", d.steps);
    assert!(
        d.steps.contains(&kill()) && d.steps.contains(&withdraw("mineru")),
        "{:?}",
        d.steps
    );
    // A wind-down already running (started a minute ago, 9 minutes of its timeout left).
    let (d, v) = sequencer_and_checker(
        &world,
        &serving_requested("mineru", "immediate", 0, Some(NOW - MIN)),
    );
    assert!(v.is_empty(), "{v:?} {:?}", d.steps);
    assert!(d.steps.contains(&kill()), "{:?}", d.steps);
}

#[test]
fn the_checker_still_catches_a_kill_before_a_graceful_shutdowns_drain_timeout() {
    let world = world_with(in_flight_ocr());
    // 9 minutes after the request, even with a wind-down that started 10 minutes ago.
    let v = run(
        &world,
        &serving_requested("mineru", "graceful", 9 * MIN, Some(NOW - 10 * MIN)),
        vec![kill()],
    );
    assert!(v.iter().any(|m| m.contains("drain_timeout")), "{v:?}");
    let v = run(
        &world,
        &serving_requested("mineru", "graceful", 10 * MIN, Some(NOW - MIN)),
        vec![kill()],
    );
    assert!(v.is_empty(), "{v:?}");
    // Not excused by a manual lease cancelling the request, nor by someone else's shutdown.
    let manual = lease("m", "cuda-central", LeaseKind::ManualHost);
    let mut with_manual = world.clone();
    with_manual.leases.push(manual);
    let v = run(
        &with_manual,
        &serving_requested("mineru", "immediate", 0, None),
        vec![kill()],
    );
    assert!(
        v.iter().any(|m| m.contains("without a running wind-down")),
        "a cancelled request forces nothing: {v:?}"
    );
    let idle_shutdown = prev(
        "    cuda-central: { power: { phase: \"on\", since: 0 }, mode: { phase: serving, model: mineru, since: 1 }, managed: true, shutdown: { cause: { kind: idle }, since: 1, withdrawal: not_needed, veto: { phase: none } } }\n",
    );
    let v = run(&world, &idle_shutdown, vec![kill()]);
    assert!(
        v.iter().any(|m| m.contains("without a running wind-down")),
        "{v:?}"
    );
}

#[test]
fn the_checker_still_requires_an_unforced_switch_to_wait_for_an_unpreemptible_class() {
    let world = world_with(busy("interactive-llm", "llm-sessions"));
    let v = run(
        &world,
        &prev(
            "    cuda-central: { power: { phase: \"on\", since: 0 }, mode: { phase: serving, model: llm, since: 1 } }\n",
        ),
        vec![withdraw("llm"), kill_llm()],
    );
    assert!(v.iter().any(|m| m.contains("unpreemptible")), "{v:?}");
}

fn kill_llm() -> Step {
    Step::KillInFlight {
        class: ClassId::new("interactive-llm"),
    }
}
