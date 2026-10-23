//! The invariant checker must itself catch what it claims to: each test hands it a decision
//! no correct sequencer would make and expects a violation.

use sched_model::test_support::example_config;
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
    }
}

fn power_off(res: &str) -> Step {
    Step::PowerOff {
        resource: ResourceId::new(res),
    }
}

const MANAGED_NFS: &str = "    basestar-nfs: { power: { phase: \"on\", since: 0 }, mode: { phase: idle }, managed: true, host_idle_since: 1 }\n";

#[test]
fn a_reclaim_does_not_excuse_powering_off_a_held_shared_host() {
    let leases = vec![
        lease("r", "basestar-nfs", LeaseKind::Reclaim),
        lease(
            "h",
            "basestar-nfs",
            LeaseKind::SharedHold { holder: "x".into() },
        ),
    ];
    let v = run(
        &snap(zero_pods_and_up(&["basestar-nfs"]), leases),
        &prev(MANAGED_NFS),
        vec![power_off("basestar-nfs")],
    );
    assert!(
        v.iter()
            .any(|m| m.contains("invariant 3") && m.contains("lease holds")),
        "{v:?}"
    );
}

#[test]
fn a_reclaim_does_excuse_powering_off_an_exclusive_host() {
    let leases = vec![
        lease("r", "cuda-central", LeaseKind::Reclaim),
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

const SERVING_OCR: &str = "    cuda-central: { power: { phase: \"on\", since: 0 }, mode: { phase: serving, model: mineru, since: 1, wind_down: { since: 90000000, timeout: 600000, reclaim: false } } }\n";

fn in_flight_ocr() -> sched_model::Observation {
    sched_model::Observation {
        source: "paperless-ocr-pending".into(),
        observed_at: Millis(NOW),
        fidelity: sched_model::Fidelity::Polled,
        value: sched_model::ObservedValue::Demand {
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

fn with_wind_down(since: u64, reclaim: bool) -> SchedState {
    let mut s = prev(SERVING_OCR);
    if let Some(ModePhase::Serving { wind_down, .. }) =
        s.resources.values_mut().next().map(|r| &mut r.mode)
    {
        *wind_down = Some(WindDown {
            since: Millis(since),
            timeout: Some(Millis(10 * MIN)),
            reclaim,
            killed: Default::default(),
        });
    }
    s
}

#[test]
fn killing_without_a_forcing_lease_is_a_violation() {
    let mut obs = zero_pods_and_up(&["cuda-central"]);
    obs.push(in_flight_ocr());
    let v = run(
        &snap(obs, vec![]),
        &with_wind_down(NOW - 11 * MIN, false),
        vec![kill()],
    );
    assert!(
        v.iter()
            .any(|m| m.contains("without a reclaim or manual model lease")),
        "{v:?}"
    );
    // An interactive lease is not a forcing lease either.
    let mut obs = zero_pods_and_up(&["cuda-central"]);
    obs.push(in_flight_ocr());
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
        &with_wind_down(NOW - 11 * MIN, false),
        vec![kill()],
    );
    assert!(
        v.iter()
            .any(|m| m.contains("without a reclaim or manual model lease")),
        "{v:?}"
    );
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
    // The wind-down began under a manual lease; a reclaim now holds, so the clock restarts.
    let l = lease("r", "cuda-central", LeaseKind::Reclaim);
    let v = run(
        &snap(obs, vec![l]),
        &with_wind_down(NOW - 20 * MIN, false),
        vec![kill()],
    );
    assert!(
        v.iter()
            .any(|m| m.contains("without a running forced wind-down")),
        "{v:?}"
    );
}

#[test]
fn killing_a_class_with_nothing_in_flight_is_a_violation() {
    let mut obs = zero_pods_and_up(&["cuda-central"]);
    let mut idle = in_flight_ocr();
    idle.value = sched_model::ObservedValue::Demand {
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

fn pods_obs(model: &str, pods: u32) -> sched_model::Observation {
    sched_model::Observation {
        source: "cluster-pods".into(),
        observed_at: Millis(NOW),
        fidelity: sched_model::Fidelity::Event,
        value: sched_model::ObservedValue::ModelPods {
            model: ModelId::new(model),
            pods,
            ready_pods: pods,
        },
    }
}

#[test]
fn tied_pod_readings_count_as_zero_only_when_every_reading_is_zero() {
    let allow = || Step::AllowModel {
        resource: ResourceId::new("cuda-central"),
        model: ModelId::new("mineru"),
    };
    // Zero and one pod of llm at the same newest instant, in both orders: not zero.
    for tied in [
        vec![pods_obs("llm", 0), pods_obs("llm", 1)],
        vec![pods_obs("llm", 1), pods_obs("llm", 0)],
    ] {
        let mut obs = zero_pods_and_up(&["cuda-central"]);
        obs.retain(|o| {
            !matches!(&o.value, sched_model::ObservedValue::ModelPods { model, .. } if model.as_str() == "llm")
        });
        obs.extend(tied);
        let v = run(&snap(obs, vec![]), &SchedState::default(), vec![allow()]);
        assert!(v.iter().any(|m| m.contains("invariant 2")), "{v:?}");
    }
    // Two zero readings are zero.
    let mut obs = zero_pods_and_up(&["cuda-central"]);
    obs.push(pods_obs("llm", 0));
    let v = run(&snap(obs, vec![]), &SchedState::default(), vec![allow()]);
    assert!(v.is_empty(), "{v:?}");
}

#[test]
fn tied_reachability_readings_count_as_up_only_when_every_reading_is_up() {
    let off = || Step::PowerOff {
        resource: ResourceId::new("cuda-central"),
    };
    let reclaim = || vec![lease("r", "cuda-central", LeaseKind::Reclaim)];
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
    let v = run(&snap(o, reclaim()), &SchedState::default(), vec![off()]);
    assert!(v.iter().any(|m| m.contains("invariant 5")), "{v:?}");
    let v = run(
        &snap(base(), reclaim()),
        &SchedState::default(),
        vec![off()],
    );
    assert!(v.is_empty(), "{v:?}");
}
