//! Observations are filtered by subject AND by the source the config says supplies them
//! before any reduction. Pod counts decide only recovery readiness and the pre-shutdown veto; no wrong-source pod reading changes a
//! decision. A stray reading (right subject, wrong source) must neither wedge
//! the scheduler nor make an unsafe step look safe, and the core and the independent
//! invariant checker must agree on that.

use sched_model::test_support::example_config;
use sched_model::{
    ClassId, Fidelity, Intent, Millis, ModelId, Observation, ObservedValue, ResourceId, SchedState,
    Snapshot, Step, WaitReason,
};
use sched_sequencer::Decision;
use sched_service::core::decide;
use sched_service::invariants::check;

const NOW: u64 = 90_000_000;

fn obs(source: &str, at: u64, value: ObservedValue) -> Observation {
    Observation {
        source: source.into(),
        observed_at: Millis(at),
        fidelity: Fidelity::Event,
        value,
    }
}

fn pods(source: &str, model: &str, at: u64, p: u32) -> Observation {
    obs(
        source,
        at,
        ObservedValue::ModelPods {
            model: ModelId::new(model),
            pods: p,
            ready_pods: p,
        },
    )
}

fn up(source: &str, res: &str, at: u64, up: bool) -> Observation {
    obs(
        source,
        at,
        ObservedValue::ResourceUp {
            resource: ResourceId::new(res),
            up,
        },
    )
}

fn demand(source: &str, class: &str, at: u64, ready: u64, oldest: Option<u64>) -> Observation {
    obs(
        source,
        at,
        ObservedValue::Demand {
            failed: 0,
            class: ClassId::new(class),
            ready,
            in_flight: 0,
            oldest_ready_at: oldest.map(Millis),
        },
    )
}

fn state(yaml: &str) -> SchedState {
    let y = format!("description: s\nsnapshot: {{ now: 0 }}\nstate:\n  resources:\n{yaml}");
    sched_model::Fixture::from_yaml(&y).unwrap().state
}

const IDLE: &str = "    cuda-central: { power: { phase: \"on\", since: 0 }, mode: { phase: idle } }\n    basestar-nfs: { power: { phase: \"on\", since: 0 }, mode: { phase: idle } }\n";
const MANAGED_NFS: &str = "    cuda-central: { power: { phase: \"on\", since: 0 }, mode: { phase: idle } }\n    basestar-nfs: { power: { phase: \"on\", since: 0, ours: true }, mode: { phase: idle }, idle_since: 1 }\n";

const MODELS: [&str; 4] = ["mineru", "llm", "whisperx", "immich-ml"];
const GPU_AND_NFS: [&str; 2] = ["cuda-central", "basestar-nfs"];
/// Every demand class and the source that supplies it.
const CLASSES: [(&str, &str); 6] = [
    ("paperless-ocr", "paperless-ocr-pending"),
    ("paperless-tagging", "paperless-post-ocr"),
    ("speakr-transcription", "speakr-pending"),
    ("speakr-summarization", "speakr-summaries"),
    ("immich-ml", "immich-queues"),
    ("interactive-llm", "llm-sessions"),
];

/// Authoritative readings: zero pods everywhere, both hosts up, all queues empty except
/// `busy`, which has `ready` items waiting.
fn authoritative(busy: Option<(&str, u64)>) -> Vec<Observation> {
    let mut o: Vec<_> = MODELS
        .iter()
        .map(|m| pods("cluster-pods", m, NOW, 0))
        .collect();
    o.extend(GPU_AND_NFS.iter().map(|r| up("cluster-pods", r, NOW, true)));
    for (c, s) in CLASSES {
        match busy {
            Some((b, n)) if b == c => o.push(demand(s, c, NOW - 1000, n, Some(NOW - 5000))),
            _ => o.push(demand(s, c, NOW - 1000, 0, None)),
        }
    }
    o
}

/// Stray readings: every subject reported by a source the config does not say supplies it.
fn strays() -> Vec<(String, Vec<Observation>)> {
    let mut groups = Vec::new();
    for (label, at) in [
        ("current", NOW),
        ("older", NOW - 2000),
        ("future", NOW + 5000),
    ] {
        // Demand of each class from a different class's source, claiming lots of work, and
        // claiming none.
        for ready in [0u64, 9] {
            let mut v = Vec::new();
            for (c, own) in CLASSES {
                for (_, other) in CLASSES.iter().filter(|(_, s)| *s != own) {
                    v.push(demand(other, c, at, ready, (ready > 0).then_some(at - 10)));
                }
            }
            groups.push((
                format!("demand from other sources, {label}, ready={ready}"),
                v,
            ));
        }
        // Pod counts and reachability from an unknown source and from configured sources that
        // cannot report cluster state, claiming pods present / hosts down, and the reverse.
        for src in ["nowhere", "paperless-ocr-pending", "backup-schedule"] {
            for (p, is_up) in [(5u32, false), (0, true)] {
                let mut v: Vec<_> = MODELS.iter().map(|m| pods(src, m, at, p)).collect();
                v.extend(GPU_AND_NFS.iter().map(|r| up(src, r, at, is_up)));
                groups.push((format!("pods/up from {src}, {label}, pods={p}"), v));
            }
        }
    }
    groups
}

fn snapshot(observations: Vec<Observation>) -> Snapshot {
    Snapshot {
        config_generation: 0,
        now: Millis(NOW),
        observations,
        leases: vec![],
        unobservable: Default::default(),
    }
}

fn violations(snap: &Snapshot, st: &SchedState, intent: &Intent, d: &Decision) -> Vec<String> {
    check(&example_config(), snap, st, intent, d)
}

fn has_wait(d: &Decision, reason: WaitReason) -> bool {
    d.steps
        .iter()
        .any(|s| matches!(s, Step::Wait { reason: r, .. } if *r == reason))
}

#[test]
fn a_stray_demand_observation_does_not_wedge_the_decision() {
    // The reported repro: 3 ready OCR items from the configured source, plus one demand
    // observation of the same class from another source at the current time.
    let config = example_config();
    let st = state(IDLE);
    let mut o = authoritative(Some(("paperless-ocr", 3)));
    let (_, d) = decide(&config, &snapshot(o.clone()), &st).unwrap();
    assert!(
        d.steps
            .iter()
            .any(|s| matches!(s, Step::AllowModel { model, .. } if model.as_str() == "mineru")),
        "baseline: {:?}",
        d.steps
    );
    o.push(demand("speakr-pending", "paperless-ocr", NOW, 0, None));
    let snap = snapshot(o);
    let (intent, d) = decide(&config, &snap, &st).unwrap();
    assert!(!has_wait(&d, WaitReason::StaleDemand), "{:?}", d.steps);
    assert!(
        d.steps
            .iter()
            .any(|s| matches!(s, Step::AllowModel { model, .. } if model.as_str() == "mineru")),
        "{:?}",
        d.steps
    );
    assert!(violations(&snap, &st, &intent, &d).is_empty());
}

#[test]
fn stray_readings_never_change_the_decision_and_the_checker_agrees() {
    let config = example_config();
    // Scenarios: work waiting on an idle GPU; and an idle, managed NFS host that may power
    // down; and a loaded-nothing host with a quiet system.
    let scenarios = vec![
        ("ocr waiting", Some(("paperless-ocr", 3)), IDLE),
        (
            "transcription waiting",
            Some(("speakr-transcription", 2)),
            IDLE,
        ),
        ("quiet, managed nfs", None, MANAGED_NFS),
    ];
    let mut compared = 0;
    for (name, busy, st) in scenarios {
        let st = state(st);
        let base = snapshot(authoritative(busy));
        let (base_intent, base_d) = decide(&config, &base, &st).unwrap();
        assert!(
            violations(&base, &st, &base_intent, &base_d).is_empty(),
            "{name}: baseline"
        );
        assert!(
            !base_d.steps.iter().any(|s| matches!(
                s,
                Step::Wait {
                    reason: WaitReason::StaleDemand | WaitReason::StaleObservation,
                    ..
                }
            )),
            "{name}: baseline waits on stale data: {:?}",
            base_d.steps
        );
        for (label, stray) in strays() {
            let mut o = authoritative(busy);
            o.extend(stray);
            let snap = snapshot(o);
            let (intent, d) = decide(&config, &snap, &st).unwrap();
            assert_eq!(base_intent, intent, "{name} / {label}: intent changed");
            assert_eq!(base_d, d, "{name} / {label}: decision changed");
            let v = violations(&snap, &st, &intent, &d);
            assert!(v.is_empty(), "{name} / {label}: checker disagrees: {v:?}");
            compared += 1;
        }
    }
    assert_eq!(compared, 3 * strays().len());
    assert_eq!(strays().len(), 24);
}

fn step_allow(model: &str) -> Step {
    Step::AllowModel {
        resource: ResourceId::new("cuda-central"),
        model: ModelId::new(model),
    }
}

fn run_check(snap: &Snapshot, st: &SchedState, steps: Vec<Step>) -> Vec<String> {
    let d = Decision {
        steps,
        allowed: Default::default(),
        state: SchedState::default(),
    };
    violations(snap, st, &Intent::default(), &d)
}

#[test]
fn the_checker_does_not_accept_stray_readings_as_proof_of_safety() {
    let st = state(IDLE);
    // Allowing mineru needs a fresh "up" reading of cuda-central from a source declaring
    // reachability. Here the authoritative reading is stale and only stray sources say
    // "up, just now".
    let mut o = authoritative(None);
    o.retain(
        |x| !matches!(&x.value, ObservedValue::ResourceUp { resource, .. } if resource.as_str() == "cuda-central"),
    );
    o.push(up("cluster-pods", "cuda-central", NOW - 10 * 60_000, true));
    o.push(up("nowhere", "cuda-central", NOW, true));
    o.push(up("paperless-ocr-pending", "cuda-central", NOW, true));
    let v = run_check(&snapshot(o), &st, vec![step_allow("mineru")]);
    assert!(v.iter().any(|m| m.contains("invariant 5")), "{v:?}");

    // Powering off the NFS host needs a fresh up observation. Strays that say "up" cannot
    // stand in for it.
    let mut o = authoritative(None);
    o.retain(|x| !matches!(x.value, ObservedValue::ResourceUp { .. }));
    o.push(up("nowhere", "basestar-nfs", NOW, true));
    let v = run_check(
        &snapshot(o),
        &state(MANAGED_NFS),
        vec![Step::PowerOff {
            resource: ResourceId::new("basestar-nfs"),
        }],
    );
    assert!(v.iter().any(|m| m.contains("invariant 5")), "{v:?}");

    // A newer "no work" reading of the class from another source does not hide the work the
    // configured source reports.
    let mut o = authoritative(Some(("paperless-ocr", 2)));
    o.push(demand("speakr-pending", "paperless-ocr", NOW, 0, None));
    let v = run_check(
        &snapshot(o),
        &state(MANAGED_NFS),
        vec![Step::PowerOff {
            resource: ResourceId::new("basestar-nfs"),
        }],
    );
    assert!(
        v.iter().any(|m| m.contains("has or may have work")),
        "{v:?}"
    );
}

#[test]
fn the_checker_is_not_wedged_by_stray_readings_either() {
    // A correct decision made on authoritative data stays clean when strays are present
    // (future-dated and older ones included).
    let st = state(IDLE);
    let mut o = authoritative(None);
    o.push(pods("nowhere", "llm", NOW + 5000, 3));
    o.push(pods("paperless-ocr-pending", "llm", NOW, 3));
    o.push(up("nowhere", "cuda-central", NOW + 5000, false));
    let v = run_check(&snapshot(o), &st, vec![step_allow("mineru")]);
    assert!(v.is_empty(), "{v:?}");
}

/// Reachability of a host outside the cluster comes from a probe source, declared as such.
#[test]
fn reachability_from_a_probe_source_drives_decisions_end_to_end() {
    // A manual NFS lease powers basestar up: managed control (M4).
    let managed = sched_model::test_support::example_yaml_managed();
    let text = managed.replace(
        "  - id: basestar-probe\n    kind: tcp_probe\n",
        "  - id: basestar-probe\n    kind: http_json\n",
    );
    // The tcp_probe-shaped params would not be valid http_json params.
    let text = text.replace(
        "    params:\n      host: basestar\n      port: 2049\n      resource: basestar-nfs\n      timeout: 2s\n      # Consecutive agreeing readings before up/down flips (as for cluster-pods).\n      reachability_confirmations: 3\n",
        "",
    );
    assert_ne!(text, managed);
    let config = sched_model::Config::from_yaml(&text).unwrap();
    let lease = sched_model::Lease {
        id: "user".into(),
        resource: ResourceId::new("basestar-nfs"),
        kind: sched_model::LeaseKind::ManualHost,
        granted_at: Millis(0),
        origin: sched_model::LeaseOrigin::Requested,
    };
    // Only the probe reports on basestar-nfs; the cluster watch does not.
    let mut o: Vec<_> = authoritative(None)
        .into_iter()
        .filter(|x| {
            !matches!(&x.value, ObservedValue::ResourceUp { resource, .. } if resource.as_str() == "basestar-nfs")
        })
        .collect();
    o.push(up("basestar-probe", "basestar-nfs", NOW, true));
    let mut snap = snapshot(o);
    snap.leases = vec![lease];
    // The scheduler has not seen either host yet, so it starts from the observations.
    let st = SchedState::default();
    let (intent, d) = decide(&config, &snap, &st).unwrap();
    assert!(!has_wait(&d, WaitReason::StaleObservation), "{:?}", d.steps);
    // NFS is up, so its classes are allowed rather than waiting on an unknown host.
    assert!(
        d.allowed
            .classes
            .iter()
            .any(|c| c.as_str() == "paperless-ngx"),
        "{:?}",
        d.allowed
    );
    assert!(check(&config, &snap, &st, &intent, &d).is_empty());
    // The probe reporting the host down makes the manual lease power it on.
    for x in snap.observations.iter_mut() {
        if x.source.as_str() == "basestar-probe" {
            x.value = ObservedValue::ResourceUp {
                resource: ResourceId::new("basestar-nfs"),
                up: false,
            };
        }
    }
    let (_, d) = decide(&config, &snap, &st).unwrap();
    assert!(
        d.steps.iter().any(
            |s| matches!(s, Step::PowerOn { resource } if resource.as_str() == "basestar-nfs")
        ),
        "{:?}",
        d.steps
    );
}

/// The example config plus a second source declaring pods and reachability.
fn two_cluster_sources() -> sched_model::Config {
    use sched_model::test_support::EXAMPLE_CONFIG;
    let text = EXAMPLE_CONFIG.replace(
        "sources:\n  - id: cluster-pods\n",
        "sources:\n  - id: cluster-pods-b\n    kind: kubernetes_watch\n    observe: [pods, reachability]\n    resync: 10s\n  - id: cluster-pods\n",
    );
    assert_ne!(text, EXAMPLE_CONFIG);
    sched_model::Config::from_yaml(&text).unwrap()
}

fn permutations<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
    if items.len() <= 1 {
        return vec![items.to_vec()];
    }
    let mut out = Vec::new();
    for i in 0..items.len() {
        let mut rest = items.to_vec();
        let head = rest.remove(i);
        for mut p in permutations(&rest) {
            p.insert(0, head.clone());
            out.push(p);
        }
    }
    out
}

fn without_cuda_up(mut o: Vec<Observation>) -> Vec<Observation> {
    o.retain(|x| {
        !matches!(&x.value, ObservedValue::ResourceUp { resource, .. } if resource.as_str() == "cuda-central")
    });
    o
}

#[test]
fn pod_counts_from_any_source_never_block_allowing_and_the_checker_agrees() {
    let config = two_cluster_sources();
    let st = state(IDLE);
    let base = authoritative(Some(("paperless-ocr", 3)));
    let (_, want) = decide(&config, &snapshot(base.clone()), &st).unwrap();
    assert!(
        want.steps.contains(&step_allow("mineru")),
        "baseline: {:?}",
        want.steps
    );
    // Disagreeing, stale, nonzero or future-dated pod counts from either declaring source,
    // in any order: the same decision, which the checker accepts.
    let readings = [
        pods("cluster-pods", "llm", NOW - 2000, 2),
        pods("cluster-pods-b", "llm", NOW - 1000, 0),
        pods("cluster-pods-b", "whisperx", NOW - 10 * 60_000, 1),
        pods("cluster-pods", "immich-ml", NOW + 5000, 1),
    ];
    for order in permutations(&readings) {
        let mut o = base.clone();
        o.extend(order);
        let snap = snapshot(o);
        let (intent, d) = decide(&config, &snap, &st).unwrap();
        assert_eq!(d, want);
        assert!(check(&config, &snap, &st, &intent, &d).is_empty());
    }
}

#[test]
fn an_older_down_reading_from_one_source_blocks_allowing_and_the_checker_agrees() {
    let config = two_cluster_sources();
    let st = state(IDLE);
    let base = without_cuda_up(authoritative(Some(("paperless-ocr", 3))));
    // One source says up just now, the other said down a second earlier: not up.
    let readings = [
        up("cluster-pods", "cuda-central", NOW - 1000, true),
        up("cluster-pods-b", "cuda-central", NOW - 2000, false),
    ];
    let swapped = [
        up("cluster-pods-b", "cuda-central", NOW - 1000, true),
        up("cluster-pods", "cuda-central", NOW - 2000, false),
    ];
    let mut first = None;
    for order in permutations(&readings)
        .into_iter()
        .chain(permutations(&swapped))
    {
        let mut o = base.clone();
        o.extend(order);
        let snap = snapshot(o.clone());
        let (intent, d) = decide(&config, &snap, &st).unwrap();
        assert!(
            !d.steps.iter().any(|s| matches!(s, Step::AllowModel { .. })),
            "{:?}",
            d.steps
        );
        assert!(d.allowed.models.is_empty());
        assert!(check(&config, &snap, &st, &intent, &d).is_empty());
        // Given the unsafe decision directly, the checker flags it.
        let v = run_check_with(&config, &snap, &st, vec![step_allow("mineru")]);
        assert!(v.iter().any(|m| m.contains("invariant 5")), "{v:?}");
        match &first {
            None => first = Some(d),
            Some(f) => assert_eq!(f, &d, "order changed the decision"),
        }
    }
    // A stale reading from either source makes the host's state unknown to the checker.
    for stale_src in ["cluster-pods", "cluster-pods-b"] {
        let other = if stale_src == "cluster-pods" {
            "cluster-pods-b"
        } else {
            "cluster-pods"
        };
        let mut o = base.clone();
        o.push(up(stale_src, "cuda-central", NOW - 10 * 60_000, true));
        o.push(up(other, "cuda-central", NOW - 1000, true));
        let v = run_check_with(&config, &snapshot(o), &st, vec![step_allow("mineru")]);
        assert!(
            v.iter().any(|m| m.contains("invariant 5")),
            "{stale_src}: {v:?}"
        );
    }
    // Once both sources agree on up, mineru is allowed (and the checker agrees).
    let mut o = base.clone();
    o.push(up("cluster-pods", "cuda-central", NOW - 500, true));
    o.push(up("cluster-pods-b", "cuda-central", NOW - 1000, true));
    let snap = snapshot(o);
    let (intent, d) = decide(&config, &snap, &st).unwrap();
    assert!(d.steps.contains(&step_allow("mineru")), "{:?}", d.steps);
    assert!(check(&config, &snap, &st, &intent, &d).is_empty());
}

fn run_check_with(
    config: &sched_model::Config,
    snap: &Snapshot,
    st: &SchedState,
    steps: Vec<Step>,
) -> Vec<String> {
    let d = Decision {
        steps,
        allowed: Default::default(),
        state: SchedState::default(),
    };
    check(config, snap, st, &Intent::default(), &d)
}

#[test]
fn an_older_down_reading_from_one_source_blocks_power_off_and_the_checker_agrees() {
    let config = two_cluster_sources();
    let mut base = authoritative(None);
    base.retain(|x| !matches!(&x.value, ObservedValue::ResourceUp { .. }));
    base.push(up("cluster-pods", "cuda-central", NOW, true));
    let readings = [
        up("cluster-pods", "basestar-nfs", NOW - 2000, false),
        up("cluster-pods-b", "basestar-nfs", NOW - 1000, true),
    ];
    let swapped = [
        up("cluster-pods-b", "basestar-nfs", NOW - 2000, false),
        up("cluster-pods", "basestar-nfs", NOW - 1000, true),
    ];
    for order in permutations(&readings)
        .into_iter()
        .chain(permutations(&swapped))
    {
        let mut o = base.clone();
        o.extend(order);
        let v = run_check_with(
            &config,
            &snapshot(o),
            &state(MANAGED_NFS),
            vec![Step::PowerOff {
                resource: ResourceId::new("basestar-nfs"),
            }],
        );
        assert!(v.iter().any(|m| m.contains("invariant 5")), "{v:?}");
    }
    // Both up and fresh: the checker accepts the power-off on the reachability criterion.
    let mut o = base.clone();
    o.push(up("cluster-pods", "basestar-nfs", NOW - 2000, true));
    o.push(up("cluster-pods-b", "basestar-nfs", NOW - 1000, true));
    let v = run_check_with(
        &config,
        &snapshot(o),
        &state(MANAGED_NFS),
        vec![Step::PowerOff {
            resource: ResourceId::new("basestar-nfs"),
        }],
    );
    assert!(!v.iter().any(|m| m.contains("invariant 5")), "{v:?}");
}
