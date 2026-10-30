//! Snapshot order must never matter: the same observations and leases in any order give the
//! same intent and decision, including duplicates tied on timestamp and conflicting leases.

use proptest::prelude::*;
// Includes power-off orderings, so managed control (M4).
use sched_model::test_support::example_config_managed as example_config;
use sched_model::{
    ClassId, Fidelity, Lease, LeaseId, LeaseKind, Millis, ModelId, Observation, ObservedValue,
    ResourceId, SchedState, Snapshot, SourceId,
};
use sched_service::core::decide;

const NOW: u64 = 90_000_000;

fn obs(source: &str, at: u64, value: ObservedValue) -> Observation {
    Observation {
        source: SourceId::new(source),
        observed_at: Millis(at),
        fidelity: Fidelity::Event,
        value,
    }
}

fn pods(model: &str, at: u64, p: u32, r: u32) -> Observation {
    obs(
        "cluster-pods",
        at,
        ObservedValue::ModelPods {
            model: ModelId::new(model),
            pods: p,
            ready_pods: r,
        },
    )
}

fn up(res: &str, at: u64, up: bool) -> Observation {
    obs(
        "cluster-pods",
        at,
        ObservedValue::ResourceUp {
            resource: ResourceId::new(res),
            up,
        },
    )
}

/// A fresh reading that no pod mounts the NFS host.
fn no_mounts(at: u64) -> Observation {
    obs(
        "basestar-mounts",
        at,
        ObservedValue::ResourceMounts {
            resource: ResourceId::new("basestar-nfs"),
            pods: 0,
        },
    )
}

fn demand(
    class: &str,
    source: &str,
    at: u64,
    ready: u64,
    in_flight: u64,
    oldest: Option<u64>,
) -> Observation {
    obs(
        source,
        at,
        ObservedValue::Demand {
            failed: 0,
            class: ClassId::new(class),
            ready,
            in_flight,
            oldest_ready_at: oldest.map(Millis),
        },
    )
}

fn lease(id: &str, res: &str, granted: u64, kind: LeaseKind) -> Lease {
    Lease {
        id: LeaseId::new(id),
        resource: ResourceId::new(res),
        kind,
        granted_at: Millis(granted),
    }
}

fn state(yaml: &str) -> SchedState {
    let y = format!("description: s\nsnapshot: {{ now: 0 }}\nstate:\n  resources:\n{yaml}");
    sched_model::Fixture::from_yaml(&y).unwrap().state
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

/// Runs every ordering of observations and of leases and requires one identical result.
fn assert_order_independent(
    name: &str,
    observations: Vec<Observation>,
    fixed: &[Observation],
    leases: Vec<Lease>,
    st: &SchedState,
) -> (sched_model::Intent, sched_sequencer::Decision) {
    let config = example_config();
    let mut first: Option<(sched_model::Intent, sched_sequencer::Decision)> = None;
    let mut runs = 0;
    for o in permutations(&observations) {
        for l in permutations(&leases) {
            let mut all = o.clone();
            all.extend_from_slice(fixed);
            let snap = Snapshot {
                config_generation: 0,
                now: Millis(NOW),
                observations: all,
                leases: l,
                unobservable: Default::default(),
            };
            let got = decide(&config, &snap, st).unwrap();
            runs += 1;
            match &first {
                None => first = Some(got),
                Some(f) => assert_eq!(f, &got, "{name}: order changed the result"),
            }
        }
    }
    assert!(runs >= 1);
    first.unwrap()
}

fn base_zero_pods(except: &[&str]) -> Vec<Observation> {
    ["mineru", "llm", "whisperx", "immich-ml"]
        .iter()
        .filter(|m| !except.contains(m))
        .map(|m| pods(m, NOW, 0, 0))
        .collect()
}

const IDLE: &str = "    cuda-central: { power: { phase: \"on\", since: 0 }, mode: { phase: idle } }\n    basestar-nfs: { power: { phase: \"on\", since: 0 }, mode: { phase: idle } }\n";

#[test]
fn equal_timestamp_pod_counts_change_nothing_in_either_order() {
    // Zero and one pod of llm at the same instant: pods are not consulted, so mineru is
    // allowed, in every order.
    let mut o = base_zero_pods(&["llm"]);
    o.push(pods("llm", NOW, 0, 0));
    o.push(pods("llm", NOW, 1, 1));
    o.push(up("cuda-central", NOW, true));
    o.push(demand(
        "paperless-ocr",
        "paperless-ocr-pending",
        NOW,
        2,
        0,
        Some(NOW - 1000),
    ));
    let (_, d) = assert_order_independent("pods tie", o, &[], vec![], &state(IDLE));
    assert!(
        d.steps.iter().any(|s| matches!(
            s,
            sched_model::Step::AllowModel { model, .. } if model.as_str() == "mineru"
        )),
        "{:?}",
        d.steps
    );
    assert_eq!(d.allowed.models.len(), 1);
}

#[test]
fn equal_timestamp_reachability_of_the_gpu_host_blocks_allowing_in_either_order() {
    let mut o = base_zero_pods(&[]);
    o.push(up("cuda-central", NOW, true));
    o.push(up("cuda-central", NOW, false));
    o.push(demand(
        "paperless-ocr",
        "paperless-ocr-pending",
        NOW,
        2,
        0,
        Some(NOW - 1000),
    ));
    let (_, d) = assert_order_independent("gpu up tie", o, &[], vec![], &state(IDLE));
    assert!(
        !d.steps
            .iter()
            .any(|s| matches!(s, sched_model::Step::AllowModel { .. })),
        "{:?}",
        d.steps
    );
    assert!(d.allowed.models.is_empty());
}

#[test]
fn equal_timestamp_reachability_reads_as_not_up_in_either_order() {
    let managed = "    cuda-central: { power: { phase: \"on\", since: 0 }, mode: { phase: idle } }\n    basestar-nfs: { power: { phase: \"on\", since: 0 }, mode: { phase: idle }, managed: true, idle_since: 1 }\n";
    // Everything that lets a managed, idle, held-by-nothing NFS host power down is in place.
    let mut fixed = base_zero_pods(&[]);
    fixed.push(up("cuda-central", NOW, true));
    fixed.push(no_mounts(NOW));
    for (c, s) in [
        ("paperless-ocr", "paperless-ocr-pending"),
        ("paperless-tagging", "paperless-post-ocr"),
        ("speakr-transcription", "speakr-pending"),
        ("speakr-summarization", "speakr-summaries"),
        ("immich-ml", "immich-queues"),
        ("interactive-llm", "llm-sessions"),
    ] {
        fixed.push(demand(c, s, NOW, 0, 0, None));
    }
    let powers_off = |d: &sched_sequencer::Decision| {
        d.steps
            .iter()
            .any(|s| matches!(s, sched_model::Step::PowerOff { .. }))
    };
    let (_, d) = assert_order_independent(
        "sanity",
        vec![up("basestar-nfs", NOW, true)],
        &fixed,
        vec![],
        &state(managed),
    );
    assert!(
        powers_off(&d),
        "baseline should power the idle host down: {:?}",
        d.steps
    );
    let both = vec![
        up("basestar-nfs", NOW, true),
        up("basestar-nfs", NOW, false),
    ];
    let (_, d) = assert_order_independent("up tie", both, &fixed, vec![], &state(managed));
    assert!(
        !powers_off(&d),
        "a tied up/down reading must not power anything off: {:?}",
        d.steps
    );
}

#[test]
fn equal_timestamp_demand_reads_as_busiest_in_either_order() {
    let mut o = base_zero_pods(&[]);
    o.push(demand(
        "paperless-ocr",
        "paperless-ocr-pending",
        NOW,
        0,
        0,
        None,
    ));
    o.push(demand(
        "paperless-ocr",
        "paperless-ocr-pending",
        NOW,
        3,
        0,
        Some(NOW - 10),
    ));
    o.push(demand(
        "paperless-ocr",
        "paperless-ocr-pending",
        NOW,
        1,
        1,
        Some(NOW - 20),
    ));
    let (intent, _) = assert_order_independent("demand tie", o, &[], vec![], &state(IDLE));
    assert_eq!(
        intent.models[&ResourceId::new("cuda-central")],
        Some(ModelId::new("mineru"))
    );
}

#[test]
fn conflicting_leases_resolve_the_same_way_in_every_order() {
    let o = base_zero_pods(&[]);
    let leases = vec![
        lease(
            "a",
            "cuda-central",
            5,
            LeaseKind::ManualModel {
                model: ModelId::new("llm"),
            },
        ),
        lease(
            "b",
            "cuda-central",
            5,
            LeaseKind::ManualModel {
                model: ModelId::new("whisperx"),
            },
        ),
        lease(
            "c",
            "cuda-central",
            1,
            LeaseKind::Interactive {
                model: ModelId::new("mineru"),
                expires_at: Millis(NOW + 1),
            },
        ),
        lease(
            "d",
            "cuda-central",
            2,
            LeaseKind::Interactive {
                model: ModelId::new("immich-ml"),
                expires_at: Millis(NOW + 1),
            },
        ),
        lease("e", "basestar-nfs", 1, LeaseKind::ManualHost),
    ];
    let (intent, _) = assert_order_independent("leases", o.clone(), &[], leases, &state(IDLE));
    assert_eq!(
        intent.models[&ResourceId::new("cuda-central")],
        Some(ModelId::new("llm"))
    );

    // Interactive only: the first granted (lease c) wins regardless of order.
    let leases = vec![
        lease(
            "d",
            "cuda-central",
            2,
            LeaseKind::Interactive {
                model: ModelId::new("immich-ml"),
                expires_at: Millis(NOW + 1),
            },
        ),
        lease(
            "c",
            "cuda-central",
            1,
            LeaseKind::Interactive {
                model: ModelId::new("mineru"),
                expires_at: Millis(NOW + 1),
            },
        ),
        lease(
            "c2",
            "cuda-central",
            1,
            LeaseKind::Interactive {
                model: ModelId::new("llm"),
                expires_at: Millis(NOW + 1),
            },
        ),
    ];
    let (intent, _) = assert_order_independent("interactive", o, &[], leases, &state(IDLE));
    assert_eq!(
        intent.models[&ResourceId::new("cuda-central")],
        Some(ModelId::new("mineru"))
    );
}

#[test]
fn a_mixed_snapshot_is_order_independent() {
    let mut o = base_zero_pods(&["mineru"]);
    o.push(pods("mineru", NOW - 10, 1, 1));
    o.push(pods("mineru", NOW - 10, 1, 0));
    o.push(up("cuda-central", NOW, true));
    o.push(demand(
        "paperless-ocr",
        "paperless-ocr-pending",
        NOW,
        2,
        1,
        Some(NOW - 5000),
    ));
    let serving = "    cuda-central: { power: { phase: \"on\", since: 0 }, mode: { phase: serving, model: mineru, since: 1 } }\n";
    let leases = vec![
        lease(
            "x",
            "basestar-nfs",
            1,
            LeaseKind::SharedHold { holder: "h".into() },
        ),
        lease(
            "y",
            "cuda-central",
            3,
            LeaseKind::ManualModel {
                model: ModelId::new("llm"),
            },
        ),
    ];
    assert_order_independent("mixed", o, &[], leases, &state(serving));
}

fn arb_obs() -> impl Strategy<Value = Observation> {
    let at = prop::sample::select(vec![NOW - 40_000, NOW - 1000, NOW, NOW + 500]);
    let model = prop::sample::select(vec!["mineru", "llm", "whisperx", "immich-ml"]);
    let class = prop::sample::select(vec![
        ("paperless-ocr", "paperless-ocr-pending"),
        ("speakr-transcription", "speakr-pending"),
        ("interactive-llm", "llm-sessions"),
    ]);
    prop_oneof![
        (at.clone(), model, 0u32..2, 0u32..2).prop_map(|(a, m, p, r)| pods(
            m,
            a,
            p.max(r),
            r.min(p.max(r))
        )),
        (
            at.clone(),
            prop::sample::select(vec!["cuda-central", "basestar-nfs"]),
            any::<bool>()
        )
            .prop_map(|(a, r, u)| up(r, a, u)),
        (at, class, 0u64..3, 0u64..2).prop_map(|(a, (c, s), ready, fl)| demand(
            c,
            s,
            a.min(NOW),
            ready,
            fl,
            (ready > 0).then_some(NOW - 9000)
        )),
    ]
}

fn arb_lease() -> impl Strategy<Value = Lease> {
    (0usize..6, 0u64..4, "[a-d]").prop_map(|(k, g, id)| {
        let (res, kind) = match k {
            0 => ("basestar-nfs", LeaseKind::ManualHost),
            1 => ("basestar-nfs", LeaseKind::SharedHold { holder: "h".into() }),
            2 => (
                "cuda-central",
                LeaseKind::ManualModel {
                    model: ModelId::new("llm"),
                },
            ),
            3 => (
                "cuda-central",
                LeaseKind::ManualModel {
                    model: ModelId::new("whisperx"),
                },
            ),
            4 => ("cuda-central", LeaseKind::Reclaim),
            _ => (
                "cuda-central",
                LeaseKind::Interactive {
                    model: ModelId::new("mineru"),
                    expires_at: Millis(NOW + 9),
                },
            ),
        };
        lease(&id, res, g, kind)
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(300))]

    #[test]
    fn shuffling_a_snapshot_never_changes_the_decision(
        mut observations in prop::collection::vec(arb_obs(), 0..9),
        leases in prop::collection::vec(arb_lease(), 0..4),
        shuffled_obs in Just(()).prop_perturb(|_, mut rng| rng.random::<u64>()),
        serving in any::<bool>(),
    ) {
        let config = example_config();
        observations.extend(base_zero_pods(&[]));
        let st = state(if serving {
            "    cuda-central: { power: { phase: \"on\", since: 0 }, mode: { phase: serving, model: mineru, since: 1 } }\n    basestar-nfs: { power: { phase: \"on\", since: 0 }, mode: { phase: idle }, managed: true, idle_since: 5 }\n"
        } else { IDLE });
        let snap = |o: Vec<Observation>, l: Vec<Lease>| Snapshot { config_generation: 0, now: Millis(NOW), observations: o, leases: l, unobservable: Default::default() };
        let a = decide(&config, &snap(observations.clone(), leases.clone()), &st).unwrap();
        // A deterministic scramble from the generated seed.
        let mut o2 = observations.clone();
        let mut l2 = leases.clone();
        let mut seed = shuffled_obs | 1;
        for v in [&mut o2 as &mut dyn Scramble, &mut l2] { v.scramble(&mut seed); }
        let b = decide(&config, &snap(o2, l2), &st).unwrap();
        prop_assert_eq!(a, b);
    }
}

trait Scramble {
    fn scramble(&mut self, seed: &mut u64);
}

impl<T> Scramble for Vec<T> {
    fn scramble(&mut self, seed: &mut u64) {
        for i in (1..self.len()).rev() {
            *seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.swap(i, (*seed >> 33) as usize % (i + 1));
        }
    }
}
