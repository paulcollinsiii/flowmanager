//! Property tests: random worlds (including stale, missing and contradictory observations
//! and arbitrary leases) are replayed through the loop. Whatever the world does, no safety
//! invariant may be violated, runs must be repeatable, and the allowed set must agree with
//! the state it was derived from.

use proptest::prelude::*;
use sched_model::test_support::example_config;
use sched_model::{
    ClassId, Fidelity, Lease, LeaseId, LeaseKind, Millis, ModePhase, ModelId, ObservedValue,
    ResourceId, SourceId,
};
use sched_service::replay::{Frame, Observe, Scenario, run};

const MODELS: [&str; 4] = ["mineru", "llm", "whisperx", "immich-ml"];
const CLASSES: [(&str, &str); 7] = [
    ("paperless-ocr", "paperless-ocr-pending"),
    ("paperless-tagging", "paperless-post-ocr"),
    ("speakr-transcription", "speakr-pending"),
    ("speakr-summarization", "speakr-summaries"),
    ("immich-ml", "immich-queues"),
    ("backup", "backup-schedule"),
    ("interactive-llm", "llm-sessions"),
];
const HOSTS: [&str; 2] = ["cuda-central", "basestar-nfs"];

#[derive(Debug, Clone)]
struct Raw {
    dt: u64,
    pods: Vec<Option<(u32, u32)>>,
    up: Vec<Option<bool>>,
    demand: Vec<Option<(u64, u64, u64)>>,
    leases: Option<Vec<usize>>,
    refresh: bool,
}

fn raw() -> impl Strategy<Value = Raw> {
    (
        1u64..1_300_000,
        prop::collection::vec(
            prop::option::weighted(0.8, (0u32..3).prop_flat_map(|p| (Just(p), 0..=p))),
            4,
        ),
        prop::collection::vec(prop::option::weighted(0.8, any::<bool>()), 2),
        prop::collection::vec(
            prop::option::weighted(0.7, (0u64..4, 0u64..3, 0u64..4_000_000)),
            7,
        ),
        prop::option::weighted(0.3, prop::collection::vec(0usize..8, 0..4)),
        prop::bool::weighted(0.6),
    )
        .prop_map(|(dt, pods, up, demand, leases, refresh)| Raw {
            dt,
            pods,
            up,
            demand,
            leases,
            refresh,
        })
}

fn lease(kind: usize, now: u64, n: usize) -> Lease {
    let (resource, kind) = match kind {
        0 => ("basestar-nfs", LeaseKind::ManualHost),
        1 => ("cuda-central", LeaseKind::ManualHost),
        2 => ("basestar-nfs", LeaseKind::SharedHold { holder: "h".into() }),
        3 => (
            "cuda-central",
            LeaseKind::ManualModel {
                model: ModelId::new("llm"),
            },
        ),
        4 => (
            "cuda-central",
            LeaseKind::ManualModel {
                model: ModelId::new("mineru"),
            },
        ),
        5 => (
            "cuda-central",
            LeaseKind::Interactive {
                model: ModelId::new("whisperx"),
                expires_at: Millis(now + 700_000),
            },
        ),
        6 => ("cuda-central", LeaseKind::Reclaim),
        _ => (
            "cuda-central",
            LeaseKind::Interactive {
                model: ModelId::new("llm"),
                expires_at: Millis(now + 1),
            },
        ),
    };
    Lease {
        id: LeaseId::new(format!("l{n}")),
        resource: ResourceId::new(resource),
        kind,
        granted_at: Millis(now),
    }
}

fn scenario(frames: &[Raw]) -> Scenario {
    let mut now = 50_000_000u64;
    let mut out = Vec::new();
    for r in frames {
        now += r.dt;
        let mut observe = Vec::new();
        for (m, p) in MODELS.iter().zip(&r.pods) {
            if let Some((pods, ready_pods)) = p {
                observe.push(Observe {
                    source: SourceId::new("cluster-pods"),
                    observed_at: None,
                    fidelity: Fidelity::Event,
                    value: ObservedValue::ModelPods {
                        model: ModelId::new(*m),
                        pods: *pods,
                        ready_pods: *ready_pods,
                    },
                });
            }
        }
        for (h, u) in HOSTS.iter().zip(&r.up) {
            if let Some(up) = u {
                observe.push(Observe {
                    source: SourceId::new("cluster-pods"),
                    observed_at: None,
                    fidelity: Fidelity::Event,
                    value: ObservedValue::ResourceUp {
                        resource: ResourceId::new(*h),
                        up: *up,
                    },
                });
            }
        }
        for ((c, s), d) in CLASSES.iter().zip(&r.demand) {
            if let Some((ready, in_flight, age)) = d {
                observe.push(Observe {
                    source: SourceId::new(*s),
                    observed_at: None,
                    fidelity: Fidelity::Polled,
                    value: ObservedValue::Demand {
                        class: ClassId::new(*c),
                        ready: *ready,
                        in_flight: *in_flight,
                        oldest_ready_at: (*ready > 0).then(|| Millis(now - age.min(&(now - 1)))),
                    },
                });
            }
        }
        let leases = r.leases.as_ref().map(|ks| {
            ks.iter()
                .enumerate()
                .map(|(n, k)| lease(*k, now - r.dt.min(now), n))
                .collect()
        });
        out.push(Frame {
            at: Millis(now),
            observe,
            refresh: r.refresh,
            leases,
            expect: None,
        });
    }
    Scenario {
        description: "random".into(),
        initial_state: Default::default(),
        frames: out,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(400))]

    #[test]
    fn no_invariant_is_violated_in_any_world(frames in prop::collection::vec(raw(), 1..40)) {
        let config = example_config();
        let sc = scenario(&frames);
        let results = run(&config, &sc).unwrap();
        for (i, r) in results.iter().enumerate() {
            prop_assert!(r.violations.is_empty(), "frame {}: {:?}", i, r.violations);
            // The allowed set agrees with the state it came from.
            for class in &r.decision.allowed.classes {
                for req in &config.class(class).unwrap().requires {
                    let st = r.decision.state.resource(&req.resource);
                    prop_assert!(st.is_some_and(|s| s.is_on()), "class {} allowed on a host that is not on", class);
                    if let Some(model) = &req.model {
                        let ok = matches!(
                            &st.unwrap().mode,
                            ModePhase::Serving { model: m, wind_down: None, .. } if m == model
                        );
                        prop_assert!(ok, "class {} allowed but {} is not serving", class, model);
                    }
                }
            }
            for model in &r.decision.allowed.models {
                let res = &config.model(model).unwrap().resource;
                let st = r.decision.state.resource(res).unwrap();
                prop_assert_eq!(st.mode.allowed_model(), Some(model));
            }
        }
    }

    #[test]
    fn replays_are_repeatable(frames in prop::collection::vec(raw(), 1..25)) {
        let config = example_config();
        let sc = scenario(&frames);
        let a = run(&config, &sc).unwrap();
        let b = run(&config, &sc).unwrap();
        for (x, y) in a.iter().zip(&b) {
            prop_assert_eq!(&x.intent, &y.intent);
            prop_assert_eq!(&x.decision, &y.decision);
        }
    }
}
