//! Property tests: random worlds (including stale, missing and contradictory observations
//! and arbitrary leases) are replayed through the loop. Whatever the world does, no safety
//! invariant may be violated, runs must be repeatable, the allowed set must agree with the
//! state it was derived from, and pod observations must not change any decision.

use proptest::prelude::*;
use sched_model::test_support::{example_config, example_config_managed};
use sched_model::{
    ClassId, Fidelity, Lease, LeaseId, LeaseKind, Millis, ModePhase, ModelId, ObservedValue,
    ResourceId, SourceId, Step, WaitReason, classes_of_model,
};
use sched_service::replay::{Frame, Observe, Scenario, run};

const MODELS: [&str; 4] = ["mineru", "llm", "whisperx", "immich-ml"];
const CLASSES: [(&str, &str); 6] = [
    ("paperless-ocr", "paperless-ocr-pending"),
    ("paperless-tagging", "paperless-post-ocr"),
    ("speakr-transcription", "speakr-pending"),
    ("speakr-summarization", "speakr-summaries"),
    ("immich-ml", "immich-queues"),
    ("interactive-llm", "llm-sessions"),
];
const HOSTS: [&str; 2] = ["cuda-central", "basestar-nfs"];

#[derive(Debug, Clone)]
struct Raw {
    dt: u64,
    pods: Vec<Option<(u32, u32)>>,
    up: Vec<Option<bool>>,
    /// How many pods mount the NFS host (`None`: no reading this frame).
    mounts: Option<u32>,
    /// (ready, in_flight, age of the oldest ready item, failed)
    demand: Vec<Option<(u64, u64, u64, u64)>>,
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
        prop::option::weighted(0.6, 0u32..3),
        prop::collection::vec(
            prop::option::weighted(0.7, (0u64..4, 0u64..3, 0u64..4_000_000, 0u64..3)),
            6,
        ),
        prop::option::weighted(0.3, prop::collection::vec(0usize..9, 0..4)),
        prop::bool::weighted(0.6),
    )
        .prop_map(|(dt, pods, up, mounts, demand, leases, refresh)| Raw {
            dt,
            pods,
            up,
            mounts,
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
        6 => ("cuda-central", LeaseKind::Lockout),
        7 => ("basestar-nfs", LeaseKind::Lockout),
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
        origin: sched_model::LeaseOrigin::Requested,
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
        if let Some(pods) = r.mounts {
            observe.push(Observe {
                source: SourceId::new("basestar-mounts"),
                observed_at: None,
                fidelity: Fidelity::Polled,
                value: ObservedValue::ResourceMounts {
                    resource: ResourceId::new("basestar-nfs"),
                    pods,
                },
            });
        }
        for ((c, s), d) in CLASSES.iter().zip(&r.demand) {
            if let Some((ready, in_flight, age, failed)) = d {
                observe.push(Observe {
                    source: SourceId::new(*s),
                    observed_at: None,
                    fidelity: Fidelity::Polled,
                    value: ObservedValue::Demand {
                        failed: *failed,
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
        power_control: None,
        min_off: None,
        frames: out,
    }
}

/// The same world without a single pod observation.
fn without_pods(frames: &[Raw]) -> Vec<Raw> {
    frames
        .iter()
        .cloned()
        .map(|mut r| {
            r.pods = vec![None; r.pods.len()];
            r
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(400))]

    #[test]
    fn no_invariant_is_violated_in_any_world(frames in prop::collection::vec(raw(), 1..40), managed in any::<bool>()) {
        let config = if managed { example_config_managed() } else { example_config() };
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
            // No wind-down waits for in-flight work past its timeout: a decision that still
            // waits on in-flight work is within the wind-down's timeout, or the longest
            // `kill_timeout` of the model's classes.
            for step in &r.decision.steps {
                let Step::Wait { resource, reason: WaitReason::InFlightWork } = step else {
                    continue;
                };
                let Some(ModePhase::Serving { model, wind_down: Some(wd), .. }) =
                    r.decision.state.resource(resource).map(|s| &s.mode)
                else {
                    continue;
                };
                let limit = classes_of_model(&config, model)
                    .filter_map(|c| c.kill_timeout)
                    .chain(wd.timeout)
                    .max();
                prop_assert!(wd.timeout.is_some(), "frame {}: a wind-down without a timeout", i);
                let due = wd.since.saturating_add(limit.unwrap());
                prop_assert!(
                    r.at < due,
                    "frame {}: still waiting on in-flight work at {:?}, past its cut-off {:?}",
                    i, r.at, due
                );
            }
            // At every tick at most one model per exclusive resource (cuda-central here).
            prop_assert!(r.decision.allowed.models.len() <= 1, "frame {}: {:?}", i, r.decision.allowed);
        }
    }

    #[test]
    fn pod_observations_only_ever_hold_back_a_recovery(frames in prop::collection::vec(raw(), 1..40)) {
        // Without power management: under it a pod reading also holds a power-off back (the
        // veto), which the sequencer tests cover.
        let config = example_config();
        let with = run(&config, &scenario(&frames)).unwrap();
        let without = run(&config, &scenario(&without_pods(&frames))).unwrap();
        for (i, (a, b)) in with.iter().zip(&without).enumerate() {
            prop_assert_eq!(&a.intent, &b.intent, "frame {}", i);
            // Pods only ever hold back a recovery request (until a pod is ready); every other
            // step, the allowed set and the state are the same with or without them.
            let strip = |d: &sched_sequencer::Decision| {
                let mut d = d.clone();
                d.steps.retain(|s| !matches!(s, sched_model::Step::RunRecovery { .. }));
                d
            };
            prop_assert_eq!(strip(&a.decision), strip(&b.decision), "frame {}", i);
            for s in &a.decision.steps {
                if matches!(s, sched_model::Step::RunRecovery { .. }) {
                    prop_assert!(b.decision.steps.contains(s), "frame {}: {:?}", i, s);
                }
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

    /// The same worlds with a drain already under way on the NFS host (managed, up): no
    /// invariant is violated (a power-off needs the consumers scaled down first, and no lease
    /// is overridden), and once scaled down no consumer of the host is allowed.
    #[test]
    fn a_drain_under_way_violates_no_invariant(
        frames in prop::collection::vec(raw(), 1..40),
        immediate in any::<bool>(),
        scaled_down in any::<bool>(),
        ago in 0u64..1_500_000,
        idle in any::<bool>(),
    ) {
        let config = example_config_managed();
        let mut sc = scenario(&frames);
        let mut st = sched_model::ResourceState::new(sched_model::PowerPhase::On { since: Millis(1), ours: false });
        st.set_ours(true);
        // An idle drain (the scheduler's own) is always immediate and already scaled down.
        let since = Millis(50_000_000 - ago);
        st.shutdown = Some(if idle {
            sched_model::Shutdown::idle_withdrawn(since)
        } else {
            let mode = if immediate { sched_model::DrainMode::Immediate } else { sched_model::DrainMode::Graceful };
            let requested = sched_model::Shutdown::requested(mode, since);
            if scaled_down { requested.withdrawn() } else { requested }
        });
        sc.initial_state.resources.insert("basestar-nfs".into(), st);
        let results = run(&config, &sc).unwrap();
        for (i, r) in results.iter().enumerate() {
            prop_assert!(r.violations.is_empty(), "frame {}: {:?}", i, r.violations);
            let draining = r
                .decision
                .state
                .resource(&"basestar-nfs".into())
                .and_then(|s| s.shutdown)
                .is_some_and(|s| s.consumers_withdrawn());
            if draining {
                for class in &r.decision.allowed.classes {
                    let needs = config
                        .class(class)
                        .unwrap()
                        .requires
                        .iter()
                        .any(|q| q.resource.as_str() == "basestar-nfs");
                    prop_assert!(!needs, "frame {}: {} allowed during a scaled-down drain", i, class);
                }
            }
        }
    }
}
