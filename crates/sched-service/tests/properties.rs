//! Property tests: random worlds (including stale, missing and contradictory observations
//! and arbitrary leases) are replayed through the loop. Whatever the world does, no safety
//! invariant may be violated, runs must be repeatable, the allowed set must agree with the
//! state it was derived from, and pod observations must not change any decision.

use proptest::prelude::*;
use sched_model::{
    ClassId, Fidelity, Lease, LeaseId, LeaseKind, Millis, ModePhase, ModelId, ObservedValue,
    ResourceId, SourceId, Step, WaitReason, classes_of_model,
};
use sched_service::replay::{Frame, Observe, Scenario, run};

const MODELS: [&str; 4] = ["mineru", "llm", "whisperx", "immich-ml"];
const CLASSES: [(&str, &str); 7] = [
    ("paperless-ocr", "paperless-ocr-pending"),
    ("paperless-tagging", "paperless-post-ocr"),
    ("speakr-transcription", "speakr-pending"),
    ("speakr-summarization", "speakr-summaries"),
    ("immich-ml", "immich-queues"),
    ("interactive-llm", "llm-sessions"),
    // The class on the NFS host that may change power (a waker while it has work): see
    // `config_for`.
    ("nfs-batch", "nfs-batch-queue"),
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
    /// The `wikijs` switch reading this frame: 0 on, 1 off, 2 off_now (`None`: no reading).
    switch: Option<u8>,
    /// The `nfs-app` switch reading this frame, as `switch`. `nfs-app` is the switched class on
    /// the powered NFS host that `paperless-ocr` (a dependent) reads from, in the config of
    /// [`config_for`].
    nfs_app: Option<u8>,
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
            CLASSES.len(),
        ),
        prop::option::weighted(0.3, prop::collection::vec(0usize..9, 0..4)),
        prop::bool::weighted(0.6),
        prop::option::weighted(0.7, 0u8..3),
        prop::option::weighted(0.8, 0u8..3),
    )
        .prop_map(
            |(dt, pods, up, mounts, demand, leases, refresh, switch, nfs_app)| Raw {
                dt,
                pods,
                up,
                mounts,
                demand,
                leases,
                refresh,
                switch,
                nfs_app,
            },
        )
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
        if let Some(s) = r.switch {
            observe.push(Observe {
                source: SourceId::new("ha-wikijs"),
                observed_at: None,
                fidelity: Fidelity::Polled,
                value: ObservedValue::Switch {
                    class: ClassId::new("wikijs"),
                    state: match s {
                        0 => sched_model::SwitchState::On,
                        1 => sched_model::SwitchState::Off,
                        _ => sched_model::SwitchState::OffNow,
                    },
                },
            });
        }
        if let Some(s) = r.nfs_app {
            observe.push(Observe {
                source: SourceId::new("ha-wikijs"),
                observed_at: None,
                fidelity: Fidelity::Polled,
                value: ObservedValue::Switch {
                    class: ClassId::new("nfs-app"),
                    state: match s {
                        0 => sched_model::SwitchState::On,
                        1 => sched_model::SwitchState::Off,
                        _ => sched_model::SwitchState::OffNow,
                    },
                },
            });
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

/// The example (managed power or not) plus `nfs-app`, a switched class on the powered NFS host
/// that `paperless-ocr` reads from (`reads_from`): the graceful off, its stored wait and the
/// dependents' offline demand are all in the generated worlds. `nfs-batch`, a class on the same
/// host that may change power and has a demand source, is there too: with `nfs-app` switched on
/// it is a waker of the NFS host (power-model 2.1d), so a graceful drain meets wakers appearing.
fn config_for(managed: bool) -> sched_model::Config {
    let yaml = sched_model::test_support::with_nfs_batch(
        &sched_model::test_support::example_yaml_switched_dependents(),
    );
    let yaml = if managed {
        yaml
    } else {
        yaml.replace("      control: managed\n", "      control: manual\n")
    };
    sched_model::Config::from_yaml(&yaml).unwrap_or_else(|e| panic!("{e}"))
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

/// Whether `st` is a host that is on and carries an idle drain without the idle clock that
/// started it (a state the sequencer must not leave: the next power-off would skip the grace).
fn idle_drain_without_clock(st: &sched_model::ResourceState) -> bool {
    st.is_on()
        && st.idle_since.is_none()
        && st
            .shutdown
            .is_some_and(|s| s.is_drain() && s.cause() == sched_model::ShutdownCause::Idle)
}

/// A frame with no reading but the two reachability ones.
fn quiet(dt: u64, up: [bool; 2]) -> Raw {
    Raw {
        dt,
        pods: vec![None; 4],
        up: up.iter().map(|u| Some(*u)).collect(),
        mounts: None,
        demand: vec![None; CLASSES.len()],
        leases: None,
        refresh: false,
        switch: None,
        nfs_app: None,
    }
}

/// The NFS host as the sequencer leaves it with an idle drain started at `since`: the drain
/// only starts once the idle clock has run for `idle_grace`, and the clock stays set while the
/// drain stands (`power.rs`: `idle_since.get_or_insert`).
fn nfs_with_idle_drain(config: &sched_model::Config, since: Millis) -> sched_model::ResourceState {
    let mut st = sched_model::ResourceState::new(sched_model::PowerPhase::On {
        since: Millis(1),
        ours: false,
    });
    st.set_ours(true);
    st.idle_since = Some(Millis(since.0.saturating_sub(config.policy().idle_grace.0)));
    st.shutdown = Some(sched_model::Shutdown::idle_withdrawn(since));
    st
}

/// Regression for the flake in `a_drain_under_way_violates_no_invariant`: one quiet frame (no
/// demand, no mounts, both switches off) on top of an idle drain that has its idle clock.
/// (Without the clock, which the sequencer never leaves with an idle drain, the checker rightly
/// reported invariant 10 for the power-off.)
#[test]
fn an_idle_drain_with_its_idle_clock_may_power_off() {
    let config = config_for(true);
    let mut f = quiet(1, [false, true]);
    f.mounts = Some(0);
    f.demand = vec![Some((0, 0, 0, 0)); CLASSES.len()];
    f.switch = Some(1);
    f.nfs_app = Some(1);
    let mut sc = scenario(&[f]);
    sc.initial_state.resources.insert(
        "basestar-nfs".into(),
        nfs_with_idle_drain(&config, Millis(50_000_000)),
    );
    let results = run(&config, &sc).unwrap();
    for (i, r) in results.iter().enumerate() {
        assert!(r.violations.is_empty(), "frame {}: {:?}", i, r.violations);
    }
}

/// A host that has been idle for the whole grace period, taken through the ways an idle drain
/// can be held back and resumed (mounts, a manual lease, an unknown reading): the state the
/// sequencer itself produces never powers it off early, and an idle drain never stands
/// without the idle clock that started it.
#[test]
fn an_idle_drain_keeps_its_idle_clock_while_it_stands() {
    let config = config_for(true);
    let nfs = || sched_model::ResourceId::new("basestar-nfs");
    let mut frames = Vec::new();
    for (mounts, leases, dt) in [
        (Some(1), None, 1u64),
        (Some(1), Some(vec![0]), 10_000),
        (Some(1), Some(vec![]), 10_000),
        (None, None, 10_000),
        (Some(1), None, 700_000),
        (Some(0), None, 10_000),
    ] {
        let mut f = quiet(dt, [true, true]);
        f.mounts = mounts;
        f.leases = leases;
        f.demand = vec![Some((0, 0, 0, 0)); CLASSES.len()];
        f.switch = Some(1);
        f.nfs_app = Some(1);
        frames.push(f);
    }
    let mut sc = scenario(&frames);
    let mut st = sched_model::ResourceState::new(sched_model::PowerPhase::On {
        since: Millis(1),
        ours: false,
    });
    st.set_ours(true);
    st.idle_since = Some(Millis(50_000_000 - config.policy().idle_grace.0));
    sc.initial_state.resources.insert(nfs(), st);
    let results = run(&config, &sc).unwrap();
    for (i, r) in results.iter().enumerate() {
        assert!(r.violations.is_empty(), "frame {}: {:?}", i, r.violations);
        let st = r.decision.state.resource(&nfs()).unwrap();
        // (Once the power-off is issued the host is no longer on and the clock is gone with it.)
        assert!(
            !idle_drain_without_clock(st),
            "frame {}: idle drain without clock: {:?}",
            i,
            st
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(400))]

    #[test]
    fn no_invariant_is_violated_in_any_world(frames in prop::collection::vec(raw(), 1..40), managed in any::<bool>()) {
        let config = config_for(managed);
        let sc = scenario(&frames);
        let results = run(&config, &sc).unwrap();
        for (i, r) in results.iter().enumerate() {
            prop_assert!(r.violations.is_empty(), "frame {}: {:?}", i, r.violations);
            for st in r.decision.state.resources.values() {
                prop_assert!(!idle_drain_without_clock(st), "frame {}: {:?}", i, st);
            }
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
        let config = config_for(false);
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
        let config = config_for(false);
        let sc = scenario(&frames);
        let a = run(&config, &sc).unwrap();
        let b = run(&config, &sc).unwrap();
        for (x, y) in a.iter().zip(&b) {
            prop_assert_eq!(&x.intent, &y.intent);
            prop_assert_eq!(&x.decision, &y.decision);
        }
    }

    /// A graceful requested shutdown under way on the NFS host meets a may-wake class becoming
    /// busy or a switched class reading on (the generated frames have both): whenever the host
    /// has a waker the shutdown is cancelled, it is never powered off, and no invariant is
    /// violated (including the one that a graceful shutdown never powers off over a waker).
    #[test]
    fn a_graceful_drain_never_outlives_a_waker(
        frames in prop::collection::vec(raw(), 1..40),
        scaled_down in any::<bool>(),
        ago in 0u64..1_500_000,
    ) {
        let config = config_for(true);
        let nfs = sched_model::ResourceId::new("basestar-nfs");
        let mut sc = scenario(&frames);
        let mut st = nfs_with_idle_drain(&config, Millis(50_000_000 - ago));
        st.idle_since = None;
        let requested = sched_model::Shutdown::requested(
            sched_model::DrainMode::Graceful,
            Millis(50_000_000 - ago),
        );
        st.shutdown = Some(if scaled_down { requested.withdrawn() } else { requested });
        sc.initial_state.resources.insert(nfs.clone(), st);
        let mut before = sc.initial_state.clone();
        let results = run(&config, &sc).unwrap();
        for (i, r) in results.iter().enumerate() {
            prop_assert!(r.violations.is_empty(), "frame {}: {:?}", i, r.violations);
            let graceful = |s: &sched_model::SchedState| {
                s.resource(&nfs).and_then(|x| x.shutdown).is_some_and(|x| {
                    x.cause() == sched_model::ShutdownCause::Requested {
                        mode: sched_model::DrainMode::Graceful,
                    }
                })
            };
            let wakers = sched_model::wakers(&config, &r.snapshot, &before, &nfs);
            if graceful(&before) && !wakers.is_empty() {
                prop_assert!(
                    !graceful(&r.decision.state),
                    "frame {}: a graceful shutdown stands with wakers {:?}", i, wakers
                );
                prop_assert!(
                    !r.decision.steps.contains(&Step::PowerOff { resource: nfs.clone() }),
                    "frame {}: powered off with wakers {:?}", i, wakers
                );
            }
            before = r.decision.state.clone();
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
        let config = config_for(true);
        let mut sc = scenario(&frames);
        let since = Millis(50_000_000 - ago);
        // An idle drain (the scheduler's own) is always immediate and already scaled down, and
        // starts only once the idle clock has run for `idle_grace` (it stays set meanwhile).
        let mut st = nfs_with_idle_drain(&config, since);
        if !idle {
            st.idle_since = None;
            let mode = if immediate { sched_model::DrainMode::Immediate } else { sched_model::DrainMode::Graceful };
            let requested = sched_model::Shutdown::requested(mode, since);
            st.shutdown = Some(if scaled_down { requested.withdrawn() } else { requested });
        }
        sc.initial_state.resources.insert("basestar-nfs".into(), st);
        let results = run(&config, &sc).unwrap();
        for (i, r) in results.iter().enumerate() {
            prop_assert!(r.violations.is_empty(), "frame {}: {:?}", i, r.violations);
            for st in r.decision.state.resources.values() {
                prop_assert!(!idle_drain_without_clock(st), "frame {}: {:?}", i, st);
            }
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
