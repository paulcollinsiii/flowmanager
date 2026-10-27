//! Multi-tick runs of the stateful scheduler: the sequencer state is persisted between
//! ticks the way the `Scheduler` object keeps it in Restate (as JSON), and the process is
//! restarted (new `Shared`, empty observation cache, state read back) in the middle of a
//! switch. A small world plays KEDA and Kubernetes: it scales models from the published
//! allowed set only, takes a few ticks to bring pods down, and keeps a newly allowed model's
//! pod pending until no other model's pod holds the GPU (one `nvidia.com/gpu`).
//!
//! Checked at every tick, from the world and from what was published, not from the
//! scheduler's own state: at most one model of the exclusive resource is allowed, a model
//! that was replaced never comes back while its successor is wanted, and the GPU never runs
//! two models (the world's exclusivity is what Kubernetes provides, and the scheduler must
//! not need anything more).

use std::collections::BTreeMap;
use std::sync::Arc;

use sched_model::test_support::EXAMPLE_CONFIG;
use sched_model::{
    ClassId, Fidelity, Lease, LeaseKind, Millis, ModePhase, ModelId, ObservedValue,
    PublishedAllowed, Step,
};
use sched_observe::Report;
use sched_service::clock::ManualClock;
use sched_service::config_source::ConfigHolder;
use sched_service::leases::LeaseBook;
use sched_service::metrics::Metrics;
use sched_service::tick::{Durable, Shared, TickReport};
use tokio::sync::watch;

const T0: u64 = 50_000_000;
const TICK: u64 = 10_000;
const SAFETY_MAX_AGE: u64 = 30_000;
const GPU: &str = "cuda-central";
const MODELS: [&str; 4] = ["mineru", "llm", "whisperx", "immich-ml"];
const CLASSES: [(&str, &str); 6] = [
    ("paperless-ocr", "paperless-ocr-pending"),
    ("paperless-tagging", "paperless-post-ocr"),
    ("speakr-transcription", "speakr-pending"),
    ("speakr-summarization", "speakr-summaries"),
    ("immich-ml", "immich-queues"),
    ("interactive-llm", "llm-sessions"),
];

/// One process of the scheduler.
struct Process {
    shared: Arc<Shared>,
    clock: Arc<ManualClock>,
    allowed: watch::Receiver<Option<PublishedAllowed>>,
}

fn start(now: u64) -> Process {
    let clock = Arc::new(ManualClock::new(Millis(now)));
    let holder = Arc::new(ConfigHolder::new(EXAMPLE_CONFIG).unwrap());
    let shared = Arc::new(Shared::new(holder, Arc::new(Metrics::new()), clock.clone()));
    let allowed = shared.subscribe_allowed();
    Process {
        shared,
        clock,
        allowed,
    }
}

/// The durable state as Restate holds it: JSON, read back on every tick.
#[derive(Default)]
struct Store {
    json: String,
}

impl Store {
    fn load(&self) -> Durable {
        if self.json.is_empty() {
            Durable::default()
        } else {
            serde_json::from_str(&self.json).unwrap()
        }
    }

    fn save(&mut self, d: &Durable) {
        self.json = serde_json::to_string(d).unwrap();
    }
}

#[derive(Clone, Copy, Default, Debug)]
struct Queue {
    ready: u64,
    in_flight: u64,
}

/// The cluster and the services, as KEDA, Kubernetes and the workloads would make them
/// behave.
struct World {
    /// (pods, ready pods) per model. A pending pod is (1, 0); a terminating one keeps its
    /// count with no ready pod until it is gone.
    pods: BTreeMap<&'static str, (u32, u32)>,
    /// Ticks left until a model that lost its allowance has no pods. Such a pod still
    /// holds the GPU.
    terminating: BTreeMap<&'static str, u32>,
    queues: BTreeMap<&'static str, Queue>,
    /// What KEDA last saw: kept while the scaler has nothing published (just restarted).
    allowed: Option<PublishedAllowed>,
}

impl World {
    fn new() -> World {
        World {
            pods: MODELS.iter().map(|m| (*m, (0, 0))).collect(),
            terminating: BTreeMap::new(),
            queues: CLASSES
                .iter()
                .map(|(c, _)| (*c, Queue::default()))
                .collect(),
            allowed: None,
        }
    }

    fn model_allowed(&self, m: &str) -> bool {
        self.allowed
            .as_ref()
            .is_some_and(|a| a.allowed.models.contains(&ModelId::from(m)))
    }

    fn class_allowed(&self, c: &str) -> bool {
        self.allowed
            .as_ref()
            .is_some_and(|a| a.allowed.classes.contains(&ClassId::from(c)))
    }

    /// Whether a pod of a model other than `m` holds the GPU (running or terminating).
    fn gpu_held_by_other(&self, m: &str) -> bool {
        MODELS
            .iter()
            .any(|o| *o != m && (self.pods[o].1 > 0 || self.terminating.contains_key(o)))
    }

    /// Models whose pods run on the GPU right now (ready).
    fn running(&self) -> Vec<&'static str> {
        MODELS
            .iter()
            .copied()
            .filter(|m| self.pods[m].1 > 0)
            .collect()
    }

    /// One tick of the world: KEDA converges on the allowed set it has, Kubernetes
    /// schedules a pending pod once the GPU is free, work proceeds.
    fn step(&mut self, published: Option<PublishedAllowed>) {
        if published.is_some() {
            self.allowed = published;
        }
        // Withdrawn models first: their pods start terminating and still hold the GPU.
        for m in MODELS {
            let (p, _) = self.pods[m];
            if self.model_allowed(m) || p == 0 {
                continue;
            }
            let left = self.terminating.entry(m).or_insert(3);
            *left -= 1;
            if *left == 0 {
                self.terminating.remove(m);
                self.pods.insert(m, (0, 0));
            } else {
                self.pods.insert(m, (p, 0));
            }
        }
        for m in MODELS {
            if !self.model_allowed(m) {
                continue;
            }
            self.terminating.remove(m);
            let next = if self.gpu_held_by_other(m) {
                (1, 0)
            } else {
                (1, 1)
            };
            self.pods.insert(m, next);
        }
        assert!(
            self.running().len() <= 1,
            "two models on one GPU: {:?}",
            self.pods
        );
        for (c, _) in CLASSES {
            let runs = self.class_allowed(c);
            let q = self.queues.get_mut(c).unwrap();
            // An item in flight finishes within a tick; a new one starts only if allowed.
            q.in_flight = 0;
            if runs && q.ready > 0 {
                q.ready -= 1;
                q.in_flight = 1;
            }
        }
    }
}

fn push(p: &Process, source: &str, values: Vec<ObservedValue>) {
    p.shared.ingest_now(Report::Observed {
        source: source.into(),
        fidelity: Fidelity::Polled,
        values,
    });
}

fn push_pods(p: &Process, w: &World) {
    let mut v: Vec<ObservedValue> = MODELS
        .iter()
        .map(|m| ObservedValue::ModelPods {
            model: (*m).into(),
            pods: w.pods[m].0,
            ready_pods: w.pods[m].1,
        })
        .collect();
    v.push(ObservedValue::ResourceUp {
        resource: GPU.into(),
        up: true,
    });
    push(p, "cluster-pods", v);
}

fn push_rest(p: &Process, w: &World) {
    push_rest_except(p, w, &[]);
}

/// As `push_rest`, leaving out the demand sources in `skip`.
fn push_rest_except(p: &Process, w: &World, skip: &[&str]) {
    push(
        p,
        "basestar-probe",
        vec![ObservedValue::ResourceUp {
            resource: "basestar-nfs".into(),
            up: true,
        }],
    );
    for (c, s) in CLASSES {
        if skip.contains(&s) {
            continue;
        }
        let q = w.queues[c];
        push(
            p,
            s,
            vec![ObservedValue::Demand {
                failed: 0,
                class: c.into(),
                ready: q.ready,
                in_flight: q.in_flight,
                oldest_ready_at: None,
            }],
        );
    }
}

fn gpu_models(a: &PublishedAllowed) -> Vec<&ModelId> {
    a.allowed.models.iter().collect()
}

fn mode(d: &Durable) -> ModePhase {
    d.state
        .resource(&GPU.into())
        .map_or(ModePhase::Idle, |r| r.mode.clone())
}

/// The decision loop as the object runs it: load, decide, store, publish.
fn tick(p: &Process, store: &mut Store, now: u64) -> TickReport {
    p.clock.set(Millis(now));
    let mut d = store.load();
    p.shared.seed_reported(&d.reported);
    let outcome = p.shared.decide(Millis(now), &d);
    d.apply(&outcome);
    store.save(&d);
    p.shared.publish(&outcome, false);
    outcome.report
}

fn allowed_has(a: &PublishedAllowed, m: &str) -> bool {
    a.allowed.models.contains(&ModelId::from(m))
}

#[test]
fn a_switch_is_one_tick_and_resumes_safely_after_a_restart_in_the_middle_of_it() {
    let mut store = Store::default();
    let mut world = World::new();
    world.queues.get_mut("paperless-ocr").unwrap().ready = 3;
    let mut p = start(T0);
    let mut log: Vec<String> = Vec::new();
    // The cluster watch stalls for longer than host_reading_stale_after when the switch comes due.
    let stall = 60..75usize;
    let mut stale_holds = 0;
    let mut switched_at: Option<usize> = None;
    let mut restarted_at: Option<usize> = None;
    let mut whisperx_ready_at: Option<usize> = None;
    let mut mineru_served = false;

    for t in 0..120usize {
        let now = T0 + t as u64 * TICK;
        if t == 8 {
            // Transcription work arrives while OCR is being served.
            world.queues.get_mut("speakr-transcription").unwrap().ready = 2;
        }
        // Restart right after the switch: mineru's pod is still terminating and holds the
        // GPU, whisperx's is pending behind it.
        if restarted_at.is_none() && switched_at.is_some_and(|s| t == s + 1) {
            assert!(
                world.terminating.contains_key("mineru"),
                "{}",
                log.join("\n")
            );
            assert_eq!(world.pods["whisperx"], (1, 0), "{}", log.join("\n"));
            restarted_at = Some(t);
            p = start(now);
            assert_eq!(
                *p.allowed.borrow(),
                None,
                "nothing published by a new process"
            );
            log.push(format!("t={t}: RESTART"));
        }
        p.clock.set(Millis(now));
        let first_after_restart = restarted_at == Some(t);
        // After the restart the demand sources are silent for a few ticks.
        let demand_silent = restarted_at.is_some_and(|r| t < r + 4);
        if !stall.contains(&t) && !first_after_restart {
            push_pods(&p, &world);
        }
        if !demand_silent {
            push_rest(&p, &world);
        }

        let r = tick(&p, &mut store, now);
        assert_eq!(r.error, None, "t={t}: {r:?}");
        let published = p.allowed.borrow().clone().expect("published after a tick");
        assert_eq!(published.at, r.at);
        log.push(format!(
            "t={t} pods={:?} terminating={:?} mode={:?} allowed={:?} steps={:?}",
            world.pods,
            world.terminating,
            mode(&store.load()),
            gpu_models(&published),
            r.steps
        ));
        let dump = || log.join("\n");

        // 1. At most one model of the exclusive resource.
        assert!(gpu_models(&published).len() <= 1, "t={t}\n{}", dump());
        // 2. The switch is one tick: mineru out and whisperx in together, never both.
        let drained = r.steps.contains(&Step::DrainModel {
            resource: GPU.into(),
            model: "mineru".into(),
        });
        let allowed = r.steps.contains(&Step::AllowModel {
            resource: GPU.into(),
            model: "whisperx".into(),
        });
        assert_eq!(drained, allowed, "t={t}: half a switch\n{}", dump());
        if drained {
            assert!(switched_at.is_none(), "switched twice\n{}", dump());
            assert!(
                !stall.contains(&t),
                "switched on stale reachability\n{}",
                dump()
            );
            // mineru's pod is still there: nothing waited for it.
            assert!(world.pods["mineru"].1 > 0, "t={t}\n{}", dump());
            switched_at = Some(t);
        }
        if switched_at.is_some() {
            assert!(
                !allowed_has(&published, "mineru"),
                "t={t}: mineru is back\n{}",
                dump()
            );
            assert!(allowed_has(&published, "whisperx"), "t={t}\n{}", dump());
        }
        // The switch was due during the stall but the host's reading was stale: it waited
        // as a whole, mineru staying allowed.
        let stale = t > stall.start + (SAFETY_MAX_AGE / TICK) as usize;
        let held = r.steps.contains(&Step::Wait {
            resource: GPU.into(),
            reason: sched_model::WaitReason::StaleObservation,
        });
        if stall.contains(&t) && stale && switched_at.is_none() && held {
            assert!(allowed_has(&published, "mineru"), "t={t}\n{}", dump());
            stale_holds += 1;
        }
        if first_after_restart {
            // Nothing observed yet in this process: the persisted state carries the switch.
            assert_eq!(
                gpu_models(&published),
                vec![&ModelId::from("whisperx")],
                "{}",
                dump()
            );
            assert!(
                r.steps.iter().all(|s| matches!(s, Step::Wait { .. })),
                "{}",
                dump()
            );
        }
        if allowed_has(&published, "mineru") && world.pods["mineru"].1 > 0 {
            mineru_served = true;
        }

        world.step(p.allowed.borrow().clone());
        if world.pods["whisperx"].1 > 0 {
            whisperx_ready_at.get_or_insert(t);
        }
    }
    let dump = log.join("\n");
    if std::env::var_os("DURABLE_SWITCH_LOG").is_some() {
        println!("{dump}\nstale_holds={stale_holds}");
    }
    assert!(mineru_served, "{dump}");
    let switched_at = switched_at.unwrap_or_else(|| panic!("never switched\n{dump}"));
    assert!(switched_at >= stall.end, "{dump}");
    assert!(
        stale_holds >= 1,
        "the stale reading never held the switch\n{dump}"
    );
    let restarted_at = restarted_at.unwrap_or_else(|| panic!("never restarted\n{dump}"));
    // whisperx ran only once mineru's pod had gone (Kubernetes), after the restart.
    let ready = whisperx_ready_at.unwrap_or_else(|| panic!("whisperx never ran\n{dump}"));
    assert!(ready >= restarted_at, "{dump}");
    assert!(
        matches!(mode(&store.load()), ModePhase::Serving { ref model, .. } if model.as_str() == "whisperx"),
        "{dump}"
    );
    assert_eq!(world.queues["speakr-transcription"].ready, 0, "{dump}");
}

#[test]
fn a_manual_lease_forces_a_switch_and_an_unexecuted_kill_does_not_stall_it() {
    let mut store = Store::default();
    let mut world = World::new();
    world.queues.get_mut("paperless-ocr").unwrap().ready = 1_000;
    let p = start(T0);
    let mut log: Vec<String> = Vec::new();
    let mut lease: Option<Lease> = None;
    let mut killed_at = None;
    let mut llm_allowed_at = None;
    let mut paused_at = None;
    let drain_timeout = 10 * 60_000;

    for t in 0..120usize {
        let now = T0 + t as u64 * TICK;
        p.clock.set(Millis(now));
        if t == 40 {
            // The user pins the LLM while OCR is busy, through the same book the object uses.
            let mut d = store.load();
            let mut book = LeaseBook {
                next: 0,
                leases: d.leases.clone(),
            };
            let config = p.shared.config.current();
            let g = book
                .acquire_manual_model(&config, &"llm".into(), Millis(now))
                .unwrap();
            d.leases = book.leases;
            store.save(&d);
            lease = Some(g.lease);
        }
        push_pods(&p, &world);
        // OCR's in-flight item never finishes on its own while mineru runs: only its pods
        // going away ends it (nothing executes the kill step).
        let ocr = world.queues.get_mut("paperless-ocr").unwrap();
        if paused_at.is_some() && world.pods["mineru"].0 > 0 {
            ocr.in_flight = 1;
        }
        push_rest(&p, &world);
        let r = tick(&p, &mut store, now);
        assert_eq!(r.error, None);
        let published = p.allowed.borrow().clone().unwrap();
        log.push(format!(
            "t={t} pods={:?} mode={:?} allowed={:?} steps={:?} not_executed={:?}",
            world.pods,
            mode(&store.load()),
            gpu_models(&published),
            r.steps,
            r.not_executed
        ));
        let dump = || log.join("\n");
        assert!(gpu_models(&published).len() <= 1, "{}", dump());
        if r.steps.contains(&Step::PauseIntake {
            class: "paperless-ocr".into(),
        }) {
            paused_at.get_or_insert(now);
        }
        let kill = Step::KillInFlight {
            class: "paperless-ocr".into(),
        };
        if r.steps.contains(&kill) {
            assert!(r.not_executed.contains(&kill), "{}", dump());
            assert!(
                r.steps.contains(&Step::DrainModel {
                    resource: GPU.into(),
                    model: "mineru".into()
                }) && r.steps.contains(&Step::AllowModel {
                    resource: GPU.into(),
                    model: "llm".into()
                }),
                "the switch follows the kill in the same tick\n{}",
                dump()
            );
            assert!(killed_at.is_none(), "killed twice\n{}", dump());
            killed_at = Some(now);
        }
        if published.allowed.models.contains(&ModelId::from("llm")) {
            assert!(!allowed_has(&published, "mineru"), "{}", dump());
            llm_allowed_at.get_or_insert(now);
        }
        let mut next = p.allowed.borrow().clone();
        world.step(next.take());
        // Pods that are gone take their in-flight work with them.
        if world.pods["mineru"].0 == 0 {
            world.queues.get_mut("paperless-ocr").unwrap().in_flight = 0;
        }
    }
    let dump = log.join("\n");
    let lease = lease.unwrap();
    assert!(matches!(lease.kind, LeaseKind::ManualModel { .. }));
    let paused = paused_at.unwrap_or_else(|| panic!("intake never paused\n{dump}"));
    let killed = killed_at.unwrap_or_else(|| panic!("never killed\n{dump}"));
    assert_eq!(killed - paused, drain_timeout, "{dump}");
    let llm = llm_allowed_at.unwrap_or_else(|| panic!("llm never allowed\n{dump}"));
    // Allowed in the tick of the kill: nothing waits for mineru's pods to go; Kubernetes
    // keeps llm pending until they have.
    assert_eq!(llm, killed, "{dump}");
    assert!(world.pods["llm"].1 > 0, "llm never ran\n{dump}");
    assert!(
        matches!(mode(&store.load()), ModePhase::Serving { ref model, .. } if model.as_str() == "llm"),
        "{dump}"
    );
    // OCR work is still queued, but the lease keeps the LLM.
    assert!(world.queues["paperless-ocr"].ready > 0);
}

/// The user's report from M3 P1: with power steps unexecuted, a released manual LLM lease
/// led to a power-off nobody carried out and the GPU stayed "shutting down" with nothing
/// allowed. Under `power.control: manual` (the example config) no power step is ever
/// emitted, the host stays on as observed, and new work is scheduled again.
#[test]
fn a_released_manual_lease_never_leads_to_a_power_off_under_manual_control() {
    let mut store = Store::default();
    let mut world = World::new();
    let p = start(T0);
    let mut released_at = None;
    let mut log = Vec::new();
    for t in 0..200usize {
        let now = T0 + t as u64 * TICK;
        p.clock.set(Millis(now));
        if t == 1 {
            let mut d = store.load();
            let mut book = LeaseBook {
                next: 0,
                leases: d.leases.clone(),
            };
            let config = p.shared.config.current();
            book.acquire_manual_model(&config, &"llm".into(), Millis(now))
                .unwrap();
            d.leases = book.leases;
            store.save(&d);
        }
        if t == 20 {
            let mut d = store.load();
            d.leases.clear();
            store.save(&d);
            released_at = Some(now);
        }
        if t == 150 {
            // New OCR work long after the release and the idle grace.
            world.queues.get_mut("paperless-ocr").unwrap().ready = 5;
        }
        push_pods(&p, &world);
        push_rest(&p, &world);
        let r = tick(&p, &mut store, now);
        let published = p.allowed.borrow().clone().unwrap();
        log.push(format!(
            "t={t} pods={:?} power={:?} mode={:?} allowed={:?} steps={:?}",
            world.pods,
            store.load().state.resource(&GPU.into()).map(|r| r.power),
            mode(&store.load()),
            gpu_models(&published),
            r.steps
        ));
        assert!(
            !r.steps
                .iter()
                .any(|s| matches!(s, Step::PowerOff { .. } | Step::PowerOn { .. })),
            "{}",
            log.join("\n")
        );
        let gpu = store.load().state.resource(&GPU.into()).cloned().unwrap();
        assert!(gpu.is_on() && !gpu.managed, "{}", log.join("\n"));
        if t > 170 {
            assert!(
                published.allowed.models.contains(&ModelId::from("mineru")),
                "{}",
                log.join("\n")
            );
        }
        world.step(p.allowed.borrow().clone());
    }
    assert!(released_at.is_some());
}

/// The user's report from M3 P1: `interactive-llm` (source `llm-sessions`, kind session, no
/// observer, and nothing can push to it) kept the LLM loaded forever as "unknown demand".
/// A source that can never report and never has now counts as no demand, so the LLM is
/// released when its own work is done and other work gets the GPU. Nothing pushes to the
/// schedule and session sources here, as in the cluster.
#[test]
fn demand_that_can_never_be_observed_does_not_keep_a_model_loaded() {
    let mut store = Store::default();
    let mut world = World::new();
    world.queues.get_mut("paperless-tagging").unwrap().ready = 2;
    let p = start(T0);
    let mut log = Vec::new();
    let mut mineru_at = None;
    for t in 0..120usize {
        let now = T0 + t as u64 * TICK;
        p.clock.set(Millis(now));
        if t == 10 {
            world.queues.get_mut("paperless-ocr").unwrap().ready = 3;
        }
        push_pods(&p, &world);
        push_rest_except(&p, &world, &["backup-schedule", "llm-sessions"]);
        let r = tick(&p, &mut store, now);
        assert_eq!(r.error, None);
        assert!(r.unknown_demand.is_empty(), "t={t}: {r:?}");
        assert!(
            r.unsupported_demand
                .contains(&ClassId::from("interactive-llm")),
            "{r:?}"
        );
        let published = p.allowed.borrow().clone().unwrap();
        log.push(format!(
            "t={t} pods={:?} mode={:?} allowed={:?} steps={:?}",
            world.pods,
            mode(&store.load()),
            gpu_models(&published),
            r.steps
        ));
        assert!(gpu_models(&published).len() <= 1);
        if published.allowed.models.contains(&ModelId::from("mineru")) {
            mineru_at.get_or_insert(t);
        }
        world.step(p.allowed.borrow().clone());
    }
    let dump = log.join("\n");
    // The LLM served tagging first, then gave way to OCR.
    assert!(log[2].contains("\"llm\""), "{dump}");
    assert!(mineru_at.is_some(), "the LLM was never released\n{dump}");
    assert_eq!(world.queues["paperless-ocr"].ready, 0, "{dump}");
}
