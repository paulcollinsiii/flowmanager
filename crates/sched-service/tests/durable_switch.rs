//! Multi-tick runs of the stateful scheduler: the sequencer state is persisted between
//! ticks the way the `Scheduler` object keeps it in Restate (as JSON), and the process is
//! restarted (new `Shared`, empty observation cache, state read back) in the middle of a
//! switch. A small world plays KEDA: it scales models from the published allowed set only,
//! and takes a few ticks to bring pods up or down.
//!
//! Checked at every tick, from the world and from what the test pushed, not from the
//! scheduler's own state: at most one model of the exclusive resource is allowed, and the
//! second model is never allowed while the first still has pods or while the newest zero
//! reading of the first model the running process received is older than `safety_max_age`.

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
const CLASSES: [(&str, &str); 7] = [
    ("paperless-ocr", "paperless-ocr-pending"),
    ("paperless-tagging", "paperless-post-ocr"),
    ("speakr-transcription", "speakr-pending"),
    ("speakr-summarization", "speakr-summaries"),
    ("immich-ml", "immich-queues"),
    ("backup", "backup-schedule"),
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

/// The cluster and the services, as KEDA and the workloads would make them behave.
struct World {
    /// (pods, ready pods) per model.
    pods: BTreeMap<&'static str, (u32, u32)>,
    /// Ticks left until a model that lost its allowance has no pods.
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

    /// One tick of the world: KEDA converges on the allowed set it has, work proceeds.
    fn step(&mut self, published: Option<PublishedAllowed>) {
        if published.is_some() {
            self.allowed = published;
        }
        for m in MODELS {
            let (p, r) = self.pods[m];
            let next = if self.model_allowed(m) {
                self.terminating.remove(m);
                match (p, r) {
                    (0, _) => (1, 0),
                    _ => (1, 1),
                }
            } else if p > 0 {
                let left = self.terminating.entry(m).or_insert(3);
                *left -= 1;
                if *left == 0 {
                    self.terminating.remove(m);
                    (0, 0)
                } else {
                    (p, 0)
                }
            } else {
                (0, 0)
            };
            self.pods.insert(m, next);
        }
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
    push(
        p,
        "basestar-probe",
        vec![ObservedValue::ResourceUp {
            resource: "basestar-nfs".into(),
            up: true,
        }],
    );
    for (c, s) in CLASSES {
        let q = w.queues[c];
        push(
            p,
            s,
            vec![ObservedValue::Demand {
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
    let outcome = p.shared.decide(Millis(now), &d);
    d.apply(&outcome);
    store.save(&d);
    p.shared.publish(&outcome);
    outcome.report
}

#[test]
fn a_switch_resumes_safely_after_a_restart_in_the_middle_of_draining() {
    let mut store = Store::default();
    let mut world = World::new();
    world.queues.get_mut("paperless-ocr").unwrap().ready = 3;
    let mut p = start(T0);
    let mut log: Vec<String> = Vec::new();
    // When the running process last received a zero pod count for mineru.
    let mut mineru_zero_seen: Option<u64> = None;
    let mut restarted_at: Option<usize> = None;
    // The tick of the first zero reading of mineru after the restart. After it the
    // cluster watch stalls for longer than safety_max_age, and the demand sources (silent
    // since the restart) come back while that zero reading is already stale.
    let mut zero_at: Option<usize> = None;
    let mut saw_draining_after_restart = false;
    let mut whisperx_allowed_at: Option<usize> = None;
    let mut mineru_served = false;
    let mut stale_holds = 0;

    for t in 0..80usize {
        let now = T0 + t as u64 * TICK;
        if t == 8 {
            // Transcription work arrives while OCR is being served.
            world.queues.get_mut("speakr-transcription").unwrap().ready = 2;
        }
        // Restart in the middle of draining mineru, while it still has pods.
        if restarted_at.is_none()
            && matches!(mode(&store.load()), ModePhase::Draining { .. })
            && world.pods["mineru"].0 > 0
        {
            restarted_at = Some(t);
            p = start(now);
            mineru_zero_seen = None;
            assert_eq!(
                *p.allowed.borrow(),
                None,
                "nothing published by a new process"
            );
            log.push(format!("t={t}: RESTART"));
        }
        p.clock.set(Millis(now));
        let first_after_restart = restarted_at == Some(t);
        let stalled = zero_at.is_some_and(|z| t > z && t < z + 6);
        let demand_silent = restarted_at.is_some() && zero_at.is_none_or(|z| t < z + 4);
        if !first_after_restart && !stalled {
            push_pods(&p, &world);
            if world.pods["mineru"].0 == 0 {
                mineru_zero_seen = Some(now);
                if restarted_at.is_some() && zero_at.is_none() {
                    zero_at = Some(t);
                }
            }
        }
        if !demand_silent {
            push_rest(&p, &world);
        }

        let r = tick(&p, &mut store, now);
        assert_eq!(r.error, None, "t={t}: {r:?}");
        let published = p.allowed.borrow().clone().expect("published after a tick");
        assert_eq!(published.at, r.at);
        let d = store.load();
        log.push(format!(
            "t={t} pods={:?} mode={:?} allowed={:?} steps={:?}",
            world.pods,
            mode(&d),
            gpu_models(&published),
            r.steps
        ));
        let dump = || log.join("\n");

        // 1. At most one model of the exclusive resource.
        assert!(gpu_models(&published).len() <= 1, "t={t}\n{}", dump());
        // 2. whisperx only once mineru has no pods and that reading is fresh.
        if published
            .allowed
            .models
            .contains(&ModelId::from("whisperx"))
        {
            assert_eq!(world.pods["mineru"].0, 0, "t={t}\n{}", dump());
            let fresh = mineru_zero_seen.is_some_and(|at| now - at <= SAFETY_MAX_AGE);
            assert!(fresh, "t={t}: zero reading stale or missing\n{}", dump());
            whisperx_allowed_at.get_or_insert(t);
        }
        if published.allowed.models.contains(&ModelId::from("mineru")) && world.pods["mineru"].1 > 0
        {
            mineru_served = true;
        }
        if restarted_at.is_some_and(|at| t >= at) && matches!(mode(&d), ModePhase::Draining { .. })
        {
            saw_draining_after_restart = true;
        }
        if first_after_restart {
            // Nothing observed yet in this process: the persisted drain holds and nothing
            // new is allowed.
            assert!(published.allowed.models.is_empty(), "{}", dump());
            assert!(
                matches!(mode(&d), ModePhase::Draining { ref model, .. } if model.as_str() == "mineru"),
                "{}",
                dump()
            );
        }
        // Demand is back and wants whisperx, but the zero reading is stale: hold.
        if stalled && !demand_silent {
            assert!(mineru_zero_seen.is_some_and(|at| now - at > SAFETY_MAX_AGE));
            assert!(
                r.steps.contains(&Step::Wait {
                    resource: GPU.into(),
                    reason: sched_model::WaitReason::StaleObservation
                }),
                "t={t}\n{}",
                dump()
            );
            stale_holds += 1;
        }

        world.step(p.allowed.borrow().clone());
    }
    let dump = log.join("\n");
    assert!(mineru_served, "{dump}");
    let restarted_at = restarted_at.unwrap_or_else(|| panic!("never drained\n{dump}"));
    assert!(saw_draining_after_restart, "{dump}");
    let zero_at = zero_at.unwrap_or_else(|| panic!("no zero reading after the restart\n{dump}"));
    assert_eq!(stale_holds, 2, "{dump}");
    let allowed_at =
        whisperx_allowed_at.unwrap_or_else(|| panic!("whisperx never allowed\n{dump}"));
    assert!(allowed_at > restarted_at, "{dump}");
    // whisperx waited for the stalled watch to report again.
    assert_eq!(allowed_at, zero_at + 6, "{dump}");
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
                }),
                "the drain follows the kill in the same tick\n{}",
                dump()
            );
            assert!(killed_at.is_none(), "killed twice\n{}", dump());
            killed_at = Some(now);
        }
        if published.allowed.models.contains(&ModelId::from("llm")) {
            assert_eq!(world.pods["mineru"].0, 0, "{}", dump());
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
    assert!(llm > killed, "{dump}");
    assert!(
        matches!(mode(&store.load()), ModePhase::Serving { ref model, .. } if model.as_str() == "llm"),
        "{dump}"
    );
    // OCR work is still queued, but the lease keeps the LLM.
    assert!(world.queues["paperless-ocr"].ready > 0);
}

/// Known gap, pinned down here so that it cannot change unnoticed: nothing executes
/// `PowerOff`, but the sequencer records the host as shutting down. A host that stays up
/// is not read back as on (only a host that went down moves on, to off), so the GPU stays
/// "shutting down" and no model is allowed, whatever the demand, until the host is seen
/// down. The scheduler takes power-off responsibility for a host once a manual model lease
/// asked for it to be up. This needs every class on the host to have known demand (with
/// the example config `llm-sessions` has no observer, so its demand is unknown and blocks
/// the power-off); this test pushes all of them.
#[test]
fn known_gap_an_unexecuted_power_off_leaves_a_running_gpu_shutting_down() {
    let mut store = Store::default();
    let mut world = World::new();
    let p = start(T0);
    let idle_grace = 10 * 60_000;
    let mut released_at = None;
    let mut power_off_at = None;
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
            // New OCR work long after the power-off.
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
        let off = Step::PowerOff {
            resource: GPU.into(),
        };
        if r.steps.contains(&off) {
            assert!(r.not_executed.contains(&off));
            power_off_at.get_or_insert(now);
        }
        if t > 150 {
            assert!(
                !published.allowed.models.contains(&ModelId::from("mineru")),
                "{}",
                log.join("\n")
            );
            assert!(r.steps.contains(&Step::Wait {
                resource: GPU.into(),
                reason: sched_model::WaitReason::ShuttingDown
            }));
        }
        world.step(p.allowed.borrow().clone());
    }
    let dump = log.join("\n");
    let off = power_off_at.unwrap_or_else(|| panic!("no power-off\n{dump}"));
    assert!(off >= released_at.unwrap() + idle_grace, "{dump}");
    assert!(
        matches!(
            store.load().state.resource(&GPU.into()).unwrap().power,
            sched_model::PowerPhase::ShuttingDown { .. }
        ),
        "{dump}"
    );
}
