//! Flapping bounds over long, oscillating arrival streams. A small reactive world feeds the
//! decision loop for hours of simulated time; the assertions are written from the policy
//! (minimum dwell from the tick a model is allowed, idle grace) and read only the recorded
//! log, not the sequencer's state. The world plays Kubernetes: a model's pod becomes ready
//! only once no other model's pod holds the GPU.

use std::collections::BTreeMap;

use proptest::prelude::*;
use sched_model::test_support::example_config_managed as example_config;
// The power machine is exercised as under managed control (M4).
use sched_model::{
    AllowedSet, ClassId, Config, Fidelity, Millis, ModelId, Observation, ObservedValue, ResourceId,
    SchedState, Snapshot, Step,
};
use sched_service::core::decide;
use sched_service::invariants;

const START: u64 = 50_000_000;
const TICK: u64 = 30_000;
const TICKS: usize = 800;

const CLASSES: [(&str, &str); 7] = [
    ("paperless-ocr", "paperless-ocr-pending"),
    ("paperless-tagging", "paperless-post-ocr"),
    ("speakr-transcription", "speakr-pending"),
    ("speakr-summarization", "speakr-summaries"),
    ("immich-ml", "immich-queues"),
    ("backup", "backup-schedule"),
    ("interactive-llm", "llm-sessions"),
];
const MODELS: [&str; 4] = ["mineru", "llm", "whisperx", "immich-ml"];

#[derive(Clone, Default)]
struct Queue {
    ready: u64,
    in_flight: u64,
    oldest: Option<u64>,
}

struct Tick {
    now: u64,
    steps: Vec<Step>,
    queues: BTreeMap<&'static str, Queue>,
    pods: BTreeMap<&'static str, (u32, u32)>,
    nfs_up: bool,
}

/// `bytes` drive arrivals and completions; `periods` set how fast each class oscillates
/// between busy and quiet.
fn simulate(config: &Config, bytes: &[u8], periods: &[usize; 7]) -> Vec<Tick> {
    let mut queues: BTreeMap<&'static str, Queue> = CLASSES
        .iter()
        .map(|(c, _)| (*c, Queue::default()))
        .collect();
    let mut pods: BTreeMap<&'static str, (u32, u32)> =
        MODELS.iter().map(|m| (*m, (0, 0))).collect();
    // (due tick, model, pods) and (due tick, nfs up)
    let mut pod_events: Vec<(usize, &'static str, (u32, u32))> = vec![];
    let mut nfs_events: Vec<(usize, bool)> = vec![];
    let mut nfs_up = false;
    let mut allowed = AllowedSet::default();
    let mut state = SchedState::default();
    let mut log = Vec::new();
    let mut r = bytes.iter().cycle();
    let mut rnd = move || *r.next().unwrap();

    for t in 0..TICKS {
        let now = START + t as u64 * TICK;
        // Kubernetes: a model's pod becomes ready only once no other model's pod holds the
        // GPU (one nvidia.com/gpu); until then it stays pending.
        let mut due: Vec<_> = pod_events.iter().filter(|e| e.0 <= t).cloned().collect();
        pod_events.retain(|e| e.0 > t);
        due.sort_by_key(|e| e.2);
        for (d, m, v) in due {
            let gpu_busy = MODELS.iter().any(|o| *o != m && pods[o].0 > 0);
            if v.1 > 0 && gpu_busy {
                pod_events.push((d, m, v));
            } else {
                pods.insert(m, v);
            }
        }
        nfs_events.retain(|(due, up)| {
            if *due <= t {
                nfs_up = *up;
                false
            } else {
                true
            }
        });
        for (i, (c, _)) in CLASSES.iter().enumerate() {
            let q = queues.get_mut(c).unwrap();
            let active = periods[i] > 0 && (t / periods[i]).is_multiple_of(2);
            if active && rnd() < 80 {
                q.ready += 1;
                q.oldest.get_or_insert(now);
            }
            let runs = allowed.classes.contains(&ClassId::new(*c));
            if q.in_flight > 0 && rnd() < 140 {
                q.in_flight -= 1;
            }
            if runs && q.ready > 0 && q.in_flight == 0 && rnd() < 200 {
                q.ready -= 1;
                q.in_flight += 1;
                if q.ready == 0 {
                    q.oldest = None;
                }
            }
        }

        let mut observations = Vec::new();
        for m in MODELS {
            let (p, rd) = pods[m];
            observations.push(obs(
                "cluster-pods",
                now,
                ObservedValue::ModelPods {
                    model: ModelId::new(m),
                    pods: p,
                    ready_pods: rd,
                },
            ));
        }
        observations.push(obs(
            "cluster-pods",
            now,
            ObservedValue::ResourceUp {
                resource: ResourceId::new("cuda-central"),
                up: true,
            },
        ));
        observations.push(obs(
            "cluster-pods",
            now,
            ObservedValue::ResourceUp {
                resource: ResourceId::new("basestar-nfs"),
                up: nfs_up,
            },
        ));
        for (c, s) in CLASSES {
            let q = &queues[c];
            observations.push(obs(
                s,
                now,
                ObservedValue::Demand {
                    class: ClassId::new(c),
                    ready: q.ready,
                    in_flight: q.in_flight,
                    oldest_ready_at: q.oldest.map(Millis),
                },
            ));
        }
        let snap = Snapshot {
            config_generation: 0,
            now: Millis(now),
            observations,
            leases: vec![],
            unobservable: Default::default(),
        };
        let (intent, decision) = decide(config, &snap, &state).unwrap();
        let bad = invariants::check(config, &snap, &state, &intent, &decision);
        assert!(bad.is_empty(), "tick {t}: {bad:?}");

        for s in &decision.steps {
            match s {
                Step::AllowModel { model, .. } => {
                    let m = MODELS.iter().find(|x| **x == model.as_str()).unwrap();
                    pods.insert(m, (1, 0));
                    pod_events.push((t + 2, m, (1, 1)));
                }
                Step::DrainModel { model, .. } => {
                    let m = MODELS.iter().find(|x| **x == model.as_str()).unwrap();
                    pod_events.retain(|e| e.1 != *m);
                    pod_events.push((t + 2, m, (0, 0)));
                }
                Step::PowerOn { resource } if resource.as_str() == "basestar-nfs" => {
                    nfs_events.push((t + 1, true))
                }
                Step::PowerOff { resource } if resource.as_str() == "basestar-nfs" => {
                    nfs_events.push((t + 1, false))
                }
                _ => {}
            }
        }
        let ready = MODELS.iter().filter(|m| pods[*m].1 > 0).count();
        assert!(ready <= 1, "tick {t}: the simulated GPU ran {ready} models");
        log.push(Tick {
            now,
            steps: decision.steps.clone(),
            queues: queues.clone(),
            pods: pods.clone(),
            nfs_up,
        });
        allowed = decision.allowed;
        state = decision.state;
    }
    log
}

fn obs(source: &str, at: u64, value: ObservedValue) -> Observation {
    Observation {
        source: source.into(),
        observed_at: Millis(at),
        fidelity: Fidelity::Event,
        value,
    }
}

fn model_busy(config: &Config, tick: &Tick, model: &str) -> bool {
    let id = ModelId::new(model);
    config
        .classes()
        .iter()
        .filter(|c| c.requires.iter().any(|r| r.model.as_ref() == Some(&id)))
        .any(|c| {
            let q = &tick.queues[c.id.as_str()];
            q.ready > 0 || q.in_flight > 0
        })
}

/// The model a class needs, if any.
fn model_of<'a>(config: &'a Config, class: &ClassId) -> Option<&'a str> {
    config
        .class(class)?
        .requires
        .iter()
        .find_map(|r| r.model.as_ref())
        .map(|m| m.as_str())
}

fn nfs_busy(config: &Config, tick: &Tick) -> bool {
    config
        .classes()
        .iter()
        .filter(|c| {
            c.demand.is_some()
                && c.requires
                    .iter()
                    .any(|r| r.resource.as_str() == "basestar-nfs")
        })
        .any(|c| {
            let q = &tick.queues[c.id.as_str()];
            q.ready > 0 || q.in_flight > 0
        })
}

#[derive(Default, Debug)]
struct Summary {
    allows: usize,
    drains: usize,
    power_offs: usize,
    /// Ticks in which a newly allowed model's pod was pending behind the previous model's.
    pending_behind: usize,
}

/// Checks the bounds against the log, independently of how the decisions were reached.
fn check_bounds(config: &Config, log: &[Tick]) -> Summary {
    let min_dwell = config.policy().min_dwell.as_u64();
    let grace = config.policy().idle_grace.as_u64();
    let max_dwell = config.policy().max_dwell.as_u64();
    let age_threshold = config.policy().age_threshold.as_u64();
    let mut sum = Summary::default();
    let mut last_allow: Option<u64> = None;
    // Per model: when it was allowed.
    let mut allowed_at: BTreeMap<&str, u64> = BTreeMap::new();
    let mut last_busy: BTreeMap<&str, u64> = BTreeMap::new();
    let mut wind_start: BTreeMap<&str, u64> = BTreeMap::new();
    let mut nfs_last_busy: Option<u64> = None;
    let mut nfs_up_since: Option<u64> = None;

    for tick in log {
        for m in MODELS {
            if model_busy(config, tick, m) {
                last_busy.insert(m, tick.now);
            }
        }
        let pending = MODELS.iter().any(|m| tick.pods[m] == (1, 0));
        let holding = MODELS.iter().filter(|m| tick.pods[*m].0 > 0).count() > 1;
        if pending && holding {
            sum.pending_behind += 1;
        }
        if nfs_busy(config, tick) {
            nfs_last_busy = Some(tick.now);
        }
        if tick.nfs_up && nfs_up_since.is_none() {
            nfs_up_since = Some(tick.now);
        }
        for step in &tick.steps {
            match step {
                Step::AllowModel { model, .. } => {
                    sum.allows += 1;
                    if let Some(prev) = last_allow {
                        assert!(
                            tick.now - prev >= min_dwell,
                            "two models allowed {}ms apart (min_dwell {min_dwell}): flapping",
                            tick.now - prev
                        );
                    }
                    last_allow = Some(tick.now);
                    allowed_at.insert(model.as_str(), tick.now);
                }
                Step::PauseIntake { class } => {
                    // The first stop of intake for a model's classes marks the moment the
                    // scheduler decided to leave it.
                    let Some(m) = model_of(config, class) else {
                        continue;
                    };
                    if wind_start.contains_key(m) {
                        continue;
                    }
                    wind_start.insert(m, tick.now);
                    // Dwell is counted from the tick the model was allowed (load included).
                    let ready = allowed_at.get(m).copied();
                    if model_busy(config, tick, m) {
                        // A yield. It needs a reason: max_dwell passed, or another queue starved.
                        let served = ready.map(|r| tick.now - r).unwrap_or(0);
                        let starved = config.classes().iter().any(|c| {
                            let q = &tick.queues[c.id.as_str()];
                            model_of(config, &c.id).is_some_and(|other| other != m)
                                && q.ready > 0
                                && q.oldest.is_some_and(|o| tick.now - o > age_threshold)
                        });
                        assert!(
                            served > max_dwell || starved,
                            "{m} yielded after {served}ms with no starved queue (max_dwell {max_dwell})"
                        );
                    } else {
                        // An idle release needs the grace period.
                        let since = last_busy
                            .get(m)
                            .copied()
                            .unwrap_or(0)
                            .max(ready.unwrap_or(0));
                        assert!(
                            tick.now - since >= grace,
                            "{m} released {}ms after it was last busy or ready (idle_grace {grace})",
                            tick.now - since
                        );
                    }
                }
                Step::DrainModel { model, .. } => {
                    sum.drains += 1;
                    let m = model.as_str();
                    if let Some(at) = allowed_at.get(m) {
                        assert!(
                            tick.now - at >= min_dwell,
                            "{m} withdrawn {}ms after it was allowed (min_dwell {min_dwell})",
                            tick.now - at
                        );
                    }
                    allowed_at.remove(m);
                    wind_start.remove(m);
                }
                Step::PowerOff { resource } if resource.as_str() == "basestar-nfs" => {
                    sum.power_offs += 1;
                    let since = nfs_last_busy.unwrap_or(0).max(nfs_up_since.unwrap_or(0));
                    assert!(
                        tick.now - since >= grace,
                        "nfs powered off {}ms after it was last needed or came up (idle_grace {grace})",
                        tick.now - since
                    );
                    nfs_up_since = None;
                }
                _ => {}
            }
        }
    }
    sum
}

fn periods() -> impl Strategy<Value = [usize; 7]> {
    prop::array::uniform7(2usize..40)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(40))]

    #[test]
    fn oscillating_arrivals_never_flap(bytes in prop::collection::vec(any::<u8>(), 256..2048), p in periods()) {
        let config = example_config();
        let log = simulate(&config, &bytes, &p);
        check_bounds(&config, &log);
    }
}

#[test]
fn the_simulated_world_exercises_switching_and_power() {
    // A fixed stream so the bounds above are known not to hold vacuously.
    let config = example_config();
    let bytes: Vec<u8> = (0..1024u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
        .collect();
    let log = simulate(&config, &bytes, &[7, 11, 5, 13, 17, 23, 29]);
    let sum = check_bounds(&config, &log);
    assert!(sum.allows >= 4, "{sum:?}");
    assert!(sum.drains >= 3, "{sum:?}");
    assert!(sum.pending_behind >= 1, "no switch overlapped in Kubernetes: {sum:?}");
}

#[test]
fn a_lone_backup_cycles_nfs_without_flapping() {
    let config = example_config();
    let bytes: Vec<u8> = (0..512u32)
        .map(|i| (i.wrapping_mul(1103515245).wrapping_add(12345) >> 16) as u8)
        .collect();
    let log = simulate(&config, &bytes, &[0, 0, 0, 0, 0, 90, 0]);
    let sum = check_bounds(&config, &log);
    assert!(sum.power_offs >= 2, "{sum:?}");
    let ons = log
        .iter()
        .flat_map(|t| &t.steps)
        .filter(|s| matches!(s, Step::PowerOn { .. }))
        .count();
    assert!(
        ons >= 2 && ons <= sum.power_offs + 1,
        "{ons} power-ons, {} power-offs",
        sum.power_offs
    );
}
