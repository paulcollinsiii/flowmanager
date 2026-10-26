//! A busy model whose queue reads empty for one tick every few minutes while another
//! model's work has waited past `age_threshold`: the planner wants to switch on every busy
//! tick and drops that wish on every blip. Idleness must be counted from consecutive idle
//! ticks only. A value left from an earlier blip once made the model look idle for as long
//! as that blip was ago, so a few blips added up to `idle_grace` and forced a switch the
//! queues never justified.
//!
//! This also records a known limit (README, *How a model is chosen and kept*): while such
//! blips keep dropping the wind-down, its clock restarts and the switch is postponed, so
//! the documented end-to-end bound holds only while the planner's intent is stable.

use sched_model::test_support::example_config;
use sched_model::{
    ClassId, Config, Fidelity, Fixture, Millis, ModelId, Observation, ObservedValue, ResourceId,
    SchedState, Snapshot, Step,
};
use sched_service::core::decide;

const START: u64 = 50_000_000;
const TICK: u64 = 30_000;
const MIN: u64 = 60_000;

const CLASSES: [(&str, &str); 7] = [
    ("paperless-ocr", "paperless-ocr-pending"),
    ("paperless-tagging", "paperless-post-ocr"),
    ("speakr-transcription", "speakr-pending"),
    ("speakr-summarization", "speakr-summaries"),
    ("immich-ml", "immich-queues"),
    ("backup", "backup-schedule"),
    ("interactive-llm", "llm-sessions"),
];

fn obs(source: &str, at: u64, value: ObservedValue) -> Observation {
    Observation {
        source: source.into(),
        observed_at: Millis(at),
        fidelity: Fidelity::Event,
        value,
    }
}

/// mineru serves from START with its queue always full, except for one tick every
/// `blip_every` minutes (none if 0); whisperx has had work waiting since START. Returns the
/// minutes after START of each tick that withdrew mineru, over `hours` hours.
fn switches(config: &Config, blip_every: u64, hours: u64) -> Vec<u64> {
    let y = format!(
        "description: t\nsnapshot: {{ now: 0 }}\nstate:\n  resources:\n    cuda-central: {{ power: {{ phase: \"on\", since: 0 }}, mode: {{ phase: serving, model: mineru, since: {START} }} }}\n    basestar-nfs: {{ power: {{ phase: \"on\", since: 0 }}, mode: {{ phase: idle }} }}\n"
    );
    let mut state: SchedState = Fixture::from_yaml(&y).unwrap().state;
    let mut out = Vec::new();
    for t in 1..=(hours * 120) {
        let now = START + t * TICK;
        let blip = blip_every > 0 && (now - START).is_multiple_of(blip_every * MIN);
        let mut observations = vec![
            obs(
                "cluster-pods",
                now,
                ObservedValue::ResourceUp {
                    resource: ResourceId::new("cuda-central"),
                    up: true,
                },
            ),
            obs(
                "cluster-pods",
                now,
                ObservedValue::ResourceUp {
                    resource: ResourceId::new("basestar-nfs"),
                    up: true,
                },
            ),
        ];
        for (class, source) in CLASSES {
            let (ready, in_flight, oldest) = match class {
                "paperless-ocr" if !blip => (3, 1, Some(now)),
                "speakr-transcription" => (2, 0, Some(START)),
                _ => (0, 0, None),
            };
            observations.push(obs(
                source,
                now,
                ObservedValue::Demand {
                    class: ClassId::new(class),
                    ready,
                    in_flight,
                    failed: 0,
                    oldest_ready_at: oldest.map(Millis),
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
        let (_, decision) = decide(config, &snap, &state).unwrap();
        if decision.steps.iter().any(
            |s| matches!(s, Step::DrainModel { model, .. } if *model == ModelId::new("mineru")),
        ) {
            out.push(t * TICK / MIN);
        }
        state = decision.state;
    }
    out
}

#[test]
fn blips_do_not_add_up_to_the_idle_grace() {
    let config = example_config();
    // Control: with no blips, whisperx's work (older than age_threshold, 30m) ends mineru's
    // turn at the drain timeout after the wind-down starts (30m + 10m).
    let steady = switches(&config, 0, 3);
    assert_eq!(steady.first(), Some(&40), "{steady:?}");
    // Blips further apart than drain_timeout leave room for the wind-down to finish: it
    // starts after the blip at 60m... and the switch follows within the policy bounds.
    let sparse = switches(&config, 20, 3);
    assert!(
        sparse.first().is_some_and(|m| *m <= 60),
        "a blip every 20m must still switch: {sparse:?}"
    );
    // Blips closer than drain_timeout drop the wind-down each time (the documented limit):
    // the switch is postponed. It must not happen by way of the idle grace (10m): the model
    // is idle for one tick at a time, never for 10m.
    let frequent = switches(&config, 9, 3);
    assert!(frequent.is_empty(), "switched at minute {frequent:?}");
}
