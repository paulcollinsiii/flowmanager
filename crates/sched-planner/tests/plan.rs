//! Table-driven tests for the fifo-exhaustive strategy against the example config.

use sched_model::test_support::{EXAMPLE_CONFIG, example_config};
use sched_model::{Config, Fixture, Intent, ModelId, PowerWant, ResourceId};
use sched_planner::{PlanError, plan, strategy_names};

const T: u64 = 100_000_000;
const G: &str = "cuda-central";

fn demand(class: &str, source: &str, ready: u64, in_flight: u64, oldest: Option<u64>) -> String {
    let o = oldest
        .filter(|_| ready > 0)
        .map_or(String::new(), |t| format!(", oldest_ready_at: {t}"));
    format!(
        "    - {{ source: {source}, observed_at: {T}, fidelity: polled, value: {{ type: demand, class: {class}, ready: {ready}, in_flight: {in_flight}{o} }} }}\n"
    )
}

fn ocr(ready: u64, oldest: u64) -> String {
    demand(
        "paperless-ocr",
        "paperless-ocr-pending",
        ready,
        0,
        Some(oldest),
    )
}
fn tagging(ready: u64, oldest: u64) -> String {
    demand(
        "paperless-tagging",
        "paperless-post-ocr",
        ready,
        0,
        Some(oldest),
    )
}
fn transcription(ready: u64, oldest: u64) -> String {
    demand(
        "speakr-transcription",
        "speakr-pending",
        ready,
        0,
        Some(oldest),
    )
}
fn summarization(ready: u64, oldest: u64) -> String {
    demand(
        "speakr-summarization",
        "speakr-summaries",
        ready,
        0,
        Some(oldest),
    )
}
fn immich(ready: u64, oldest: u64) -> String {
    demand("immich-ml", "immich-queues", ready, 0, Some(oldest))
}
fn interactive(ready: u64, in_flight: u64) -> String {
    demand("interactive-llm", "llm-sessions", ready, in_flight, None)
}

/// Both hosts up; `state_extra` is extra YAML under `state.resources`.
fn fixture(obs: &[String], leases: &[&str], serving: Option<(&str, u64, Option<u64>)>) -> Fixture {
    let y = fixture_yaml(obs, leases, serving);
    Fixture::from_yaml_checked(&y, &example_config()).unwrap_or_else(|e| panic!("{e}\n{y}"))
}

fn fixture_yaml(
    obs: &[String],
    leases: &[&str],
    serving: Option<(&str, u64, Option<u64>)>,
) -> String {
    let mut y = format!("description: t\nsnapshot:\n  now: {T}\n  observations:\n");
    for o in obs {
        y.push_str(o);
    }
    y.push_str(&format!(
        "    - {{ source: cluster-pods, observed_at: {T}, fidelity: event, value: {{ type: resource_up, resource: {G}, up: true }} }}\n"
    ));
    y.push_str("  leases:\n");
    if leases.is_empty() {
        y.push_str("    []\n");
    }
    for l in leases {
        y.push_str(&format!("    - {l}\n"));
    }
    y.push_str("state:\n  resources:\n");
    y.push_str(
        "    basestar-nfs: { power: { phase: \"on\", since: 0 }, mode: { phase: idle } }
",
    );
    match serving {
        Some((model, since, idle)) => {
            let idle = idle.map_or(String::new(), |i| format!(", idle_since: {i}"));
            y.push_str(&format!(
                "    {G}: {{ power: {{ phase: \"on\", since: 0 }}, mode: {{ phase: serving, model: {model}, since: {since}{idle} }} }}\n"
            ));
        }
        None => y.push_str(&format!(
            "    {G}: {{ power: {{ phase: \"on\", since: 0 }}, mode: {{ phase: idle }} }}\n"
        )),
    }
    y
}

fn run(f: &Fixture) -> Intent {
    plan(&example_config(), &f.snapshot, &f.state).unwrap()
}

fn target(f: &Fixture) -> Option<String> {
    run(f).models[&ResourceId::new(G)]
        .as_ref()
        .map(|m| m.as_str().to_string())
}

const MIN: u64 = 60_000;

#[test]
fn selection_table() {
    // (name, observations, expected model)
    let cases: Vec<(&str, Vec<String>, Option<&str>)> = vec![
        ("nothing ready", vec![ocr(0, T)], None),
        (
            "higher priority tier wins over older work",
            vec![ocr(1, T - 20 * MIN), transcription(1, T - MIN)],
            Some("whisperx"),
        ),
        (
            "fifo within a tier: older tagging beats ocr",
            vec![ocr(1, T - 5 * MIN), tagging(1, T - 9 * MIN)],
            Some("llm"),
        ),
        (
            "fifo within a tier: older ocr beats tagging",
            vec![ocr(1, T - 9 * MIN), tagging(1, T - 5 * MIN)],
            Some("mineru"),
        ),
        (
            "equal age: lower load time (llm 90s < mineru 120s)",
            vec![ocr(1, T - 5 * MIN), tagging(1, T - 5 * MIN)],
            Some("llm"),
        ),
        (
            "model priority is its best ready class",
            vec![ocr(1, T - MIN), summarization(1, T - MIN)],
            Some("llm"),
        ),
        (
            "lower-priority class of a model does not raise it",
            vec![ocr(1, T - MIN), tagging(1, T - MIN), immich(1, T - 2 * MIN)],
            Some("llm"),
        ),
        (
            "starved low tier jumps the tier",
            vec![immich(1, T - 31 * MIN), transcription(1, T - 20 * MIN)],
            Some("immich-ml"),
        ),
        (
            "age exactly at threshold is not starved",
            vec![immich(1, T - 30 * MIN), transcription(1, T - 20 * MIN)],
            Some("whisperx"),
        ),
        (
            "fifo among starved queues",
            vec![immich(1, T - 40 * MIN), transcription(1, T - 50 * MIN)],
            Some("whisperx"),
        ),
        (
            "stale demand is ignored",
            vec![
                demand(
                    "speakr-transcription",
                    "speakr-pending",
                    5,
                    0,
                    Some(T - 12 * MIN),
                )
                .replace(
                    &format!("observed_at: {T}"),
                    &format!("observed_at: {}", T - 11 * MIN),
                ),
                ocr(1, T - MIN),
            ],
            Some("mineru"),
        ),
    ];
    for (name, obs, want) in cases {
        let f = fixture(&obs, &[], None);
        assert_eq!(target(&f).as_deref(), want, "{name}");
    }
}

#[test]
fn nfs_down_makes_classes_unrunnable() {
    let mut f = fixture(&[ocr(1, T - MIN)], &[], None);
    f.state
        .resources
        .get_mut(&"basestar-nfs".into())
        .unwrap()
        .power = sched_model::PowerPhase::Off;
    assert_eq!(target(&f), None);
}

#[test]
fn yield_table() {
    // (name, serving model, served ms, idle_since, observations, expected)
    let h = 60 * MIN;
    type Case<'a> = (
        &'a str,
        &'a str,
        u64,
        Option<u64>,
        Vec<String>,
        Option<&'a str>,
    );
    let cases: Vec<Case> = vec![
        (
            "exhaustive: keeps serving with other fresh work",
            "mineru",
            10 * MIN,
            None,
            vec![ocr(1, T - MIN), transcription(1, T - MIN)],
            Some("mineru"),
        ),
        (
            "starved other but min_dwell not reached",
            "mineru",
            4 * MIN,
            None,
            vec![ocr(1, T - MIN), transcription(1, T - 31 * MIN)],
            Some("mineru"),
        ),
        (
            "starved other after min_dwell yields",
            "mineru",
            6 * MIN,
            None,
            vec![ocr(1, T - MIN), transcription(1, T - 31 * MIN)],
            Some("whisperx"),
        ),
        (
            "max_dwell exceeded yields to best other",
            "mineru",
            2 * h + MIN,
            None,
            vec![
                ocr(1, T - MIN),
                tagging(1, T - MIN),
                transcription(1, T - MIN),
            ],
            Some("whisperx"),
        ),
        (
            "exactly max_dwell does not yield",
            "mineru",
            2 * h,
            None,
            vec![ocr(1, T - MIN), transcription(1, T - MIN)],
            Some("mineru"),
        ),
        (
            "max_dwell exceeded but no other ready work",
            "mineru",
            3 * h,
            None,
            vec![ocr(1, T - MIN)],
            Some("mineru"),
        ),
        (
            "unpreemptible busy class blocks yield",
            "llm",
            3 * h,
            None,
            vec![interactive(1, 0), immich(1, T - 40 * MIN)],
            Some("llm"),
        ),
        (
            "unpreemptible in-flight class blocks yield",
            "llm",
            3 * h,
            None,
            vec![interactive(0, 1), immich(1, T - 40 * MIN)],
            Some("llm"),
        ),
        (
            "idle but grace not elapsed",
            "mineru",
            20 * MIN,
            Some(T - 9 * MIN),
            vec![transcription(1, T - MIN)],
            Some("mineru"),
        ),
        (
            "idle past grace switches to ready work",
            "mineru",
            20 * MIN,
            Some(T - 10 * MIN),
            vec![transcription(1, T - MIN)],
            Some("whisperx"),
        ),
        (
            "idle past grace with nothing ready releases",
            "mineru",
            20 * MIN,
            Some(T - 10 * MIN),
            vec![],
            None,
        ),
        (
            "idle past grace but min_dwell not reached",
            "mineru",
            2 * MIN,
            Some(T - 10 * MIN),
            vec![],
            Some("mineru"),
        ),
        (
            "first idle observation counts from now",
            "mineru",
            20 * MIN,
            None,
            vec![],
            Some("mineru"),
        ),
        (
            "unknown demand of a class counts as busy",
            "mineru",
            20 * MIN,
            Some(T - 20 * MIN),
            vec![],
            None,
        ),
    ];
    for (name, model, served, idle, mut obs, want) in cases {
        // Explicit observations come first; classes not mentioned report no work.
        obs.push(ocr(0, T));
        obs.push(tagging(0, T));
        obs.push(summarization(0, T));
        obs.push(interactive(0, 0));
        let f = fixture(&dedupe(obs), &[], Some((model, T - served, idle)));
        assert_eq!(target(&f).as_deref(), want, "{name}");
    }
}

#[test]
fn unknown_demand_counts_as_busy() {
    // Idle past grace, but mineru's class has no observation at all.
    let f = fixture(&[], &[], Some(("mineru", T - 20 * MIN, Some(T - 20 * MIN))));
    assert_eq!(target(&f).as_deref(), Some("mineru"));
}

/// Keeps the first observation per class, so explicit cases override the zero defaults.
fn dedupe(obs: Vec<String>) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    obs.into_iter()
        .filter(|o| {
            let key = o
                .split("class: ")
                .nth(1)
                .map(|s| s.split(',').next().unwrap().to_string());
            key.is_none_or(|k| seen.insert(k))
        })
        .collect()
}

#[test]
fn leases_override_selection() {
    let work = [ocr(1, T - MIN), transcription(1, T - MIN)];
    let reclaim = format!("{{ id: r, resource: {G}, granted_at: 0, kind: {{ type: reclaim }} }}");
    let f = fixture(&work, &[&reclaim], None);
    let i = run(&f);
    assert_eq!(i.models[&ResourceId::new(G)], None);
    assert!(i.forced.contains(&ResourceId::new(G)));
    assert!(i.reclaim.contains(&ResourceId::new(G)));
    assert!(i.power_up.is_empty());

    let manual = format!(
        "{{ id: m, resource: {G}, granted_at: 5, kind: {{ type: manual_model, model: llm }} }}"
    );
    let f = fixture(&work, &[&manual], Some(("mineru", 0, None)));
    let i = run(&f);
    assert_eq!(i.models[&ResourceId::new(G)], Some(ModelId::new("llm")));
    assert!(i.forced.contains(&ResourceId::new(G)));
    assert_eq!(
        i.power_up.get(&ResourceId::new(G)),
        Some(&PowerWant::Manual)
    );

    let live = format!(
        "{{ id: i, resource: {G}, granted_at: 0, kind: {{ type: interactive, model: llm, expires_at: {} }} }}",
        T + 1
    );
    let f = fixture(&work, &[&live], Some(("mineru", 0, None)));
    let i = run(&f);
    assert_eq!(i.models[&ResourceId::new(G)], Some(ModelId::new("llm")));
    assert!(!i.forced.contains(&ResourceId::new(G)));
    assert!(
        i.power_up.is_empty(),
        "interactive leases never power a host on"
    );

    let expired = format!(
        "{{ id: i, resource: {G}, granted_at: 0, kind: {{ type: interactive, model: llm, expires_at: {T} }} }}"
    );
    let f = fixture(&work, &[&expired], None);
    assert_eq!(target(&f).as_deref(), Some("whisperx"));

    let later = format!(
        "{{ id: z, resource: {G}, granted_at: 9, kind: {{ type: manual_model, model: whisperx }} }}"
    );
    let y = fixture_yaml(&work, &[&manual, &later], None);
    let f = Fixture::from_yaml(&y).unwrap();
    assert_eq!(
        target(&f).as_deref(),
        Some("whisperx"),
        "the most recent manual request wins"
    );
}

#[test]
fn power_wants() {
    let nfs = ResourceId::new("basestar-nfs");
    let host = "{ id: h, resource: basestar-nfs, granted_at: 0, kind: { type: manual_host } }";
    let f = fixture(&[], &[host], None);
    assert_eq!(run(&f).power_up.get(&nfs), Some(&PowerWant::Manual));

    let backup = demand("backup", "backup-schedule", 1, 0, None);
    let f = fixture(std::slice::from_ref(&backup), &[], None);
    assert_eq!(run(&f).power_up.get(&nfs), Some(&PowerWant::Workload));

    let f = fixture(&[backup], &[host], None);
    assert_eq!(
        run(&f).power_up.get(&nfs),
        Some(&PowerWant::Manual),
        "manual wins over workload"
    );

    let idle_backup = demand("backup", "backup-schedule", 0, 0, None);
    let f = fixture(&[idle_backup], &[], None);
    assert!(run(&f).power_up.is_empty());

    // Gpu demand alone never powers anything on in v1.
    let f = fixture(&[ocr(5, T - MIN)], &[], None);
    assert!(run(&f).power_up.is_empty());
}

#[test]
fn is_deterministic() {
    let f = fixture(
        &[
            ocr(1, T - MIN),
            transcription(1, T - MIN),
            tagging(1, T - MIN),
        ],
        &[],
        None,
    );
    assert_eq!(run(&f), run(&f));
}

#[test]
fn rejects_generation_mismatch_and_unknown_strategy() {
    let f = fixture(&[], &[], None);
    let c = Config::from_yaml_with_generation(EXAMPLE_CONFIG, 7).unwrap();
    assert_eq!(
        plan(&c, &f.snapshot, &f.state),
        Err(PlanError::GenerationMismatch {
            config: 7,
            snapshot: 0
        })
    );
    let bad = EXAMPLE_CONFIG.replace("fifo-exhaustive", "round-robin");
    let c = Config::from_yaml(&bad).unwrap();
    let err = plan(&c, &f.snapshot, &f.state).unwrap_err();
    assert_eq!(err, PlanError::UnknownStrategy("round-robin".into()));
    assert!(err.to_string().contains("fifo-exhaustive"));
    assert!(strategy_names().contains(&"fifo-exhaustive"));
}

#[test]
fn bundled_fixture_with_state_yields_to_the_starved_queue() {
    let config = example_config();
    let f = sched_model::test_support::fixture("serving-with-starved-queue");
    let intent = plan(&config, &f.snapshot, &f.state).unwrap();
    assert_eq!(
        intent.models[&ResourceId::new(G)],
        Some(ModelId::new("whisperx"))
    );
}
