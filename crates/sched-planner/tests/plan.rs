//! Table-driven tests for the fifo-exhaustive strategy against the example config.

use sched_model::test_support::{EXAMPLE_CONFIG, example_config, example_config_with_nfs_batch};
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

/// Demand of the test-only NFS batch class (see `example_config_with_nfs_batch`).
fn batch(ready: u64) -> String {
    demand("nfs-batch", "nfs-batch-queue", ready, 0, None)
}

/// As `fixture`, checked against the example plus the NFS batch class.
fn batch_fixture(obs: &[String], leases: &[&str]) -> Fixture {
    let y = fixture_yaml(obs, leases, None);
    Fixture::from_yaml_checked(&y, &example_config_with_nfs_batch())
        .unwrap_or_else(|e| panic!("{e}\n{y}"))
}

fn run_batch(f: &Fixture) -> Intent {
    plan(&example_config_with_nfs_batch(), &f.snapshot, &f.state).unwrap()
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
    let lockout = format!("{{ id: r, resource: {G}, granted_at: 0, kind: {{ type: lockout }} }}");
    let f = fixture(&work, &[&lockout], None);
    let i = run(&f);
    assert_eq!(i.models[&ResourceId::new(G)], None);
    assert!(i.forced.contains(&ResourceId::new(G)));
    assert!(i.lockout.contains(&ResourceId::new(G)));
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

    let backup = batch(1);
    let f = batch_fixture(std::slice::from_ref(&backup), &[]);
    assert_eq!(run_batch(&f).power_up.get(&nfs), Some(&PowerWant::Workload));

    let f = batch_fixture(&[backup], &[host]);
    assert_eq!(
        run_batch(&f).power_up.get(&nfs),
        Some(&PowerWant::Manual),
        "manual wins over workload"
    );

    let idle_backup = batch(0);
    let f = batch_fixture(&[idle_backup], &[]);
    assert!(run_batch(&f).power_up.is_empty());

    // A shared hold (a scheduled job's) wants the host up like a workload, never as a
    // manual request, so it does not take a host over.
    let hold = "{ id: s, resource: basestar-nfs, granted_at: 0, kind: { type: shared_hold, holder: \"job:x\" } }";
    let f = fixture(&[], &[hold], None);
    assert_eq!(run(&f).power_up.get(&nfs), Some(&PowerWant::Workload));
    let f = fixture(&[], &[hold, host], None);
    assert_eq!(run(&f).power_up.get(&nfs), Some(&PowerWant::Manual));
    let f = fixture(&[], &[], None);
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

fn winner(obs: &[String], config: &Config) -> Option<String> {
    let f = fixture(obs, &[], None);
    let i = plan(config, &f.snapshot, &f.state).unwrap();
    i.models[&ResourceId::new(G)]
        .as_ref()
        .map(|m| m.as_str().to_string())
}

#[test]
fn work_without_a_timestamp_counts_as_newest() {
    // Tagging (llm, cheaper to load) has no timestamp; ocr (mineru) has waited a minute.
    // The same tier is decided by age first, so the timestamped queue wins.
    let untimed = demand("paperless-tagging", "paperless-post-ocr", 1, 0, None);
    assert_eq!(
        winner(&[untimed.clone(), ocr(1, T - MIN)], &example_config()).as_deref(),
        Some("mineru")
    );
    // And the other way round, an untimed ocr loses to a timestamped tagging.
    let untimed_ocr = demand("paperless-ocr", "paperless-ocr-pending", 1, 0, None);
    assert_eq!(
        winner(&[untimed_ocr, tagging(1, T - MIN)], &example_config()).as_deref(),
        Some("llm")
    );
}

#[test]
fn ties_fall_to_load_time_before_id() {
    // Same tier (60), same age. whisperx loads in 60s, llm in 90s; "llm" sorts first by id,
    // so only the load-time rule picks whisperx.
    let obs = [transcription(1, T - 5 * MIN), summarization(1, T - 5 * MIN)];
    assert_eq!(winner(&obs, &example_config()).as_deref(), Some("whisperx"));
}

#[test]
fn equal_load_time_falls_to_the_lower_id() {
    // Make llm and mineru equally expensive; ocr (mineru) is listed first in the config, so
    // only the id rule picks llm.
    let yaml = EXAMPLE_CONFIG.replace("load_time: 90s", "load_time: 120s");
    assert_ne!(yaml, EXAMPLE_CONFIG);
    let config = Config::from_yaml(&yaml).unwrap();
    let obs = [ocr(1, T - 5 * MIN), tagging(1, T - 5 * MIN)];
    let f = fixture(&obs, &[], None);
    let i = plan(&config, &f.snapshot, &f.state).unwrap();
    assert_eq!(i.models[&ResourceId::new(G)], Some(ModelId::new("llm")));
}

#[test]
fn snapshot_order_never_changes_the_intent() {
    // Conflicting manual leases and interactive leases, listed in every order.
    let a = format!(
        "{{ id: a, resource: {G}, granted_at: 5, kind: {{ type: manual_model, model: llm }} }}"
    );
    let b = format!(
        "{{ id: b, resource: {G}, granted_at: 5, kind: {{ type: manual_model, model: whisperx }} }}"
    );
    let c = format!(
        "{{ id: c, resource: {G}, granted_at: 1, kind: {{ type: manual_model, model: mineru }} }}"
    );
    let orders = [
        [&a, &b, &c],
        [&a, &c, &b],
        [&b, &a, &c],
        [&b, &c, &a],
        [&c, &a, &b],
        [&c, &b, &a],
    ];
    let intents: Vec<Intent> = orders
        .iter()
        .map(|o| {
            let y = fixture_yaml(&[], &[o[0].as_str(), o[1].as_str(), o[2].as_str()], None);
            let f = Fixture::from_yaml(&y).unwrap();
            run(&f)
        })
        .collect();
    assert!(intents.windows(2).all(|w| w[0] == w[1]));
    // Same grant time: the lowest lease id wins.
    assert_eq!(
        intents[0].models[&ResourceId::new(G)],
        Some(ModelId::new("llm"))
    );
}

#[test]
fn a_lockout_lease_on_a_shared_host_is_dropped_by_the_planner() {
    let lockout = "{ id: r, resource: basestar-nfs, granted_at: 0, kind: { type: lockout } }";
    let host = "{ id: h, resource: basestar-nfs, granted_at: 0, kind: { type: manual_host } }";
    let y = fixture_yaml(&[], &[lockout, host], None);
    // The fixture checker rejects the lockout; the planner must cope with it anyway.
    let f = Fixture::from_yaml(&y).unwrap();
    let i = run(&f);
    assert!(i.lockout.is_empty());
    assert_eq!(
        i.power_up.get(&ResourceId::new("basestar-nfs")),
        Some(&PowerWant::Manual)
    );
    // A lockout on the exclusive host is kept.
    let lockout = format!("{{ id: r, resource: {G}, granted_at: 0, kind: {{ type: lockout }} }}");
    let f = fixture(&[], &[&lockout], None);
    assert!(run(&f).lockout.contains(&ResourceId::new(G)));
}

#[test]
fn a_lockout_removes_the_power_wants_of_the_locked_out_host_only() {
    let host = ResourceId::new(G);
    let nfs = ResourceId::new("basestar-nfs");
    let manual_host =
        format!("{{ id: h, resource: {G}, granted_at: 0, kind: {{ type: manual_host }} }}");
    let manual_model = format!(
        "{{ id: m, resource: {G}, granted_at: 0, kind: {{ type: manual_model, model: llm }} }}"
    );
    let nfs_host = "{ id: n, resource: basestar-nfs, granted_at: 0, kind: { type: manual_host } }";
    let lockout = format!("{{ id: r, resource: {G}, granted_at: 0, kind: {{ type: lockout }} }}");
    let backup = batch(1);

    // Without the lockout, both manual leases want the host powered.
    let f = fixture(&[], &[&manual_host, &manual_model], None);
    assert_eq!(run(&f).power_up.get(&host), Some(&PowerWant::Manual));

    // With it, intake stops and no want to power the locked out host survives, whatever
    // leases ask for it.
    let f = batch_fixture(
        std::slice::from_ref(&backup),
        &[&manual_host, &manual_model, &lockout, nfs_host],
    );
    let i = run_batch(&f);
    assert!(i.lockout.contains(&host));
    assert!(
        !i.power_up.contains_key(&host),
        "locked-out host still has a power want: {:?}",
        i.power_up
    );
    assert_eq!(i.models[&host], None);
    assert!(i.forced.contains(&host));
    // Other resources keep theirs.
    assert_eq!(i.power_up.get(&nfs), Some(&PowerWant::Manual));
}

/// The example config without the whisperx model and the class that needs it.
fn without_whisperx() -> Config {
    let line = "  - { id: whisperx, resource: cuda-central, load_time: 60s, workloads: [whisperx], namespaces: [ai] }\n";
    let start = EXAMPLE_CONFIG
        .find("  - id: speakr-transcription\n")
        .unwrap();
    let end = start + EXAMPLE_CONFIG[start..].find("\n\n").unwrap() + 2;
    let y = format!("{}{}", &EXAMPLE_CONFIG[..start], &EXAMPLE_CONFIG[end..]).replace(line, "");
    assert!(!y.contains("model: whisperx") && !y.contains("id: whisperx"));
    Config::from_yaml(&y).unwrap()
}

#[test]
fn a_model_removed_from_the_config_gets_no_dwell_and_no_grace() {
    let config = without_whisperx();
    // whisperx was allowed a minute ago (well inside min_dwell) and has just been removed.
    let unchecked = |obs: &[String]| {
        let y = fixture_yaml(obs, &[], Some(("whisperx", T - MIN, None)));
        Fixture::from_yaml(&y).unwrap()
    };
    let f = unchecked(&[ocr(2, T - MIN)]);
    let i = plan(&config, &f.snapshot, &f.state).unwrap();
    assert_eq!(
        i.models[&ResourceId::new(G)],
        Some(ModelId::new("mineru")),
        "the best candidate at once"
    );
    let f = unchecked(&[ocr(0, T)]);
    let i = plan(&config, &f.snapshot, &f.state).unwrap();
    assert_eq!(i.models[&ResourceId::new(G)], None, "no grace either");
    // The same state with the model still configured keeps it (min_dwell).
    let f = fixture(&[ocr(2, T - MIN)], &[], Some(("whisperx", T - MIN, None)));
    assert_eq!(target(&f).as_deref(), Some("whisperx"));
}

#[test]
fn gate_classes_never_influence_model_choice() {
    // The example has a gate class (requires the GPU host and NFS, no model, no demand).
    let config = example_config();
    let gate = config.class(&"paperless-gpt".into()).unwrap();
    assert!(gate.demand.is_none() && gate.requires.iter().all(|r| r.model.is_none()));
    let start = EXAMPLE_CONFIG.find("  - id: paperless-gpt\n").unwrap();
    let end = start + EXAMPLE_CONFIG[start..].find("\n\n").unwrap() + 2;
    let without = Config::from_yaml(&format!(
        "{}{}",
        &EXAMPLE_CONFIG[..start],
        &EXAMPLE_CONFIG[end..]
    ))
    .unwrap();
    assert!(without.class(&"paperless-gpt".into()).is_none());
    type Serving<'a> = Option<(&'a str, u64, Option<u64>)>;
    let worlds: Vec<(Vec<String>, Serving)> = vec![
        (vec![ocr(0, T)], None),
        (vec![ocr(2, T - 9 * MIN), tagging(1, T - 5 * MIN)], None),
        (
            vec![transcription(1, T - 40 * MIN)],
            Some(("mineru", T - 3 * 60 * MIN, None)),
        ),
        (
            vec![ocr(0, T)],
            Some(("llm", T - 60 * MIN, Some(T - 20 * MIN))),
        ),
    ];
    for (obs, serving) in worlds {
        let f = fixture(&obs, &[], serving);
        assert_eq!(
            plan(&config, &f.snapshot, &f.state).unwrap(),
            plan(&without, &f.snapshot, &f.state).unwrap(),
            "{obs:?} {serving:?}"
        );
    }
}

// Failed items: a separate demand bucket that brings a model in but never ages, never holds
// the model that is serving and never counts as ready work.

/// A demand observation with `failed` items and `ready` ready ones (oldest as given).
fn failed_demand(
    class: &str,
    source: &str,
    failed: u64,
    ready: u64,
    oldest: Option<u64>,
) -> String {
    let o = oldest
        .filter(|_| ready > 0)
        .map_or(String::new(), |t| format!(", oldest_ready_at: {t}"));
    format!(
        "    - {{ source: {source}, observed_at: {T}, fidelity: polled, value: {{ type: demand, class: {class}, ready: {ready}, in_flight: 0, failed: {failed}{o} }} }}\n"
    )
}

fn failed_transcription(failed: u64) -> String {
    // Failed items carry no timestamp: a fixture refuses one on a demand with nothing ready.
    failed_demand("speakr-transcription", "speakr-pending", failed, 0, None)
}

const HOUR: u64 = 60 * MIN;

#[test]
fn failed_items_alone_bring_their_model_in_at_the_class_priority() {
    // Idle resource: whisperx is wanted although nothing is ready.
    let f = fixture(&[failed_transcription(2)], &[], None);
    assert_eq!(target(&f), Some("whisperx".into()));
    // Normal priority: transcription (60) beats ocr (50) even for failed items only.
    let f = fixture(&[failed_transcription(2), ocr(1, T - 20 * MIN)], &[], None);
    assert_eq!(target(&f), Some("whisperx".into()));
    // A class that does not outrank real work does not win: immich (10) failed vs ocr ready.
    let f = fixture(
        &[
            failed_demand("immich-ml", "immich-queues", 5, 0, None),
            ocr(1, T - MIN),
        ],
        &[],
        None,
    );
    assert_eq!(target(&f), Some("mineru".into()));
    // Zero failed and zero ready is nothing.
    let f = fixture(&[failed_transcription(0)], &[], None);
    assert_eq!(target(&f), None);
}

#[test]
fn failed_items_have_no_age_and_lose_ties_to_real_work_of_their_tier() {
    // Same priority (50): ocr ready beats tagging failed-only, which ranks as brand new.
    let f = fixture(
        &[
            ocr(1, T - 2 * MIN),
            failed_demand("paperless-tagging", "paperless-post-ocr", 4, 0, None),
        ],
        &[],
        None,
    );
    assert_eq!(target(&f), Some("mineru".into()));
}

#[test]
fn failed_items_never_age_into_starvation_phantom_demand() {
    // mineru has served 10 minutes (past min_dwell) and has ready work. whisperx has failed
    // items only. If they counted as ready work with a 90 minute old timestamp, whisperx
    // would be starved (> age_threshold 30m) and win the switch.
    let f = fixture(
        &[ocr(1, T - MIN), failed_transcription(3)],
        &[],
        Some(("mineru", T - 10 * MIN, None)),
    );
    assert_eq!(target(&f), Some("mineru".into()));
    // Control: the same items as READY work, 90 minutes old, do starve and switch.
    let f = fixture(
        &[ocr(1, T - MIN), transcription(3, T - 90 * MIN)],
        &[],
        Some(("mineru", T - 10 * MIN, None)),
    );
    assert_eq!(target(&f), Some("whisperx".into()));
    // Past max_dwell the busy model yields to the best other candidate by the usual rule.
    let f = fixture(
        &[ocr(1, T - MIN), failed_transcription(3)],
        &[],
        Some(("mineru", T - (2 * HOUR + MIN), None)),
    );
    assert_eq!(target(&f), Some("whisperx".into()));
}

#[test]
fn failed_items_never_hold_the_model_that_is_serving() {
    let serving = |since: u64, idle: Option<u64>| Some(("whisperx", since, idle));
    // Just loaded for the failed items, idle clock not past idle_grace: stays.
    let f = fixture(
        &[failed_transcription(2)],
        &[],
        serving(T - 6 * MIN, Some(T - 6 * MIN)),
    );
    assert_eq!(target(&f), Some("whisperx".into()));
    // Idle for idle_grace (10m) and past min_dwell: released although failed items remain.
    let f = fixture(
        &[failed_transcription(2)],
        &[],
        serving(T - 11 * MIN, Some(T - 10 * MIN)),
    );
    assert_eq!(target(&f), None);
    // ... and the other model with real work gets the GPU.
    let f = fixture(
        &[failed_transcription(2), ocr(1, T - MIN)],
        &[],
        serving(T - 11 * MIN, Some(T - 10 * MIN)),
    );
    assert_eq!(target(&f), Some("mineru".into()));
    // Not released before min_dwell.
    let f = fixture(
        &[failed_transcription(2)],
        &[],
        serving(T - 4 * MIN, Some(T - 10 * MIN)),
    );
    assert_eq!(target(&f), Some("whisperx".into()));
    // With ready work as well, the model is busy and stays.
    let f = fixture(
        &[failed_demand(
            "speakr-transcription",
            "speakr-pending",
            2,
            1,
            Some(T - MIN),
        )],
        &[],
        serving(T - 30 * MIN, None),
    );
    assert_eq!(target(&f), Some("whisperx".into()));
}

#[test]
fn an_exclusive_host_is_wanted_up_only_by_a_manual_host_or_model_lease() {
    let gpu = ResourceId::new(G);
    let nfs = ResourceId::new("basestar-nfs");
    let work = [ocr(5, T - MIN), failed_transcription(2)];
    // Queued work and failed items alone never power the GPU host on.
    let f = fixture(&work, &[], None);
    assert!(run(&f).power_up.is_empty());
    // Nor does an interactive session.
    let live = format!(
        "{{ id: i, resource: {G}, granted_at: 0, kind: {{ type: interactive, model: llm, expires_at: {} }} }}",
        T + 1
    );
    let f = fixture(&work, &[&live], None);
    assert!(run(&f).power_up.is_empty());
    // Nor a job whose needs are only on the shared host: the NFS host is wanted, the GPU is not.
    let hold = "{ id: s, resource: basestar-nfs, granted_at: 0, kind: { type: shared_hold, holder: \"job:x\" } }";
    let f = fixture(&work, &[hold], None);
    let i = run(&f);
    assert_eq!(i.power_up.get(&nfs), Some(&PowerWant::Workload));
    assert!(!i.power_up.contains_key(&gpu), "{:?}", i.power_up);
    let f = batch_fixture(&[batch(3), ocr(5, T - MIN)], &[]);
    let i = run_batch(&f);
    assert_eq!(i.power_up.get(&nfs), Some(&PowerWant::Workload));
    assert!(!i.power_up.contains_key(&gpu), "{:?}", i.power_up);
    // A manual host lease does.
    let host = format!("{{ id: h, resource: {G}, granted_at: 0, kind: {{ type: manual_host }} }}");
    let f = fixture(&work, &[&host], None);
    assert_eq!(run(&f).power_up.get(&gpu), Some(&PowerWant::Manual));
    // A lockout wins over it.
    let lockout = format!("{{ id: r, resource: {G}, granted_at: 1, kind: {{ type: lockout }} }}");
    let f = fixture(&work, &[&host, &lockout], None);
    assert!(!run(&f).power_up.contains_key(&gpu));
}

#[test]
fn a_host_lease_on_an_exclusive_host_does_not_pin_the_model() {
    let gpu = ResourceId::new(G);
    let host = format!("{{ id: h, resource: {G}, granted_at: 0, kind: {{ type: manual_host }} }}");
    // The queues choose the model, as without the lease, and nothing is forced.
    let f = fixture(&[ocr(5, T - MIN)], &[&host], None);
    let i = run(&f);
    assert_eq!(i.models[&gpu], Some(ModelId::new("mineru")));
    assert!(!i.forced.contains(&gpu));
    let f = fixture(&[transcription(5, T - MIN)], &[&host], None);
    assert_eq!(run(&f).models[&gpu], Some(ModelId::new("whisperx")));
    // Idle queues: no model, the host stays up on the lease alone.
    let f = fixture(&[], &[&host], None);
    let i = run(&f);
    assert_eq!(i.models[&gpu], None);
    assert_eq!(i.power_up.get(&gpu), Some(&PowerWant::Manual));
    // A model that is serving and idle gives way to the queue after the grace, as ever.
    let f = fixture(
        &[transcription(5, T - MIN), ocr(0, T)],
        &[&host],
        Some(("mineru", 0, Some(0))),
    );
    assert_eq!(run(&f).models[&gpu], Some(ModelId::new("whisperx")));
}
