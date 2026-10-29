//! Table-driven tests for the sequencer's state machines and safety rules.

use sched_model::test_support::{example_config, example_config_managed, example_yaml_managed};

/// Most tables here exercise the managed power machine (M4): the example config with
/// `power.control: managed`. The manual-control tests at the end use the example as is.
fn managed_yaml() -> String {
    example_yaml_managed()
}
use sched_model::{
    AllowedSet, ClassId, Config, Fixture, Intent, ModePhase, ModelId, PowerPhase, PowerWant,
    ResourceId, SchedState, Step, WaitReason,
};
use sched_sequencer::{Decision, TickError, tick};

const T: u64 = 100_000_000;
const MIN: u64 = 60_000;
const G: &str = "cuda-central";
const N: &str = "basestar-nfs";

fn rid(s: &str) -> ResourceId {
    ResourceId::new(s)
}
fn mid(s: &str) -> ModelId {
    ModelId::new(s)
}
fn cid(s: &str) -> ClassId {
    ClassId::new(s)
}

/// Builder for one tick's inputs.
#[derive(Clone)]
struct Case {
    obs: Vec<String>,
    leases: Vec<String>,
    gpu: String,
    nfs: String,
    intent: Intent,
    config: Config,
}

fn pods(model: &str, pods: u32, ready: u32, age: u64) -> String {
    format!(
        "    - {{ source: cluster-pods, observed_at: {}, fidelity: event, value: {{ type: model_pods, model: {model}, pods: {pods}, ready_pods: {ready} }} }}\n",
        T - age
    )
}

fn up(resource: &str, up: bool, age: u64) -> String {
    format!(
        "    - {{ source: cluster-pods, observed_at: {}, fidelity: event, value: {{ type: resource_up, resource: {resource}, up: {up} }} }}\n",
        T - age
    )
}

/// A fresh reading (aged `age` ms) of how many pods mount the NFS host.
fn mounts(pods: u32, age: u64) -> String {
    format!(
        "    - {{ source: basestar-mounts, observed_at: {}, fidelity: polled, value: {{ type: resource_mounts, resource: {N}, pods: {pods} }} }}\n",
        T - age
    )
}

fn demand(class: &str, source: &str, ready: u64, in_flight: u64, age: u64) -> String {
    let oldest = if ready > 0 {
        format!(", oldest_ready_at: {}", T - age - 1000)
    } else {
        String::new()
    };
    format!(
        "    - {{ source: {source}, observed_at: {}, fidelity: polled, value: {{ type: demand, class: {class}, ready: {ready}, in_flight: {in_flight}{oldest} }} }}\n",
        T - age
    )
}

const CLASSES: &[(&str, &str)] = &[
    ("paperless-ocr", "paperless-ocr-pending"),
    ("paperless-tagging", "paperless-post-ocr"),
    ("speakr-transcription", "speakr-pending"),
    ("speakr-summarization", "speakr-summaries"),
    ("immich-ml", "immich-queues"),
    ("interactive-llm", "llm-sessions"),
];

fn idle_demand() -> Vec<String> {
    CLASSES.iter().map(|(c, s)| demand(c, s, 0, 0, 0)).collect()
}

fn zero_pods() -> Vec<String> {
    ["mineru", "llm", "whisperx", "immich-ml"]
        .iter()
        .map(|m| pods(m, 0, 0, 0))
        .collect()
}

fn mode_idle() -> String {
    "{ phase: idle }".into()
}

fn serving(model: &str, since_ago: u64) -> String {
    format!(
        "{{ phase: serving, model: {model}, since: {} }}",
        T - since_ago
    )
}

fn on() -> String {
    "{ phase: \"on\", since: 0 }".into()
}

impl Case {
    fn new() -> Case {
        let mut obs = idle_demand();
        obs.extend(zero_pods());
        obs.push(up(G, true, 0));
        obs.push(up(N, true, 0));
        obs.push(mounts(0, 0));
        Case {
            obs,
            leases: vec![],
            gpu: format!("{{ power: {}, mode: {} }}", on(), mode_idle()),
            nfs: format!("{{ power: {}, mode: {} }}", on(), mode_idle()),
            intent: Intent::default(),
            config: example_config_managed(),
        }
    }

    /// Replaces any observation of the same subject.
    fn obs(mut self, line: String) -> Case {
        let key = |l: &str| {
            let v = l.split("value:").nth(1).unwrap().to_string();
            let name = |k: &str| {
                v.split(&format!("{k}: "))
                    .nth(1)
                    .map(|s| s.split([',', ' ']).next().unwrap().to_string())
            };
            (
                v.contains("type: demand"),
                v.contains("type: resource_mounts"),
                name("class"),
                name("model"),
                name("resource"),
            )
        };
        let k = key(&line);
        self.obs.retain(|o| key(o) != k);
        self.obs.push(line);
        self
    }

    fn drop_obs(mut self, needle: &str) -> Case {
        self.obs.retain(|o| !o.contains(needle));
        self
    }

    fn gpu(mut self, mode: &str) -> Case {
        self.gpu = format!("{{ power: {}, mode: {mode} }}", on());
        self
    }

    fn gpu_state(mut self, s: &str) -> Case {
        self.gpu = s.into();
        self
    }

    fn nfs_state(mut self, s: &str) -> Case {
        self.nfs = s.into();
        self
    }

    fn lease(mut self, l: String) -> Case {
        self.leases.push(l);
        self
    }

    fn target(mut self, m: Option<&str>) -> Case {
        self.intent.models.insert(rid(G), m.map(mid));
        self
    }

    fn forced(mut self) -> Case {
        self.intent.forced.insert(rid(G));
        self
    }

    fn reclaim(mut self) -> Case {
        self.intent.reclaim.insert(rid(G));
        self.intent.models.insert(rid(G), None);
        self.intent.forced.insert(rid(G));
        self
    }

    fn want(mut self, r: &str, w: PowerWant) -> Case {
        self.intent.power_up.insert(rid(r), w);
        self
    }

    fn state(&self) -> SchedState {
        let y = format!(
            "description: t\nsnapshot: {{ now: 0 }}\nstate:\n  resources:\n    {G}: {}\n    {N}: {}\n",
            self.gpu, self.nfs
        );
        Fixture::from_yaml(&y)
            .unwrap_or_else(|e| panic!("{e}\n{y}"))
            .state
    }

    fn run_at(&self, state: &SchedState, now: u64) -> Decision {
        let mut y = format!("description: t\nsnapshot:\n  now: {now}\n  observations:\n");
        for o in &self.obs {
            y.push_str(o);
        }
        y.push_str("  leases:\n");
        if self.leases.is_empty() {
            y.push_str("    []\n");
        }
        for l in &self.leases {
            y.push_str(&format!("    - {l}\n"));
        }
        let f = Fixture::from_yaml(&y).unwrap_or_else(|e| panic!("{e}\n{y}"));
        tick(&self.config, &f.snapshot, state, &self.intent).unwrap()
    }

    /// Like `run_at`, but every observation is moved forward with the clock, as if the
    /// observers kept reporting the same thing.
    fn run_fresh_at(&self, state: &SchedState, now: u64) -> Decision {
        let mut c = Case {
            obs: vec![],
            ..Case::new()
        };
        c.leases = self.leases.clone();
        c.config = self.config.clone();
        c.intent = self.intent.clone();
        let shift = now - T;
        c.obs = self
            .obs
            .iter()
            .map(|o| {
                let mut o = o.clone();
                let at = o
                    .split("observed_at: ")
                    .nth(1)
                    .unwrap()
                    .split(',')
                    .next()
                    .unwrap()
                    .to_string();
                o = o.replace(
                    &format!("observed_at: {at}"),
                    &format!("observed_at: {}", at.parse::<u64>().unwrap() + shift),
                );
                if let Some(old) = o.split("oldest_ready_at: ").nth(1) {
                    let old = old.split([',', ' ']).next().unwrap().to_string();
                    o = o.replace(
                        &format!("oldest_ready_at: {old}"),
                        &format!("oldest_ready_at: {}", old.parse::<u64>().unwrap() + shift),
                    );
                }
                o
            })
            .collect();
        c.run_at(state, now)
    }

    fn run(&self) -> Decision {
        self.run_at(&self.state(), T)
    }
}

fn wait(r: &str, reason: WaitReason) -> Step {
    Step::Wait {
        resource: rid(r),
        reason,
    }
}
fn allow(m: &str) -> Step {
    Step::AllowModel {
        resource: rid(G),
        model: mid(m),
    }
}
fn drain(m: &str) -> Step {
    Step::DrainModel {
        resource: rid(G),
        model: mid(m),
    }
}
fn pause(c: &str) -> Step {
    Step::PauseIntake { class: cid(c) }
}
fn kill(c: &str) -> Step {
    Step::KillInFlight { class: cid(c) }
}

fn sorted(mut v: Vec<Step>) -> Vec<Step> {
    v.sort();
    v
}

fn assert_steps(d: &Decision, want: Vec<Step>, name: &str) {
    assert_eq!(sorted(d.steps.clone()), sorted(want), "{name}");
}

fn mode_of(d: &Decision, r: &str) -> ModePhase {
    d.state.resource(&rid(r)).unwrap().mode.clone()
}

fn power_of(d: &Decision, r: &str) -> PowerPhase {
    d.state.resource(&rid(r)).unwrap().power
}

/// Whether any class that needs the GPU is allowed.
fn uses_gpu(d: &Decision) -> bool {
    let config = example_config();
    d.allowed.classes.iter().any(|c| {
        config
            .class(c)
            .unwrap()
            .requires
            .iter()
            .any(|r| r.resource == rid(G))
    })
}

#[test]
fn allowing_a_model_never_waits_on_pods_but_needs_the_host_freshly_up() {
    let table: Vec<(&str, Case, Vec<Step>)> = vec![
        (
            "all zero and fresh",
            Case::new().target(Some("mineru")),
            vec![allow("mineru")],
        ),
        (
            "another model has pods: Kubernetes keeps the new pod pending, not the scheduler",
            Case::new().target(Some("mineru")).obs(pods("llm", 1, 1, 0)),
            vec![allow("mineru")],
        ),
        (
            "a pod observation is missing",
            Case::new()
                .target(Some("mineru"))
                .drop_obs("model: whisperx"),
            vec![allow("mineru")],
        ),
        (
            "every pod observation is stale",
            Case::new()
                .target(Some("mineru"))
                .drop_obs("type: model_pods")
                .obs(pods("llm", 3, 3, 99 * MIN)),
            vec![allow("mineru")],
        ),
        (
            "the host's reachability is 1ms too old",
            Case::new().target(Some("mineru")).obs(up(G, true, 30_001)),
            vec![wait(G, WaitReason::StaleObservation)],
        ),
        (
            "the host's reachability exactly at the bound",
            Case::new().target(Some("mineru")).obs(up(G, true, 30_000)),
            vec![allow("mineru")],
        ),
        (
            "the host's reachability is missing",
            Case::new()
                .target(Some("mineru"))
                .drop_obs("type: resource_up, resource: cuda-central"),
            vec![wait(G, WaitReason::StaleObservation)],
        ),
        ("no target, nothing to do", Case::new().target(None), vec![]),
    ];
    for (name, case, want) in table {
        let d = case.run();
        assert_steps(&d, want, name);
        assert!(d.allowed.models.len() <= 1, "{name}");
    }
}

#[test]
fn pod_observations_never_change_a_decision() {
    // Cases that reach every branch of the mode machine: allow, stay, wind down, switch,
    // forced switch with a kill, reclaim, and an idle host that may power off.
    let cases: Vec<(&str, Case)> = vec![
        ("allow", Case::new().target(Some("mineru"))),
        (
            "stay",
            Case::new()
                .gpu(&serving("mineru", 10 * MIN))
                .target(Some("mineru")),
        ),
        (
            "wind down",
            Case::new()
                .gpu(&serving("mineru", 10 * MIN))
                .obs(demand("paperless-ocr", "paperless-ocr-pending", 2, 1, 0))
                .target(Some("llm")),
        ),
        (
            "switch",
            Case::new()
                .gpu(&serving("mineru", 10 * MIN))
                .target(Some("llm")),
        ),
        (
            "min dwell",
            Case::new().gpu(&serving("mineru", MIN)).target(Some("llm")),
        ),
        (
            "reclaim",
            Case::new().gpu(&serving("mineru", 10 * MIN)).reclaim(),
        ),
        ("power off", Case::new().gpu_state(&managed_idle(99 * MIN))),
    ];
    let variants: Vec<(&str, Vec<String>)> = vec![
        ("no pod readings", vec![]),
        (
            "every model running",
            ["mineru", "llm", "whisperx", "immich-ml"]
                .iter()
                .map(|m| pods(m, 2, 1, 0))
                .collect(),
        ),
        (
            "stale readings",
            ["mineru", "llm"]
                .iter()
                .map(|m| pods(m, 1, 1, 99 * MIN))
                .collect(),
        ),
        (
            "a future-dated reading",
            vec![future(&pods("llm", 1, 0, 0))],
        ),
    ];
    for (name, case) in cases {
        let base = case.run();
        let st = case.state();
        for (label, extra) in &variants {
            let mut c = case.clone().drop_obs("type: model_pods");
            c.obs.extend(extra.iter().cloned());
            assert_eq!(c.run_at(&st, T), base, "{name} / {label}");
        }
    }
}

#[test]
fn an_allowed_model_serves_from_the_tick_it_is_allowed() {
    let d = Case::new()
        .target(Some("mineru"))
        .obs(pods("llm", 1, 1, 0))
        .run();
    assert_steps(&d, vec![allow("mineru")], "allowed");
    assert_eq!(
        mode_of(&d, G),
        ModePhase::Serving {
            model: mid("mineru"),
            since: sched_model::Millis(T),
            idle_since: None,
            wind_down: None
        }
    );
    // Its classes are allowed in the same tick: its pods are still pending, but a consumer
    // that sends work only waits for them.
    assert!(d.allowed.models.contains(&mid("mineru")));
    assert!(d.allowed.classes.contains(&cid("paperless-ocr")));
    // min_dwell counts from that tick: load time is part of it.
    let c = Case::new().target(Some("llm"));
    let early = c.run_fresh_at(&d.state, T + 5 * MIN - 1);
    assert_steps(
        &early,
        vec![wait(G, WaitReason::MinDwell)],
        "within min_dwell",
    );
    let at = c.run_fresh_at(&d.state, T + 5 * MIN);
    assert_steps(
        &at,
        vec![pause("paperless-ocr"), drain("mineru"), allow("llm")],
        "min_dwell after the allow",
    );
}

#[test]
fn modes_persisted_by_the_build_with_pod_gating_converge_without_blocking() {
    // `loading` reads as serving since the same instant: it keeps its dwell clock.
    let loading = Case::new().gpu(&format!(
        "{{ phase: loading, model: mineru, since: {} }}",
        T - 1000
    ));
    assert_eq!(
        mode_of(&loading.clone().target(Some("mineru")).run(), G),
        ModePhase::Serving {
            model: mid("mineru"),
            since: sched_model::Millis(T - 1000),
            idle_since: Some(sched_model::Millis(T)),
            wind_down: None
        }
    );
    let d = loading.clone().target(Some("mineru")).run();
    assert_steps(&d, vec![], "no wait for readiness");
    assert!(d.allowed.classes.contains(&cid("paperless-ocr")));
    let d = loading.clone().target(Some("llm")).run();
    assert_steps(
        &d,
        vec![wait(G, WaitReason::MinDwell)],
        "dwell from the allow",
    );
    // `draining` reads as idle: the next model is allowed at once, whatever the pods say.
    let draining = || {
        Case::new()
            .gpu(&format!(
                "{{ phase: draining, model: mineru, since: {} }}",
                T - 1000
            ))
            .obs(pods("mineru", 1, 1, 0))
    };
    let d = draining().target(Some("llm")).run();
    assert_steps(&d, vec![allow("llm")], "next model at once");
    assert_eq!(d.allowed.models, [mid("llm")].into());
    let d = draining().target(None).run();
    assert_steps(&d, vec![], "nothing wanted");
    assert!(matches!(mode_of(&d, G), ModePhase::Idle));
    // A recovery recorded by an older build while it waited for the drain is not run: its
    // action re-queues failed items and needs its model allowed, which a drain just ended.
    // It runs while the model serves and items fail (see the recovery tests below).
    let d = Case::new()
        .gpu_state(&format!(
            "{{ power: {}, mode: {{ phase: draining, model: whisperx, since: 1 }}, pending_recovery: [speakr-transcription] }}",
            on()
        ))
        .target(None)
        .run();
    assert_steps(
        &d,
        vec![],
        "pending recovery of an action that needs the model",
    );
    assert!(
        d.state
            .resource(&rid(G))
            .unwrap()
            .pending_recovery
            .is_empty()
    );
}

#[test]
fn a_switch_withdraws_the_old_model_and_allows_the_new_one_in_the_same_tick() {
    let serve = || {
        Case::new()
            .gpu(&serving("mineru", 10 * MIN))
            .obs(pods("mineru", 1, 1, 0))
    };
    let d = serve().target(Some("llm")).run();
    assert_steps(
        &d,
        vec![pause("paperless-ocr"), drain("mineru"), allow("llm")],
        "switch",
    );
    assert_eq!(d.allowed.models, [mid("llm")].into(), "never both");
    assert!(d.allowed.classes.contains(&cid("paperless-tagging")));
    assert!(!d.allowed.classes.contains(&cid("paperless-ocr")));
    assert!(matches!(
        mode_of(&d, G),
        ModePhase::Serving { ref model, since, .. } if *model == mid("llm") && since == sched_model::Millis(T)
    ));
    // Without a fresh reading of the host the switch waits as a whole: the old model stays.
    let d = serve().target(Some("llm")).obs(up(G, true, 31_000)).run();
    assert_steps(
        &d,
        vec![
            pause("paperless-ocr"),
            wait(G, WaitReason::StaleObservation),
        ],
        "stale reachability",
    );
    assert_eq!(d.allowed.models, [mid("mineru")].into());
    // Releasing (no target) withdraws alone, and needs no reading.
    let d = serve().target(None).obs(up(G, true, 31_000)).run();
    assert_steps(&d, vec![pause("paperless-ocr"), drain("mineru")], "release");
    assert!(d.allowed.models.is_empty());
    assert!(matches!(mode_of(&d, G), ModePhase::Idle));
}

/// The example config without the whisperx model and the class that needs it.
fn without_whisperx() -> Config {
    let yaml = managed_yaml();
    let line =
        "  - { id: whisperx, resource: cuda-central, load_time: 60s, workloads: [whisperx] }\n";
    let start = yaml.find("  - id: speakr-transcription\n").unwrap();
    let end = start + yaml[start..].find("\n\n").unwrap() + 2;
    let y = format!("{}{}", &yaml[..start], &yaml[end..]).replace(line, "");
    assert!(!y.contains("id: whisperx"));
    Config::from_yaml(&y).unwrap()
}

#[test]
fn a_model_removed_from_the_config_leaves_the_allowed_set_at_the_next_tick() {
    let removed = |target: Option<&str>| {
        let mut c = Case::new()
            .gpu(&serving("whisperx", MIN))
            .obs(pods("whisperx", 1, 1, 0))
            .target(target);
        c.config = without_whisperx();
        c
    };
    // Inside min_dwell, its pods still up: it goes now, and the next model comes in.
    let d = removed(Some("mineru")).run();
    assert_steps(&d, vec![drain("whisperx"), allow("mineru")], "replaced");
    assert_eq!(d.allowed.models, [mid("mineru")].into());
    let d = removed(None).run();
    assert_steps(&d, vec![drain("whisperx")], "withdrawn");
    assert!(d.allowed.models.is_empty() && matches!(mode_of(&d, G), ModePhase::Idle));
    // Nothing is left to wait for afterwards.
    let next = removed(None).run_at(&d.state, T + 1000);
    assert_steps(&next, vec![], "settled");
    // An intent (or a stale lease) naming an unconfigured model allows nothing.
    let d = Case {
        config: without_whisperx(),
        ..Case::new()
    }
    .target(Some("whisperx"))
    .run();
    assert_steps(&d, vec![], "never allowed");
    assert!(d.allowed.models.is_empty());
}

#[test]
fn a_removed_model_stays_out_of_the_allowed_set_while_the_switch_waits_for_the_host() {
    // whisperx is gone from the config but still recorded as serving; mineru is wanted and
    // the host reading is stale, so the switch cannot happen yet. The state keeps the old
    // mode, but the published set must not contain a model the config does not have.
    let d = Case::new()
        .drop_obs("resource: cuda-central")
        .obs(up(G, true, 5 * 60_000))
        .gpu(&serving("whisperx", MIN))
        .obs(pods("whisperx", 1, 1, 0))
        .target(Some("mineru"));
    let mut d = d;
    d.config = without_whisperx();
    let d = d.run();
    assert_steps(
        &d,
        vec![wait(G, WaitReason::StaleObservation)],
        "waits for a fresh reading",
    );
    assert!(
        matches!(mode_of(&d, G), ModePhase::Serving { model, .. } if model == mid("whisperx")),
        "the recorded mode is unchanged"
    );
    assert!(
        d.allowed.models.is_empty(),
        "an unconfigured model is never published: {:?}",
        d.allowed
    );
}

#[test]
fn gate_classes_follow_their_hosts_and_nothing_else() {
    let gate = cid("paperless-gpt");
    // Both hosts up, no model: the gate is open.
    let d = Case::new().run();
    assert!(d.allowed.classes.contains(&gate), "{:?}", d.allowed);
    // Whatever model serves, or is wound down, it stays open.
    let d = Case::new()
        .gpu(&serving("mineru", 10 * MIN))
        .obs(demand("paperless-ocr", "paperless-ocr-pending", 2, 1, 0))
        .target(Some("llm"))
        .run();
    assert!(d.allowed.classes.contains(&gate), "{:?}", d.steps);
    // The GPU host or NFS down closes it.
    let d = Case::new()
        .gpu_state("{ power: { phase: \"off\" }, mode: { phase: idle } }")
        .obs(up(G, false, 0))
        .run();
    assert!(!d.allowed.classes.contains(&gate));
    let d = Case::new()
        .nfs_state("{ power: { phase: \"off\" }, mode: { phase: idle } }")
        .obs(up(N, false, 0))
        .run();
    assert!(!d.allowed.classes.contains(&gate));
    // A reclaimed GPU host closes it too.
    assert!(!Case::new().reclaim().run().allowed.classes.contains(&gate));
}

#[test]
fn switching_away_from_a_serving_model() {
    let serve = |ago: u64| {
        Case::new()
            .gpu(&serving("mineru", ago))
            .obs(pods("mineru", 1, 1, 0))
    };
    // min_dwell is 5m.
    let d = serve(4 * MIN).target(Some("llm")).run();
    assert_steps(&d, vec![wait(G, WaitReason::MinDwell)], "before min_dwell");
    let d = serve(5 * MIN).target(Some("llm")).run();
    assert_steps(
        &d,
        vec![pause("paperless-ocr"), drain("mineru"), allow("llm")],
        "at min_dwell with nothing in flight",
    );

    let d = serve(10 * MIN)
        .target(Some("llm"))
        .obs(demand("paperless-ocr", "paperless-ocr-pending", 2, 1, 0))
        .run();
    assert_steps(
        &d,
        vec![pause("paperless-ocr"), wait(G, WaitReason::InFlightWork)],
        "in flight work is awaited",
    );
    assert!(
        d.allowed.models.contains(&mid("mineru")),
        "model stays allowed while winding down"
    );
    assert!(
        !d.allowed.classes.contains(&cid("paperless-ocr")),
        "intake stopped"
    );
    let again = serve(10 * MIN)
        .target(Some("llm"))
        .obs(demand("paperless-ocr", "paperless-ocr-pending", 2, 1, 0))
        .run_at(&d.state, T + 1000);
    assert_steps(
        &again,
        vec![wait(G, WaitReason::InFlightWork)],
        "intake is stopped only once",
    );

    // Not forced: in-flight work is still given only drain_timeout (10m) from the start of
    // the wind-down, then cut off by withdrawing the model.
    let feed = || {
        serve(10 * MIN).target(Some("llm")).obs(demand(
            "paperless-ocr",
            "paperless-ocr-pending",
            2,
            1,
            0,
        ))
    };
    let before = feed().run_fresh_at(&d.state, T + 10 * MIN - 1);
    assert_steps(
        &before,
        vec![wait(G, WaitReason::InFlightWork)],
        "one ms before drain_timeout",
    );
    assert!(
        !before
            .steps
            .iter()
            .any(|s| matches!(s, Step::KillInFlight { .. } | Step::DrainModel { .. }))
    );
    let late = feed().run_fresh_at(&d.state, T + 10 * MIN);
    assert_steps(
        &late,
        vec![kill("paperless-ocr"), drain("mineru"), allow("llm")],
        "cut off at drain_timeout",
    );

    let d = serve(10 * MIN)
        .target(Some("llm"))
        .obs(demand("paperless-ocr", "paperless-ocr-pending", 2, 0, 0))
        .obs(pods("mineru", 1, 1, 0))
        .run();
    assert_steps(
        &d,
        vec![pause("paperless-ocr"), drain("mineru"), allow("llm")],
        "ready but not in flight does not block",
    );

    let d = serve(10 * MIN)
        .target(Some("llm"))
        .drop_obs("class: paperless-ocr")
        .run();
    assert_steps(
        &d,
        vec![wait(G, WaitReason::StaleDemand)],
        "missing demand hides possible in-flight work",
    );

    let d = serve(10 * MIN)
        .obs(pods("mineru", 1, 1, 0))
        .gpu(&serving("llm", 10 * MIN))
        .obs(pods("llm", 1, 1, 0))
        .obs(pods("mineru", 0, 0, 0))
        .target(Some("mineru"))
        .obs(demand("interactive-llm", "llm-sessions", 1, 0, 0))
        .run();
    assert_steps(
        &d,
        vec![wait(G, WaitReason::NotPreemptible)],
        "unpreemptible class has work",
    );

    // Resuming: the intended model is the serving one again.
    let d = serve(10 * MIN)
        .target(Some("llm"))
        .obs(demand("paperless-ocr", "paperless-ocr-pending", 2, 1, 0))
        .run();
    let back = serve(10 * MIN)
        .target(Some("mineru"))
        .obs(demand("paperless-ocr", "paperless-ocr-pending", 2, 1, 0))
        .run_at(&d.state, T + 1000);
    assert_steps(&back, vec![], "abandoned switch");
    assert!(
        back.allowed.classes.contains(&cid("paperless-ocr")),
        "intake resumes"
    );
}

#[test]
fn forced_switches_give_in_flight_work_its_timeout() {
    let busy = || {
        Case::new()
            .gpu(&serving("mineru", 10 * MIN))
            .obs(pods("mineru", 1, 1, 0))
            .obs(demand("paperless-ocr", "paperless-ocr-pending", 2, 1, 0))
            .target(Some("llm"))
            .forced()
    };
    // Even before min_dwell a forced switch starts, but waits for the work.
    let early = Case::new()
        .gpu(&serving("mineru", MIN))
        .obs(pods("mineru", 1, 1, 0))
        .obs(demand("paperless-ocr", "paperless-ocr-pending", 2, 1, 0))
        .target(Some("llm"))
        .forced();
    let d = early.run();
    assert_steps(
        &d,
        vec![pause("paperless-ocr"), wait(G, WaitReason::InFlightWork)],
        "forced ignores min_dwell",
    );

    let d = busy().run();
    let c = busy();
    let before = c.run_at(&d.state, T + 10 * MIN - 1);
    assert_steps(
        &before,
        vec![wait(G, WaitReason::InFlightWork)],
        "one ms before the timeout",
    );
    let after = c.run_fresh_at(&d.state, T + 10 * MIN);
    assert_steps(
        &after,
        vec![kill("paperless-ocr"), drain("mineru"), allow("llm")],
        "at the timeout",
    );
    assert!(
        after
            .state
            .resource(&rid(G))
            .unwrap()
            .pending_recovery
            .is_empty(),
        "ocr declares no recovery"
    );
}

#[test]
fn reclaim_uses_reclaim_timeout_and_class_kill_timeout() {
    // A class-level kill_timeout shortens the wait for that class.
    let yaml = managed_yaml().replace(
        "    preemptibility: kill\n    demand: { source: paperless-ocr-pending }",
        "    preemptibility: kill\n    kill_timeout: 1m\n    demand: { source: paperless-ocr-pending }",
    );
    assert_ne!(yaml, managed_yaml());
    let config = Config::from_yaml(&yaml).unwrap();
    let mut c = Case::new()
        .gpu(&serving("mineru", 10 * MIN))
        .obs(pods("mineru", 1, 1, 0))
        .obs(demand("paperless-ocr", "paperless-ocr-pending", 2, 1, 0))
        .reclaim();
    c.config = config;
    let d = c.run();
    assert_steps(
        &d,
        vec![pause("paperless-ocr"), wait(G, WaitReason::InFlightWork)],
        "reclaim starts",
    );
    assert_steps(
        &c.run_at(&d.state, T + MIN - 1),
        vec![wait(G, WaitReason::InFlightWork)],
        "before class timeout",
    );
    assert_steps(
        &c.run_at(&d.state, T + MIN),
        vec![kill("paperless-ocr"), drain("mineru")],
        "class timeout elapsed",
    );

    // With a distinct reclaim_timeout the policy value applies.
    let yaml = managed_yaml().replace(
        "  drain_timeout: 10m",
        "  drain_timeout: 10m\n  reclaim_timeout: 2m",
    );
    let mut c = Case::new()
        .gpu(&serving("mineru", 10 * MIN))
        .obs(pods("mineru", 1, 1, 0))
        .obs(demand("paperless-ocr", "paperless-ocr-pending", 2, 1, 0))
        .reclaim();
    c.config = Config::from_yaml(&yaml).unwrap();
    let d = c.run();
    assert_steps(
        &c.run_at(&d.state, T + 2 * MIN - 1),
        vec![wait(G, WaitReason::InFlightWork)],
        "before reclaim timeout",
    );
    assert_steps(
        &c.run_at(&d.state, T + 2 * MIN),
        vec![kill("paperless-ocr"), drain("mineru")],
        "reclaim timeout elapsed",
    );
}

#[test]
fn reclaim_shuts_the_host_down_overriding_holds_and_grace() {
    let lease = format!(
        "{{ id: h, resource: {G}, granted_at: 0, kind: {{ type: shared_hold, holder: x }} }}"
    );
    // Idle, fresh and zero pods: powers off at once despite a hold and queued work.
    let d = Case::new()
        .lease(lease.clone())
        .obs(demand("paperless-ocr", "paperless-ocr-pending", 4, 0, 0))
        .reclaim()
        .run();
    assert_steps(
        &d,
        vec![Step::PowerOff { resource: rid(G) }],
        "reclaim powers off",
    );
    assert!(matches!(power_of(&d, G), PowerPhase::ShuttingDown { .. }));
    assert!(d.allowed.models.is_empty());

    let d = Case::new().obs(pods("llm", 2, 2, 31_000)).reclaim().run();
    assert_steps(
        &d,
        vec![Step::PowerOff { resource: rid(G) }],
        "pod readings, stale or not, are not consulted",
    );
    let d = Case::new().obs(up(G, true, 31_000)).reclaim().run();
    assert_steps(
        &d,
        vec![wait(G, WaitReason::StaleObservation)],
        "stale reachability holds the shutdown",
    );

    // A model allowed a second ago, with nothing in flight, is wound down and withdrawn
    // at once (a reclaim ignores min_dwell); the power-off follows on the next tick.
    let c = Case::new()
        .gpu(&format!(
            "{{ phase: loading, model: mineru, since: {} }}",
            T - 1000
        ))
        .reclaim();
    let d = c.obs(pods("mineru", 1, 0, 0)).run();
    assert_steps(
        &d,
        vec![pause("paperless-ocr"), drain("mineru")],
        "a just-allowed model is withdrawn at once",
    );

    let off = Case::new()
        .gpu_state("{ power: { phase: \"off\" }, mode: { phase: idle } }")
        .obs(up(G, false, 0))
        .reclaim()
        .want(G, PowerWant::Manual);
    let d = off.run();
    assert_steps(
        &d,
        vec![wait(G, WaitReason::Reclaimed)],
        "a reclaimed host is not woken, even if wanted",
    );
    assert!(!uses_gpu(&d));
}

#[test]
fn power_on_only_with_a_reason() {
    let off = || {
        Case::new()
            .gpu_state("{ power: { phase: \"off\" }, mode: { phase: idle } }")
            .obs(up(G, false, 0))
    };
    let d = off().run();
    assert_steps(&d, vec![], "no reason, no wake");
    assert!(matches!(power_of(&d, G), PowerPhase::Off));

    let d = off().target(Some("llm")).run();
    assert_steps(
        &d,
        vec![wait(G, WaitReason::HostNotUp)],
        "demand alone does not wake",
    );

    for want in [PowerWant::Manual, PowerWant::Workload] {
        let d = off().want(G, want).run();
        assert_steps(&d, vec![Step::PowerOn { resource: rid(G) }], "wanted");
        assert!(matches!(power_of(&d, G), PowerPhase::PoweringOn { .. }));
        assert!(d.state.resource(&rid(G)).unwrap().managed);
    }
}

#[test]
fn powering_on_completes_only_on_a_fresh_observation() {
    let c = || {
        Case::new().gpu_state(&format!(
            "{{ power: {{ phase: powering_on, since: {} }}, mode: {{ phase: idle }} }}",
            T - MIN
        ))
    };
    let d = c().obs(up(G, false, 0)).run();
    assert_steps(&d, vec![wait(G, WaitReason::PoweringOn)], "not yet");
    let d = c().obs(up(G, true, 31_000)).run();
    assert!(
        matches!(power_of(&d, G), PowerPhase::PoweringOn { .. }),
        "stale observation does not count"
    );
    let d = c().target(Some("llm")).run();
    assert!(matches!(power_of(&d, G), PowerPhase::On { .. }));
    assert_steps(
        &d,
        vec![allow("llm")],
        "the model can be allowed in the tick the host comes up",
    );
}

#[test]
fn a_host_that_vanishes_resets_its_mode() {
    let d = Case::new()
        .gpu(&serving("mineru", 10 * MIN))
        .obs(up(G, false, 0))
        .run();
    assert!(matches!(power_of(&d, G), PowerPhase::Off));
    assert!(matches!(mode_of(&d, G), ModePhase::Idle));
    assert!(d.allowed.models.is_empty() && !uses_gpu(&d));
}

#[test]
fn power_down_rules() {
    let managed = "{ power: { phase: \"on\", since: 0 }, mode: { phase: idle }, managed: true, host_idle_since: ";
    let nfs_idle = |since: u64| format!("{managed}{since} }}");
    let c = |since: u64| Case::new().nfs_state(&nfs_idle(since));

    let d = c(T - 10 * MIN + 1).run();
    assert_steps(&d, vec![], "grace not over");
    let d = c(T - 10 * MIN).run();
    assert_steps(&d, vec![Step::PowerOff { resource: rid(N) }], "grace over");
    assert!(!d.state.resource(&rid(N)).unwrap().managed);
    assert!(
        d.allowed.classes.is_empty(),
        "dependents are no longer allowed"
    );

    let d = Case::new()
        .nfs_state(&format!("{managed}{} }}", T - 10 * MIN))
        .run();
    assert_steps(&d, vec![Step::PowerOff { resource: rid(N) }], "baseline");

    let hold = format!(
        "{{ id: h, resource: {N}, granted_at: 0, kind: {{ type: shared_hold, holder: x }} }}"
    );
    let d = c(T - 99 * MIN).lease(hold).run();
    assert_steps(&d, vec![], "a shared hold keeps it up");
    assert_eq!(d.state.resource(&rid(N)).unwrap().host_idle_since, None);

    let manual =
        format!("{{ id: m, resource: {N}, granted_at: 0, kind: {{ type: manual_host }} }}");
    let d = c(T - 99 * MIN).lease(manual).run();
    assert_steps(&d, vec![], "a manual lease keeps it up");

    let expired = format!(
        "{{ id: i, resource: {G}, granted_at: 0, kind: {{ type: interactive, model: llm, expires_at: {T} }} }}"
    );
    let d = Case::new()
        .gpu_state(&format!("{managed}{} }}", T - 99 * MIN))
        .lease(expired)
        .run();
    assert_steps(
        &d,
        vec![Step::PowerOff { resource: rid(G) }],
        "an expired lease does not hold",
    );

    let live = format!(
        "{{ id: i, resource: {G}, granted_at: 0, kind: {{ type: interactive, model: llm, expires_at: {} }} }}",
        T + 1
    );
    let d = Case::new()
        .gpu_state(&format!("{managed}{} }}", T - 99 * MIN))
        .lease(live)
        .run();
    assert_steps(&d, vec![], "a live interactive lease holds the host");

    let d = c(T - 99 * MIN)
        .obs(demand("paperless-ocr", "paperless-ocr-pending", 1, 0, 0))
        .run();
    assert_steps(&d, vec![], "queued work holds");
    let d = c(T - 99 * MIN)
        .obs(demand("paperless-ocr", "paperless-ocr-pending", 0, 1, 0))
        .run();
    assert_steps(&d, vec![], "in-flight work of a class that needs it holds");
    let d = c(T - 99 * MIN).drop_obs("class: immich-ml").run();
    assert_steps(
        &d,
        vec![wait(N, WaitReason::StaleDemand)],
        "unknown demand holds, visibly",
    );

    let unmanaged = "{ power: { phase: \"on\", since: 0 }, mode: { phase: idle } }";
    let d = Case::new().nfs_state(unmanaged).run();
    assert_steps(&d, vec![], "a host found running is never powered down");

    // No model is allowed (mode idle): pod readings, stale or showing pods, are not
    // consulted. Kubernetes and KEDA have scaled the withdrawn model down.
    for p in [pods("llm", 0, 0, 31_000), pods("llm", 1, 1, 0)] {
        let d = Case::new()
            .gpu_state(&format!("{managed}{} }}", T - 99 * MIN))
            .obs(p)
            .run();
        assert_steps(
            &d,
            vec![Step::PowerOff { resource: rid(G) }],
            "pods do not stop the power down",
        );
    }
    let d = Case::new()
        .gpu_state(&format!("{managed}{} }}", T - 99 * MIN))
        .obs(up(G, true, 31_000))
        .run();
    assert_steps(
        &d,
        vec![wait(G, WaitReason::StaleObservation)],
        "stale reachability stops the power down",
    );

    let d = Case::new()
        .gpu_state(&format!("{{ power: {{ phase: \"on\", since: 0 }}, mode: {}, managed: true, host_idle_since: {} }}", serving("mineru", 99 * MIN), T - 99 * MIN))
        .obs(pods("mineru", 1, 1, 0))
        .target(Some("mineru"))
        .run();
    assert_steps(&d, vec![], "a host with a model loaded is not powered down");
}

#[test]
fn manual_request_takes_over_a_running_host() {
    let d = Case::new().want(N, PowerWant::Manual).run();
    assert!(d.state.resource(&rid(N)).unwrap().managed);
    let d = Case::new().want(N, PowerWant::Workload).run();
    assert!(
        !d.state.resource(&rid(N)).unwrap().managed,
        "workload demand never adopts a running host"
    );
}

#[test]
fn unseen_resources_are_initialised_from_observation() {
    let fresh = |c: &Case| c.run_at(&SchedState::default(), T);
    let d = fresh(&Case::new());
    assert!(matches!(power_of(&d, G), PowerPhase::On { .. }));
    assert!(matches!(power_of(&d, N), PowerPhase::On { .. }));

    let d = fresh(&Case::new().obs(up(G, false, 0)));
    assert!(matches!(power_of(&d, G), PowerPhase::Off));

    let d = fresh(&Case::new().drop_obs("resource: cuda-central"));
    assert_steps(
        &d,
        vec![wait(G, WaitReason::StaleObservation)],
        "no observation yet",
    );
    assert!(d.state.resource(&rid(G)).is_none(), "state is not invented");
    assert!(!uses_gpu(&d), "nothing is allowed on an unknown host");

    let d = fresh(&Case::new().obs(up(G, true, 31_000)));
    assert!(d.state.resource(&rid(G)).is_none());
}

#[test]
fn allowed_set_requires_every_requirement() {
    // The NFS-only classes, and the gate class that needs the GPU host and NFS up.
    let all_nfs: Vec<ClassId> = ["paperless-ngx", "immich-server", "paperless-gpt"]
        .iter()
        .map(|c| cid(c))
        .collect();
    let d = Case::new().run();
    assert_eq!(
        d.allowed,
        AllowedSet {
            models: [].into(),
            classes: all_nfs.iter().cloned().collect()
        }
    );

    let d = Case::new()
        .nfs_state("{ power: { phase: \"off\" }, mode: { phase: idle } }")
        .obs(up(N, false, 0))
        .run();
    assert!(d.allowed.classes.is_empty(), "NFS-only classes follow NFS");

    let d = Case::new()
        .gpu(&serving("llm", 10 * MIN))
        .obs(pods("llm", 1, 1, 0))
        .target(Some("llm"))
        .run();
    for c in [
        "paperless-tagging",
        "speakr-summarization",
        "interactive-llm",
    ] {
        assert!(d.allowed.classes.contains(&cid(c)), "{c}");
    }
    assert!(!d.allowed.classes.contains(&cid("paperless-ocr")));

    let d = Case::new()
        .gpu(&serving("llm", 10 * MIN))
        .nfs_state("{ power: { phase: \"off\" }, mode: { phase: idle } }")
        .obs(up(N, false, 0))
        .obs(pods("llm", 1, 1, 0))
        .target(Some("llm"))
        .run();
    assert!(
        d.allowed.classes.contains(&cid("interactive-llm")),
        "GPU-only class needs no NFS"
    );
    assert!(
        !d.allowed.classes.contains(&cid("paperless-tagging")),
        "needs NFS as well"
    );
    assert!(d.allowed.models.contains(&mid("llm")));
}

#[test]
fn rejects_generation_mismatch() {
    let c = Case::new();
    let y = "description: t\nsnapshot:\n  config_generation: 3\n  now: 1\n";
    let f = Fixture::from_yaml(y).unwrap();
    assert_eq!(
        tick(&c.config, &f.snapshot, &c.state(), &c.intent).unwrap_err(),
        TickError::GenerationMismatch {
            config: 0,
            snapshot: 3
        }
    );
}

#[test]
fn ticks_are_deterministic() {
    let c = Case::new().target(Some("mineru"));
    assert_eq!(c.run(), c.run());
}

fn managed_idle(since_ago: u64) -> String {
    format!(
        "{{ power: {{ phase: \"on\", since: 0 }}, mode: {{ phase: idle }}, managed: true, host_idle_since: {} }}",
        T - since_ago
    )
}

fn lease_on(id: &str, res: &str, kind: &str) -> String {
    format!("{{ id: {id}, resource: {res}, granted_at: 0, kind: {kind} }}")
}

#[test]
fn a_reclaim_on_a_shared_resource_is_ignored_and_never_overrides_holds() {
    let hold = lease_on("h", N, "{ type: shared_hold, holder: x }");
    let reclaim = lease_on("r", N, "{ type: reclaim }");
    let mut c = Case::new()
        .nfs_state(&managed_idle(99 * MIN))
        .lease(hold)
        .lease(reclaim);
    // A planner bug (or a hand-made intent) that names the shared resource changes nothing.
    c.intent.reclaim.insert(rid(N));
    let d = c.run();
    assert!(
        !d.steps.contains(&Step::PowerOff { resource: rid(N) }),
        "{:?}",
        d.steps
    );
    assert!(d.steps.contains(&wait(N, WaitReason::ReclaimUnsupported)));
    assert!(matches!(power_of(&d, N), PowerPhase::On { .. }));
    assert!(
        d.allowed.classes.contains(&cid("paperless-ngx")),
        "NFS-only classes keep running"
    );

    // The same lease with an empty intent (the planner filtered it) is still reported.
    let d = Case::new()
        .nfs_state(&managed_idle(99 * MIN))
        .lease(lease_on("h", N, "{ type: shared_hold, holder: x }"))
        .lease(lease_on("r", N, "{ type: reclaim }"))
        .run();
    assert!(d.steps.contains(&wait(N, WaitReason::ReclaimUnsupported)));
    assert!(!d.steps.contains(&Step::PowerOff { resource: rid(N) }));
}

#[test]
fn an_unforced_switch_is_cut_off_at_the_class_timeout() {
    let yaml = managed_yaml().replace(
        "    preemptibility: kill\n    demand: { source: paperless-ocr-pending }",
        "    preemptibility: kill\n    kill_timeout: 1m\n    demand: { source: paperless-ocr-pending }",
    );
    assert_ne!(yaml, managed_yaml());
    let busy = || {
        let mut c = Case::new()
            .gpu(&serving("mineru", 10 * MIN))
            .obs(pods("mineru", 1, 1, 0))
            .obs(demand("paperless-ocr", "paperless-ocr-pending", 2, 1, 0))
            .target(Some("llm"));
        c.config = Config::from_yaml(&yaml).unwrap();
        c
    };
    let d = busy().run();
    // The wind-down's own timeout is drain_timeout; the class's kill_timeout is shorter.
    assert_eq!(
        wind_down(&d).unwrap().timeout,
        Some(sched_model::Millis(10 * MIN))
    );
    let before = busy().run_fresh_at(&d.state, T + MIN - 1);
    assert_steps(
        &before,
        vec![wait(G, WaitReason::InFlightWork)],
        "just before",
    );
    let after = busy().run_fresh_at(&d.state, T + MIN);
    assert_steps(
        &after,
        vec![kill("paperless-ocr"), drain("mineru"), allow("llm")],
        "at the class's kill_timeout, long before drain_timeout",
    );
}

/// A serving class that is fed for ever and another that is starved: the switch happens
/// exactly drain_timeout after the wind-down starts, minute by minute, whichever class is fed.
#[test]
fn a_continuously_fed_serving_class_cannot_hold_the_resource() {
    for (fed, source, preempt_note) in [
        ("speakr-summarization", "speakr-summaries", "pause_intake"),
        ("paperless-tagging", "paperless-post-ocr", "kill"),
    ] {
        let tick = |state: &SchedState, now: u64| {
            let mut c = Case::new()
                .gpu(&serving("llm", 30 * MIN))
                .obs(pods("llm", 1, 1, 0))
                .obs(demand(fed, source, 5, 1, 0))
                .target(Some("mineru"));
            c.config = Config::from_yaml(&managed_yaml()).unwrap();
            c.run_fresh_at(state, now)
        };
        // The serving state is built by the helper; its first tick starts the wind-down.
        let mut d = Case::new()
            .gpu(&serving("llm", 30 * MIN))
            .obs(pods("llm", 1, 1, 0))
            .obs(demand(fed, source, 5, 1, 0))
            .target(Some("mineru"))
            .run();
        let start = T;
        assert_eq!(wind_down(&d).unwrap().since, sched_model::Millis(start));
        for m in 1..10 {
            d = tick(&d.state, start + m * MIN);
            assert!(
                !d.steps
                    .iter()
                    .any(|s| matches!(s, Step::KillInFlight { .. } | Step::DrainModel { .. })),
                "{fed} ({preempt_note}) switched at +{m}m: {:?}",
                d.steps
            );
        }
        let d = tick(&d.state, start + 10 * MIN - 1);
        assert_steps(&d, vec![wait(G, WaitReason::InFlightWork)], "1ms before");
        let d = tick(&d.state, start + 10 * MIN);
        assert_steps(
            &d,
            vec![kill(fed), drain("llm"), allow("mineru")],
            &format!("{fed} cut off exactly at drain_timeout"),
        );
    }
}

/// Only a busy `none` class blocks without a timeout.
#[test]
fn a_busy_class_that_cannot_be_preempted_still_blocks_without_a_timeout() {
    let busy = |interactive_in_flight: u64| {
        Case::new()
            .gpu(&serving("llm", 30 * MIN))
            .obs(pods("llm", 1, 1, 0))
            .obs(demand("speakr-summarization", "speakr-summaries", 5, 1, 0))
            .obs(demand(
                "interactive-llm",
                "llm-sessions",
                0,
                interactive_in_flight,
                0,
            ))
            .target(Some("mineru"))
    };
    // Blocked from the start: no wind-down begins, the other class is not even paused.
    let d = busy(1).run();
    assert_steps(&d, vec![wait(G, WaitReason::NotPreemptible)], "blocked");
    assert_eq!(wind_down(&d), None);
    for later in [10 * MIN, 24 * 60 * MIN, 7 * 24 * 60 * MIN] {
        let d = busy(1).run_fresh_at(&d.state, T + later);
        assert_steps(
            &d,
            vec![wait(G, WaitReason::NotPreemptible)],
            "no timeout for a none class",
        );
    }
    // It becomes busy during a wind-down that is already past its timeout: still blocked,
    // and the other class is cut off as soon as it is free (its clock kept running).
    let started = busy(0).run();
    assert_steps(
        &started,
        vec![
            pause("paperless-tagging"),
            pause("speakr-summarization"),
            pause("interactive-llm"),
            wait(G, WaitReason::InFlightWork),
        ],
        "wind-down started",
    );
    let held = busy(1).run_fresh_at(&started.state, T + 60 * MIN);
    assert_steps(&held, vec![wait(G, WaitReason::NotPreemptible)], "held");
    let freed = busy(0).run_fresh_at(&held.state, T + 61 * MIN);
    assert_steps(
        &freed,
        vec![kill("speakr-summarization"), drain("llm"), allow("mineru")],
        "released: the elapsed timeout applies",
    );
}

#[test]
fn per_class_timeouts_also_apply_unforced() {
    let yaml = managed_yaml().replace(
        "    preemptibility: kill\n    demand: { source: paperless-post-ocr }",
        "    preemptibility: kill\n    kill_timeout: 1m\n    demand: { source: paperless-post-ocr }",
    );
    assert_ne!(yaml, managed_yaml());
    let busy = || {
        let mut c = Case::new()
            .gpu(&serving("llm", 10 * MIN))
            .obs(pods("llm", 1, 1, 0))
            .obs(demand("paperless-tagging", "paperless-post-ocr", 0, 1, 0))
            .obs(demand("speakr-summarization", "speakr-summaries", 0, 1, 0))
            .target(Some("mineru"));
        c.config = Config::from_yaml(&yaml).unwrap();
        c
    };
    let d0 = busy().run();
    let d1 = busy().run_fresh_at(&d0.state, T + MIN);
    assert_steps(
        &d1,
        vec![kill("paperless-tagging"), wait(G, WaitReason::InFlightWork)],
        "the shorter class is cut off first, alone",
    );
    let d2 = busy().run_fresh_at(&d1.state, T + 10 * MIN - 1);
    assert_steps(
        &d2,
        vec![wait(G, WaitReason::InFlightWork)],
        "no second kill",
    );
    let d3 = busy().run_fresh_at(&d2.state, T + 10 * MIN);
    assert_steps(
        &d3,
        vec![kill("speakr-summarization"), drain("llm"), allow("mineru")],
        "the other at drain_timeout",
    );
}

fn wind_down(d: &Decision) -> Option<sched_model::WindDown> {
    match mode_of(d, G) {
        ModePhase::Serving { wind_down, .. } => wind_down,
        _ => None,
    }
}

/// A wind-down persisted before unforced wind-downs had a timeout (`timeout` absent) gets the
/// policy's timeout filled in, and keeps its own `since`: the clock neither restarts nor
/// stays without a bound.
#[test]
fn a_legacy_wind_down_without_a_timeout_gets_one_and_keeps_its_clock() {
    let busy = |state: &str| {
        Case::new()
            .gpu(state)
            .obs(pods("mineru", 1, 1, 0))
            .obs(demand("paperless-ocr", "paperless-ocr-pending", 2, 1, 0))
            .target(Some("llm"))
            .run()
    };
    let legacy = format!(
        "{{ phase: serving, model: mineru, since: {}, wind_down: {{ since: {} }} }}",
        T - 30 * MIN,
        T - 3 * MIN
    );
    let d = busy(&legacy);
    assert_eq!(
        wind_down(&d),
        Some(sched_model::WindDown {
            since: sched_model::Millis(T - 3 * MIN),
            timeout: Some(sched_model::Millis(10 * MIN)),
            reclaim: false,
            killed: Default::default(),
        })
    );
    // Not started again: no new PauseIntake, and still waiting for the in-flight work.
    assert_steps(&d, vec![wait(G, WaitReason::InFlightWork)], "continuing");
}

/// The planner reads `idle_since` to decide when an idle model has waited `idle_grace`, so
/// it must follow the queue on every path: a value left from a tick when the queue read empty
/// must not survive into ticks when the model is busy (here, while it winds down).
#[test]
fn idle_since_is_cleared_while_busy_even_when_winding_down_or_waiting() {
    let stale = |since_ago: u64, wind_down: &str| {
        format!(
            "{{ phase: serving, model: mineru, since: {}, idle_since: {}{wind_down} }}",
            T - since_ago,
            T - 9 * MIN
        )
    };
    let idle_since = |d: &Decision| match mode_of(d, G) {
        ModePhase::Serving { idle_since, .. } => idle_since,
        other => panic!("{other:?}"),
    };
    let busy = |state: String| {
        Case::new()
            .gpu(&state)
            .obs(pods("mineru", 1, 1, 0))
            .obs(demand("paperless-ocr", "paperless-ocr-pending", 2, 1, 0))
            .target(Some("llm"))
            .run()
    };
    // Starting a wind-down.
    let d = busy(stale(30 * MIN, ""));
    assert!(wind_down(&d).is_some());
    assert_eq!(idle_since(&d), None, "starting a wind-down");
    // Continuing one.
    let d = busy(stale(
        30 * MIN,
        &format!(", wind_down: {{ since: {}, timeout: 600000 }}", T - MIN),
    ));
    assert_eq!(idle_since(&d), None, "continuing a wind-down");
    // Waiting out min_dwell.
    let d = busy(stale(MIN, ""));
    assert_steps(&d, vec![wait(G, WaitReason::MinDwell)], "min dwell");
    assert_eq!(idle_since(&d), None, "waiting out min_dwell");
}

/// A state that says the host is off but a model is serving (never written by this build, but
/// persisted state is data) must not publish the model: with no fresh reachability reading to
/// reconcile it, the allowed set itself must refuse a model on a host that is not on.
#[test]
fn a_model_on_a_host_recorded_as_off_is_never_allowed() {
    let c = Case::new()
        .gpu_state(&format!(
            "{{ power: {{ phase: off }}, mode: {{ phase: serving, model: mineru, since: {} }} }}",
            T - 30 * MIN
        ))
        .drop_obs("resource: cuda-central")
        .obs(demand("paperless-ocr", "paperless-ocr-pending", 2, 1, 0))
        .target(Some("mineru"));
    let d = c.run();
    assert!(
        !d.allowed.models.contains(&mid("mineru")),
        "host recorded off and unobserved, yet {:?}",
        d.allowed
    );
    assert!(!d.allowed.classes.contains(&cid("paperless-ocr")));
}

#[test]
fn each_class_is_stopped_when_its_own_timeout_expires() {
    let yaml = managed_yaml().replace(
        "    preemptibility: kill\n    demand: { source: paperless-post-ocr }",
        "    preemptibility: kill\n    kill_timeout: 1m\n    demand: { source: paperless-post-ocr }",
    );
    assert_ne!(yaml, managed_yaml());
    let busy = || {
        let mut c = Case::new()
            .gpu(&serving("llm", 10 * MIN))
            .obs(pods("llm", 1, 1, 0))
            .obs(demand("paperless-tagging", "paperless-post-ocr", 0, 1, 0))
            .obs(demand("speakr-summarization", "speakr-summaries", 0, 1, 0))
            .reclaim();
        c.config = Config::from_yaml(&yaml).unwrap();
        c
    };
    let d0 = busy().run();
    assert_steps(
        &d0,
        vec![
            pause("paperless-tagging"),
            pause("speakr-summarization"),
            pause("interactive-llm"),
            wait(G, WaitReason::InFlightWork),
        ],
        "start",
    );
    // Tagging's own timeout (1m) is shorter than the reclaim timeout (10m).
    let d1 = busy().run_at(&d0.state, T + MIN);
    assert_steps(
        &d1,
        vec![kill("paperless-tagging"), wait(G, WaitReason::InFlightWork)],
        "tagging stopped alone",
    );
    assert!(
        wind_down(&d1)
            .unwrap()
            .killed
            .contains(&cid("paperless-tagging"))
    );
    // The observation still shows tagging in flight; it is not stopped twice and no longer blocks.
    let d2 = busy().run_at(&d1.state, T + 2 * MIN);
    assert_steps(
        &d2,
        vec![wait(G, WaitReason::InFlightWork)],
        "no second kill",
    );
    let d3 = busy().run_at(&d2.state, T + 10 * MIN);
    assert_steps(
        &d3,
        vec![kill("speakr-summarization"), drain("llm")],
        "the slower class at its own timeout",
    );
}

#[test]
fn idle_clock_restarts_when_work_reappears() {
    let c = |t: Option<&str>| {
        let mut x = Case::new()
            .gpu(&format!(
                "{{ phase: serving, model: mineru, since: {}, idle_since: {} }}",
                T - 30 * MIN,
                T - 9 * MIN
            ))
            .obs(pods("mineru", 1, 1, 0))
            .target(Some("mineru"));
        if let Some(t) = t {
            x = x
                .obs(demand("paperless-ocr", "paperless-ocr-pending", 1, 0, 0))
                .obs(demand("paperless-ocr", t, 1, 0, 0));
        }
        x
    };
    let idle_since = |d: &Decision| match mode_of(d, G) {
        ModePhase::Serving { idle_since, .. } => idle_since,
        other => panic!("{other:?}"),
    };
    let d = c(Some("paperless-ocr-pending")).run();
    assert_eq!(idle_since(&d), None, "work cleared the idle clock");
    // Work ends again five minutes later: the clock starts then, not nine minutes earlier.
    let later = c(None).run_at(&d.state, T + 5 * MIN);
    assert_eq!(idle_since(&later), Some(sched_model::Millis(T + 5 * MIN)));
    // Without work at all the original clock is kept.
    let kept = c(None).run();
    assert_eq!(idle_since(&kept), Some(sched_model::Millis(T - 9 * MIN)));
}

fn future(model_line: &str) -> String {
    model_line.replace(
        &format!("observed_at: {T}"),
        &format!("observed_at: {}", T + 1),
    )
}

#[test]
fn future_dated_observations_are_not_fresh() {
    let line = up(G, true, 0);
    assert!(line.contains(&format!("observed_at: {T}")));
    let d = Case::new().target(Some("mineru")).obs(future(&line)).run();
    assert_steps(
        &d,
        vec![wait(G, WaitReason::StaleObservation)],
        "a future-dated up reading blocks allowing",
    );

    // A fresh older observation does not rescue a future-dated newest one.
    let d = Case::new()
        .target(Some("mineru"))
        .obs(up(G, true, 1000))
        .run();
    assert_steps(
        &d,
        vec![allow("mineru")],
        "baseline: an older fresh observation is fine alone",
    );
    let mut c = Case::new().target(Some("mineru")).obs(up(G, true, 1000));
    c.obs.push(future(&line));
    assert_steps(
        &c.run(),
        vec![wait(G, WaitReason::StaleObservation)],
        "newest wins even if future",
    );

    // Future-dated demand is unknown, so it cannot prove a model idle.
    let c = Case::new()
        .gpu(&serving("mineru", 10 * MIN))
        .obs(pods("mineru", 1, 1, 0))
        .target(Some("llm"))
        .obs(future(&demand(
            "paperless-ocr",
            "paperless-ocr-pending",
            0,
            0,
            0,
        )));
    assert_steps(
        &c.run(),
        vec![wait(G, WaitReason::StaleDemand)],
        "future demand is unknown",
    );

    // An unseen host with a future-dated up observation is not initialised.
    let d = Case::new().drop_obs("resource: cuda-central");
    let d = d
        .obs(future(&up(G, true, 0)))
        .run_at(&SchedState::default(), T);
    assert!(d.state.resource(&rid(G)).is_none());
}

/// What a change of cause during a wind-down does to `since` and the timeout: the clock
/// restarts exactly when the cause flips between reclaim and not-reclaim; a manual lease
/// coming or going (both use drain_timeout) leaves it alone.
#[test]
fn a_cause_change_restarts_the_clock_only_when_reclaim_flips() {
    let yaml = managed_yaml().replace(
        "  drain_timeout: 10m",
        "  drain_timeout: 10m\n  reclaim_timeout: 2m",
    );
    // `cause`: 0 unforced, 1 manual lease, 2 reclaim.
    let base = |cause: u8| {
        let mut c = Case::new()
            .gpu(&serving("mineru", 30 * MIN))
            .obs(pods("mineru", 1, 1, 0))
            .obs(demand("paperless-ocr", "paperless-ocr-pending", 2, 1, 0));
        c = if cause == 2 {
            c.reclaim()
        } else {
            c.target(Some("llm"))
        };
        if cause == 1 {
            c = c.forced();
        }
        c.config = Config::from_yaml(&yaml).unwrap();
        c
    };
    let ms = sched_model::Millis;
    let shape = |d: &Decision| {
        let wd = wind_down(d).unwrap();
        (wd.since, wd.timeout, wd.reclaim)
    };
    // Unforced start: already bounded by drain_timeout.
    let d0 = base(0).run();
    assert_eq!(shape(&d0), (ms(T), Some(ms(10 * MIN)), false));

    // unforced -> manual lease: same timeout, same clock.
    let d1 = base(1).run_fresh_at(&d0.state, T + 5 * MIN);
    assert_eq!(shape(&d1), (ms(T), Some(ms(10 * MIN)), false));
    // manual lease -> unforced again: likewise.
    let d1b = base(0).run_fresh_at(&d1.state, T + 6 * MIN);
    assert_eq!(shape(&d1b), (ms(T), Some(ms(10 * MIN)), false));
    assert_steps(
        &base(0).run_fresh_at(&d1b.state, T + 10 * MIN),
        vec![kill("paperless-ocr"), drain("mineru"), allow("llm")],
        "the original cut-off still holds after the lease came and went",
    );

    // unforced -> reclaim: restarts with the reclaim timeout.
    let d2 = base(2).run_fresh_at(&d0.state, T + 5 * MIN);
    assert_steps(
        &d2,
        vec![wait(G, WaitReason::InFlightWork)],
        "the reclaim restarts rather than inheriting the elapsed time",
    );
    assert_eq!(shape(&d2), (ms(T + 5 * MIN), Some(ms(2 * MIN)), true));
    let d2b = base(2).run_fresh_at(&d2.state, T + 7 * MIN - 1);
    assert_steps(&d2b, vec![wait(G, WaitReason::InFlightWork)], "not yet");
    assert_steps(
        &base(2).run_fresh_at(&d2b.state, T + 7 * MIN),
        vec![kill("paperless-ocr"), drain("mineru")],
        "reclaim timeout over",
    );

    // reclaim -> unforced (the reclaim ended): restarts with drain_timeout, never no timeout.
    let d3 = base(0).run_fresh_at(&d2.state, T + 6 * MIN);
    assert_eq!(shape(&d3), (ms(T + 6 * MIN), Some(ms(10 * MIN)), false));
    assert_steps(
        &base(0).run_fresh_at(&d3.state, T + 16 * MIN - 1),
        vec![wait(G, WaitReason::InFlightWork)],
        "fresh drain_timeout not yet over",
    );
    assert_steps(
        &base(0).run_fresh_at(&d3.state, T + 16 * MIN),
        vec![kill("paperless-ocr"), drain("mineru"), allow("llm")],
        "fresh drain_timeout over",
    );

    // manual lease -> reclaim -> manual lease: each flip restarts.
    let m0 = base(1).run();
    let m1 = base(2).run_fresh_at(&m0.state, T + MIN);
    let m2 = base(1).run_fresh_at(&m1.state, T + 2 * MIN);
    assert_eq!(shape(&m2), (ms(T + 2 * MIN), Some(ms(10 * MIN)), false));

    // A state written before unforced wind-downs had a timeout (None) gets one, keeping
    // `since`; it is then due at once if that is old enough.
    let mut old = d0.state.clone();
    if let Some(ModePhase::Serving { wind_down, .. }) =
        old.resources.values_mut().next().map(|r| &mut r.mode)
    {
        wind_down.as_mut().unwrap().timeout = None;
    }
    let d4 = base(0).run_fresh_at(&old, T + 3 * MIN);
    assert_eq!(shape(&d4), (ms(T), Some(ms(10 * MIN)), false));
    assert_steps(&d4, vec![wait(G, WaitReason::InFlightWork)], "filled in");

    // Classes already stopped stay recorded across a flip.
    let yaml_k = yaml.replace(
        "    preemptibility: kill\n    demand: { source: paperless-post-ocr }",
        "    preemptibility: kill\n    kill_timeout: 1m\n    demand: { source: paperless-post-ocr }",
    );
    assert_ne!(yaml_k, yaml);
    let llm = |reclaim: bool| {
        let mut c = Case::new()
            .gpu(&serving("llm", 30 * MIN))
            .obs(pods("llm", 1, 1, 0))
            .obs(demand("paperless-tagging", "paperless-post-ocr", 0, 1, 0))
            .obs(demand("speakr-summarization", "speakr-summaries", 0, 1, 0));
        c = if reclaim {
            c.reclaim()
        } else {
            c.target(Some("mineru"))
        };
        c.config = Config::from_yaml(&yaml_k).unwrap();
        c
    };
    let k0 = llm(false).run();
    let k1 = llm(false).run_fresh_at(&k0.state, T + MIN);
    assert!(
        k1.steps.contains(&kill("paperless-tagging")),
        "{:?}",
        k1.steps
    );
    let k2 = llm(true).run_fresh_at(&k1.state, T + 2 * MIN);
    assert_steps(&k2, vec![wait(G, WaitReason::InFlightWork)], "flipped");
    let wd = wind_down(&k2).unwrap();
    assert_eq!((wd.since, wd.reclaim), (ms(T + 2 * MIN), true));
    assert!(wd.killed.contains(&cid("paperless-tagging")));
}

// ---- power.control: manual (the example config as shipped) ----

fn manual(mut c: Case) -> Case {
    c.config = example_config();
    c
}

fn host_state(power: &str, mode: &str, managed: bool) -> String {
    format!("{{ power: {power}, mode: {mode}, managed: {managed} }}")
}

#[test]
fn manual_control_never_powers_a_host_on_and_waits_for_a_person() {
    let lease = lease_on("m", G, "{ type: manual_model, model: llm }");
    let c = manual(
        Case::new()
            .gpu_state(&host_state("{ phase: \"off\" }", "{ phase: idle }", false))
            .obs(up(G, false, 0))
            .lease(lease)
            .target(Some("llm"))
            .want(G, PowerWant::Manual),
    );
    let d = c.run();
    assert_steps(
        &d,
        vec![wait(G, WaitReason::HostNotUp)],
        "manual: no power on",
    );
    assert_eq!(power_of(&d, G), PowerPhase::Off);
    assert!(!d.state.resource(&rid(G)).unwrap().managed);
    // The same request under managed control powers it on.
    let mut m = c;
    m.config = example_config_managed();
    assert!(m.run().steps.contains(&Step::PowerOn { resource: rid(G) }));
    // A person powers it on: the phase follows the observation and the model is allowed.
    let on = manual(
        Case::new()
            .gpu_state(&host_state("{ phase: \"off\" }", "{ phase: idle }", false))
            .target(Some("llm"))
            .want(G, PowerWant::Manual),
    );
    let d = on.run();
    assert!(matches!(power_of(&d, G), PowerPhase::On { .. }));
    assert_steps(&d, vec![allow("llm")], "allowed once up");
    assert!(
        !d.state.resource(&rid(G)).unwrap().managed,
        "never taken over"
    );
}

#[test]
fn manual_control_never_powers_a_host_off_however_long_it_is_idle() {
    // Idle for hours, nothing holding it, every demand known and zero, and the state even
    // says it is managed (left from a managed build): still no power off.
    for nfs_mode in [managed_idle(99 * MIN), managed_idle(0)] {
        let c = manual(
            Case::new()
                .gpu_state(&managed_idle(99 * MIN))
                .nfs_state(&nfs_mode),
        );
        let mut st = c.state();
        for i in 0..5 {
            let d = c.run_fresh_at(&st, T + i * 30 * MIN);
            assert!(
                !d.steps
                    .iter()
                    .any(|s| matches!(s, Step::PowerOff { .. } | Step::PowerOn { .. })),
                "{:?}",
                d.steps
            );
            for r in [G, N] {
                let rs = d.state.resource(&rid(r)).unwrap();
                assert!(rs.is_on() && !rs.managed && rs.host_idle_since.is_none());
            }
            st = d.state;
        }
    }
    // Under managed control the same state powers off.
    let mut m = Case::new().gpu_state(&managed_idle(99 * MIN));
    m.config = example_config_managed();
    assert!(m.run().steps.contains(&Step::PowerOff { resource: rid(G) }));
}

#[test]
fn manual_control_converges_transitional_phases_to_what_is_observed() {
    let shutting = "{ phase: shutting_down, since: 5 }";
    let powering = "{ phase: powering_on, since: 5 }";
    // Shutting down but observed up: on again, and the model can be allowed.
    let d = manual(
        Case::new()
            .gpu_state(&host_state(shutting, "{ phase: idle }", true))
            .target(Some("mineru")),
    )
    .run();
    assert!(matches!(power_of(&d, G), PowerPhase::On { .. }), "{d:?}");
    assert!(!d.state.resource(&rid(G)).unwrap().managed);
    assert_steps(&d, vec![allow("mineru")], "converged and allowed");
    // Powering on but observed down: off.
    let d = manual(
        Case::new()
            .gpu_state(&host_state(powering, "{ phase: idle }", true))
            .obs(up(G, false, 0)),
    )
    .run();
    assert_eq!(power_of(&d, G), PowerPhase::Off);
    // Without a fresh reading the phase cannot be known yet: it holds, visibly.
    let d = manual(
        Case::new()
            .gpu_state(&host_state(shutting, "{ phase: idle }", true))
            .drop_obs("type: resource_up, resource: cuda-central")
            .target(Some("mineru")),
    )
    .run();
    assert_steps(&d, vec![wait(G, WaitReason::ShuttingDown)], "stale: hold");
    assert!(d.allowed.models.is_empty());
    // Under managed control a host observed up stays shutting down (the managed machine
    // waits for it to go down).
    let recent = format!("{{ phase: shutting_down, since: {} }}", T - 1000);
    let mut m = Case::new().gpu_state(&host_state(&recent, "{ phase: idle }", true));
    m.config = example_config_managed();
    assert!(matches!(
        power_of(&m.run(), G),
        PowerPhase::ShuttingDown { .. }
    ));
}

#[test]
fn manual_control_reclaim_clears_the_model_but_does_not_power_off() {
    let d = manual(Case::new().reclaim()).run();
    assert!(
        !d.steps.iter().any(|s| matches!(s, Step::PowerOff { .. })),
        "{:?}",
        d.steps
    );
    assert!(d.allowed.models.is_empty());
    assert!(matches!(power_of(&d, G), PowerPhase::On { .. }));
}

// RunRecovery for a class whose action re-queues failed items: while the class is allowed
// and the snapshot reports failed items, never otherwise.

fn failed(class: &str, source: &str, failed: u64, age: u64) -> String {
    format!(
        "    - {{ source: {source}, observed_at: {}, fidelity: polled, value: {{ type: demand, class: {class}, ready: 0, in_flight: 0, failed: {failed} }} }}\n",
        T - age
    )
}

fn failed_transcriptions(n: u64) -> String {
    failed("speakr-transcription", "speakr-pending", n, 0)
}

fn recovery() -> Step {
    Step::RunRecovery {
        class: cid("speakr-transcription"),
    }
}

fn recoveries(d: &Decision) -> usize {
    d.steps
        .iter()
        .filter(|s| matches!(s, Step::RunRecovery { .. }))
        .count()
}

#[test]
fn recovery_runs_while_the_model_serves_and_items_have_failed() {
    let serve = || {
        Case::new()
            .gpu(&serving("whisperx", 10 * MIN))
            .target(Some("whisperx"))
            .obs(pods("whisperx", 1, 1, 0))
    };
    // Failed items, model serving: emitted, on every tick (it is idempotent).
    let d = serve().obs(failed_transcriptions(2)).run();
    assert_steps(&d, vec![recovery()], "failed items");
    let d2 = serve().obs(failed_transcriptions(2)).run_at(&d.state, T);
    assert_steps(&d2, vec![recovery()], "again on the next tick");
    // No failed items: nothing.
    let d = serve().obs(failed_transcriptions(0)).run();
    assert_eq!(recoveries(&d), 0, "{:?}", d.steps);
    // Ready work alone is no failure.
    let d = serve()
        .obs(demand("speakr-transcription", "speakr-pending", 3, 0, 0))
        .run();
    assert_eq!(recoveries(&d), 0, "{:?}", d.steps);
}

#[test]
fn recovery_never_runs_while_the_class_is_not_allowed() {
    let items = || failed_transcriptions(2);
    // Another model is serving.
    let d = Case::new()
        .gpu(&serving("mineru", 10 * MIN))
        .target(Some("mineru"))
        .obs(items())
        .run();
    assert_eq!(recoveries(&d), 0, "{:?}", d.steps);
    // Nothing serving and nothing wanted.
    let d = Case::new().target(None).obs(items()).run();
    assert_eq!(recoveries(&d), 0, "{:?}", d.steps);
    // The model is winding down (forced switch with work in flight).
    let d = Case::new()
        .gpu(&serving("whisperx", 10 * MIN))
        .target(Some("llm"))
        .forced()
        .obs(items())
        .obs(
            demand("speakr-transcription", "speakr-pending", 0, 1, 0).replace(
                "ready: 0, in_flight: 1",
                "ready: 0, in_flight: 1, failed: 2",
            ),
        )
        .run();
    assert_eq!(recoveries(&d), 0, "{:?}", d.steps);
    assert!(matches!(
        mode_of(&d, G),
        ModePhase::Serving {
            wind_down: Some(_),
            ..
        }
    ));
    // The model is withdrawn in this tick: no recovery with it.
    let d = Case::new()
        .gpu(&serving("whisperx", 10 * MIN))
        .target(Some("mineru"))
        .obs(items())
        .run();
    assert_eq!(recoveries(&d), 0, "{:?}", d.steps);
    // The NFS share the class also needs is down.
    let d = Case::new()
        .gpu(&serving("whisperx", 10 * MIN))
        .target(Some("whisperx"))
        .nfs_state("{ power: { phase: \"off\" }, mode: { phase: idle } }")
        .obs(up(N, false, 0))
        .obs(items())
        .run();
    assert_eq!(recoveries(&d), 0, "{:?}", d.steps);
    // Reclaimed.
    let d = Case::new()
        .gpu(&serving("whisperx", 10 * MIN))
        .reclaim()
        .obs(items())
        .run();
    assert_eq!(recoveries(&d), 0, "{:?}", d.steps);
    // Stale demand is unknown, not failed.
    let d = Case::new()
        .gpu(&serving("whisperx", 10 * MIN))
        .target(Some("whisperx"))
        .obs(failed(
            "speakr-transcription",
            "speakr-pending",
            2,
            11 * MIN,
        ))
        .run();
    assert_eq!(recoveries(&d), 0, "{:?}", d.steps);
}

#[test]
fn recovery_waits_for_a_ready_pod_but_not_for_an_unknown_reading() {
    let case = |p: Option<String>| {
        let c = Case::new()
            .target(Some("whisperx"))
            .obs(failed_transcriptions(1));
        match p {
            Some(p) => c.obs(p),
            None => c,
        }
    };
    // The tick that allows the model: the pod does not exist yet, so the requeue would be lost.
    let d = case(Some(pods("whisperx", 0, 0, 0))).run();
    assert!(d.steps.contains(&allow("whisperx")), "{:?}", d.steps);
    assert_eq!(recoveries(&d), 0, "{:?}", d.steps);
    // Starting, not ready.
    let d = case(Some(pods("whisperx", 1, 0, 0))).run();
    assert_eq!(recoveries(&d), 0, "{:?}", d.steps);
    // Ready: the first tick that sees it.
    let d = case(Some(pods("whisperx", 1, 1, 0))).run();
    assert!(d.steps.contains(&recovery()), "{:?}", d.steps);
    // Unknown (stale) is not a reason to hold back: pods sources are optional.
    let d = case(Some(pods("whisperx", 0, 0, 11 * MIN))).run();
    assert!(d.steps.contains(&recovery()), "{:?}", d.steps);
    // A rollout with two pods and none ready yet.
    let d = case(Some(pods("whisperx", 2, 0, 0))).run();
    assert_eq!(recoveries(&d), 0, "{:?}", d.steps);
}

#[test]
fn a_killed_class_whose_action_needs_its_model_records_no_recovery() {
    // Cut off after its timeout: the model is withdrawn, so re-queueing would have to wait
    // for the model anyway; the failed items it leaves bring the model back instead.
    let busy = || {
        Case::new()
            .gpu(&serving("whisperx", 10 * MIN))
            .target(Some("llm"))
            .forced()
            .obs(demand("speakr-transcription", "speakr-pending", 0, 1, 0))
    };
    let d = busy().run();
    let after = busy().run_fresh_at(&d.state, T + 10 * MIN);
    assert!(
        after.steps.contains(&kill("speakr-transcription")),
        "{:?}",
        after.steps
    );
    assert_eq!(recoveries(&after), 0);
    assert!(
        after
            .state
            .resource(&rid(G))
            .unwrap()
            .pending_recovery
            .is_empty()
    );
}

// ---- power transition timeouts: alert and stop, never retry --------------------------

const POWER_ON_TIMEOUT: u64 = 10 * MIN;
const SHUTDOWN_TIMEOUT: u64 = 15 * MIN;

fn powering_on_since(ago: u64) -> String {
    format!(
        "{{ power: {{ phase: powering_on, since: {} }}, mode: {{ phase: idle }}, managed: true }}",
        T - ago
    )
}

fn shutting_down_since(ago: u64) -> String {
    format!(
        "{{ power: {{ phase: shutting_down, since: {} }}, mode: {{ phase: idle }} }}",
        T - ago
    )
}

fn power_failed(during: &str) -> String {
    format!(
        "{{ power: {{ phase: failed, since: {}, during: {during} }}, mode: {{ phase: idle }} }}",
        T - 1000
    )
}

fn no_power_step(d: &Decision) -> bool {
    !d.steps
        .iter()
        .any(|s| matches!(s, Step::PowerOn { .. } | Step::PowerOff { .. }))
}

#[test]
fn a_host_that_never_comes_up_fails_at_the_timeout_and_is_never_woken_again() {
    let c = |ago: u64| {
        Case::new()
            .gpu_state(&powering_on_since(ago))
            .obs(up(G, false, 0))
            .target(Some("llm"))
            .want(G, PowerWant::Workload)
    };
    // One millisecond short: still powering on.
    let d = c(POWER_ON_TIMEOUT - 1).run();
    assert_steps(
        &d,
        vec![wait(G, WaitReason::PoweringOn)],
        "before the timeout",
    );
    assert!(matches!(power_of(&d, G), PowerPhase::PoweringOn { .. }));

    // At the timeout: the failed state, a visible wait, and no power step.
    let d = c(POWER_ON_TIMEOUT).run();
    assert_eq!(
        power_of(&d, G),
        PowerPhase::Failed {
            since: sched_model::Millis(T),
            during: sched_model::PowerTransition::PoweringOn
        }
    );
    assert_steps(&d, vec![wait(G, WaitReason::PowerFailed)], "at the timeout");
    assert!(d.allowed.models.is_empty());
    assert!(!uses_gpu(&d));

    // Hours later, still wanted, still down, observations fresh: failed, no power step.
    let mut state = d.state.clone();
    for hours in 1..=6u64 {
        let d = c(0).run_fresh_at(&state, T + hours * 60 * MIN);
        assert!(
            matches!(power_of(&d, G), PowerPhase::Failed { .. }),
            "{hours}h"
        );
        assert!(no_power_step(&d), "{hours}h: {:?}", d.steps);
        assert_steps(&d, vec![wait(G, WaitReason::PowerFailed)], "stays failed");
        assert!(d.allowed.models.is_empty());
        state = d.state;
    }
}

#[test]
fn the_power_on_timeout_needs_no_fresh_reading() {
    let d = Case::new()
        .gpu_state(&powering_on_since(POWER_ON_TIMEOUT + 5))
        .obs(up(G, false, 3_600_000))
        .run();
    assert!(matches!(power_of(&d, G), PowerPhase::Failed { .. }));
}

#[test]
fn a_host_that_never_goes_down_fails_at_the_timeout_and_is_never_shut_down_again() {
    let c = |ago: u64| {
        Case::new()
            .gpu_state(&shutting_down_since(ago))
            .obs(up(G, true, 0))
    };
    let d = c(SHUTDOWN_TIMEOUT - 1).run();
    assert_steps(
        &d,
        vec![wait(G, WaitReason::ShuttingDown)],
        "before the timeout",
    );
    assert!(matches!(power_of(&d, G), PowerPhase::ShuttingDown { .. }));

    let d = c(SHUTDOWN_TIMEOUT).run();
    assert_eq!(
        power_of(&d, G),
        PowerPhase::Failed {
            since: sched_model::Millis(T),
            during: sched_model::PowerTransition::ShuttingDown
        }
    );
    assert_steps(&d, vec![wait(G, WaitReason::PowerFailed)], "at the timeout");
    // The host is observed up, but nothing is allowed on it.
    assert!(d.allowed.models.is_empty());
    assert!(!uses_gpu(&d));

    let mut state = d.state.clone();
    for hours in 1..=6u64 {
        let d = c(0)
            .target(Some("llm"))
            .want(G, PowerWant::Manual)
            .run_fresh_at(&state, T + hours * 60 * MIN);
        assert!(
            matches!(power_of(&d, G), PowerPhase::Failed { .. }),
            "{hours}h"
        );
        assert!(no_power_step(&d), "{hours}h: {:?}", d.steps);
        assert!(d.allowed.models.is_empty(), "{hours}h");
        state = d.state;
    }
}

#[test]
fn the_failed_state_clears_only_on_the_state_that_was_wanted() {
    // Failed while powering on: a host observed down stays failed; observed up it is on.
    let c = Case::new().gpu_state(&power_failed("powering_on"));
    let d = c.clone().obs(up(G, false, 0)).run();
    assert!(matches!(power_of(&d, G), PowerPhase::Failed { .. }));
    let d = c.clone().obs(up(G, true, 31_000)).run();
    assert!(
        matches!(power_of(&d, G), PowerPhase::Failed { .. }),
        "a stale reading does not clear it"
    );
    let d = c.obs(up(G, true, 0)).run();
    assert!(matches!(power_of(&d, G), PowerPhase::On { .. }));

    // Failed while shutting down: observed up stays failed; observed down it is off.
    let c = Case::new().gpu_state(&power_failed("shutting_down"));
    let d = c.clone().obs(up(G, true, 0)).run();
    assert!(matches!(power_of(&d, G), PowerPhase::Failed { .. }));
    let d = c.obs(up(G, false, 0)).run();
    assert!(matches!(power_of(&d, G), PowerPhase::Off));
    assert!(no_power_step(&d), "{:?}", d.steps);
}

#[test]
fn a_host_that_recovers_from_a_failed_power_on_is_not_powered_off_by_us() {
    // Entering the failed state drops the flag the PowerOn step set.
    let d = Case::new()
        .gpu_state(&powering_on_since(POWER_ON_TIMEOUT))
        .obs(up(G, false, 0))
        .run();
    assert!(matches!(power_of(&d, G), PowerPhase::Failed { .. }));
    assert!(!d.state.resource(&rid(G)).unwrap().managed);

    // State persisted with the flag still set (an earlier build) is cleared on recovery too.
    let persisted = format!(
        "{{ power: {{ phase: failed, since: {}, during: powering_on }}, mode: {{ phase: idle }}, managed: true }}",
        T - 1000
    );
    let c = Case::new().gpu_state(&persisted).obs(up(G, true, 0));
    let d = c.run();
    assert!(matches!(power_of(&d, G), PowerPhase::On { .. }));
    assert!(!d.state.resource(&rid(G)).unwrap().managed);
    // No demand, no leases: far past idle_grace, never a power step.
    let mut state = d.state;
    for hours in 1..=6u64 {
        let d = c.run_fresh_at(&state, T + hours * 60 * MIN);
        assert!(no_power_step(&d), "{hours}h: {:?}", d.steps);
        assert!(matches!(power_of(&d, G), PowerPhase::On { .. }));
        state = d.state;
    }
}
#[test]
fn manual_control_has_no_transition_to_time_out_and_clears_a_failed_state() {
    let mut c = Case::new().gpu_state(&powering_on_since(POWER_ON_TIMEOUT * 3));
    c.config = example_config();
    let d = c.obs(up(G, false, 0)).run();
    assert!(
        matches!(power_of(&d, G), PowerPhase::Off),
        "{:?}",
        power_of(&d, G)
    );
    let mut c = Case::new().gpu_state(&power_failed("shutting_down"));
    c.config = example_config();
    let d = c.obs(up(G, true, 0)).run();
    assert!(matches!(power_of(&d, G), PowerPhase::On { .. }));
}

#[test]
fn timeouts_are_per_resource_and_configurable() {
    // The example gives basestar-nfs 5m / 10m and cuda-central 10m / 15m.
    let d = Case::new()
        .gpu_state(&powering_on_since(6 * MIN))
        .nfs_state(&format!(
            "{{ power: {{ phase: powering_on, since: {} }}, mode: {{ phase: idle }}, managed: true }}",
            T - 6 * MIN
        ))
        .obs(up(G, false, 0))
        .obs(up(N, false, 0))
        .run();
    assert!(matches!(power_of(&d, G), PowerPhase::PoweringOn { .. }));
    assert!(matches!(power_of(&d, N), PowerPhase::Failed { .. }));

    let mut c = Case::new()
        .gpu_state(&shutting_down_since(2 * MIN))
        .obs(up(G, true, 0));
    c.config = Config::from_yaml(
        &example_yaml_managed().replace("shutdown_timeout: 15m", "shutdown_timeout: 90s"),
    )
    .unwrap();
    let d = c.run();
    assert!(matches!(power_of(&d, G), PowerPhase::Failed { .. }));
}

#[test]
fn other_resources_keep_scheduling_while_one_is_failed() {
    // The GPU host failed; the NFS host is up and what needs it runs on.
    let d = Case::new()
        .gpu_state(&power_failed("powering_on"))
        .obs(up(G, false, 0))
        .obs(demand("immich-ml", "immich-queues", 0, 1, 0))
        .run();
    assert!(matches!(power_of(&d, G), PowerPhase::Failed { .. }));
    assert!(d.allowed.classes.contains(&cid("paperless-ngx")));
    assert!(!d.allowed.classes.contains(&cid("paperless-gpt")));
}

// The pre-shutdown mount check: a managed NFS host that is idle and due to be powered off is
// held up while pods mount it, and an unknown count counts as mounted.

/// The NFS host managed and idle since `idle` ms ago, with a veto (`veto` is YAML) if any.
fn nfs_due(idle: u64, veto: Option<&str>) -> String {
    let veto = veto
        .map(|v| format!(", mounts_veto: {v}"))
        .unwrap_or_default();
    format!(
        "{{ power: {}, mode: {}, managed: true, host_idle_since: {}{veto} }}",
        on(),
        mode_idle(),
        T - idle
    )
}

fn waiting_since(ago: u64) -> String {
    format!("{{ phase: waiting, since: {} }}", T - ago)
}

fn veto_of(d: &Decision) -> Option<sched_model::MountsVeto> {
    d.state.resource(&rid(N)).unwrap().mounts_veto
}

#[test]
fn a_host_that_pods_mount_is_not_powered_off_and_the_wait_begins_when_it_is_due() {
    let due = || Case::new().nfs_state(&nfs_due(10 * MIN, None));
    let d = due().obs(mounts(2, 0)).run();
    assert_steps(&d, vec![wait(N, WaitReason::PodsMounted)], "mounted");
    assert_eq!(
        veto_of(&d),
        Some(sched_model::MountsVeto::Waiting {
            since: sched_model::Millis(T)
        })
    );
    let n = d.state.resource(&rid(N)).unwrap();
    assert!(n.managed && n.is_on(), "still ours and still up");
    // An idle drain scaled its consumers down so their pods stop mounting it.
    assert!(d.allowed.classes.is_empty(), "{:?}", d.allowed);
    assert_eq!(
        n.drain.map(|x| x.origin),
        Some(sched_model::DrainOrigin::Idle),
        "the scheduler's own drain"
    );
    // Not yet due (grace not over): no veto, no wait.
    let d = Case::new()
        .nfs_state(&nfs_due(10 * MIN - 1, None))
        .obs(mounts(2, 0))
        .run();
    assert_steps(&d, vec![], "grace not over");
    assert_eq!(veto_of(&d), None);
}

#[test]
fn an_unknown_mount_count_counts_as_mounted() {
    let due = || Case::new().nfs_state(&nfs_due(10 * MIN, None));
    for (name, case) in [
        ("never reported", due().drop_obs("resource_mounts")),
        ("stale", due().obs(mounts(0, 31_000))),
    ] {
        let d = case.run();
        assert_steps(&d, vec![wait(N, WaitReason::MountsUnknown)], name);
        assert!(
            !d.steps.iter().any(|s| matches!(s, Step::PowerOff { .. })),
            "{name}"
        );
    }
    // A reading exactly at the bound is still fresh, and zero lets the shutdown through.
    let d = due().obs(mounts(0, 30_000)).run();
    assert_steps(&d, vec![Step::PowerOff { resource: rid(N) }], "fresh zero");
    assert_eq!(veto_of(&d), None);
}

#[test]
fn a_reading_from_a_source_without_the_capability_does_not_stand_in() {
    // The scheduler ignores it (the fixture loader refuses it, so it is built by hand).
    let c = Case::new()
        .nfs_state(&nfs_due(10 * MIN, None))
        .drop_obs("resource_mounts");
    let snap_text = {
        let mut y = format!("description: t\nsnapshot:\n  now: {T}\n  observations:\n");
        for o in &c.obs {
            y.push_str(o);
        }
        y.push_str("  leases: []\n");
        y
    };
    let mut snap = Fixture::from_yaml(&snap_text).unwrap().snapshot;
    snap.observations.push(sched_model::Observation {
        source: "cluster-pods".into(),
        observed_at: sched_model::Millis(T),
        fidelity: sched_model::Fidelity::Polled,
        value: sched_model::ObservedValue::ResourceMounts {
            resource: rid(N),
            pods: 0,
        },
    });
    let d = tick(&c.config, &snap, &c.state(), &c.intent).unwrap();
    assert_steps(&d, vec![wait(N, WaitReason::MountsUnknown)], "stray source");
}

#[test]
fn the_wait_ends_in_the_shutdown_when_the_pods_are_gone() {
    let case = |ago: u64, pods: u32| {
        Case::new()
            .nfs_state(&nfs_due(20 * MIN, Some(&waiting_since(ago))))
            .obs(mounts(pods, 0))
    };
    let d = case(5 * MIN, 1).run();
    assert_steps(&d, vec![wait(N, WaitReason::PodsMounted)], "still mounted");
    assert_eq!(
        veto_of(&d),
        Some(sched_model::MountsVeto::Waiting {
            since: sched_model::Millis(T - 5 * MIN)
        }),
        "the wait keeps its start"
    );
    let d = case(5 * MIN, 0).run();
    assert_steps(&d, vec![Step::PowerOff { resource: rid(N) }], "unmounted");
    assert_eq!(veto_of(&d), None);
    assert!(matches!(power_of(&d, N), PowerPhase::ShuttingDown { .. }));
}

#[test]
fn the_wait_is_bounded_and_then_the_scheduler_stops_without_a_shutdown_or_a_retry() {
    // mounts_timeout of the example is 30 minutes.
    let case = |ago: u64, pods: u32| {
        Case::new()
            .nfs_state(&nfs_due(60 * MIN, Some(&waiting_since(ago))))
            .obs(mounts(pods, 0))
    };
    let d = case(30 * MIN - 1, 1).run();
    assert_steps(&d, vec![wait(N, WaitReason::PodsMounted)], "just inside");
    let d = case(30 * MIN, 1).run();
    assert_steps(&d, vec![wait(N, WaitReason::ShutdownStopped)], "timed out");
    assert_eq!(
        veto_of(&d),
        Some(sched_model::MountsVeto::Stopped {
            since: sched_model::Millis(T)
        })
    );
    let n = d.state.resource(&rid(N)).unwrap();
    assert!(!n.managed, "no automatic power-off follows");
    assert!(n.is_on(), "the host stays up");
    assert!(
        !d.allowed.classes.is_empty(),
        "and what needs it stays allowed"
    );
    // A timeout is also reached on unknown readings.
    let d = case(30 * MIN, 0).drop_obs("resource_mounts").run();
    assert_steps(
        &d,
        vec![wait(N, WaitReason::ShutdownStopped)],
        "unknown too",
    );

    // Stopped stays stopped, however long, even once the pods are gone: no automatic retry.
    let stopped = |pods: u32| {
        Case::new()
            .nfs_state(
                &nfs_due(0, Some("{ phase: stopped, since: 1 }"))
                    .replace("managed: true", "managed: false")
                    .replace(&format!("host_idle_since: {}", T), "host_idle_since: null"),
            )
            .obs(mounts(pods, 0))
    };
    for pods in [0, 3] {
        let d = stopped(pods).run_at(&stopped(pods).state(), T + 99 * 60 * MIN);
        assert!(
            !d.steps.iter().any(|s| matches!(s, Step::PowerOff { .. })),
            "{d:?}"
        );
        assert!(
            d.steps.contains(&wait(N, WaitReason::ShutdownStopped)),
            "{d:?}"
        );
        assert!(matches!(
            veto_of(&d),
            Some(sched_model::MountsVeto::Stopped { .. })
        ));
    }
}

#[test]
fn a_stopped_host_is_never_shut_down_even_if_a_state_says_it_is_managed_and_nothing_mounts_it() {
    // Normal transitions never leave a host both managed and stopped (giving up clears
    // `managed`, and a manual request that sets it clears the veto), so the state is built by
    // hand: the check itself must still refuse, whatever the reading says, rather than rely on
    // the caller having cleared the flag.
    let managed_stopped = format!(
        "{{ power: {}, mode: {}, managed: true, host_idle_since: {}, mounts_veto: {{ phase: stopped, since: 1 }} }}",
        on(),
        mode_idle(),
        T - 20 * MIN
    );
    for (name, case) in [
        ("zero mounts", Case::new().obs(mounts(0, 0))),
        ("unknown mounts", Case::new().drop_obs("resource_mounts")),
    ] {
        let d = case.nfs_state(&managed_stopped).run();
        assert_steps(&d, vec![wait(N, WaitReason::ShutdownStopped)], name);
        assert!(matches!(power_of(&d, N), PowerPhase::On { .. }), "{name}");
        assert!(matches!(
            veto_of(&d),
            Some(sched_model::MountsVeto::Stopped { .. })
        ));
    }
}

#[test]
fn a_new_manual_request_takes_a_stopped_host_over_and_the_next_idle_period_decides_afresh() {
    let stopped = "{ power: { phase: \"on\", since: 0 }, mode: { phase: idle }, managed: false, mounts_veto: { phase: stopped, since: 1 } }";
    let d = Case::new()
        .nfs_state(stopped)
        .want(N, PowerWant::Manual)
        .obs(mounts(4, 0))
        .run();
    assert_eq!(veto_of(&d), None);
    assert!(d.state.resource(&rid(N)).unwrap().managed);
    assert_steps(&d, vec![], "held by the request");
    // A workload want alone does not take it over.
    let d = Case::new()
        .nfs_state(stopped)
        .want(N, PowerWant::Workload)
        .obs(mounts(4, 0))
        .run();
    assert!(matches!(
        veto_of(&d),
        Some(sched_model::MountsVeto::Stopped { .. })
    ));
    assert!(!d.state.resource(&rid(N)).unwrap().managed);
}

#[test]
fn something_holding_the_host_ends_the_wait_and_a_host_seen_down_ends_a_stop() {
    let hold = format!(
        "{{ id: h, resource: {N}, granted_at: 0, kind: {{ type: shared_hold, holder: x }} }}"
    );
    let d = Case::new()
        .nfs_state(&nfs_due(60 * MIN, Some(&waiting_since(10 * MIN))))
        .obs(mounts(1, 0))
        .lease(hold)
        .run();
    assert_eq!(veto_of(&d), None, "a hold makes the shutdown moot");
    assert_steps(&d, vec![], "kept up");
    // The host went down by itself (a person, a power cut): nothing left to veto.
    for veto in [
        "{ phase: waiting, since: 1 }",
        "{ phase: stopped, since: 1 }",
    ] {
        let d = Case::new()
            .nfs_state(&nfs_due(60 * MIN, Some(veto)))
            .obs(up(N, false, 0))
            .run();
        assert_eq!(power_of(&d, N), PowerPhase::Off);
        assert_eq!(veto_of(&d), None, "{veto}");
    }
}

#[test]
fn an_unchecked_host_is_powered_off_without_a_mount_reading() {
    // The GPU host has no mounts source: the veto does not apply to it (its drain is the
    // model wind-down), and a mount reading is not needed.
    let managed = "{ power: { phase: \"on\", since: 0 }, mode: { phase: idle }, managed: true, host_idle_since: ";
    let d = Case::new()
        .gpu_state(&format!("{managed}{} }}", T - 10 * MIN))
        .run();
    assert_steps(&d, vec![Step::PowerOff { resource: rid(G) }], "gpu");
}

// Before a managed GPU host is powered off its model is wound down: the shutdown follows the
// mode going idle (after the bounded wind-down of in-flight work, see `drain_timeout` and
// `reclaim_timeout`), never the other way round.

#[test]
fn a_managed_gpu_host_is_never_powered_off_while_a_model_is_allowed_and_only_after_the_drain() {
    let state = format!(
        "{{ power: {}, mode: {}, managed: true }}",
        on(),
        serving("mineru", 99 * MIN)
    );
    let c = Case::new().gpu_state(&state).target(None);
    // The planner released the idle model: it is drained in this tick, and the host is not
    // powered off in the same tick (nor is the idle clock of the host running yet).
    let d = c.run();
    assert!(
        d.steps.contains(&drain("mineru"))
            && !d.steps.iter().any(|s| matches!(s, Step::PowerOff { .. })),
        "{d:?}"
    );
    assert_eq!(mode_of(&d, G), ModePhase::Idle);
    assert_eq!(d.state.resource(&rid(G)).unwrap().host_idle_since, None);
    // A model that is still allowed blocks the shutdown, however long the host was idle.
    let still = Case::new().gpu_state(&state).target(Some("mineru")).run();
    assert!(
        !still
            .steps
            .iter()
            .any(|s| matches!(s, Step::PowerOff { .. })),
        "{still:?}"
    );
    // Once idle, the host needs its own grace period before it is shut down.
    let mut st = d.state;
    let first = c.run_fresh_at(&st, T + MIN);
    assert!(
        !first
            .steps
            .iter()
            .any(|s| matches!(s, Step::PowerOff { .. })),
        "{first:?}"
    );
    st = first.state;
    let late = c.run_fresh_at(&st, T + 12 * MIN);
    assert!(
        late.steps.contains(&Step::PowerOff { resource: rid(G) }),
        "{late:?}"
    );
}

#[test]
fn a_reclaim_gives_in_flight_work_its_timeout_before_the_managed_gpu_host_is_shut_down() {
    // whisperx serving with work in flight, a reclaim: the model is wound down first. Nothing
    // powers the host off while it is winding down, or in the tick that drains the model.
    let serving_whisperx = format!(
        "{{ phase: serving, model: whisperx, since: {} }}",
        T - 99 * MIN
    );
    let case = Case::new()
        .gpu_state(&format!(
            "{{ power: {}, mode: {serving_whisperx}, managed: true }}",
            on()
        ))
        .obs(demand("speakr-transcription", "speakr-pending", 0, 1, 0))
        .reclaim();
    let mut st = case.state();
    let mut powered_off_at = None;
    // The example's drain_timeout (the reclaim timeout default) is 10 minutes.
    for i in 0..=12u64 {
        let d = case.run_fresh_at(&st, T + i * MIN);
        if d.steps.iter().any(|s| matches!(s, Step::PowerOff { .. })) {
            powered_off_at = Some(i);
            assert_eq!(mode_of(&d, G), ModePhase::Idle, "drained before it");
            break;
        }
        st = d.state;
    }
    // Wind-down starts at minute 0, work is cut off at minute 10 (with the drain), and the
    // host is shut down on the tick after.
    assert_eq!(powered_off_at, Some(11));
}

// The minimum off time (`power.min_off`): after a shutdown flowmanager started and saw
// confirmed, an automatic power-on waits; a manual lease does not.

const MIN_OFF: u64 = 10 * MIN;

/// The managed example with `min_off: 10m` on both powered resources.
fn with_min_off(c: Case) -> Case {
    let y = example_yaml_managed();
    assert!(y.contains("mounts_timeout: 30m"), "the example changed");
    let y = y.replacen(
        "power_on_timeout:",
        "min_off: 10m\n      power_on_timeout:",
        2,
    );
    Case {
        config: Config::from_yaml(&y).unwrap(),
        ..c
    }
}

fn nfs_off(confirmed_ago: Option<u64>) -> String {
    let stamp = confirmed_ago
        .map(|a| format!(", shutdown_confirmed_at: {}", T - a))
        .unwrap_or_default();
    format!("{{ power: {{ phase: \"off\" }}, mode: {{ phase: idle }}{stamp} }}")
}

fn nfs_down(c: Case) -> Case {
    c.obs(up(N, false, 0))
}

fn stamp_of(d: &Decision, r: &str) -> Option<sched_model::Millis> {
    d.state.resource(&rid(r)).unwrap().shutdown_confirmed_at
}

fn powers_on(d: &Decision, r: &str) -> bool {
    d.steps
        .iter()
        .any(|s| matches!(s, Step::PowerOn { resource } if *resource == rid(r)))
}

#[test]
fn a_confirmed_shutdown_is_stamped_and_a_host_found_down_is_not() {
    let sd = |ago: u64| {
        format!(
            "{{ power: {{ phase: shutting_down, since: {} }}, mode: {{ phase: idle }} }}",
            T - ago
        )
    };
    // Observed down while shutting down: the stamp is now.
    let d = nfs_down(with_min_off(Case::new()).nfs_state(&sd(MIN))).run();
    assert!(matches!(power_of(&d, N), PowerPhase::Off));
    assert_eq!(stamp_of(&d, N), Some(sched_model::Millis(T)));
    // Still shutting down (observed up): no stamp.
    let d = with_min_off(Case::new()).nfs_state(&sd(MIN)).run();
    assert!(matches!(power_of(&d, N), PowerPhase::ShuttingDown { .. }));
    assert_eq!(stamp_of(&d, N), None);
    // A host that was up and went down by itself, or was found down: no stamp.
    let d = nfs_down(with_min_off(Case::new())).run();
    assert!(matches!(power_of(&d, N), PowerPhase::Off));
    assert_eq!(stamp_of(&d, N), None, "went down by itself");
    let d = nfs_down(with_min_off(Case::new()).nfs_state(&nfs_off(None))).run();
    assert_eq!(stamp_of(&d, N), None, "found down");
    // A shutdown that timed out and is later seen down: stamped when it is seen down.
    let d = with_min_off(Case::new()).nfs_state(&sd(15 * MIN)).run();
    assert!(matches!(power_of(&d, N), PowerPhase::Failed { .. }));
    assert_eq!(stamp_of(&d, N), None);
    let d2 = nfs_down(with_min_off(Case::new())).run_at(&d.state, T);
    assert!(matches!(power_of(&d2, N), PowerPhase::Off));
    assert_eq!(stamp_of(&d2, N), Some(sched_model::Millis(T)));
    // The stamp is kept while the host stays off, and dropped when it comes up.
    let kept = nfs_down(with_min_off(Case::new()).nfs_state(&nfs_off(Some(3 * MIN)))).run();
    assert_eq!(stamp_of(&kept, N), Some(sched_model::Millis(T - 3 * MIN)));
    let back = with_min_off(Case::new())
        .nfs_state(&nfs_off(Some(3 * MIN)))
        .run();
    assert!(matches!(power_of(&back, N), PowerPhase::On { .. }));
    assert_eq!(stamp_of(&back, N), None);
}

#[test]
fn an_automatic_want_waits_for_min_off_after_our_shutdown_and_then_powers_on() {
    let c = |ago: u64, w: PowerWant| {
        nfs_down(with_min_off(Case::new()).nfs_state(&nfs_off(Some(ago)))).want(N, w)
    };
    let w = PowerWant::Workload;
    {
        let d = c(MIN_OFF - 1, w).run();
        assert_steps(&d, vec![wait(N, WaitReason::MinOff)], "one ms short");
        assert!(matches!(power_of(&d, N), PowerPhase::Off));
        assert_eq!(
            stamp_of(&d, N),
            Some(sched_model::Millis(T - (MIN_OFF - 1)))
        );
        let d = c(MIN_OFF, w).run();
        assert_steps(&d, vec![Step::PowerOn { resource: rid(N) }], "at min_off");
        assert!(matches!(power_of(&d, N), PowerPhase::PoweringOn { .. }));
        assert_eq!(stamp_of(&d, N), None, "the stamp ends with the phase off");
    }
    // Nobody wants it: nothing is said about min_off.
    let d = nfs_down(with_min_off(Case::new()).nfs_state(&nfs_off(Some(MIN)))).run();
    assert_steps(&d, vec![], "no want, no wait");
}

#[test]
fn a_manual_lease_overrides_min_off() {
    let d = nfs_down(with_min_off(Case::new()).nfs_state(&nfs_off(Some(1000))))
        .want(N, PowerWant::Manual)
        .run();
    assert_steps(&d, vec![Step::PowerOn { resource: rid(N) }], "manual");
    assert!(d.state.resource(&rid(N)).unwrap().managed);
    assert_eq!(stamp_of(&d, N), None);
}

#[test]
fn a_host_found_down_or_gone_down_by_itself_is_powered_on_at_once() {
    let d = nfs_down(with_min_off(Case::new()).nfs_state(&nfs_off(None)))
        .want(N, PowerWant::Workload)
        .run();
    assert_steps(&d, vec![Step::PowerOn { resource: rid(N) }], "found down");
    // Up one tick, down the next, wanted: it went down on its own.
    let first = with_min_off(Case::new()).run();
    let d = nfs_down(with_min_off(Case::new()))
        .want(N, PowerWant::Workload)
        .run_at(&first.state, T);
    assert_steps(&d, vec![Step::PowerOn { resource: rid(N) }], "went down");
}

#[test]
fn the_default_of_zero_changes_nothing() {
    // The stamp is written (it is state), but nothing waits on it.
    let d = nfs_down(Case::new().nfs_state(&nfs_off(Some(1))))
        .want(N, PowerWant::Workload)
        .run();
    assert_steps(&d, vec![Step::PowerOn { resource: rid(N) }], "no min_off");
    let sd = format!(
        "{{ power: {{ phase: shutting_down, since: {} }}, mode: {{ phase: idle }} }}",
        T - MIN
    );
    let d = nfs_down(Case::new().nfs_state(&sd))
        .want(N, PowerWant::Workload)
        .run();
    assert_steps(
        &d,
        vec![Step::PowerOn { resource: rid(N) }],
        "as before: at once",
    );
    assert_eq!(stamp_of(&d, N), None);
    // With a minimum the same tick waits instead.
    let d = nfs_down(with_min_off(Case::new()).nfs_state(&sd))
        .want(N, PowerWant::Workload)
        .run();
    assert_steps(
        &d,
        vec![wait(N, WaitReason::MinOff)],
        "min_off from the first tick",
    );
}

#[test]
fn a_clock_that_goes_back_never_lengthens_the_wait_beyond_min_off() {
    // The stamp is five minutes in the future.
    let future = format!(
        "{{ power: {{ phase: \"off\" }}, mode: {{ phase: idle }}, shutdown_confirmed_at: {} }}",
        T + 5 * MIN
    );
    let c = nfs_down(with_min_off(Case::new()).nfs_state(&future)).want(N, PowerWant::Workload);
    let d = c.run();
    assert_steps(&d, vec![wait(N, WaitReason::MinOff)], "waits");
    assert_eq!(
        stamp_of(&d, N),
        Some(sched_model::Millis(T)),
        "made again at now"
    );
    let d = c.run_fresh_at(&d.state, T + MIN_OFF - 1);
    assert_steps(&d, vec![wait(N, WaitReason::MinOff)], "still short");
    let d = c.run_fresh_at(&d.state, T + MIN_OFF);
    assert_steps(&d, vec![Step::PowerOn { resource: rid(N) }], "due");
}

#[test]
fn the_wait_is_a_delay_of_a_fresh_decision_never_a_retry() {
    // Many ticks inside the wait: only waits, and exactly one power-on once it is over.
    let c = nfs_down(with_min_off(Case::new()).nfs_state(&nfs_off(Some(0))))
        .want(N, PowerWant::Workload);
    let mut state = c.state();
    let mut power_ons = 0;
    for tick_no in 0..=(MIN_OFF / 30_000 + 4) {
        let now = T + tick_no * 30_000;
        let d = c.run_fresh_at(&state, now);
        if powers_on(&d, N) {
            power_ons += 1;
            assert!(now >= T + MIN_OFF, "powered on early at +{}", now - T);
        } else if now < T + MIN_OFF {
            assert_steps(&d, vec![wait(N, WaitReason::MinOff)], "inside the wait");
        }
        state = d.state;
    }
    assert_eq!(power_ons, 1);
    assert!(matches!(
        state.resource(&rid(N)).unwrap().power,
        PowerPhase::PoweringOn { .. } | PowerPhase::On { .. } | PowerPhase::Failed { .. }
    ));
}

#[test]
fn min_off_also_holds_back_an_exclusive_host_and_a_reclaim_is_not_delayed() {
    let off = format!(
        "{{ power: {{ phase: \"off\" }}, mode: {{ phase: idle }}, shutdown_confirmed_at: {} }}",
        T - MIN
    );
    let c = with_min_off(Case::new())
        .gpu_state(&off)
        .obs(up(G, false, 0))
        .target(Some("llm"))
        .want(G, PowerWant::Workload);
    let d = c.run();
    assert!(!powers_on(&d, G), "{:?}", d.steps);
    assert!(
        d.steps.contains(&wait(G, WaitReason::MinOff)),
        "{:?}",
        d.steps
    );
    assert!(d.allowed.models.is_empty());
    // A reclaim removes the want and the wait says reclaimed, as before.
    let d = with_min_off(Case::new())
        .gpu_state(&off)
        .obs(up(G, false, 0))
        .reclaim()
        .run();
    assert!(
        !d.steps.contains(&wait(G, WaitReason::MinOff)),
        "{:?}",
        d.steps
    );
}

#[test]
fn the_stamp_is_state_that_survives_a_restart() {
    let sd = format!(
        "{{ power: {{ phase: shutting_down, since: {} }}, mode: {{ phase: idle }} }}",
        T - MIN
    );
    let c = nfs_down(with_min_off(Case::new()).nfs_state(&sd)).want(N, PowerWant::Workload);
    let d = c.run();
    assert_eq!(stamp_of(&d, N), Some(sched_model::Millis(T)));
    // Written and read back, as the service does between ticks.
    let restored: SchedState =
        serde_json::from_str(&serde_json::to_string(&d.state).unwrap()).unwrap();
    assert_eq!(restored, d.state);
    let d2 = c.run_fresh_at(&restored, T + MIN_OFF - 1);
    assert_steps(
        &d2,
        vec![wait(N, WaitReason::MinOff)],
        "still waits after a restart",
    );
    let d3 = c.run_fresh_at(&restored, T + MIN_OFF);
    assert_steps(
        &d3,
        vec![Step::PowerOn { resource: rid(N) }],
        "due after a restart",
    );
}

#[test]
fn without_a_min_off_no_stamp_is_ever_written_and_a_leftover_one_is_dropped() {
    let sd = format!(
        "{{ power: {{ phase: shutting_down, since: {} }}, mode: {{ phase: idle }} }}",
        T - MIN
    );
    let d = nfs_down(Case::new().nfs_state(&sd)).run();
    assert!(matches!(power_of(&d, N), PowerPhase::Off));
    assert_eq!(
        stamp_of(&d, N),
        None,
        "the stored state keeps its old shape"
    );
    let d = nfs_down(Case::new().nfs_state(&nfs_off(Some(MIN)))).run();
    assert_eq!(stamp_of(&d, N), None);
}

// ---- drain: a person released the host's manual lease asking for the host to go down ----

/// The NFS host managed and up with a drain `mode` requested `ago` ms before the tick.
fn nfs_draining(mode: &str, ago: u64) -> String {
    format!(
        "{{ power: {}, mode: {}, managed: true, drain: {{ mode: {mode}, since: {} }} }}",
        on(),
        mode_idle(),
        T - ago
    )
}

fn nfs_drain_of(d: &Decision) -> Option<sched_model::Drain> {
    d.state.resource(&rid(N)).unwrap().drain
}

/// Whether the gate class of paperless-gpt (which needs basestar-nfs and the GPU host) is allowed.
fn gate_allowed(d: &Decision) -> bool {
    d.allowed.classes.contains(&cid("paperless-gpt"))
}

/// Work queued and in flight for a class that needs the NFS host.
fn nfs_busy() -> Case {
    Case::new().obs(demand("paperless-ocr", "paperless-ocr-pending", 5, 1, 0))
}

#[test]
fn an_immediate_drain_scales_the_consumers_down_and_shuts_the_host_down_despite_queued_work() {
    // Without the drain, the same state and work keep the host up.
    let held = nfs_busy().nfs_state(&nfs_due(60 * MIN, None)).run();
    assert!(
        !held
            .steps
            .iter()
            .any(|s| matches!(s, Step::PowerOff { .. }))
    );
    assert!(gate_allowed(&held));

    let d = nfs_busy().nfs_state(&nfs_draining("immediate", 0)).run();
    assert_steps(&d, vec![Step::PowerOff { resource: rid(N) }], "drain");
    assert!(!gate_allowed(&d), "{:?}", d.allowed);
    assert_eq!(
        nfs_drain_of(&d).map(|x| (x.mode, x.scaled_down)),
        Some((sched_model::DrainMode::Immediate, true))
    );
    assert!(matches!(power_of(&d, N), PowerPhase::ShuttingDown { .. }));
}

#[test]
fn a_graceful_drain_lets_in_flight_work_run_for_the_drain_timeout_and_no_longer() {
    // drain_timeout of the example is 10 minutes; work is in flight throughout.
    let at = |ago: u64| nfs_busy().nfs_state(&nfs_draining("graceful", ago)).run();
    let d = at(10 * MIN - 1);
    assert_steps(
        &d,
        vec![wait(N, WaitReason::Draining)],
        "inside the timeout",
    );
    assert!(gate_allowed(&d), "consumers still allowed");
    assert_eq!(nfs_drain_of(&d).map(|x| x.scaled_down), Some(false));

    let d = at(10 * MIN);
    assert_steps(&d, vec![Step::PowerOff { resource: rid(N) }], "timed out");
    assert!(!gate_allowed(&d));
}

#[test]
fn a_graceful_drain_scales_down_as_soon_as_nothing_is_in_flight() {
    // Queued work alone does not hold a drain.
    let queued = Case::new()
        .obs(demand("paperless-ocr", "paperless-ocr-pending", 5, 0, 0))
        .nfs_state(&nfs_draining("graceful", MIN))
        .run();
    assert_steps(
        &queued,
        vec![Step::PowerOff { resource: rid(N) }],
        "queued only",
    );
    assert!(!gate_allowed(&queued));
    // An unknown reading of in-flight work is not "none": the timeout decides.
    let unknown = Case::new()
        .drop_obs("class: paperless-ocr,")
        .nfs_state(&nfs_draining("graceful", MIN))
        .run();
    assert_steps(&unknown, vec![wait(N, WaitReason::Draining)], "unknown");
    assert!(gate_allowed(&unknown));
}

#[test]
fn a_drain_never_overrides_a_lease_and_the_shutdown_follows_its_release() {
    let hold = lease_on("h", N, "{ type: shared_hold, holder: x }");
    let c = Case::new()
        .lease(hold)
        .nfs_state(&nfs_draining("immediate", 0));
    let d = c.run();
    assert_steps(&d, vec![wait(N, WaitReason::DrainHeld)], "held by a lease");
    assert!(
        !gate_allowed(&d),
        "the consumers are scaled down all the same"
    );
    assert!(nfs_drain_of(&d).is_some_and(|x| x.scaled_down));
    // Next tick the scale-down is remembered and the lease is gone.
    let gone = Case::new();
    let d2 = gone.run_fresh_at(&d.state, T + MIN);
    assert_steps(&d2, vec![Step::PowerOff { resource: rid(N) }], "released");
}

#[test]
fn the_mount_veto_and_its_timeout_still_apply_after_the_scale_down() {
    let c = Case::new()
        .obs(mounts(2, 0))
        .nfs_state(&nfs_draining("immediate", 0));
    let d = c.run();
    assert_steps(&d, vec![wait(N, WaitReason::PodsMounted)], "mounted");
    assert!(!gate_allowed(&d));
    assert!(matches!(
        veto_of(&d),
        Some(sched_model::MountsVeto::Waiting { .. })
    ));
    // The pods are gone: the shutdown goes ahead.
    let d2 = Case::new().run_fresh_at(&d.state, T + MIN);
    assert_steps(&d2, vec![Step::PowerOff { resource: rid(N) }], "unmounted");
    // A wait that outlasts mounts_timeout (30m) stops the scheduler.
    let d3 = c.run_fresh_at(&d.state, T + 30 * MIN);
    assert_steps(&d3, vec![wait(N, WaitReason::ShutdownStopped)], "timed out");
}

#[test]
fn a_new_manual_lease_cancels_a_drain_and_a_host_seen_off_ends_it() {
    let lease = lease_on("m", N, "{ type: manual_host }");
    let d = Case::new()
        .lease(lease)
        .want(N, PowerWant::Manual)
        .nfs_state(&nfs_draining("immediate", 0))
        .run();
    assert_eq!(nfs_drain_of(&d), None);
    assert!(gate_allowed(&d), "consumers are allowed again");
    assert!(!d.steps.iter().any(|s| matches!(s, Step::PowerOff { .. })));

    let d = Case::new()
        .obs(up(N, false, 0))
        .nfs_state(&nfs_draining("immediate", 0))
        .run();
    assert_eq!(nfs_drain_of(&d), None);
    assert_eq!(power_of(&d, N), PowerPhase::Off);
}

#[test]
fn a_drain_state_reads_and_writes_without_changing_a_state_that_has_none() {
    let d = Case::new().nfs_state(&nfs_draining("graceful", 0)).run();
    let text = serde_json::to_string(&d.state).unwrap();
    assert_eq!(text.matches("\"drain\"").count(), 1, "{text}");
    let back: SchedState = serde_json::from_str(&text).unwrap();
    assert_eq!(back, d.state);
    let none = serde_json::to_string(&Case::new().run().state).unwrap();
    assert!(!none.contains("drain"), "{none}");
}

// ---- idle drain: a managed shared host that would be powered off scales its consumers down first ----

/// The NFS host managed, idle since `idle` ms ago, with an idle drain started `ago` ms ago.
fn nfs_idle_drain(idle: u64, ago: u64, veto: Option<&str>) -> String {
    let veto = veto
        .map(|v| format!(", mounts_veto: {v}"))
        .unwrap_or_default();
    format!(
        "{{ power: {}, mode: {}, managed: true, host_idle_since: {}, drain: {{ mode: immediate, since: {}, scaled_down: true, origin: idle }}{veto} }}",
        on(),
        mode_idle(),
        T - idle,
        T - ago
    )
}

fn nfs_state_of(d: &Decision) -> sched_model::ResourceState {
    d.state.resource(&rid(N)).unwrap().clone()
}

fn is_idle_drain(d: &Decision) -> bool {
    nfs_drain_of(d).is_some_and(|x| x.origin == sched_model::DrainOrigin::Idle)
}

#[test]
fn a_host_that_goes_idle_scales_its_consumers_down_before_it_is_shut_down() {
    // Grace not over: nothing yet, the consumers run.
    let d = Case::new().nfs_state(&nfs_due(10 * MIN - 1, None)).run();
    assert!(d.steps.is_empty(), "{:?}", d.steps);
    assert_eq!(nfs_drain_of(&d), None);
    assert!(gate_allowed(&d));
    // Grace over, no pod mounts it: the idle drain starts, the consumers leave the allowed
    // set in this very tick, and the shutdown goes ahead.
    let d = Case::new().nfs_state(&nfs_due(10 * MIN, None)).run();
    assert_steps(&d, vec![Step::PowerOff { resource: rid(N) }], "idle");
    assert!(is_idle_drain(&d), "{:?}", nfs_drain_of(&d));
    let x = nfs_drain_of(&d).unwrap();
    assert!(x.scaled_down && x.mode == sched_model::DrainMode::Immediate);
    assert!(!gate_allowed(&d), "{:?}", d.allowed);
    assert!(matches!(power_of(&d, N), PowerPhase::ShuttingDown { .. }));
}

#[test]
fn an_idle_drain_waits_for_the_pods_to_unmount_and_then_shuts_the_host_down() {
    let c = Case::new()
        .nfs_state(&nfs_due(10 * MIN, None))
        .obs(mounts(2, 0));
    let d = c.run();
    assert_steps(&d, vec![wait(N, WaitReason::PodsMounted)], "mounted");
    assert!(is_idle_drain(&d) && !gate_allowed(&d));
    // The state is carried (as persisted): still draining, pods gone, shut down.
    let d2 = Case::new().run_fresh_at(&d.state, T + MIN);
    assert_steps(&d2, vec![Step::PowerOff { resource: rid(N) }], "unmounted");
    assert!(is_idle_drain(&d2));
}

#[test]
fn any_hold_cancels_an_idle_drain_and_the_idle_clock_starts_again() {
    let drained = nfs_idle_drain(11 * MIN, MIN, None);
    let cases: Vec<(&str, Case)> = vec![
        (
            "a job's lease",
            Case::new().lease(lease_on("h", N, "{ type: shared_hold, holder: backup }")),
        ),
        (
            "a manual or adopted lease",
            Case::new()
                .lease(lease_on("m", N, "{ type: manual_host }"))
                .want(N, PowerWant::Manual),
        ),
        (
            "busy demand on a class that needs the host",
            Case::new().obs(demand("paperless-ocr", "paperless-ocr-pending", 3, 0, 0)),
        ),
        (
            "work in flight",
            Case::new().obs(demand("paperless-ocr", "paperless-ocr-pending", 0, 1, 0)),
        ),
    ];
    for (name, c) in cases {
        let d = c.nfs_state(&drained).run();
        assert_eq!(nfs_drain_of(&d), None, "{name}");
        assert!(gate_allowed(&d), "{name}: the consumers are allowed again");
        assert!(
            !d.steps.iter().any(|s| matches!(s, Step::PowerOff { .. })),
            "{name}"
        );
        assert_eq!(nfs_state_of(&d).host_idle_since, None, "{name}");
    }
    // Once the hold is gone the clock starts from scratch: no new idle drain until a full
    // idle_grace has passed again.
    let d = Case::new().nfs_state(&nfs_due(0, None)).run();
    assert_eq!(nfs_drain_of(&d), None);
    assert_eq!(
        nfs_state_of(&d).host_idle_since,
        Some(sched_model::Millis(T))
    );
    let d = Case::new().run_fresh_at(&d.state, T + 10 * MIN - 1);
    assert_eq!(nfs_drain_of(&d), None);
    let d = Case::new().run_fresh_at(&d.state, T + 10 * MIN);
    assert!(is_idle_drain(&d));
}

#[test]
fn unknown_demand_starts_no_idle_drain_and_keeps_one_that_is_set_without_a_shutdown() {
    let c = || Case::new().drop_obs("class: paperless-ocr,");
    let d = c().nfs_state(&nfs_due(10 * MIN, None)).run();
    assert_steps(&d, vec![wait(N, WaitReason::StaleDemand)], "not started");
    assert_eq!(nfs_drain_of(&d), None);
    assert!(gate_allowed(&d));
    let d = c().nfs_state(&nfs_idle_drain(11 * MIN, MIN, None)).run();
    assert_steps(&d, vec![wait(N, WaitReason::StaleDemand)], "kept");
    assert!(is_idle_drain(&d) && !gate_allowed(&d));
}

#[test]
fn a_stopped_mount_veto_ends_the_idle_drain_and_it_does_not_start_again() {
    // Pods kept mounting past mounts_timeout (30m): the scheduler stops.
    let c = Case::new()
        .nfs_state(&nfs_idle_drain(
            60 * MIN,
            31 * MIN,
            Some(&waiting_since(30 * MIN)),
        ))
        .obs(mounts(2, 0));
    let d = c.run();
    assert_steps(&d, vec![wait(N, WaitReason::ShutdownStopped)], "stopped");
    assert!(matches!(
        veto_of(&d),
        Some(sched_model::MountsVeto::Stopped { .. })
    ));
    assert_eq!(nfs_drain_of(&d), None, "the idle drain ends");
    assert!(gate_allowed(&d), "the consumers come back");
    assert!(!nfs_state_of(&d).managed);
    // Later ticks (the pods are gone, the host has been idle for ever): it stays stopped, no
    // new idle drain, no shutdown.
    let mut state = d.state.clone();
    for k in 1..=4u64 {
        let d = Case::new().run_fresh_at(&state, T + k * 20 * MIN);
        assert_steps(
            &d,
            vec![wait(N, WaitReason::ShutdownStopped)],
            "still stopped",
        );
        assert_eq!(nfs_drain_of(&d), None);
        assert!(gate_allowed(&d));
        state = d.state;
    }
}

#[test]
fn a_host_seen_down_ends_an_idle_drain_and_manual_control_never_starts_one() {
    let d = Case::new()
        .obs(up(N, false, 0))
        .nfs_state(&nfs_idle_drain(11 * MIN, MIN, None))
        .run();
    assert_eq!(nfs_drain_of(&d), None);
    assert_eq!(power_of(&d, N), PowerPhase::Off);
    // control: manual: nothing is ever drained or powered off.
    let mut c = Case::new().nfs_state(&nfs_due(60 * MIN, None));
    c.config = example_config();
    let d = c.run();
    assert!(d.steps.is_empty(), "{:?}", d.steps);
    assert_eq!(nfs_drain_of(&d), None);
    assert!(gate_allowed(&d));
}

#[test]
fn an_exclusive_host_is_shut_down_without_an_idle_drain() {
    let gpu = format!(
        "{{ power: {}, mode: {}, managed: true, host_idle_since: {} }}",
        on(),
        mode_idle(),
        T - 60 * MIN
    );
    let d = Case::new().gpu_state(&gpu).run();
    assert!(
        d.steps.contains(&Step::PowerOff { resource: rid(G) }),
        "{:?}",
        d.steps
    );
    assert_eq!(d.state.resource(&rid(G)).unwrap().drain, None);
}
