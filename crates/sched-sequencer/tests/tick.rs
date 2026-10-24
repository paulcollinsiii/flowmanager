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
    ("backup", "backup-schedule"),
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
fn allowing_a_model_needs_fresh_zero_pods_of_every_other_model() {
    let table: Vec<(&str, Case, Vec<Step>)> = vec![
        (
            "all zero and fresh",
            Case::new().target(Some("mineru")),
            vec![allow("mineru")],
        ),
        (
            "another model has pods",
            Case::new().target(Some("mineru")).obs(pods("llm", 1, 1, 0)),
            vec![wait(G, WaitReason::OtherModelPresent)],
        ),
        (
            "a pod observation is missing",
            Case::new()
                .target(Some("mineru"))
                .drop_obs("model: whisperx"),
            vec![wait(G, WaitReason::StaleObservation)],
        ),
        (
            "a pod observation is 1ms too old",
            Case::new()
                .target(Some("mineru"))
                .obs(pods("llm", 0, 0, 30_001)),
            vec![wait(G, WaitReason::StaleObservation)],
        ),
        (
            "a pod observation exactly at the bound",
            Case::new()
                .target(Some("mineru"))
                .obs(pods("llm", 0, 0, 30_000)),
            vec![allow("mineru")],
        ),
        (
            "the target's own residual pods do not block",
            Case::new()
                .target(Some("mineru"))
                .obs(pods("mineru", 1, 0, 0)),
            vec![allow("mineru")],
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
fn loading_transitions() {
    let loading = || {
        Case::new().gpu(&format!(
            "{{ phase: loading, model: mineru, since: {} }}",
            T - 1000
        ))
    };
    let d = loading()
        .target(Some("mineru"))
        .obs(pods("mineru", 1, 0, 0))
        .run();
    assert_steps(&d, vec![wait(G, WaitReason::Loading)], "not ready");
    assert!(d.allowed.models.contains(&mid("mineru")));
    assert!(d.allowed.classes.is_empty() || !d.allowed.classes.contains(&cid("paperless-ocr")));

    let d = loading()
        .target(Some("mineru"))
        .obs(pods("mineru", 1, 1, 0))
        .run();
    assert_steps(&d, vec![], "ready");
    assert!(matches!(mode_of(&d, G), ModePhase::Serving { .. }));
    assert!(d.allowed.classes.contains(&cid("paperless-ocr")));

    let d = loading()
        .target(Some("mineru"))
        .obs(pods("mineru", 1, 1, 31_000))
        .run();
    assert_steps(
        &d,
        vec![wait(G, WaitReason::StaleObservation)],
        "stale readiness",
    );

    let d = loading()
        .target(Some("llm"))
        .obs(pods("mineru", 1, 0, 0))
        .run();
    assert_steps(&d, vec![drain("mineru")], "target changed while loading");
    assert!(d.allowed.models.is_empty());
}

#[test]
fn draining_transitions() {
    let draining = || {
        Case::new().gpu(&format!(
            "{{ phase: draining, model: mineru, since: {} }}",
            T - 1000
        ))
    };
    let d = draining()
        .target(Some("llm"))
        .obs(pods("mineru", 1, 1, 0))
        .run();
    assert_steps(&d, vec![wait(G, WaitReason::Draining)], "still has pods");
    assert!(d.allowed.models.is_empty());

    let d = draining()
        .target(Some("llm"))
        .obs(pods("mineru", 1, 1, 31_000))
        .run();
    assert_steps(&d, vec![wait(G, WaitReason::StaleObservation)], "stale");

    let d = draining().target(Some("llm")).run();
    assert_steps(
        &d,
        vec![allow("llm")],
        "zero pods: next model in the same tick",
    );

    let d = draining().target(None).run();
    assert_steps(&d, vec![], "zero pods, nothing wanted");
    assert!(matches!(mode_of(&d, G), ModePhase::Idle));

    let d = draining()
        .target(Some("llm"))
        .obs(pods("whisperx", 1, 0, 0))
        .run();
    assert_steps(
        &d,
        vec![wait(G, WaitReason::OtherModelPresent)],
        "another model still present blocks the next",
    );
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
        vec![pause("paperless-ocr"), drain("mineru")],
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

    // Without a timeout (not forced) in-flight work is awaited forever.
    let late = serve(10 * MIN)
        .target(Some("llm"))
        .obs(demand("paperless-ocr", "paperless-ocr-pending", 2, 1, 0))
        .run_fresh_at(&d.state, T + 24 * 60 * MIN);
    assert_steps(
        &late,
        vec![wait(G, WaitReason::InFlightWork)],
        "waits a day",
    );
    assert!(
        !late
            .steps
            .iter()
            .any(|s| matches!(s, Step::KillInFlight { .. } | Step::DrainModel { .. }))
    );

    let d = serve(10 * MIN)
        .target(Some("llm"))
        .obs(demand("paperless-ocr", "paperless-ocr-pending", 2, 0, 0))
        .obs(pods("mineru", 1, 1, 0))
        .run();
    assert_steps(
        &d,
        vec![pause("paperless-ocr"), drain("mineru")],
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
    let after = c.run_at(&d.state, T + 10 * MIN);
    assert_steps(
        &after,
        vec![kill("paperless-ocr"), drain("mineru")],
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
        "    priority: 50\n    preemptibility: pause_intake\n    demand: { source: paperless-ocr-pending }",
        "    priority: 50\n    preemptibility: pause_intake\n    kill_timeout: 1m\n    demand: { source: paperless-ocr-pending }",
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

    let d = Case::new().obs(pods("llm", 0, 0, 31_000)).reclaim().run();
    assert_steps(
        &d,
        vec![wait(G, WaitReason::StaleObservation)],
        "stale pods hold the shutdown",
    );

    let c = Case::new()
        .gpu(&format!(
            "{{ phase: loading, model: mineru, since: {} }}",
            T - 1000
        ))
        .reclaim();
    let d = c.obs(pods("mineru", 1, 0, 0)).run();
    assert_steps(
        &d,
        vec![drain("mineru")],
        "a loading model is withdrawn at once",
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
        .obs(demand("backup", "backup-schedule", 1, 0, 0))
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

    let d = Case::new()
        .gpu_state(&format!("{managed}{} }}", T - 99 * MIN))
        .obs(pods("llm", 0, 0, 31_000))
        .run();
    assert_steps(
        &d,
        vec![wait(G, WaitReason::StaleObservation)],
        "stale pods stop the power down",
    );
    let d = Case::new()
        .gpu_state(&format!("{managed}{} }}", T - 99 * MIN))
        .obs(pods("llm", 1, 1, 0))
        .run();
    assert_steps(
        &d,
        vec![wait(G, WaitReason::OtherModelPresent)],
        "pods present stop the power down",
    );
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
    let all_nfs: Vec<ClassId> = ["backup", "paperless-ngx", "immich-server"]
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
        "backup",
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
fn only_a_forced_switch_ever_stops_in_flight_work() {
    let yaml = managed_yaml().replace(
        "    priority: 50\n    preemptibility: pause_intake\n    demand: { source: paperless-ocr-pending }",
        "    priority: 50\n    preemptibility: pause_intake\n    kill_timeout: 1m\n    demand: { source: paperless-ocr-pending }",
    );
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
    for later in [MIN, 10 * MIN, 24 * 60 * MIN] {
        let d = busy().run_fresh_at(&d.state, T + later);
        assert!(
            !d.steps
                .iter()
                .any(|s| matches!(s, Step::KillInFlight { .. } | Step::DrainModel { .. })),
            "unforced switch acted at +{later}: {:?}",
            d.steps
        );
    }
    assert_eq!(wind_down(&d).unwrap().timeout, None);
}

fn wind_down(d: &Decision) -> Option<sched_model::WindDown> {
    match mode_of(d, G) {
        ModePhase::Serving { wind_down, .. } => wind_down,
        _ => None,
    }
}

#[test]
fn each_class_is_stopped_when_its_own_timeout_expires() {
    let yaml = managed_yaml().replace(
        "    priority: 50\n    preemptibility: pause_intake\n    demand: { source: paperless-post-ocr }",
        "    priority: 50\n    preemptibility: pause_intake\n    kill_timeout: 1m\n    demand: { source: paperless-post-ocr }",
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
    let line = pods("llm", 0, 0, 0);
    assert!(line.contains(&format!("observed_at: {T}")));
    let d = Case::new().target(Some("mineru")).obs(future(&line)).run();
    assert_steps(
        &d,
        vec![wait(G, WaitReason::StaleObservation)],
        "future pods block allowing",
    );

    // A fresh older observation does not rescue a future-dated newest one.
    let d = Case::new()
        .target(Some("mineru"))
        .obs(pods("llm", 0, 0, 1000))
        .run();
    assert_steps(
        &d,
        vec![allow("mineru")],
        "baseline: an older fresh observation is fine alone",
    );
    let mut c = Case::new()
        .target(Some("mineru"))
        .obs(pods("llm", 0, 0, 1000));
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

#[test]
fn a_different_forcing_cause_restarts_the_clock_and_losing_it_removes_the_timeout() {
    let yaml = managed_yaml().replace(
        "  drain_timeout: 10m",
        "  drain_timeout: 10m\n  reclaim_timeout: 2m",
    );
    let base = |reclaim: bool, forced: bool| {
        let mut c = Case::new()
            .gpu(&serving("mineru", 30 * MIN))
            .obs(pods("mineru", 1, 1, 0))
            .obs(demand("paperless-ocr", "paperless-ocr-pending", 2, 1, 0));
        c = if reclaim {
            c.reclaim()
        } else {
            c.target(Some("llm"))
        };
        if forced && !reclaim {
            c = c.forced();
        }
        c.config = Config::from_yaml(&yaml).unwrap();
        c
    };
    // A manual lease starts a 10m wind-down; five minutes in, a reclaim arrives.
    let d0 = base(false, true).run();
    assert_eq!(
        wind_down(&d0).unwrap().timeout,
        Some(sched_model::Millis(10 * MIN))
    );
    let d1 = base(true, true).run_at(&d0.state, T + 5 * MIN);
    assert_steps(
        &d1,
        vec![wait(G, WaitReason::InFlightWork)],
        "the reclaim restarts rather than inheriting the elapsed time",
    );
    let wd = wind_down(&d1).unwrap();
    assert_eq!(
        (wd.since, wd.timeout, wd.reclaim),
        (
            sched_model::Millis(T + 5 * MIN),
            Some(sched_model::Millis(2 * MIN)),
            true
        )
    );
    let d2 = base(true, true).run_at(&d1.state, T + 7 * MIN - 1);
    assert_steps(
        &d2,
        vec![wait(G, WaitReason::InFlightWork)],
        "reclaim timeout not yet over",
    );
    let d3 = base(true, true).run_at(&d2.state, T + 7 * MIN);
    assert_steps(
        &d3,
        vec![kill("paperless-ocr"), drain("mineru")],
        "reclaim timeout over",
    );

    // The manual lease is released while winding down: nothing may be stopped any more.
    let d4 = base(false, false).run_fresh_at(&d0.state, T + 60 * MIN);
    assert_eq!(wind_down(&d4).unwrap().timeout, None);
    assert_steps(
        &d4,
        vec![wait(G, WaitReason::InFlightWork)],
        "unforced again",
    );
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
    let mut m = Case::new().gpu_state(&host_state(shutting, "{ phase: idle }", true));
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
