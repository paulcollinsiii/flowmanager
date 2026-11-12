//! Helpers for tests in other crates (enable the `test-support` feature): the repository's
//! example configuration, bundled fixtures, and file loading.

use std::path::Path;

use crate::config::Config;
use crate::fixture::Fixture;

/// The example configuration describing every current workload.
pub const EXAMPLE_CONFIG: &str = include_str!("../../../config/example.yaml");

/// Bundled fixtures as `(name, yaml)`; each is valid against [`EXAMPLE_CONFIG`].
pub const FIXTURES: &[(&str, &str)] = &[
    ("idle", include_str!("../fixtures/idle.yaml")),
    (
        "manual-llm-lease",
        include_str!("../fixtures/manual-llm-lease.yaml"),
    ),
    (
        "ocr-loaded-transcription-waiting",
        include_str!("../fixtures/ocr-loaded-transcription-waiting.yaml"),
    ),
    (
        "serving-with-starved-queue",
        include_str!("../fixtures/serving-with-starved-queue.yaml"),
    ),
    (
        "stale-pod-observation",
        include_str!("../fixtures/stale-pod-observation.yaml"),
    ),
];

/// The example's `basestar-probe` source, a `tcp_probe`, as written in [`EXAMPLE_CONFIG`].
pub const EXAMPLE_TCP_PROBE_SOURCE: &str = "  - id: basestar-probe\n    kind: tcp_probe\n    observe: [reachability]\n    resync: 10s\n    params:\n      host: basestar\n      port: 2049\n      resource: basestar-nfs\n      timeout: 2s\n      # Consecutive agreeing readings before up/down flips (as for cluster-pods).\n      reachability_confirmations: 3\n";

/// The same source as a `prometheus` probe, for tests of that kind; see
/// [`example_with_prometheus_probe`].
pub const PROMETHEUS_PROBE_SOURCE: &str = "  - id: basestar-probe\n    kind: prometheus\n    observe: [reachability]\n    resync: 10s\n    params:\n      url: http://prometheus.monitoring.svc:9090\n      query: probe_success{instance=\"basestar\"}\n      resource: basestar-nfs\n      timeout: 5s\n      # Consecutive agreeing readings before up/down flips (as for cluster-pods).\n      reachability_confirmations: 3\n";

/// [`EXAMPLE_CONFIG`] with `basestar-probe` read through Prometheus instead of a TCP probe,
/// so the `prometheus` kind keeps a realistic test configuration.
pub fn example_with_prometheus_probe() -> String {
    assert!(
        EXAMPLE_CONFIG.contains(EXAMPLE_TCP_PROBE_SOURCE),
        "the example's basestar-probe source changed; update EXAMPLE_TCP_PROBE_SOURCE"
    );
    EXAMPLE_CONFIG.replace(EXAMPLE_TCP_PROBE_SOURCE, PROMETHEUS_PROBE_SOURCE)
}

pub fn example_config() -> Config {
    Config::from_yaml(EXAMPLE_CONFIG).expect("bundled example config is valid")
}

/// A class on the NFS share that may change power and the source that feeds its demand, for
/// tests of workload-driven power. The shipped example has no such class (its scheduled jobs
/// hold the share instead), but the planner and the sequencer still honour `may_change_power`.
pub const NFS_BATCH_CLASS: &str = "  - id: nfs-batch\n    requires:\n      - { resource: basestar-nfs }\n    priority: 70\n    preemptibility: none\n    may_change_power: true\n    demand: { source: nfs-batch-queue }\n\n";

/// The source of [`NFS_BATCH_CLASS`]: an observable kind without params, so only pushes feed it.
pub const NFS_BATCH_SOURCE: &str =
    "  - id: nfs-batch-queue\n    kind: http_json\n    observe: [demand]\n    resync: 60s\n\n";

/// `yaml` (the example, possibly edited) with [`NFS_BATCH_CLASS`] and [`NFS_BATCH_SOURCE`]
/// added before the `interactive-llm` class and the `llm-sessions` source.
pub fn with_nfs_batch(yaml: &str) -> String {
    let (class_anchor, source_anchor) = ("  - id: interactive-llm\n", "  - id: llm-sessions\n");
    assert!(
        yaml.contains(class_anchor) && yaml.contains(source_anchor),
        "the example's interactive-llm class or llm-sessions source changed; update with_nfs_batch"
    );
    yaml.replacen(class_anchor, &format!("{NFS_BATCH_CLASS}{class_anchor}"), 1)
        .replacen(
            source_anchor,
            &format!("{NFS_BATCH_SOURCE}{source_anchor}"),
            1,
        )
}

/// The example configuration with the power-changing NFS batch class (see [`with_nfs_batch`]).
pub fn example_config_with_nfs_batch() -> Config {
    Config::from_yaml(&with_nfs_batch(EXAMPLE_CONFIG)).expect("example with nfs batch is valid")
}

/// `yaml` with every `control: manual` power section switched to `control: managed`: the
/// scheduler powers hosts on and off (M4). Tests of the power machine use it.
pub fn managed_power(yaml: &str) -> String {
    let out = yaml.replace("      control: manual\n", "      control: managed\n");
    assert_ne!(out, yaml, "no `control: manual` line to switch");
    out
}

/// The example configuration with managed power (see [`managed_power`]).
pub fn example_yaml_managed() -> String {
    managed_power(EXAMPLE_CONFIG)
}

/// The example with managed power on the shared NFS host only: the exclusive GPU host
/// (the first power section) stays under `control: manual`, so a GPU host reading "up" is
/// never adopted and tests of the shared-host machinery are not about it.
pub fn example_yaml_managed_nfs_only() -> String {
    let out =
        example_yaml_managed().replacen("      control: managed\n", "      control: manual\n", 1);
    let config = Config::from_yaml(&out).expect("nfs-only managed example config is valid");
    assert!(
        !config.power_managed(&"cuda-central".into())
            && config.power_managed(&"basestar-nfs".into()),
        "the first power section of the example is no longer the GPU host's"
    );
    out
}

/// The example config with managed power.
pub fn example_config_managed() -> Config {
    Config::from_yaml(&example_yaml_managed()).expect("managed example config is valid")
}

/// The managed example plus `nfs-app`, a switched class on the powered NFS host (it may change
/// power) whose switch is read from the example's `ha-wikijs` source, and with
/// `paperless-ocr-pending` declaring `reads_from: nfs-app`: `paperless-ocr` is its one
/// dependent. The example itself keeps `paperless-ngx` a plain gate class, so this is where a
/// switched class on a host with a power section, and a dependent, are exercised.
pub fn example_yaml_switched_dependents() -> String {
    let class = "  - id: nfs-app\n    requires:\n      - { resource: basestar-nfs }\n    priority: 0\n    preemptibility: kill\n    may_change_power: true\n    switch: { source: ha-wikijs }\n\nsources:\n";
    let reads = "  - id: paperless-ocr-pending\n    kind: http_json\n    reads_from: nfs-app\n";
    let yaml = example_yaml_managed()
        .replacen("\nsources:\n", &format!("\n{class}"), 1)
        .replacen(
            "  - id: paperless-ocr-pending\n    kind: http_json\n",
            reads,
            1,
        );
    assert!(
        yaml.contains("reads_from: nfs-app") && yaml.contains("id: nfs-app"),
        "the example changed shape"
    );
    yaml
}

/// [`example_yaml_switched_dependents`], loaded.
pub fn example_config_switched_dependents() -> Config {
    Config::from_yaml(&example_yaml_switched_dependents())
        .unwrap_or_else(|e| panic!("switched example config is invalid: {e}"))
}

/// A bundled fixture by name, checked against the example config.
pub fn fixture(name: &str) -> Fixture {
    let (_, yaml) = FIXTURES
        .iter()
        .find(|(n, _)| *n == name)
        .unwrap_or_else(|| panic!("no bundled fixture named {name:?}"));
    Fixture::from_yaml_checked(yaml, &example_config()).expect("bundled fixture is valid")
}

/// Loads a config from a file. Reads the file system, so it is for tests and tools only.
pub fn load_config_file(path: impl AsRef<Path>) -> Result<Config, String> {
    let text = std::fs::read_to_string(path.as_ref())
        .map_err(|e| format!("{}: {e}", path.as_ref().display()))?;
    Config::from_yaml(&text).map_err(|e| e.to_string())
}

/// Loads a fixture from a file and checks it against `config`.
pub fn load_fixture_file(path: impl AsRef<Path>, config: &Config) -> Result<Fixture, String> {
    let text = std::fs::read_to_string(path.as_ref())
        .map_err(|e| format!("{}: {e}", path.as_ref().display()))?;
    Fixture::from_yaml_checked(&text, config).map_err(|e| e.to_string())
}

/// A recovery spec from YAML, for tests in other crates.
pub fn recovery_spec(yaml: &str) -> crate::RecoverySpec {
    serde_yaml_ng::from_str(yaml).expect("a recovery spec")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn example_config_describes_every_workload() {
        let c = example_config();
        for id in [
            "paperless-ocr",
            "paperless-tagging",
            "speakr-transcription",
            "speakr-summarization",
            "immich-ml",
            "interactive-llm",
            "paperless-ngx",
            "immich-server",
            "wikijs",
        ] {
            assert!(c.class(&id.into()).is_some(), "missing class {id}");
        }
        assert_eq!(c.resources().len(), 3);
        assert_eq!(c.models().len(), 4);
        assert!(c.class(&"paperless-ngx".into()).unwrap().demand.is_none());
    }

    #[test]
    fn the_prometheus_variant_of_the_example_is_valid_and_differs_only_in_the_probe() {
        let tcp = example_config();
        let prom = Config::from_yaml(&example_with_prometheus_probe()).unwrap();
        let id = "basestar-probe".into();
        assert_eq!(
            tcp.source(&id).unwrap().kind,
            crate::config::SourceKind::TcpProbe
        );
        assert_eq!(
            prom.source(&id).unwrap().kind,
            crate::config::SourceKind::Prometheus
        );
        assert_eq!(tcp.resources(), prom.resources());
        assert_eq!(tcp.classes(), prom.classes());
        assert_eq!(tcp.sources().len(), prom.sources().len());
        for (a, b) in tcp.sources().iter().zip(prom.sources()) {
            assert!(a == b || a.id == id, "{} differs", a.id);
        }
    }

    #[test]
    fn bundled_fixtures_load_and_match_config() {
        for (name, _) in FIXTURES {
            let f = fixture(name);
            assert!(!f.description.is_empty());
        }
    }

    #[test]
    fn fixture_with_dangling_references_is_rejected() {
        let bad = "description: x\nsnapshot:\n  now: 10\n  observations:\n    - source: nope\n      observed_at: 20\n      fidelity: event\n      value: { type: demand, class: ghost, ready: 1 }\n  leases:\n    - id: l\n      resource: cuda-central\n      granted_at: 0\n      kind: { type: manual_model, model: whisperx }\n    - id: l\n      resource: basestar-nfs\n      granted_at: 0\n      kind: { type: interactive, model: llm, expires_at: 5 }\n";
        let msg = Fixture::from_yaml_checked(bad, &example_config())
            .unwrap_err()
            .to_string();
        for needle in [
            "unknown source \"nope\"",
            "after now",
            "unknown class \"ghost\"",
            "duplicate lease id",
            "does not belong to resource",
        ] {
            assert!(msg.contains(needle), "missing {needle:?} in {msg}");
        }
    }

    fn check_msgs(obs: &str, leases: &str) -> String {
        let yaml = format!(
            "description: x\nsnapshot:\n  now: 1000\n  observations:\n{obs}  leases:\n{leases}"
        );
        match Fixture::from_yaml_checked(&yaml, &example_config()) {
            Err(e @ crate::fixture::FixtureError::Inconsistent(_)) => e.to_string(),
            other => format!("OK-or-parse: {other:?}"),
        }
    }

    fn demand(class: &str, source: &str, rest: &str) -> String {
        format!(
            "    - {{ source: {source}, observed_at: 900, fidelity: polled, value: {{ type: demand, class: {class}, {rest} }} }}\n"
        )
    }

    #[test]
    fn a_switch_observation_is_checked_against_the_switched_class() {
        let config = Config::from_yaml(EXAMPLE_CONFIG).unwrap();
        let check = |class: &str, source: &str, state: &str| {
            let yaml = format!(
                "description: x\nsnapshot:\n  now: 1000\n  observations:\n    - {{ source: {source}, observed_at: 900, fidelity: polled, value: {{ type: switch, class: {class}, state: {state} }} }}\n  leases: []\n"
            );
            match Fixture::from_yaml_checked(&yaml, &config) {
                Ok(f) => Ok(f.snapshot.observations[0].value.clone()),
                Err(e) => Err(e.to_string()),
            }
        };
        for (text, state) in [
            ("on", crate::SwitchState::On),
            ("off", crate::SwitchState::Off),
            ("off_now", crate::SwitchState::OffNow),
        ] {
            assert_eq!(
                check("wikijs", "ha-wikijs", text),
                Ok(crate::ObservedValue::Switch {
                    class: "wikijs".into(),
                    state
                })
            );
        }
        let m = check("wikijs", "ha-wikijs", "off-now").unwrap_err();
        assert!(m.contains("off-now"), "{m}");
        let m = check("paperless-ngx", "ha-wikijs", "on").unwrap_err();
        assert!(m.contains("has no switch source"), "{m}");
        let m = check("wikijs", "immich-queues", "on").unwrap_err();
        assert!(
            m.contains("takes its switch from source \"ha-wikijs\""),
            "{m}"
        );
        assert!(m.contains("does not declare the switch capability"), "{m}");
        let m = check("ghost", "ha-wikijs", "on").unwrap_err();
        assert!(m.contains("unknown class \"ghost\""), "{m}");
    }

    #[test]
    fn rejects_inconsistent_observations() {
        let m = check_msgs(
            &demand("paperless-ngx", "immich-queues", "ready: 1"),
            "    []\n",
        );
        assert!(m.contains("has no demand source"), "{m}");
        let m = check_msgs(
            &demand("paperless-ocr", "immich-queues", "ready: 1"),
            "    []\n",
        );
        assert!(
            m.contains("takes demand from source \"paperless-ocr-pending\""),
            "{m}"
        );
        let m = check_msgs(
            &demand(
                "paperless-ocr",
                "paperless-ocr-pending",
                "ready: 0, oldest_ready_at: 10",
            ),
            "    []\n",
        );
        assert!(m.contains("nothing is ready"), "{m}");
        let m = check_msgs(
            &demand(
                "paperless-ocr",
                "paperless-ocr-pending",
                "ready: 1, oldest_ready_at: 950",
            ),
            "    []\n",
        );
        assert!(m.contains("is after observed_at"), "{m}");
        let dup = demand("paperless-ocr", "paperless-ocr-pending", "ready: 1")
            + &demand("paperless-ocr", "paperless-ocr-pending", "ready: 2");
        assert!(check_msgs(&dup, "    []\n").contains("duplicate observation"));
        let pods = "    - { source: cluster-pods, observed_at: 900, fidelity: event, value: { type: model_pods, model: llm, pods: 1, ready_pods: 2 } }\n";
        assert!(check_msgs(pods, "    []\n").contains("ready_pods (2) exceeds pods (1)"));
        let up = "    - { source: cluster-pods, observed_at: 900, fidelity: event, value: { type: resource_up, resource: cuda-central, up: true } }\n";
        assert!(!check_msgs(up, "    []\n").contains("problem"));
    }

    #[test]
    fn rejects_inconsistent_leases() {
        let none = "    []\n";
        let m = check_msgs(
            none,
            "    - { id: a, resource: cuda-central, granted_at: 2000, kind: { type: manual_host } }\n",
        );
        assert!(m.contains("granted_at (2000ms) is after now"), "{m}");
        let m = check_msgs(
            none,
            "    - { id: a, resource: cuda-central, granted_at: 500, kind: { type: interactive, model: llm, expires_at: 400 } }\n",
        );
        assert!(m.contains("not after granted_at"), "{m}");
        let two = "    - { id: a, resource: cuda-central, granted_at: 5, kind: { type: manual_model, model: llm } }\n    - { id: b, resource: cuda-central, granted_at: 5, kind: { type: manual_model, model: whisperx } }\n";
        assert!(check_msgs(none, two).contains("conflict"));
        let m = check_msgs(
            none,
            "    - { id: a, resource: cuda-central, granted_at: 5, kind: { type: shared_hold, holder: h } }\n",
        );
        assert!(m.contains("non-shared resource"), "{m}");
    }

    #[test]
    fn fixture_errors_use_fixture_wording() {
        let e = Fixture::from_yaml("description: x\nsnapshot: {}\n")
            .unwrap_err()
            .to_string();
        assert!(e.starts_with("cannot parse fixture"), "{e}");
        assert!(
            check_msgs(&demand("ghost", "immich-queues", "ready: 1"), "    []\n")
                .starts_with("fixture disagrees")
        );
    }

    #[test]
    fn fixture_queries_find_newest() {
        let f = fixture("ocr-loaded-transcription-waiting");
        let s = &f.snapshot;
        let cfg = example_config();
        assert!(s.demand(&cfg, &"paperless-ocr".into()).is_some());
        assert_eq!(
            s.model_pods(&cfg, &"mineru".into())
                .unwrap()
                .age(s.now)
                .as_u64(),
            5000
        );
        assert!(s.model_pods(&cfg, &"llm".into()).is_none());
    }

    #[test]
    fn files_load_from_disk() {
        let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
        let cfg = load_config_file(format!("{root}/config/example.yaml")).unwrap();
        let f = load_fixture_file(
            concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/idle.yaml"),
            &cfg,
        )
        .unwrap();
        assert_eq!(f.snapshot.leases.len(), 0);
    }

    #[test]
    fn fixtures_accept_readings_only_from_sources_declaring_the_capability() {
        let cfg = example_config();
        let obs = |source: &str, value: &str| {
            format!(
                "description: x\nsnapshot:\n  now: 1000\n  observations:\n    - {{ source: {source}, observed_at: 900, fidelity: event, value: {value} }}\n"
            )
        };
        let pods = "{ type: model_pods, model: llm, pods: 1, ready_pods: 1 }";
        let up = "{ type: resource_up, resource: cuda-central, up: true }";
        let dem = "{ type: demand, class: paperless-ocr, ready: 1 }";
        // A pods reading from a source that declares only demand, or only reachability.
        for src in ["paperless-ocr-pending", "basestar-probe"] {
            let m = Fixture::from_yaml_checked(&obs(src, pods), &cfg)
                .unwrap_err()
                .to_string();
            assert!(
                m.contains("does not declare the pods capability"),
                "{src}: {m}"
            );
        }
        // Reachability from a source that does not declare it.
        let m = Fixture::from_yaml_checked(&obs("paperless-ocr-pending", up), &cfg)
            .unwrap_err()
            .to_string();
        assert!(
            m.contains("does not declare the reachability capability"),
            "{m}"
        );
        // Demand from a source that does not declare it.
        let m = Fixture::from_yaml_checked(&obs("cluster-pods", dem), &cfg)
            .unwrap_err()
            .to_string();
        assert!(m.contains("does not declare the demand capability"), "{m}");
        // The declaring sources pass, including a non-Kubernetes source for reachability.
        assert!(Fixture::from_yaml_checked(&obs("cluster-pods", pods), &cfg).is_ok());
        assert!(Fixture::from_yaml_checked(&obs("cluster-pods", up), &cfg).is_ok());
        assert!(Fixture::from_yaml_checked(&obs("basestar-probe", up), &cfg).is_ok());
        // A typo in the source id is reported, not silently ignored.
        let m = Fixture::from_yaml_checked(&obs("cluster-pod", pods), &cfg)
            .unwrap_err()
            .to_string();
        assert!(m.contains("unknown source \"cluster-pod\""), "{m}");
    }

    /// The example with the NFS host's power section removed (an always-on resource).
    fn example_without_nfs_power() -> String {
        EXAMPLE_CONFIG.replace(
            "    power:\n      wake_on_lan: { router_url: \"https://router.example.internal\", interface: lan, mac: \"02:00:00:00:00:02\" }\n      shutdown: { host: basestar, port: 22, user: poweroff, host_key: \"SHA256:BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB\" }\n      control: manual\n      # Set per resource: the NFS host boots and stops faster than the GPU host.\n      power_on_timeout: 5m\n      shutdown_timeout: 10m\n      # Under managed control, how long a shutdown may wait for pods to stop mounting this\n      # host (see the basestar-mounts source) before the scheduler alerts and stops, without\n      # shutting it down and without retrying (default 30m, at most 1d).\n      veto_timeout: 30m\n",
            "",
        )
    }

    #[test]
    fn resource_up_needs_a_power_section() {
        let yaml = example_without_nfs_power();
        let yaml = yaml.replace("    may_change_power: true\n", "");
        assert_ne!(yaml, EXAMPLE_CONFIG);
        let config = Config::from_yaml(&yaml).unwrap();
        let fx = "description: x\nsnapshot:\n  now: 10\n  observations:\n    - { source: cluster-pods, observed_at: 5, fidelity: event, value: { type: resource_up, resource: basestar-nfs, up: true } }\n";
        let msg = Fixture::from_yaml_checked(fx, &config)
            .unwrap_err()
            .to_string();
        assert!(msg.contains("has no power section"), "{msg}");
        assert!(
            Fixture::from_yaml_checked(&fx.replace("basestar-nfs", "cuda-central"), &config)
                .is_ok()
        );
    }

    #[test]
    fn fixture_state_and_lockout_are_checked() {
        let cfg = Config::from_yaml(
            &example_without_nfs_power().replace("    may_change_power: true\n", ""),
        )
        .unwrap();
        let bad = "description: x\nsnapshot:\n  config_generation: 4\n  now: 10\n  leases:\n    - { id: r, resource: basestar-nfs, granted_at: 1, kind: { type: lockout } }\nstate:\n  resources:\n    ghost: { power: { phase: \"on\", since: 0 }, mode: { phase: idle } }\n    cuda-central: { power: { phase: \"on\", since: 0 }, mode: { phase: serving, model: immich-ml, since: 0 } }\n    basestar-nfs: { power: { phase: \"on\", since: 0 }, mode: { phase: loading, model: llm, since: 0 } }\n";
        let msg = Fixture::from_yaml_checked(bad, &cfg)
            .unwrap_err()
            .to_string();
        for needle in [
            "does not match the config generation",
            "lockout on resource",
            "unknown resource \"ghost\"",
            "a shared resource has no model mode",
            "does not belong to this resource",
        ] {
            assert!(msg.contains(needle), "missing {needle:?} in {msg}");
        }
        // immich-ml does belong to cuda-central; only the NFS entry is wrong there.
        assert_eq!(msg.matches("does not belong to this resource").count(), 1);
    }
}
