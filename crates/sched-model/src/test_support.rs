//! Helpers for tests in other crates (enable the `test-support` feature): the repository's
//! example configuration, bundled fixtures, and file loading.

use std::path::Path;

use crate::config::{Config, ConfigError};
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
        "stale-pod-observation",
        include_str!("../fixtures/stale-pod-observation.yaml"),
    ),
];

pub fn example_config() -> Config {
    Config::from_yaml(EXAMPLE_CONFIG).expect("bundled example config is valid")
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
    Config::from_yaml(&text).map_err(|e: ConfigError| e.to_string())
}

/// Loads a fixture from a file and checks it against `config`.
pub fn load_fixture_file(path: impl AsRef<Path>, config: &Config) -> Result<Fixture, String> {
    let text = std::fs::read_to_string(path.as_ref())
        .map_err(|e| format!("{}: {e}", path.as_ref().display()))?;
    Fixture::from_yaml_checked(&text, config).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Preemptibility;

    #[test]
    fn example_config_describes_every_workload() {
        let c = example_config();
        for id in [
            "paperless-ocr",
            "paperless-tagging",
            "speakr-transcription",
            "speakr-summarization",
            "immich-ml",
            "backup",
            "interactive-llm",
            "paperless-ngx",
            "immich-server",
        ] {
            assert!(c.class(&id.into()).is_some(), "missing class {id}");
        }
        assert_eq!(c.resources().len(), 2);
        assert_eq!(c.models().len(), 4);
        let backup = c.class(&"backup".into()).unwrap();
        assert!(backup.may_change_power);
        assert_eq!(backup.preemptibility, Preemptibility::None);
        assert!(c.class(&"paperless-ngx".into()).unwrap().demand.is_none());
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

    #[test]
    fn fixture_queries_find_newest() {
        let f = fixture("ocr-loaded-transcription-waiting");
        let s = &f.snapshot;
        assert!(s.demand(&"paperless-ocr".into()).is_some());
        assert_eq!(
            s.model_pods(&"mineru".into()).unwrap().age(s.now).as_u64(),
            5000
        );
        assert!(s.model_pods(&"llm".into()).is_none());
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
}
