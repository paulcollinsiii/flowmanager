//! Replays every scenario file against the example config and checks its expectations and
//! the safety invariants.

use std::path::Path;

use sched_model::test_support::example_config;
use sched_service::replay::{Scenario, run_checked};

fn replay(name: &str) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scenarios")
        .join(name);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let scenario = Scenario::from_yaml(&text).unwrap_or_else(|e| panic!("{name}: {e}"));
    if let Err(problems) = run_checked(&example_config(), &scenario) {
        panic!("{name}: {}\n{}", scenario.description, problems.join("\n"));
    }
}

macro_rules! scenarios {
    ($($test:ident => $file:literal),* $(,)?) => {
        $(#[test] fn $test() { replay($file); })*
    };
}

scenarios! {
    lifecycle_ocr_to_transcription => "lifecycle-ocr-to-transcription.yaml",
    stale_pods_block_allow => "stale-pods-block-allow.yaml",
    manual_nfs_lease => "manual-nfs-lease.yaml",
    backup_power_cycle => "backup-power-cycle.yaml",
    reclaim => "reclaim.yaml",
    manual_llm_lease => "manual-llm-lease.yaml",
    interactive_pin => "interactive-pin.yaml",
    readiness_precedence => "readiness-precedence.yaml",
    idle_release => "idle-release.yaml",
}

#[test]
fn every_scenario_file_has_a_test() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("scenarios");
    let count = std::fs::read_dir(dir).unwrap().count();
    assert_eq!(count, 9, "add new scenario files to the scenarios! list");
}
