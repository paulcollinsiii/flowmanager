//! Replays every scenario file against the example config and checks its expectations and
//! the safety invariants.

use std::path::Path;

use sched_model::PowerControl;
use sched_model::test_support::{example_config, example_config_managed};
use sched_service::replay::{Scenario, run_checked};

fn replay(name: &str) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scenarios")
        .join(name);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let scenario = Scenario::from_yaml(&text).unwrap_or_else(|e| panic!("{name}: {e}"));
    let config = match scenario.power_control {
        Some(PowerControl::Managed) => example_config_managed(),
        Some(PowerControl::Manual) | None => example_config(),
    };
    let config = match scenario.min_off {
        Some(m) => {
            // The example text, not a Config, carries the key: set it on both powered hosts.
            let y = sched_model::test_support::example_yaml_managed().replacen(
                "power_on_timeout:",
                &format!("min_off: {}ms\n      power_on_timeout:", m.as_u64()),
                2,
            );
            sched_model::Config::from_yaml(&y).unwrap()
        }
        None => config,
    };
    if let Err(problems) = run_checked(&config, &scenario) {
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
    stale_reachability_blocks_allow => "stale-reachability-blocks-allow.yaml",
    manual_nfs_lease => "manual-nfs-lease.yaml",
    job_hold_power_cycle => "job-hold-power-cycle.yaml",
    lockout => "lockout.yaml",
    manual_llm_lease => "manual-llm-lease.yaml",
    interactive_pin => "interactive-pin.yaml",
    readiness_precedence => "readiness-precedence.yaml",
    idle_release => "idle-release.yaml",
    nfs_probe_reachability => "nfs-probe-reachability.yaml",
    gate_follows_host_readiness => "gate-follows-host-readiness.yaml",
    failed_transcriptions => "failed-transcriptions.yaml",
    failed_never_ages => "failed-never-ages.yaml",
    host_never_comes_up => "host-never-comes-up.yaml",
    host_never_goes_down => "host-never-goes-down.yaml",
    nfs_lease_mount_check => "nfs-lease-mount-check.yaml",
    nfs_two_holders => "nfs-two-holders.yaml",
    nfs_unmount_timeout => "nfs-unmount-timeout.yaml",
    min_off_wait => "min-off-wait.yaml",
    min_off_manual_lease => "min-off-manual-lease.yaml",
    exclusive_host_lease => "exclusive-host-lease.yaml",
}

#[test]
fn every_scenario_file_has_a_test() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("scenarios");
    let count = std::fs::read_dir(dir).unwrap().count();
    assert_eq!(count, 21, "add new scenario files to the scenarios! list");
}
