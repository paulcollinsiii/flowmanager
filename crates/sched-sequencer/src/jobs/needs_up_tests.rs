//! `needs_up`: when a job's needs count as up (the lockout and the stopped-veto rules).

use sched_model::test_support::example_config;
use sched_model::{
    Config, Lease, LeaseId, LeaseKind, LeaseOrigin, Millis, PowerPhase, ResourceState, SchedState,
    Shutdown, Snapshot,
};

use super::needs_up;

fn on() -> ResourceState {
    ResourceState::new(PowerPhase::On {
        since: Millis(1),
        ours: false,
    })
}

fn lockout(res: &str) -> Lease {
    Lease {
        id: LeaseId::new("lockout-1"),
        resource: res.into(),
        kind: LeaseKind::Lockout,
        granted_at: Millis(0),
        origin: LeaseOrigin::Requested,
    }
}

#[test]
fn a_locked_out_host_is_not_up_for_a_job_whatever_its_kind_and_power() {
    let config = example_config();
    let mut spec = config.jobs()[0].clone();
    let snapshot = Snapshot::default();
    // A job only needs a shared host in a validated config; the rule is by host, so it is
    // also checked with a need on the exclusive host (hand-built).
    for res in ["basestar-nfs", "cuda-central"] {
        spec.needs = vec![sched_model::JobNeed::Nfs {
            resource: res.into(),
        }];
        let mut state = SchedState::default();
        state.resources.insert(res.into(), on());
        assert!(needs_up(&config, &snapshot, &state, &[], &spec), "{res}");
        assert!(
            !needs_up(&config, &snapshot, &state, &[lockout(res)], &spec),
            "{res}: locked out"
        );
        // A lockout of another host does not matter.
        let other = if res == "basestar-nfs" {
            "cuda-central"
        } else {
            "basestar-nfs"
        };
        assert!(needs_up(
            &config,
            &snapshot,
            &state,
            &[lockout(other)],
            &spec
        ));
    }
}

#[test]
fn a_lockout_lease_on_a_resource_without_a_power_section_does_not_stop_a_job() {
    // After a config reload took the NFS host's power section away it is always on: a
    // lockout lease left in the book means nothing there (the sequencer reports it as
    // unsupported), so a job needing it still starts.
    let y = sched_model::test_support::EXAMPLE_CONFIG;
    let marker = "  - id: basestar-nfs\n    kind: shared\n";
    let start = y.find(marker).unwrap() + marker.len();
    let end = y.find("\nmodels:").unwrap();
    let y = format!("{}{}", &y[..start], &y[end..]).replace("    may_change_power: true\n", "");
    let config = Config::from_yaml(&y).unwrap();
    let spec = config.jobs()[0].clone();
    let nfs = spec.needs[0].resource().clone();
    assert!(!config.supports_lockout(&nfs));
    let lock = lockout(nfs.as_str());
    let mut state = SchedState::default();
    state.resources.insert(nfs, on());
    assert!(needs_up(
        &config,
        &Snapshot::default(),
        &state,
        &[lock],
        &spec
    ));
}

#[test]
fn a_host_that_is_on_but_whose_shutdown_flowmanager_gave_up_on_is_not_up_for_a_job() {
    let config = example_config();
    let spec = config.jobs()[0].clone();
    let nfs = spec.needs[0].resource().clone();
    let snapshot = Snapshot::default();
    let mut st = on();
    let mut state = SchedState::default();
    state.resources.insert(nfs.clone(), st.clone());
    assert!(needs_up(&config, &snapshot, &state, &[], &spec), "on: up");
    // Waiting for the mounts to go away is still up (a shutdown is merely held back).
    st.shutdown = Some(Shutdown::idle_withdrawn(Millis(2)).vetoing(Millis(2)));
    state.resources.insert(nfs.clone(), st.clone());
    assert!(needs_up(&config, &snapshot, &state, &[], &spec));
    // Stopped: the host is On, but flowmanager stopped managing it; a job does not start.
    st.shutdown = Some(
        Shutdown::idle_withdrawn(Millis(2))
            .vetoing(Millis(2))
            .stopped(Millis(3)),
    );
    state.resources.insert(nfs, st);
    assert!(!needs_up(&config, &snapshot, &state, &[], &spec));
}
