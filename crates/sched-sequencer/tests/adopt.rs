//! Which hosts are taken to be held by the user because they were found running.

use sched_model::test_support::{example_config, example_config_managed};
use sched_model::{Config, Fixture, Lease, ResourceId, SchedState, Snapshot};
use sched_sequencer::adopt::{adopted_id, adopted_lease, found_running, gone_down};

const T: u64 = 100_000_000;
const N: &str = "basestar-nfs";
const G: &str = "cuda-central";

fn snapshot(nfs_up: Option<bool>, gpu_up: Option<bool>, leases: &[&str]) -> Snapshot {
    let mut y = format!("description: t\nsnapshot:\n  now: {T}\n  observations:\n");
    for (r, u) in [(N, nfs_up), (G, gpu_up)] {
        if let Some(u) = u {
            y.push_str(&format!(
                "    - {{ source: cluster-pods, observed_at: {T}, fidelity: event, value: {{ type: resource_up, resource: {r}, up: {u} }} }}\n"
            ));
        }
    }
    y.push_str("  leases:\n");
    if leases.is_empty() {
        y.push_str("    []\n");
    }
    for l in leases {
        y.push_str(&format!("    - {l}\n"));
    }
    Fixture::from_yaml(&y)
        .unwrap_or_else(|e| panic!("{e}\n{y}"))
        .snapshot
}

fn state(nfs: &str) -> SchedState {
    let y =
        format!("description: t\nsnapshot: {{ now: 0 }}\nstate:\n  resources:\n    {N}: {nfs}\n");
    Fixture::from_yaml(&y).unwrap().state
}

fn found(config: &Config, snap: &Snapshot, prev: &SchedState) -> Vec<String> {
    found_with(config, snap, prev, &[])
}

fn found_with(
    config: &Config,
    snap: &Snapshot,
    prev: &SchedState,
    leases: &[Lease],
) -> Vec<String> {
    found_running(config, snap, prev, leases)
        .iter()
        .map(|r| r.to_string())
        .collect()
}

#[test]
fn a_shared_host_found_up_without_having_been_powered_on_by_us_is_adopted() {
    let config = example_config_managed();
    let up = snapshot(Some(true), Some(true), &[]);
    // First start, or discarded state: no entry. Only the shared host; the GPU host is exclusive.
    assert_eq!(found(&config, &up, &SchedState::default()), vec![N]);
    // A host that was off (a power button, Wake-on-LAN, or it went down and came back).
    let off = state("{ power: { phase: off }, mode: { phase: idle } }");
    assert_eq!(found(&config, &up, &off), vec![N]);
    // A power-on that timed out and was then seen up is treated like a host found running.
    let failed =
        state("{ power: { phase: failed, since: 5, during: powering_on }, mode: { phase: idle } }");
    assert_eq!(found(&config, &up, &failed), vec![N]);
}

#[test]
fn a_host_flowmanager_powered_on_or_already_held_or_not_read_up_is_not_adopted() {
    let config = example_config_managed();
    let up = snapshot(Some(true), None, &[]);
    for (name, s) in [
        (
            "we powered it on",
            "{ power: { phase: powering_on, since: 5 }, mode: { phase: idle }, managed: true }",
        ),
        (
            "restart with persisted state",
            "{ power: { phase: \"on\", since: 5 }, mode: { phase: idle }, managed: true }",
        ),
        (
            "already on and not managed (adopted before, or lease released)",
            "{ power: { phase: \"on\", since: 5 }, mode: { phase: idle } }",
        ),
        (
            "shutting down",
            "{ power: { phase: shutting_down, since: 5 }, mode: { phase: idle } }",
        ),
        (
            "failed shutdown",
            "{ power: { phase: failed, since: 5, during: shutting_down }, mode: { phase: idle } }",
        ),
    ] {
        assert!(found(&config, &up, &state(s)).is_empty(), "{name}");
    }
    let lease = "{ id: m, resource: basestar-nfs, granted_at: 1, kind: { type: manual_host } }";
    let held = snapshot(Some(true), None, &[lease]);
    assert!(
        found_with(&config, &held, &SchedState::default(), &held.leases).is_empty(),
        "held"
    );
    assert!(
        found(
            &config,
            &snapshot(Some(false), None, &[]),
            &SchedState::default()
        )
        .is_empty(),
        "down"
    );
    assert!(
        found(&config, &snapshot(None, None, &[]), &SchedState::default()).is_empty(),
        "no reading"
    );
}

#[test]
fn under_manual_control_nothing_is_adopted() {
    let config = example_config();
    let up = snapshot(Some(true), Some(true), &[]);
    assert!(found(&config, &up, &SchedState::default()).is_empty());
}

#[test]
fn only_an_adopted_lease_ends_when_its_host_is_seen_down() {
    let config = example_config_managed();
    let nfs = ResourceId::new(N);
    let adopted = adopted_lease(&nfs, sched_model::Millis(7));
    assert_eq!(adopted.id, adopted_id(&nfs));
    assert!(adopted.is_adopted_host());
    let person: Lease = serde_json::from_str(
        r#"{"id":"manual-host-1","resource":"basestar-nfs","kind":{"type":"manual_host"},"granted_at":1}"#,
    )
    .unwrap();
    assert!(!person.is_adopted_host());
    let leases = vec![adopted.clone(), person];
    let down = snapshot(Some(false), None, &[]);
    assert_eq!(gone_down(&config, &down, &leases), vec![adopted.id.clone()]);
    for snap in [snapshot(Some(true), None, &[]), snapshot(None, None, &[])] {
        assert!(gone_down(&config, &snap, &leases).is_empty());
    }
}
