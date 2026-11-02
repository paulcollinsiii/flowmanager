//! Which hosts are taken to be held by the user because they were found running.

use sched_model::test_support::{example_config, example_config_managed};
use sched_model::{Config, Fixture, Lease, LeaseOrigin, Millis, ResourceId, SchedState, Snapshot};
use sched_sequencer::adopt::{adopt, adopted_id, adopted_lease, found_running, gone_down};

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
    found_running(config, snap, prev, leases, Millis(T))
        .iter()
        .map(|r| r.to_string())
        .collect()
}

#[test]
fn a_shared_host_found_up_without_having_been_powered_on_by_us_is_adopted() {
    let config = example_config_managed();
    let up = snapshot(Some(true), None, &[]);
    // First start, or discarded state: no entry. The shared host.
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
            "{ power: { phase: powering_on, since: 5 }, mode: { phase: idle } }",
        ),
        (
            "restart with persisted state",
            "{ power: { phase: \"on\", since: 5, ours: true }, mode: { phase: idle } }",
        ),
        (
            "ours, and a drain is under way",
            "{ power: { phase: \"on\", since: 5, ours: true }, mode: { phase: idle }, shutdown: { cause: { kind: requested, mode: graceful }, since: 5, withdrawal: pending, veto: { phase: none } } }",
        ),
        (
            "up, not ours, but the shutdown was stopped by mounting pods",
            "{ power: { phase: \"on\", since: 5 }, mode: { phase: idle }, shutdown: { cause: { kind: idle }, since: 5, withdrawal: not_needed, veto: { phase: stopped, since: 6 } } }",
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
    assert_eq!(adopted.origin, LeaseOrigin::Adopted);
    let person: Lease = serde_json::from_str(
        r#"{"id":"manual-host-1","resource":"basestar-nfs","kind":{"type":"manual_host"},"granted_at":1}"#,
    )
    .unwrap();
    assert!(!person.is_adopted_host());
    // A person's lease is never adopted, whatever its id says: the origin decides.
    let lookalike = Lease {
        id: adopted_id(&nfs),
        origin: LeaseOrigin::Requested,
        ..adopted.clone()
    };
    assert!(!lookalike.is_adopted_host());
    assert!(
        gone_down(&config, &snapshot(Some(false), None, &[]), &[lookalike]).is_empty(),
        "a person's lease is not dropped when the host goes down"
    );
    let leases = vec![adopted.clone(), person];
    let down = snapshot(Some(false), None, &[]);
    assert_eq!(gone_down(&config, &down, &leases), vec![adopted.id.clone()]);
    for snap in [snapshot(Some(true), None, &[]), snapshot(None, None, &[])] {
        assert!(gone_down(&config, &snap, &leases).is_empty());
    }
}

#[test]
fn a_host_that_is_up_and_not_ours_is_adopted_even_while_a_job_holds_it() {
    let config = example_config_managed();
    let up = snapshot(Some(true), None, &[]);
    // Up under managed control and not ours: a host switched from manual control while it
    // was up, or one whose stopped shutdown was cleared by `resume`.
    let old = state("{ power: { phase: \"on\", since: 5 }, mode: { phase: idle } }");
    assert_eq!(found(&config, &up, &old), vec![N]);
    // A job's hold plays no part (D6): the host is adopted at once.
    let hold =
        "{ id: h, resource: basestar-nfs, granted_at: 1, kind: { type: shared_hold, holder: x } }";
    let held = snapshot(Some(true), None, &[hold]);
    assert_eq!(found_with(&config, &held, &old, &held.leases), vec![N]);
    assert_eq!(
        found_with(&config, &held, &SchedState::default(), &held.leases),
        vec![N]
    );
    // Nor one that already has a manual host lease.
    let lease = "{ id: m, resource: basestar-nfs, granted_at: 1, kind: { type: manual_host } }";
    let leased = snapshot(Some(true), None, &[lease]);
    assert!(found_with(&config, &leased, &old, &leased.leases).is_empty());
    // A host we manage (powered on by us, or taken over) is never found again.
    let ours = state("{ power: { phase: \"on\", since: 5, ours: true }, mode: { phase: idle } }");
    assert!(found(&config, &up, &ours).is_empty());
}

#[test]
fn a_host_we_powered_on_that_went_down_for_a_moment_is_not_found_running_when_it_is_back() {
    let config = example_config_managed();
    let up = snapshot(Some(true), None, &[]);
    // Off with the flag still set: only a shutdown we started (`power_off`) clears it.
    let blip = state("{ power: { phase: off, ours: true }, mode: { phase: idle } }");
    assert!(found(&config, &up, &blip).is_empty());
    // Off after our own shutdown, or found down: adopted when it comes back.
    let after = state("{ power: { phase: off }, mode: { phase: idle } }");
    assert_eq!(found(&config, &up, &after), vec![N]);
}

fn gpu_state(gpu: &str) -> SchedState {
    let y =
        format!("description: t\nsnapshot: {{ now: 0 }}\nstate:\n  resources:\n    {G}: {gpu}\n");
    Fixture::from_yaml(&y).unwrap().state
}

#[test]
fn an_exclusive_managed_host_found_up_is_adopted_like_a_shared_one() {
    let config = example_config_managed();
    let up = snapshot(Some(true), Some(true), &[]);
    assert_eq!(
        found(&config, &up, &SchedState::default()),
        vec![G, N],
        "first start: both hosts are found running"
    );
    let gpu_up = snapshot(None, Some(true), &[]);
    assert_eq!(found(&config, &gpu_up, &SchedState::default()), vec![G]);
    // Off after a shutdown we started (not ours), or a power-on that failed: adopted.
    let off = gpu_state("{ power: { phase: off }, mode: { phase: idle } }");
    assert_eq!(found(&config, &gpu_up, &off), vec![G]);
    let failed = gpu_state(
        "{ power: { phase: failed, since: 5, during: powering_on }, mode: { phase: idle } }",
    );
    assert_eq!(found(&config, &gpu_up, &failed), vec![G]);
    // Down, or no reading: nothing.
    assert!(
        found(
            &config,
            &snapshot(None, Some(false), &[]),
            &SchedState::default()
        )
        .is_empty()
    );
    assert!(found(&config, &snapshot(None, None, &[]), &SchedState::default()).is_empty());
    // A manual host lease already held: not adopted again.
    let lease = "{ id: m, resource: cuda-central, granted_at: 1, kind: { type: manual_host } }";
    let leased = snapshot(None, Some(true), &[lease]);
    assert!(found_with(&config, &leased, &SchedState::default(), &leased.leases).is_empty());
}

#[test]
fn an_exclusive_host_flowmanager_powered_on_or_that_blipped_down_and_back_is_not_adopted() {
    let config = example_config_managed();
    let up = snapshot(None, Some(true), &[]);
    for (name, s) in [
        (
            "we powered it on",
            "{ power: { phase: powering_on, since: 5 }, mode: { phase: idle } }",
        ),
        (
            "restart with persisted state",
            "{ power: { phase: \"on\", since: 5, ours: true }, mode: { phase: serving, model: llm, since: 5 } }",
        ),
        (
            "down for a moment with the flag still set",
            "{ power: { phase: off, ours: true }, mode: { phase: idle } }",
        ),
        (
            "shutting down",
            "{ power: { phase: shutting_down, since: 5 }, mode: { phase: idle } }",
        ),
    ] {
        assert!(found(&config, &up, &gpu_state(s)).is_empty(), "{name}");
    }
}

#[test]
fn an_exclusive_host_under_manual_control_is_never_adopted() {
    // The GPU host is `control: manual` while the NFS host is managed.
    let config = Config::from_yaml(&sched_model::test_support::example_yaml_managed_nfs_only())
        .expect("valid");
    let up = snapshot(Some(true), Some(true), &[]);
    assert_eq!(found(&config, &up, &SchedState::default()), vec![N]);
}

#[test]
fn a_locked_out_host_is_never_adopted_on_either_kind_until_the_lockout_is_cleared() {
    let config = example_config_managed();
    let locks = [
        "{ id: l1, resource: basestar-nfs, granted_at: 1, kind: { type: lockout } }",
        "{ id: l2, resource: cuda-central, granted_at: 1, kind: { type: lockout } }",
    ];
    let locked = snapshot(Some(true), Some(true), &locks);
    let cleared = snapshot(Some(true), Some(true), &[]);
    let settled_up =
        state("{ power: { phase: \"on\", since: 5 }, mode: { phase: idle }, locked_down: true }");
    let settled_off = state("{ power: { phase: off }, mode: { phase: idle }, locked_down: true }");
    for (name, prev) in [
        ("no state", SchedState::default()),
        ("settled, booted", settled_off),
        ("settled, seen up", settled_up),
    ] {
        assert!(
            found_with(&config, &locked, &prev, &locked.leases).is_empty(),
            "{name}: locked out"
        );
        // Cleared: found running like any other host (the GPU host has no entry in `state`).
        assert!(
            found_with(&config, &cleared, &prev, &[]).contains(&N.to_string()),
            "{name}: cleared"
        );
    }
}

#[test]
fn a_not_ours_host_with_a_lockout_shutdown_left_over_is_adopted_the_tick_the_lockout_is_gone() {
    let config = example_config_managed();
    let up = snapshot(Some(true), None, &[]);
    // The previous state still carries the lockout's shutdown; the lockout lease is gone.
    let left_over = state(
        "{ power: { phase: \"on\", since: 5 }, mode: { phase: idle }, shutdown: { cause: { kind: lockout }, since: 5, withdrawal: pending, veto: { phase: none } } }",
    );
    assert_eq!(found(&config, &up, &left_over), vec![N]);
    // While the lockout lease stands nothing is adopted.
    let lock = "{ id: l, resource: basestar-nfs, granted_at: 1, kind: { type: lockout } }";
    let locked = snapshot(Some(true), None, &[lock]);
    assert!(found_with(&config, &locked, &left_over, &locked.leases).is_empty());
    // A stopped veto still blocks adoption.
    let stopped = state(
        "{ power: { phase: \"on\", since: 5 }, mode: { phase: idle }, shutdown: { cause: { kind: lockout }, since: 5, withdrawal: done, veto: { phase: stopped, since: 6 } } }",
    );
    assert!(found(&config, &up, &stopped).is_empty());
}

fn adopt_at(
    config: &Config,
    snap: &Snapshot,
    prev: &SchedState,
) -> sched_sequencer::adopt::Adoption {
    adopt(config, snap, prev, &snap.leases, Millis(T))
}

#[test]
fn adopt_mints_an_adopted_lease_for_each_host_found_running_and_changes_nothing_else() {
    let config = example_config_managed();
    let keep =
        "{ id: h, resource: basestar-nfs, granted_at: 1, kind: { type: shared_hold, holder: x } }";
    let snap = snapshot(Some(true), Some(true), &[keep]);
    let prev = SchedState::default();
    let a = adopt_at(&config, &snap, &prev);
    assert_eq!(
        a.adopted,
        vec![ResourceId::new(G), ResourceId::new(N)],
        "both hosts are up and nothing is ours"
    );
    assert!(a.ended.is_empty());
    assert_eq!(a.prev, prev, "no state to edit");
    assert_eq!(a.leases.len(), 3);
    assert_eq!(a.leases[0], snap.leases[0], "the hold is untouched, first");
    for l in &a.leases[1..] {
        assert!(l.is_adopted_host() && l.granted_at == Millis(T), "{l:?}");
    }
    // Deterministic: the same inputs give the same answer, and applying it is idempotent (the
    // next tick finds the manual host lease it just minted).
    assert_eq!(a, adopt_at(&config, &snap, &prev));
    let mut again = snap.clone();
    again.leases = a.leases.clone();
    let b = adopt_at(&config, &again, &prev);
    assert!(b.adopted.is_empty() && b.leases == a.leases, "{b:?}");
}

#[test]
fn adopt_ends_an_adopted_lease_of_a_host_seen_down_and_the_host_stops_being_ours() {
    let config = example_config_managed();
    let nfs = ResourceId::new(N);
    // Adopted, then ours through the manual want the lease makes: on and ours, idle clock set.
    let mut prev = state(
        "{ power: { phase: \"on\", since: 5, ours: true }, mode: { phase: idle }, idle_since: 7 }",
    );
    let lease = adopted_lease(&nfs, Millis(5));
    let down = snapshot(Some(false), None, &[]);
    let a = adopt(
        &config,
        &down,
        &prev,
        std::slice::from_ref(&lease),
        Millis(T),
    );
    assert_eq!(a.ended, vec![nfs.clone()]);
    assert!(a.leases.is_empty() && a.adopted.is_empty());
    let st = a.prev.resource(&nfs).unwrap();
    assert!(!st.ours() && st.idle_since.is_none(), "{st:?}");
    // Only those two fields changed.
    let before = prev.resources.get_mut(&nfs).unwrap();
    before.set_ours(false);
    before.idle_since = None;
    assert_eq!(&a.prev, &prev);
    // The person's next power-on is found running and adopted afresh.
    let up = snapshot(Some(true), None, &[]);
    let b = adopt(&config, &up, &a.prev, &a.leases, Millis(T + 1));
    assert_eq!(b.adopted, vec![nfs]);
}

#[test]
fn a_person_s_lease_and_a_host_flowmanager_woke_survive_a_blip_untouched() {
    let config = example_config_managed();
    let nfs = ResourceId::new(N);
    let person =
        "{ id: manual-host-1, resource: basestar-nfs, granted_at: 1, kind: { type: manual_host } }";
    let prev = state("{ power: { phase: \"on\", since: 5, ours: true }, mode: { phase: idle } }");
    let down = snapshot(Some(false), None, &[person]);
    let a = adopt_at(&config, &down, &prev);
    assert!(a.ended.is_empty() && a.adopted.is_empty());
    assert_eq!(a.leases, down.leases, "a person's lease is never dropped");
    assert!(a.prev.resource(&nfs).unwrap().ours());
}
