//! The derived status of each host and the one-time events between two ticks
//! (`sched_sequencer::status`), on both host kinds: the shared NFS host and the exclusive GPU
//! host.

use sched_model::test_support::{example_config_managed, example_yaml_managed};
use sched_model::{
    Config, DrainMode, Fixture, LeaseOrigin, Millis, PowerTransition, ResourceId, ResourceKind,
    SchedState, ShutdownCause, Snapshot, Step, VetoReading, WaitReason, Withdrawal,
};
use sched_sequencer::status::{
    BlockerState, Decided, DrainCause, HostEvent, HostMode, HostStatus, LockoutStatus, VetoBlocker,
    VetoStatus, blockers_label, derive,
};

const T: u64 = 100_000_000;
const N: &str = "basestar-nfs";
const G: &str = "cuda-central";

fn rid(s: &str) -> ResourceId {
    ResourceId::new(s)
}

/// A snapshot at `T` where both hosts read up, `mounts` pods mount the NFS host (absent: no
/// reading), and the given leases are held.
fn snapshot(mounts: Option<u32>, leases: &[&str]) -> Snapshot {
    let mut y = format!("description: t\nsnapshot:\n  now: {T}\n  observations:\n");
    for r in [N, G] {
        y.push_str(&format!(
            "    - {{ source: cluster-pods, observed_at: {T}, fidelity: event, value: {{ type: resource_up, resource: {r}, up: true }} }}\n"
        ));
    }
    if let Some(p) = mounts {
        y.push_str(&format!(
            "    - {{ source: basestar-mounts, observed_at: {T}, fidelity: polled, value: {{ type: resource_mounts, resource: {N}, pods: {p} }} }}\n"
        ));
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

/// A state with the given entries for the NFS and the GPU host.
fn state(nfs: &str, gpu: &str) -> SchedState {
    let y = format!(
        "description: t\nsnapshot: {{ now: 0 }}\nstate:\n  resources:\n    {N}: {nfs}\n    {G}: {gpu}\n"
    );
    Fixture::from_yaml(&y)
        .unwrap_or_else(|e| panic!("{e}\n{y}"))
        .state
}

fn entry(power: &str, extra: &str) -> String {
    format!("{{ power: {power}, mode: {{ phase: idle }}{extra} }}")
}

const ON: &str = "{ phase: \"on\", since: 5 }";
const ON_OURS: &str = "{ phase: \"on\", since: 5, ours: true }";

struct Run {
    hosts: Vec<HostStatus>,
    events: Vec<HostEvent>,
}

impl Run {
    fn host(&self, id: &str) -> &HostStatus {
        self.hosts
            .iter()
            .find(|h| h.resource == rid(id))
            .unwrap_or_else(|| panic!("no status for {id}: {:?}", self.hosts))
    }
}

fn run_with(
    config: &Config,
    snap: &Snapshot,
    prev: &SchedState,
    next: &SchedState,
    steps: &[Step],
    adopted: &[ResourceId],
    ended: &[ResourceId],
) -> Run {
    let (hosts, events) = derive(&Decided {
        config,
        snapshot: snap,
        prev,
        next,
        steps,
        now: Millis(T),
        adopted,
        ended,
    });
    Run { hosts, events }
}

fn run(snap: &Snapshot, prev: &SchedState, next: &SchedState, steps: &[Step]) -> Run {
    run_with(&example_config_managed(), snap, prev, next, steps, &[], &[])
}

fn lockout_lease(resource: &str) -> String {
    format!("{{ id: lockout-1, resource: {resource}, granted_at: 1, kind: {{ type: lockout }} }}")
}

#[test]
fn every_lifecycle_phase_is_reported_on_both_host_kinds() {
    let snap = snapshot(Some(0), &[]);
    let cases: [(&str, &str, &str, bool, bool); 6] = [
        // power, phase name, up, ours, attention
        ("{ phase: off }", "off", "off", false, false),
        ("{ phase: off, ours: true }", "off", "off", true, false),
        (
            "{ phase: powering_on, since: 5 }",
            "powering_on",
            "",
            false,
            false,
        ),
        (ON, "on", "", false, false),
        (ON_OURS, "on", "", true, false),
        (
            "{ phase: shutting_down, since: 5 }",
            "shutting_down",
            "",
            false,
            false,
        ),
    ];
    for (power, phase, _, ours, attention) in cases {
        let st = state(&entry(power, ""), &entry(power, ""));
        let r = run(&snap, &st, &st, &[]);
        for (id, kind) in [(N, ResourceKind::Shared), (G, ResourceKind::Exclusive)] {
            let h = r.host(id);
            assert_eq!(h.phase(), phase, "{id} {power}");
            assert_eq!(h.kind, kind, "{id}");
            assert_eq!(h.ours, ours, "{id} {power}");
            assert_eq!(h.attention, attention, "{id} {power}");
            assert!(h.has_power && h.managed, "{id}");
            assert!(h.shutdown.is_none() && h.lockout.is_none() && h.lease.is_none());
            assert_eq!(h.is_up(), phase == "on", "{id}");
            assert_eq!(h.is_off(), phase == "off", "{id}");
        }
        assert!(r.events.is_empty(), "{power}: {:?}", r.events);
    }
    // A failed transition needs a person, on either kind.
    let failed = "{ phase: failed, since: 9, during: shutting_down }";
    let st = state(&entry(failed, ""), &entry(failed, ""));
    let r = run(&snap, &st, &st, &[]);
    for id in [N, G] {
        assert!(r.host(id).attention, "{id}");
        assert_eq!(r.host(id).phase(), "failed");
    }
}

#[test]
fn the_exclusive_mode_is_reported_as_idle_serving_or_winding_down() {
    let snap = snapshot(Some(0), &[]);
    let serving = |wind: &str| {
        format!("{{ power: {ON}, mode: {{ phase: serving, model: llm, since: 5{wind} }} }}")
    };
    let nfs = entry(ON, "");
    let st = state(&nfs, &serving(""));
    assert_eq!(
        run(&snap, &st, &st, &[]).host(G).mode,
        HostMode::Serving {
            model: "llm".into()
        }
    );
    let st = state(&nfs, &serving(", wind_down: { since: 6, timeout: 600000 }"));
    assert_eq!(
        run(&snap, &st, &st, &[]).host(G).mode,
        HostMode::WindingDown {
            model: "llm".into()
        }
    );
    let st = state(&nfs, &nfs);
    assert_eq!(run(&snap, &st, &st, &[]).host(G).mode, HostMode::Idle);
}

#[test]
fn a_shutdown_reports_its_cause_withdrawal_and_veto_with_the_blocking_readings() {
    let config = example_config_managed();
    let waiting =
        "{ cause: { kind: idle }, since: 6, withdrawal: done, veto: { phase: vetoing, since: 7 } }";
    // The NFS host: pods mount it (in use) or the reading is missing (unknown).
    for (mounts, want) in [
        (Some(2), BlockerState::InUse),
        (None, BlockerState::Unknown),
    ] {
        let snap = snapshot(mounts, &[]);
        let st = state(
            &entry(ON_OURS, &format!(", shutdown: {waiting}")),
            &entry(ON, ""),
        );
        let r = run(&snap, &st, &st, &[]);
        let h = r.host(N);
        assert_eq!(h.veto_readings, config.veto_readings(&rid(N)));
        assert_eq!(
            h.veto(),
            Some(&VetoStatus::Waiting {
                since: Millis(7),
                blocking: vec![VetoBlocker {
                    reading: VetoReading::Mounts,
                    state: want
                }]
            }),
            "{mounts:?}"
        );
        assert!(!h.stopped() && !h.attention);
        assert_eq!(
            blockers_label(h.blocking()),
            format!("mounts:{}", want.as_str())
        );
        let s = h.shutdown.as_ref().unwrap();
        assert_eq!(
            (s.cause, s.since, s.withdrawal),
            (ShutdownCause::Idle, Millis(6), Withdrawal::Done)
        );
        // The exclusive host has no shutdown.
        assert!(r.host(G).shutdown.is_none() && r.host(G).veto().is_none());
    }
    // Stopped: the host needs a person; blocking is empty once the pods are gone.
    let stopped = "{ cause: { kind: idle }, since: 6, withdrawal: not_needed, veto: { phase: stopped, since: 8 } }";
    let st = state(
        &entry(ON, &format!(", shutdown: {stopped}")),
        &entry(ON, ""),
    );
    let r = run(&snapshot(Some(0), &[]), &st, &st, &[]);
    let h = r.host(N);
    assert!(h.stopped() && h.attention);
    assert_eq!(
        h.veto(),
        Some(&VetoStatus::Stopped {
            since: Millis(8),
            blocking: vec![]
        })
    );
    assert!(h.blocking().is_empty());
    // The exclusive host's shutdown carries the same shape.
    let gpu_waiting = "{ cause: { kind: requested, mode: graceful }, since: 6, withdrawal: done, veto: { phase: vetoing, since: 7 } }";
    let st = state(
        &entry(ON, ""),
        &entry(ON_OURS, &format!(", shutdown: {gpu_waiting}")),
    );
    let r = run(&snapshot(Some(0), &[]), &st, &st, &[]);
    assert!(matches!(
        r.host(G).veto(),
        Some(VetoStatus::Waiting { since, .. }) if *since == Millis(7)
    ));
}

#[test]
fn a_drain_is_a_requested_or_idle_shutdown_with_a_withdrawal_and_never_a_lockouts() {
    let snap = snapshot(Some(0), &[]);
    let sd = |cause: &str, withdrawal: &str| {
        format!(
            ", shutdown: {{ cause: {cause}, since: 6, withdrawal: {withdrawal}, veto: {{ phase: none }} }}"
        )
    };
    let drain_of = |extra: &str| {
        let st = state(&entry(ON_OURS, extra), &entry(ON_OURS, extra));
        let r = run(&snap, &st, &st, &[]);
        (r.host(N).drain(), r.host(G).drain())
    };
    // A person's graceful request still has its consumers running.
    for extra in [sd("{ kind: requested, mode: graceful }", "pending")] {
        let (n, g) = drain_of(&extra);
        for d in [n.unwrap(), g.unwrap()] {
            assert_eq!(
                (d.cause, d.mode, d.since, d.scaled_down),
                (DrainCause::Requested, DrainMode::Graceful, Millis(6), false)
            );
        }
    }
    // Immediate and done: scaled down.
    let (n, _) = drain_of(&sd("{ kind: requested, mode: immediate }", "done"));
    let d = n.unwrap();
    assert_eq!((d.mode, d.scaled_down), (DrainMode::Immediate, true));
    // An idle drain scales the consumers down at once.
    let (n, _) = drain_of(&sd("{ kind: idle }", "done"));
    let d = n.unwrap();
    assert_eq!(
        (d.cause, d.mode, d.scaled_down),
        (DrainCause::Idle, DrainMode::Immediate, true)
    );
    // Nothing to withdraw, or a lockout's: not a drain.
    assert_eq!(drain_of(&sd("{ kind: idle }", "not_needed")), (None, None));
    assert_eq!(drain_of(&sd("{ kind: lockout }", "pending")), (None, None));
}

#[test]
fn a_lockout_is_held_withdrawing_or_left_alone_and_a_lease_reports_its_origin() {
    let leases_n = [lockout_lease(N)];
    let leases_g = [lockout_lease(G)];
    for (id, lease) in [(N, &leases_n), (G, &leases_g)] {
        let snap = snapshot(Some(0), &[lease[0].as_str()]);
        let at = |nfs: &str, gpu: &str| {
            let st = state(nfs, gpu);
            run(&snap, &st, &st, &[]).host(id).lockout
        };
        let both = |extra: &str, power: &str| (entry(power, extra), entry(power, extra));
        let lockout_pending = ", shutdown: { cause: { kind: lockout }, since: 6, withdrawal: pending, veto: { phase: none } }";
        let (n, g) = both(lockout_pending, ON);
        assert_eq!(at(&n, &g), Some(LockoutStatus::Withdrawing), "{id}");
        let (n, g) = both("", ON);
        assert_eq!(
            at(&n, &g),
            Some(LockoutStatus::Held),
            "{id} up, not yet shut down"
        );
        let (n, g) = both(", locked_down: true", ON);
        assert_eq!(at(&n, &g), Some(LockoutStatus::LeftAlone), "{id}");
        let (n, g) = both(", locked_down: true", "{ phase: off }");
        assert_eq!(at(&n, &g), Some(LockoutStatus::Held), "{id} off");
        let (n, g) = both("", "{ phase: shutting_down, since: 5 }");
        assert_eq!(at(&n, &g), Some(LockoutStatus::Held), "{id} shutting down");
    }
    // The exclusive host winding its model down under the lockout is withdrawing.
    let snap = snapshot(Some(0), &[leases_g[0].as_str()]);
    let st = state(
        &entry(ON, ""),
        &format!(
            "{{ power: {ON}, mode: {{ phase: serving, model: llm, since: 5, wind_down: {{ since: 6, timeout: 600000, lockout: true }} }} }}"
        ),
    );
    assert_eq!(
        run(&snap, &st, &st, &[]).host(G).lockout,
        Some(LockoutStatus::Withdrawing)
    );
    // No lockout lease: none.
    let st = state(&entry(ON, ""), &entry(ON, ""));
    let r = run(&snapshot(Some(0), &[]), &st, &st, &[]);
    assert!(r.host(N).lockout.is_none() && r.host(G).lockout.is_none());

    // Manual host leases: who made them.
    let person = format!(
        "{{ id: manual-host-1, resource: {N}, granted_at: 1, kind: {{ type: manual_host }} }}"
    );
    let adopted = format!(
        "{{ id: adopted-host-{G}, resource: {G}, granted_at: 1, origin: adopted, kind: {{ type: manual_host }} }}"
    );
    let snap = snapshot(Some(0), &[person.as_str(), adopted.as_str()]);
    let r = run(&snap, &st, &st, &[]);
    assert_eq!(r.host(N).lease, Some(LeaseOrigin::Requested));
    assert_eq!(r.host(G).lease, Some(LeaseOrigin::Adopted));
}

#[test]
fn min_off_and_the_wait_reason_are_reported_for_a_host_with_a_minimum() {
    let yaml = example_yaml_managed().replace("      # min_off: 10m", "      min_off: 10m");
    let config = Config::from_yaml(&yaml).unwrap();
    let snap = snapshot(Some(0), &[]);
    // Shut down by flowmanager and seen down 4 minutes ago.
    let off = format!(
        "{{ power: {{ phase: off }}, mode: {{ phase: idle }}, off_since: {} }}",
        T - 4 * 60_000
    );
    let st = state(&off, &off);
    let wait = [Step::Wait {
        resource: rid(N),
        reason: WaitReason::MinOff,
    }];
    let r = run_with(&config, &snap, &st, &st, &wait, &[], &[]);
    let h = r.host(N);
    let m = h.min_off.expect("the NFS host has a minimum");
    assert_eq!(
        (m.since, m.remaining, m.until),
        (
            Some(Millis(T - 4 * 60_000)),
            Millis(6 * 60_000),
            Millis(T + 6 * 60_000)
        )
    );
    assert_eq!(h.wait, Some(WaitReason::MinOff));
    // The host without a minimum, and the step that is another host's, give nothing.
    assert!(r.host(G).min_off.is_none() && r.host(G).wait.is_none());
    // A host found down (no stamp) has a minimum with nothing left to wait for.
    let found = entry("{ phase: off }", "");
    let st = state(&found, &found);
    let r = run_with(&config, &snap, &st, &st, &[], &[], &[]);
    assert_eq!(r.host(N).min_off.map(|m| m.remaining), Some(Millis::ZERO));
    assert_eq!(r.host(N).wait, None);
}

#[test]
fn a_timeout_is_an_event_in_the_tick_that_makes_it_and_not_in_the_next() {
    let snap = snapshot(Some(0), &[]);
    let on_and_off = |nfs: &str, gpu: &str| state(&entry(nfs, ""), &entry(gpu, ""));
    for (during, from) in [
        (
            PowerTransition::PoweringOn,
            "{ phase: powering_on, since: 5 }",
        ),
        (
            PowerTransition::ShuttingDown,
            "{ phase: shutting_down, since: 5 }",
        ),
    ] {
        let failed = format!("{{ phase: failed, since: 9, during: {} }}", during.as_str());
        let prev = on_and_off(from, from);
        let next = on_and_off(&failed, &failed);
        let r = run(&snap, &prev, &next, &[]);
        let want = |id: &str| HostEvent::PowerTimeout {
            resource: rid(id),
            during,
            started_at: Some(Millis(5)),
            at: Millis(9),
        };
        assert_eq!(r.events, vec![want(N), want(G)], "{during:?}");
        // The next tick finds it failed already: no event, the level stays.
        let r = run(&snap, &next, &next, &[]);
        assert!(r.events.is_empty(), "{:?}", r.events);
        assert!(r.host(N).attention && r.host(G).attention);
    }
    // A host with no entry before, and one that was up, has no start to report.
    let failed = "{ phase: failed, since: 9, during: powering_on }";
    let next = on_and_off(failed, failed);
    let r = run(&snap, &SchedState::default(), &next, &[]);
    assert!(
        r.events.iter().all(|e| matches!(
            e,
            HostEvent::PowerTimeout {
                started_at: None,
                ..
            }
        )),
        "{:?}",
        r.events
    );
    assert_eq!(r.events.len(), 2);
}

#[test]
fn a_veto_stopping_is_an_event_once_with_the_readings_and_when_the_wait_began() {
    let snap = snapshot(Some(3), &[]);
    let sd = |veto: &str, withdrawal: &str| {
        entry(
            ON_OURS,
            &format!(
                ", shutdown: {{ cause: {{ kind: idle }}, since: 6, withdrawal: {withdrawal}, veto: {veto} }}"
            ),
        )
    };
    let waiting = state(&sd("{ phase: vetoing, since: 7 }", "done"), &entry(ON, ""));
    let stopped = state(
        &sd("{ phase: stopped, since: 9 }", "not_needed"),
        &entry(ON, ""),
    );
    let r = run(&snap, &waiting, &stopped, &[]);
    assert_eq!(
        r.events,
        vec![HostEvent::VetoTimeout {
            resource: rid(N),
            readings: vec![VetoReading::Mounts],
            blocking: vec![VetoBlocker {
                reading: VetoReading::Mounts,
                state: BlockerState::InUse
            }],
            waiting_since: Some(Millis(7)),
            at: Millis(9),
        }]
    );
    // Still stopped at the next tick: a level, not an event again.
    let r = run(&snap, &stopped, &stopped, &[]);
    assert!(r.events.is_empty(), "{:?}", r.events);
    assert!(r.host(N).stopped());
    // Waiting is not an event.
    let r = run(&snap, &waiting, &waiting, &[]);
    assert!(r.events.is_empty());
    // No shutdown before: the wait began before anyone knows, so no start is reported.
    let r = run(&snap, &state(&entry(ON, ""), &entry(ON, "")), &stopped, &[]);
    assert!(matches!(
        r.events[..],
        [HostEvent::VetoTimeout {
            waiting_since: None,
            ..
        }]
    ));
}

#[test]
fn adoptions_are_passed_through_as_events_before_the_hosts_own() {
    let snap = snapshot(Some(0), &[]);
    let st = state(&entry(ON, ""), &entry(ON, ""));
    let r = run_with(
        &example_config_managed(),
        &snap,
        &st,
        &st,
        &[],
        &[rid(N)],
        &[rid(G)],
    );
    assert_eq!(
        r.events,
        vec![
            HostEvent::Adopted { resource: rid(N) },
            HostEvent::AdoptionEnded { resource: rid(G) },
        ]
    );
}

#[test]
fn a_host_under_manual_control_or_without_a_power_section_is_reported_as_such() {
    // The example as shipped: both power sections are `control: manual`.
    let config = sched_model::test_support::example_config();
    let snap = snapshot(Some(0), &[]);
    let st = state(&entry(ON, ""), &entry(ON, ""));
    let r = run_with(&config, &snap, &st, &st, &[], &[], &[]);
    for id in [N, G] {
        let h = r.host(id);
        assert!(h.has_power && !h.managed, "{id}");
        assert!(h.is_up() && h.min_off.is_none());
    }
    // A resource that is not configured has no status (it is not in the next state either).
    assert!(
        r.hosts
            .iter()
            .all(|h| h.resource == rid(N) || h.resource == rid(G))
    );
}
