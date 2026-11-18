//! Wakers (`docs/architecture/power.md`, *Wakers*): a waker appearing cancels a graceful requested
//! shutdown before the host is powered off, on a shared and on an exclusive host; nothing else
//! does. The status of each host lists them.

use super::*;
use sched_model::DrainMode;
use sched_model::ShutdownCause;
use sched_model::test_support::with_nfs_batch;
use sched_sequencer::status::{Decided, derive};

/// The managed example with `nfs-batch`, a class on the NFS host that may change power and has
/// a demand source.
fn batch_config() -> Config {
    Config::from_yaml(&with_nfs_batch(&managed_yaml())).unwrap_or_else(|e| panic!("{e}"))
}

/// The managed example plus `gpu-app`, a switched class on the exclusive GPU host.
fn gpu_app_config(may_change_power: bool) -> Config {
    let class = format!(
        "  - id: gpu-app\n    requires:\n      - {{ resource: cuda-central }}\n    priority: 0\n    preemptibility: kill\n    may_change_power: {may_change_power}\n    switch: {{ source: ha-wikijs }}\n\nsources:\n"
    );
    let yaml = managed_yaml().replacen("\nsources:\n", &format!("\n{class}"), 1);
    Config::from_yaml(&yaml).unwrap_or_else(|e| panic!("{e}"))
}

fn batch(ready: u64, in_flight: u64) -> String {
    demand("nfs-batch", "nfs-batch-queue", ready, in_flight, 0)
}

/// The NFS host with a graceful shutdown a minute old (its consumers not yet scaled down).
fn nfs_graceful(config: Config) -> Case {
    let mut c = Case::new().nfs_state(&nfs_draining("graceful", MIN));
    c.config = config;
    c
}

fn powers_off(d: &Decision, r: &str) -> bool {
    d.steps.contains(&Step::PowerOff { resource: rid(r) })
}

fn requested(d: &Decision, r: &str) -> Option<DrainMode> {
    match shutdown_of(d, r)?.cause() {
        ShutdownCause::Requested { mode } => Some(mode),
        _ => None,
    }
}

#[test]
fn a_busy_class_that_may_change_power_cancels_a_graceful_shutdown_on_a_shared_host() {
    // Without a waker the same shutdown goes ahead (nothing in flight, so the consumers are
    // scaled down at once).
    let d = nfs_graceful(batch_config()).obs(batch(0, 0)).run();
    assert!(powers_off(&d, N), "{:?}", d.steps);
    for (ready, in_flight) in [(3, 0), (0, 1), (2, 2)] {
        let d = nfs_graceful(batch_config())
            .obs(batch(ready, in_flight))
            .run();
        assert!(
            !powers_off(&d, N),
            "ready {ready} in flight {in_flight}: {:?}",
            d.steps
        );
        assert_eq!(shutdown_of(&d, N), None, "cancelled");
        assert!(gate_allowed(&d), "the consumers are allowed again");
        assert!(matches!(power_of(&d, N), PowerPhase::On { .. }));
    }
}

#[test]
fn a_switched_class_reading_on_cancels_a_graceful_shutdown_on_a_shared_host() {
    let case = |state: &str| {
        let mut c = nfs_graceful(switched_config(true)).obs(sw("nfs-app", state));
        c.config = switched_config(true);
        c
    };
    let d = case("on").run();
    assert_eq!(shutdown_of(&d, N), None);
    assert!(!powers_off(&d, N), "{:?}", d.steps);
    // A switch that is not a usable `on` is no waker: the shutdown goes ahead.
    for off in ["off", "off_now"] {
        let d = case(off).run();
        assert!(powers_off(&d, N), "{off}: {:?}", d.steps);
    }
    let missing = nfs_graceful(switched_config(true)).run();
    assert!(powers_off(&missing, N), "missing: {:?}", missing.steps);
    let mut stale = nfs_graceful(switched_config(true));
    stale = stale.obs(sw_at("nfs-app", "ha-wikijs", "on", T - 10 * MIN - 1));
    assert!(powers_off(&stale.run(), N), "stale");
}

#[test]
fn a_class_that_may_not_wake_the_host_does_not_cancel_a_graceful_shutdown() {
    // Switched on but `may_change_power: false`: withdrawn as before, the host goes down.
    let d = nfs_graceful(switched_config(false))
        .obs(sw("nfs-app", "on"))
        .run();
    assert!(powers_off(&d, N), "{:?}", d.steps);
    assert_eq!(requested(&d, N), Some(DrainMode::Graceful));
    // Work in flight of a class that may not wake the host: the drain keeps waiting for it.
    let d = nfs_busy().nfs_state(&nfs_draining("graceful", MIN)).run();
    assert_eq!(requested(&d, N), Some(DrainMode::Graceful), "{d:?}");
    assert_steps(
        &d,
        vec![wait(N, WaitReason::Draining)],
        "waits for the work",
    );
    assert!(!gate_allowed(&d) || shutdown_of(&d, N).is_some());
}

#[test]
fn unknown_demand_and_a_job_hold_do_not_cancel_a_graceful_shutdown() {
    // No reading of nfs-batch at all: unknown, which wakes nothing.
    let d = nfs_graceful(batch_config()).run();
    assert_eq!(requested(&d, N), Some(DrainMode::Graceful), "{d:?}");
    assert!(!powers_off(&d, N));
    // A job's hold is a lease, which the drain waits for.
    let held = nfs_graceful(batch_config())
        .obs(batch(0, 0))
        .lease(lease_on("h", N, "{ type: shared_hold, holder: backup }"))
        .run();
    assert_eq!(requested(&held, N), Some(DrainMode::Graceful));
    assert_steps(&held, vec![wait(N, WaitReason::DrainHeld)], "held");
}

#[test]
fn an_immediate_shutdown_is_not_cancelled_by_a_waker() {
    let mut c = Case::new()
        .nfs_state(&nfs_draining("immediate", 0))
        .obs(batch(5, 5))
        .obs(sw("nfs-app", "on"));
    c.config = {
        // Both kinds of waker at once.
        let yaml = with_nfs_batch(&managed_yaml());
        let class = "  - id: nfs-app\n    requires:\n      - { resource: basestar-nfs }\n    priority: 0\n    preemptibility: kill\n    may_change_power: true\n    switch: { source: ha-wikijs }\n\nsources:\n";
        Config::from_yaml(&yaml.replacen("\nsources:\n", &format!("\n{class}"), 1)).unwrap()
    };
    let d = c.run();
    assert!(powers_off(&d, N), "{:?}", d.steps);
    assert_eq!(requested(&d, N), Some(DrainMode::Immediate));
}

#[test]
fn a_locked_out_host_is_not_cancelled_by_a_waker() {
    let mut c = nfs_graceful(batch_config())
        .obs(batch(5, 0))
        .lease(lease_on("lo", N, "{ type: lockout }"));
    c.intent.lockout.insert(rid(N));
    let d = c.run();
    // The lockout took the shutdown over: it is not dropped for the work.
    assert!(shutdown_of(&d, N).is_some_and(|s| s.is_lockout()), "{d:?}");
}

#[test]
fn a_waker_cancels_a_graceful_shutdown_on_an_exclusive_host_too() {
    let gpu = |mode: &str| gpu_requested(mode, "graceful", MIN, "pending");
    let case = |config: Config, state: &str, mode: &str| {
        let mut c = Case::new()
            .gpu_state(&gpu(mode))
            .obs(pods("mineru", 1, 1, 0))
            .obs(sw("gpu-app", state));
        c.config = config;
        c
    };
    for mode in [mode_idle(), serving("mineru", 20 * MIN)] {
        let d = case(gpu_app_config(true), "on", &mode).run();
        assert_eq!(shutdown_of(&d, G), None, "{mode}: cancelled");
        assert!(!powers_off(&d, G), "{mode}: {:?}", d.steps);
        // Not a waker (switch off, or it may not wake the host): the shutdown stays.
        for (config, state) in [
            (gpu_app_config(true), "off"),
            (gpu_app_config(true), "off_now"),
            (gpu_app_config(false), "on"),
        ] {
            let d = case(config, state, &mode).run();
            assert_eq!(
                requested(&d, G),
                Some(DrainMode::Graceful),
                "{mode} {state}: {:?}",
                d.state.resource(&rid(G))
            );
        }
    }
    // A serving model is left serving, not wound down for the cancelled shutdown.
    let d = case(gpu_app_config(true), "on", &serving("mineru", 20 * MIN))
        .target(Some("mineru"))
        .run();
    assert!(
        matches!(
            mode_of(&d, G),
            ModePhase::Serving {
                wind_down: None,
                ..
            }
        ),
        "{:?}",
        mode_of(&d, G)
    );
}

#[test]
fn an_immediate_shutdown_of_an_exclusive_host_is_not_cancelled_by_a_waker() {
    let mut c = Case::new()
        .gpu_state(&gpu_requested(&mode_idle(), "immediate", MIN, "pending"))
        .obs(sw("gpu-app", "on"));
    c.config = gpu_app_config(true);
    let d = c.run();
    assert_eq!(requested(&d, G), Some(DrainMode::Immediate));
    assert!(powers_off(&d, G), "{:?}", d.steps);
}

#[test]
fn the_status_of_each_powered_host_lists_its_wakers_with_why() {
    let mut c = Case::new().obs(batch(2, 0)).obs(sw("gpu-app", "on"));
    // Both classes in one config: `nfs-batch` on the NFS host, `gpu-app` on the GPU host.
    let class = "  - id: gpu-app\n    requires:\n      - { resource: cuda-central }\n    priority: 0\n    preemptibility: kill\n    may_change_power: true\n    switch: { source: ha-wikijs }\n\nsources:\n";
    c.config = Config::from_yaml(&with_nfs_batch(&managed_yaml()).replacen(
        "\nsources:\n",
        &format!("\n{class}"),
        1,
    ))
    .unwrap();
    let state = c.state();
    let d = c.run_at(&state, T);
    let snapshot = c.snapshot_at(T);
    let (hosts, _) = derive(&Decided {
        config: &c.config,
        snapshot: &snapshot,
        prev: &state,
        next: &d.state,
        steps: &d.steps,
        now: sched_model::Millis(T),
        adopted: &[],
        ended: &[],
    });
    let of = |r: &str| {
        hosts
            .iter()
            .find(|h| h.resource == rid(r))
            .unwrap()
            .wakers
            .iter()
            .map(|w| (w.class.as_str().to_string(), w.why))
            .collect::<Vec<_>>()
    };
    use sched_model::WakeReason;
    assert_eq!(of(N), vec![("nfs-batch".to_string(), WakeReason::Work)]);
    assert_eq!(of(G), vec![("gpu-app".to_string(), WakeReason::SwitchedOn)]);
    // None when nothing wakes them.
    let quiet = Case::new();
    let st = quiet.state();
    let d = quiet.run_at(&st, T);
    let snapshot = quiet.snapshot_at(T);
    let (hosts, _) = derive(&Decided {
        config: &quiet.config,
        snapshot: &snapshot,
        prev: &st,
        next: &d.state,
        steps: &d.steps,
        now: sched_model::Millis(T),
        adopted: &[],
        ended: &[],
    });
    assert!(hosts.iter().all(|h| h.wakers.is_empty()), "{hosts:?}");
}

/// Whether the status of `r` after the decision shows the host as needing attention.
fn needs_attention(c: &Case, d: &Decision, r: &str) -> bool {
    let state = c.state();
    let snapshot = c.snapshot_at(T);
    let (hosts, _) = derive(&Decided {
        config: &c.config,
        snapshot: &snapshot,
        prev: &state,
        next: &d.state,
        steps: &d.steps,
        now: sched_model::Millis(T),
        adopted: &[],
        ended: &[],
    });
    hosts
        .iter()
        .find(|h| h.resource == rid(r))
        .unwrap()
        .attention
}

fn stopped_graceful() -> String {
    format!(
        "{{ power: {}, mode: {}, shutdown: {{ cause: {{ kind: requested, mode: graceful }}, since: {}, withdrawal: done, veto: {{ phase: stopped, since: {} }} }} }}",
        on_ours(),
        mode_idle(),
        T - 5 * MIN,
        T - MIN
    )
}

#[test]
fn a_stopped_graceful_shutdown_survives_a_waker_on_a_shared_host() {
    let mut c = Case::new().nfs_state(&stopped_graceful()).obs(batch(3, 1));
    c.config = batch_config();
    let d = c.run();
    let s = shutdown_of(&d, N).expect("the stopped shutdown stays");
    assert_eq!(s.stopped_since(), Some(sched_model::Millis(T - MIN)));
    assert_eq!(requested(&d, N), Some(DrainMode::Graceful));
    assert!(!powers_off(&d, N), "{:?}", d.steps);
    assert_steps(
        &d,
        vec![wait(N, WaitReason::ShutdownStopped)],
        "stays stopped",
    );
    assert!(needs_attention(&c, &d, N));
}

#[test]
fn a_stopped_graceful_shutdown_survives_a_waker_on_an_exclusive_host() {
    let mut c = Case::new()
        .gpu_state(&stopped_graceful())
        .obs(sw("gpu-app", "on"));
    c.config = gpu_app_config(true);
    let d = c.run();
    let s = shutdown_of(&d, G).expect("the stopped shutdown stays");
    assert_eq!(s.stopped_since(), Some(sched_model::Millis(T - MIN)));
    assert!(!powers_off(&d, G), "{:?}", d.steps);
    assert!(needs_attention(&c, &d, G));
}
