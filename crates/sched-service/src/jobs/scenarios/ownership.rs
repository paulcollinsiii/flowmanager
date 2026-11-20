//! Who owns a host (`docs/architecture/power.md`, *Ownership and adoption*), end to end through the `Scheduler`
//! object's tick with the real job machine and the stored documents: a host flowmanager woke
//! is flowmanager's and goes down when nothing holds it, a host something else started is
//! adopted at once, a manual lease always keeps a host up.

use sched_model::test_support::{EXAMPLE_CONFIG, example_yaml_managed};
use sched_model::{Lease, LeaseKind, Millis, PowerPhase, ResourceId, SchedState, Shutdown};

use super::*;
use crate::resume::Resume;

fn nfs() -> ResourceId {
    "basestar-nfs".into()
}

impl World {
    /// `tick`, with every demand source reporting an empty queue first: demand that is
    /// unknown is not idle, and an idle host is what a shutdown needs.
    async fn tick_idle(&self, at: u64, nfs_up: bool, read: bool) -> TickReport {
        for (class, source) in [
            ("paperless-ocr", "paperless-ocr-pending"),
            ("paperless-tagging", "paperless-post-ocr"),
            ("speakr-transcription", "speakr-pending"),
            ("speakr-summarization", "speakr-summaries"),
            ("immich-ml", "immich-queues"),
        ] {
            self.clock.set(Millis(at));
            self.push(
                source,
                vec![ObservedValue::Demand {
                    class: class.into(),
                    ready: 0,
                    in_flight: 0,
                    failed: 0,
                    oldest_ready_at: None,
                }],
            );
        }
        self.tick(at, nfs_up, read).await
    }
}

fn leases(w: &World) -> Vec<Lease> {
    persist::decode_leases(w.ctx.stored(persist::LEASES_KEY).as_deref())
        .unwrap()
        .leases
}

/// The stored manual host leases of basestar (a person's and the adopted one).
fn host_leases(w: &World) -> Vec<Lease> {
    leases(w)
        .into_iter()
        .filter(|l| l.resource == nfs() && l.kind == LeaseKind::ManualHost)
        .collect()
}

fn state(w: &World) -> SchedState {
    persist::decode_state(w.ctx.stored(persist::STATE_KEY).as_deref())
        .unwrap()
        .state
}

fn power(w: &World) -> PowerPhase {
    state(w).resource(&nfs()).unwrap().power
}

fn ours(w: &World) -> bool {
    state(w).resource(&nfs()).unwrap().ours()
}

fn offs(w: &World) -> usize {
    w.ctx
        .powers
        .lock()
        .unwrap()
        .iter()
        .filter(|(r, a)| r == "basestar-nfs" && *a == PowerAction::Off)
        .count()
}

fn adopted_nfs(r: &TickReport) -> bool {
    r.adopted().contains(&nfs())
}

/// Basestar off and unowned, the backup due: the due tick wakes it, the next one sees it up
/// (the host is flowmanager's from then on), and the one after starts the run. Ends with the
/// run running and holding the host, at `DUE + 2 * MIN`.
async fn woken_by_the_job(w: &World) {
    w.tick_idle(SUN + H, false, false).await;
    w.tick_idle(DUE - MIN, false, false).await;
    w.tick_idle(DUE, false, false).await;
    let r = w.tick_idle(DUE + MIN, true, false).await;
    assert!(!adopted_nfs(&r), "powered on by us: {r:?}");
    assert!(ours(w) && matches!(power(w), PowerPhase::On { .. }));
    let r = w.tick_idle(DUE + 2 * MIN, true, false).await;
    assert_eq!(kinds(&r), vec!["backup:started"], "{r:?}");
    assert_eq!(w.holds(), vec![hold()]);
}

/// Ends the run at `DUE + 5 * MIN` and ticks every `step` for `n` ticks after it. Returns
/// whether basestar was adopted in any of them.
async fn job_ends_then_idle(w: &mut World, n: u64, step: u64) -> bool {
    w.states.insert(NAME.into(), JobRunState::Complete);
    let r = w.tick_idle(DUE + 5 * MIN, true, true).await;
    assert_eq!(kinds(&r), vec!["backup:succeeded"], "{r:?}");
    assert!(w.holds().is_empty());
    let mut adopted = adopted_nfs(&r);
    for k in 1..=n {
        let r = w.tick_idle(DUE + 5 * MIN + k * step, true, true).await;
        adopted |= adopted_nfs(&r);
    }
    adopted
}

#[tokio::test]
async fn a_job_woken_host_shuts_down_after_the_job_with_no_lease_and_is_never_adopted() {
    let mut w = World::new(&example_yaml_managed());
    woken_by_the_job(&w).await;
    assert!(host_leases(&w).is_empty(), "no lease while the job runs");
    // idle_grace is 10m: one tick a minute for 15 minutes after the run ends.
    let adopted = job_ends_then_idle(&mut w, 15, MIN).await;
    assert!(!adopted, "a host flowmanager woke is never adopted");
    assert!(host_leases(&w).is_empty(), "{:?}", leases(&w));
    assert_eq!(offs(&w), 1, "{:?}", w.ctx.powers.lock().unwrap());
    assert!(matches!(power(&w), PowerPhase::ShuttingDown { .. }));
}

#[tokio::test]
async fn the_same_host_with_a_manual_lease_taken_mid_job_stays_up_after_the_job() {
    let mut w = World::new(&example_yaml_managed());
    woken_by_the_job(&w).await;
    object::acquire_host_lease(
        &w.shared,
        &w.ctx,
        object::ResourceRequest { resource: nfs() },
    )
    .await
    .unwrap();
    w.ctx.commit();
    // Far beyond idle_grace after the run ends.
    let adopted = job_ends_then_idle(&mut w, 12, 5 * MIN).await;
    assert!(!adopted);
    assert_eq!(offs(&w), 0, "{:?}", w.ctx.powers.lock().unwrap());
    assert!(matches!(power(&w), PowerPhase::On { .. }) && ours(&w));
    let held = host_leases(&w);
    assert_eq!(held.len(), 1, "{held:?}");
    assert!(
        !held[0].is_adopted_host(),
        "a person's lease stays requested"
    );
}

#[tokio::test]
async fn a_host_found_running_while_a_job_holds_it_is_adopted_at_once_and_stays_up_after_the_job() {
    let mut w = World::new(&example_yaml_managed());
    w.tick_idle(SUN + H, false, false).await;
    w.tick_idle(DUE - MIN, false, false).await;
    // A person powers the host on just as the job comes due: the job's hold is in the lease
    // book in this very tick, and the host is found running all the same.
    let r = w.tick_idle(DUE, true, false).await;
    assert!(
        adopted_nfs(&r),
        "adopted at once (even while a job holds it): {r:?}"
    );
    assert_eq!(w.holds(), vec![hold()], "the hold is still there");
    let held = host_leases(&w);
    assert_eq!(held.len(), 1);
    assert!(held[0].is_adopted_host());
    let r = w.tick_idle(DUE + MIN, true, false).await;
    assert_eq!(
        kinds(&r),
        vec!["backup:started"],
        "started as soon as the host is up: {r:?}"
    );
    let adopted_again = job_ends_then_idle(&mut w, 12, 5 * MIN).await;
    assert!(!adopted_again, "once only");
    assert_eq!(offs(&w), 0, "{:?}", w.ctx.powers.lock().unwrap());
    assert!(matches!(power(&w), PowerPhase::On { .. }));
    assert_eq!(host_leases(&w).len(), 1, "the adopted lease keeps it up");
}

#[tokio::test]
async fn a_blip_on_a_job_woken_host_changes_nothing_about_who_owns_it() {
    let mut w = World::new(&example_yaml_managed());
    woken_by_the_job(&w).await;
    // Down for one tick while the job runs, then back.
    w.tick_idle(DUE + 3 * MIN, false, true).await;
    // The job's hold still wants the host, so it is woken again at once, as before; the phase
    // that follows an own power-on is ours by construction.
    assert!(
        matches!(power(&w), PowerPhase::PoweringOn { .. }),
        "{:?}",
        power(&w)
    );
    let r = w.tick_idle(DUE + 4 * MIN, true, true).await;
    assert!(!adopted_nfs(&r), "a blip is not a start: {r:?}");
    assert!(host_leases(&w).is_empty(), "{:?}", leases(&w));
    assert!(ours(&w) && matches!(power(&w), PowerPhase::On { .. }));
    // The job ends and the host goes down as for any host flowmanager woke.
    let adopted = job_ends_then_idle(&mut w, 15, MIN).await;
    assert!(!adopted && host_leases(&w).is_empty());
    assert_eq!(offs(&w), 1, "{:?}", w.ctx.powers.lock().unwrap());
}

#[tokio::test]
async fn a_host_that_comes_up_after_a_timed_out_power_on_is_adopted_even_under_a_job_hold() {
    let mut w = World::new(&example_yaml_managed());
    w.tick_idle(SUN + H, false, false).await;
    w.tick_idle(DUE - MIN, false, false).await;
    w.tick_idle(DUE, false, false).await;
    // power_on_timeout is 5m: never up, so the transition fails and an alert is out.
    for k in 1..=6 {
        w.tick_idle(DUE + k * MIN, false, false).await;
    }
    assert!(
        matches!(power(&w), PowerPhase::Failed { .. }),
        "{:?}",
        power(&w)
    );
    // A person gets it up: found running, adopted at once although the run's hold is there.
    let r = w.tick_idle(DUE + 7 * MIN, true, false).await;
    assert!(adopted_nfs(&r), "{r:?}");
    assert_eq!(w.holds(), vec![hold()]);
    assert!(host_leases(&w)[0].is_adopted_host());
    let r = w.tick_idle(DUE + 8 * MIN, true, false).await;
    assert_eq!(kinds(&r), vec!["backup:started"], "{r:?}");
    // The run is named after the instant it was due.
    let name = w.kube.made.lock().unwrap()[0].clone();
    w.states.insert(name, JobRunState::Complete);
    let r = w.tick_idle(DUE + 10 * MIN, true, true).await;
    assert_eq!(kinds(&r), vec!["backup:succeeded"], "{r:?}");
    for k in 1..=12 {
        w.tick_idle(DUE + 10 * MIN + k * 5 * MIN, true, true).await;
    }
    assert_eq!(offs(&w), 0, "{:?}", w.ctx.powers.lock().unwrap());
    assert!(matches!(power(&w), PowerPhase::On { .. }));
}

#[tokio::test]
async fn a_host_switched_from_manual_to_managed_control_while_up_is_adopted() {
    let w = World::new(EXAMPLE_CONFIG);
    let r = w.tick_idle(SUN + H, true, false).await;
    assert!(
        r.adopted().is_empty(),
        "manual control adopts nothing: {r:?}"
    );
    assert!(host_leases(&w).is_empty());
    assert!(!ours(&w));
    let managed = example_yaml_managed();
    assert!(matches!(
        w.shared.config.apply_text(&managed),
        crate::config_source::ReloadOutcome::Applied(_)
    ));
    let r = w.tick_idle(SUN + H + MIN, true, false).await;
    assert!(adopted_nfs(&r), "{r:?}");
    assert!(host_leases(&w)[0].is_adopted_host());
    assert!(ours(&w), "the adopted lease is a manual want");
    // Once only.
    let r = w.tick_idle(SUN + H + 2 * MIN, true, false).await;
    assert!(r.adopted().is_empty(), "{r:?}");
}

#[tokio::test]
async fn resuming_a_shutdown_the_veto_stopped_adopts_the_host() {
    let w = World::new(&example_yaml_managed());
    // Up, no longer ours, its idle shutdown stopped by mounting pods.
    let mut st = sched_model::ResourceState::new(PowerPhase::On {
        since: Millis(SUN),
        ours: false,
    });
    st.shutdown = Some(
        Shutdown::idle_withdrawn(Millis(SUN))
            .vetoing(Millis(SUN))
            .stopped(Millis(SUN + 1)),
    );
    let mut core = persist::CoreState::default();
    core.state.resources.insert(nfs(), st);
    w.ctx.put(persist::STATE_KEY, &persist::encode_state(&core));
    let r = w.tick_idle(SUN + H, true, false).await;
    assert!(
        !adopted_nfs(&r),
        "left as it is until a person decides: {r:?}"
    );
    assert!(host_leases(&w).is_empty());
    // The person resumes it: found running, adopted.
    object::resume(&w.shared, &w.ctx, Resume { resource: nfs() })
        .await
        .unwrap();
    w.ctx.commit();
    let r = w.tick_idle(SUN + H + MIN, true, false).await;
    assert!(adopted_nfs(&r), "{r:?}");
    assert!(host_leases(&w)[0].is_adopted_host());
}

#[tokio::test]
async fn a_host_known_as_up_and_not_ours_is_adopted_the_tick_a_job_takes_its_hold() {
    let mut w = World::new(&example_yaml_managed());
    // A first tick lets the job book learn the schedule (the host is down, nothing is adopted),
    // then the stored state says: up and not ours (as after a reload from manual control).
    w.tick_idle(SUN + H, false, false).await;
    let mut core = persist::CoreState::default();
    core.state.resources.insert(
        nfs(),
        sched_model::ResourceState::new(PowerPhase::On {
            since: Millis(SUN),
            ours: false,
        }),
    );
    w.ctx.put(persist::STATE_KEY, &persist::encode_state(&core));
    // The backup comes due: its hold and the adoption are decided in the same tick.
    let r = w.tick_idle(DUE, true, false).await;
    assert!(adopted_nfs(&r), "{r:?}");
    assert_eq!(kinds(&r), vec!["backup:started"], "{r:?}");
    assert_eq!(w.holds(), vec![hold()]);
    // The run ends; the host stays up on the adopted lease, however long.
    let adopted = job_ends_then_idle(&mut w, 12, 5 * MIN).await;
    assert!(!adopted);
    assert_eq!(offs(&w), 0, "{:?}", w.ctx.powers.lock().unwrap());
    assert_eq!(host_leases(&w).len(), 1);
}

#[tokio::test]
async fn adoption_works_with_an_unreadable_job_book_and_survives_the_next_tick() {
    let w = World::new(&example_yaml_managed());
    w.ctx.put(persist::JOBS_KEY, b"not json");
    let r = w.tick_idle(SUN + H, true, false).await;
    assert!(adopted_nfs(&r), "{r:?}");
    // What is written: the lease book, with the adopted lease. The job book is not touched.
    assert!(host_leases(&w)[0].is_adopted_host());
    assert_eq!(
        w.ctx.stored(persist::JOBS_KEY).as_deref(),
        Some(&b"not json"[..])
    );
    // The next tick reads that lease back: nothing is adopted twice.
    let r = w.tick_idle(SUN + H + MIN, true, false).await;
    assert!(r.adopted().is_empty(), "{r:?}");
    assert_eq!(host_leases(&w).len(), 1);
    assert!(ours(&w));
    // Seen down, the adopted lease ends in the stored book too; seen up again it is adopted
    // afresh. The job book is still not touched.
    let r = w.tick_idle(SUN + H + 2 * MIN, false, false).await;
    assert_eq!(r.adoptions_ended(), vec![nfs()], "{r:?}");
    assert!(host_leases(&w).is_empty());
    let r = w.tick_idle(SUN + H + 3 * MIN, true, false).await;
    assert!(adopted_nfs(&r), "{r:?}");
    assert_eq!(host_leases(&w).len(), 1);
    assert_eq!(
        w.ctx.stored(persist::JOBS_KEY).as_deref(),
        Some(&b"not json"[..])
    );
}
