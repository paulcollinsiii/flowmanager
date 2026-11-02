//! Scheduled jobs end to end through the `Scheduler` object's tick, on the fake Restate
//! context (journal, replay and crash injection) with a fake Kubernetes backend and a clock
//! that only moves when told to.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use sched_actuate::{JobBackend, kube_jobs::Created};
use sched_model::test_support::{EXAMPLE_CONFIG, example_yaml_managed};
use sched_model::{
    CronJobRef, Fidelity, JobBook, JobEvent, JobId, JobRunState, LeaseKind, Millis, ObservedValue,
};
use sched_observe::Report;

use crate::clock::ManualClock;
use crate::config_source::ConfigHolder;
use crate::metrics::Metrics;
use crate::object::fake::{Crash, FakeCtx};
use crate::object::{self, TriggerJob};
use crate::persist;
use crate::power::PowerAction;
use crate::tick::{Shared, TickReport};

mod ownership;

const MIN: u64 = 60_000;
const H: u64 = 60 * MIN;
const D: u64 = 24 * H;
/// 2026-03-01T00:00:00Z, a Sunday; the example's backup is due at 03:00.
const SUN: u64 = 1_772_323_200_000;
const DUE: u64 = SUN + 3 * H;
const NAME: &str = "backup-1772334000-a1";

/// A Kubernetes that remembers the Jobs it was asked for.
#[derive(Default)]
struct FakeKube {
    made: Mutex<Vec<String>>,
    calls: Mutex<Vec<String>>,
    refuse: AtomicBool,
    /// Runs when a creation starts, to let the world change while the call is in progress.
    on_create: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
}

impl JobBackend for FakeKube {
    fn create(
        &self,
        _cronjob: CronJobRef,
        _job: JobId,
        name: String,
        _at: Millis,
    ) -> sched_actuate::BoxFut<'static, Result<Created, String>> {
        self.calls.lock().unwrap().push(name.clone());
        if let Some(hook) = self.on_create.lock().unwrap().as_ref() {
            hook();
        }
        let r = if self.refuse.load(Ordering::SeqCst) {
            Err("forbidden".to_string())
        } else {
            let mut made = self.made.lock().unwrap();
            if made.contains(&name) {
                Ok(Created::AlreadyExisted)
            } else {
                made.push(name);
                Ok(Created::Made)
            }
        };
        Box::pin(async move { r })
    }
}

struct World {
    shared: Arc<Shared>,
    clock: Arc<ManualClock>,
    ctx: FakeCtx,
    kube: Arc<FakeKube>,
    yaml: String,
    /// The state each Job is reported in, when the observer reads.
    states: BTreeMap<String, JobRunState>,
}

const BUDGET: &str = "  - id: budget\n    cron: \"0 3 * * 0\"\n    cronjob: { namespace: backups, name: budget-backup }\n    needs:\n      - { type: nfs, resource: basestar-nfs }\n    missed_run: catch_up\n";

impl World {
    fn new(yaml: &str) -> World {
        let mut w = World {
            shared: Arc::new(Shared::new(
                Arc::new(ConfigHolder::new(yaml).unwrap()),
                Arc::new(Metrics::new()),
                Arc::new(ManualClock::new(Millis(SUN + H))),
            )),
            clock: Arc::new(ManualClock::new(Millis(SUN + H))),
            ctx: FakeCtx::default(),
            kube: Arc::new(FakeKube::default()),
            yaml: yaml.to_string(),
            states: BTreeMap::new(),
        };
        w.restart();
        w
    }

    /// A new process: empty cache, a fresh metrics registry, the same stored state.
    fn restart(&mut self) {
        self.shared = Arc::new(Shared::new(
            Arc::new(ConfigHolder::new(&self.yaml).unwrap()),
            Arc::new(Metrics::new()),
            self.clock.clone(),
        ));
        self.shared.set_job_backend(self.kube.clone());
    }

    fn example() -> World {
        World::new(EXAMPLE_CONFIG)
    }

    fn push(&self, source: &str, values: Vec<ObservedValue>) {
        self.shared.ingest_now(Report::Observed {
            source: source.into(),
            fidelity: Fidelity::Polled,
            values,
        });
    }

    /// One tick at `at`: every observer reports afresh (NFS up or down, and the jobs'
    /// Jobs if `read`), then the tick runs and its writes are committed.
    async fn tick(&self, at: u64, nfs_up: bool, read: bool) -> TickReport {
        self.clock.set(Millis(at));
        self.push(
            "cluster-pods",
            vec![ObservedValue::ResourceUp {
                resource: "cuda-central".into(),
                up: true,
            }],
        );
        self.push(
            "basestar-probe",
            vec![ObservedValue::ResourceUp {
                resource: "basestar-nfs".into(),
                up: nfs_up,
            }],
        );
        self.push(
            "basestar-mounts",
            vec![ObservedValue::ResourceMounts {
                resource: "basestar-nfs".into(),
                pods: 0,
            }],
        );
        if read {
            self.push_jobs();
        }
        let r = object::tick(&self.shared, &self.ctx).await.unwrap();
        self.ctx.commit();
        r
    }

    fn push_jobs(&self) {
        let config = self.shared.config.current();
        let values = config
            .jobs()
            .iter()
            .map(|j| ObservedValue::JobRuns {
                job: j.id.clone(),
                runs: self
                    .kube
                    .made
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|n| n.starts_with(&format!("{}-", j.id)))
                    .map(|n| sched_model::JobRunReading {
                        name: n.clone(),
                        state: self.states.get(n).copied().unwrap_or(JobRunState::Active),
                    })
                    .collect(),
            })
            .collect();
        self.push("scheduled-jobs", values);
    }

    fn book(&self) -> JobBook {
        persist::decode_jobs(self.ctx.stored(persist::JOBS_KEY).as_deref()).unwrap()
    }

    /// The job holds in the stored lease book, as `job:<id>` on a resource.
    fn holds(&self) -> Vec<String> {
        let book = persist::decode_leases(self.ctx.stored(persist::LEASES_KEY).as_deref()).unwrap();
        book.leases
            .iter()
            .filter_map(|l| match &l.kind {
                LeaseKind::SharedHold { holder } => Some(format!("{holder}@{}", l.resource)),
                _ => None,
            })
            .collect()
    }

    fn counter(&self, job: &str, event: &str) -> String {
        let text = self.shared.metrics.render();
        let want = format!("sched_job_events_total{{event=\"{event}\",scheduled_job=\"{job}\"}} ");
        text.lines()
            .find_map(|l| l.strip_prefix(&want).map(str::to_string))
            .unwrap_or_else(|| "0".into())
    }
}

fn kinds(r: &TickReport) -> Vec<String> {
    r.job_events
        .iter()
        .map(|e: &JobEvent| format!("{}:{}", e.job, e.kind.counter()))
        .collect()
}

fn hold() -> String {
    "job:backup@basestar-nfs".to_string()
}

#[tokio::test]
async fn a_normal_run_creates_the_job_holds_nfs_while_it_runs_and_releases_it_when_it_completes() {
    let mut w = World::example();
    assert!(w.tick(SUN + H, true, false).await.job_events.is_empty());
    assert!(w.holds().is_empty());
    // Due: the Job is created in this very tick, and the hold is in the lease book.
    let r = w.tick(DUE, true, false).await;
    assert_eq!(kinds(&r), vec!["backup:started"], "{r:?}");
    assert_eq!(*w.kube.made.lock().unwrap(), vec![NAME.to_string()]);
    assert_eq!(w.holds(), vec![hold()]);
    // Running: nothing changes, the hold stays.
    for k in 1..4 {
        let r = w.tick(DUE + k * MIN, true, true).await;
        assert!(r.job_events.is_empty(), "{r:?}");
        assert_eq!(w.holds(), vec![hold()]);
    }
    assert_eq!(w.counter("backup", "started"), "1");
    // Complete: succeeded, the hold released, the success remembered.
    w.states.insert(NAME.into(), JobRunState::Complete);
    let r = w.tick(DUE + 5 * MIN, true, true).await;
    assert_eq!(kinds(&r), vec!["backup:succeeded"]);
    assert!(w.holds().is_empty());
    assert!(w.book().jobs[&JobId::new("backup")].run.is_none());
    let text = w.shared.metrics.render();
    assert!(
        text.contains(
            "sched_job_last_success_timestamp_seconds{scheduled_job=\"backup\"} 1772334300"
        ),
        "{text}"
    );
    assert!(
        text.contains("sched_job_running{scheduled_job=\"backup\"} 0"),
        "{text}"
    );
    // The next run is a week away.
    let next = w.book().jobs[&JobId::new("backup")].next_due;
    assert_eq!(next, Some(Millis(DUE + 7 * D)));
}

#[tokio::test]
async fn nfs_that_comes_up_late_still_runs_the_job_and_a_manual_host_is_never_powered() {
    let w = World::example();
    w.tick(SUN + H, false, false).await;
    let r = w.tick(DUE, false, false).await;
    assert!(r.job_events.is_empty());
    assert_eq!(w.holds(), vec![hold()], "the hold is taken while it waits");
    assert!(w.kube.calls.lock().unwrap().is_empty());
    assert!(
        w.ctx.powers.lock().unwrap().is_empty(),
        "manual control: a person powers it"
    );
    let r = w.tick(DUE + 20 * MIN, false, false).await;
    assert!(r.job_events.is_empty());
    // The sequencer learns the host is up in this tick; the job step, which runs before it,
    // sees it in the next (a backstop interval later at most).
    let r = w.tick(DUE + 21 * MIN, true, false).await;
    assert!(r.job_events.is_empty());
    let r = w.tick(DUE + 22 * MIN, true, false).await;
    assert_eq!(kinds(&r), vec!["backup:started"], "{r:?}");
}

#[tokio::test]
async fn a_managed_host_is_woken_by_the_hold_and_flowmanager_never_retries_power() {
    let w = World::new(&example_yaml_managed());
    w.tick(SUN + H, false, false).await;
    // The first tick that sees the host off only learns its state; the due tick wakes it.
    w.tick(DUE - MIN, false, false).await;
    w.tick(DUE, false, false).await;
    let ons = || {
        w.ctx
            .powers
            .lock()
            .unwrap()
            .iter()
            .filter(|(r, a)| r == "basestar-nfs" && *a == PowerAction::On)
            .count()
    };
    assert_eq!(ons(), 1, "{:?}", w.ctx.powers.lock().unwrap());
    // It never comes up (power_on_timeout 5m): failed, and the job's needs stay down; no
    // further power-on however long it waits, and the run is missed at the end of its window
    // (catch_up keeps waiting, with one alert).
    let mut seen = vec![];
    for k in 1..=40 {
        let r = w.tick(DUE + k * MIN, false, false).await;
        seen.extend(kinds(&r));
    }
    assert_eq!(ons(), 1, "power is never retried");
    assert_eq!(
        seen,
        vec!["backup:start_late"],
        "alerted once at the end of the window"
    );
    assert!(w.kube.calls.lock().unwrap().is_empty());
    // A person fixes it by hand: the host is seen up (resume is the person's call), the
    // catch-up run goes ahead, late, once.
    w.tick(DUE + 50 * MIN, true, false).await;
    let r = w.tick(DUE + 51 * MIN, true, false).await;
    assert!(
        r.job_events.iter().any(|e| e.kind.counter() == "started"),
        "needs up again: {r:?} {:?}",
        r.state
    );
}

#[tokio::test]
async fn nfs_that_never_comes_up_is_missed_per_policy_and_the_next_window_starts_no_burst() {
    // catch_up (the example's backup): alerts at the end of the window, keeps waiting, and at
    // the next Sunday drops the old run; the new one waits, a single hold throughout.
    let w = World::example();
    w.tick(SUN + H, false, false).await;
    w.tick(DUE, false, false).await;
    let r = w.tick(DUE + 30 * MIN, false, false).await;
    assert_eq!(kinds(&r), vec!["backup:start_late"]);
    assert_eq!(w.counter("backup", "start_late"), "1");
    assert_eq!(w.holds(), vec![hold()]);
    let mut seen = vec![];
    let mut t = DUE + 30 * MIN;
    while t < DUE + 7 * D + 10 * MIN {
        t += 10 * MIN;
        seen.extend(kinds(&w.tick(t, false, true).await));
    }
    assert_eq!(seen, vec!["backup:missed"], "{seen:?}");
    assert_eq!(w.counter("backup", "missed"), "1");
    assert_eq!(
        w.holds(),
        vec![hold()],
        "one hold, for the new week's run only"
    );
    assert!(w.kube.calls.lock().unwrap().is_empty());

    // skip: dropped at the end of the window, the hold released at once.
    let skip = EXAMPLE_CONFIG.replace("missed_run: catch_up", "missed_run: skip");
    let w = World::new(&skip);
    w.tick(SUN + H, false, false).await;
    w.tick(DUE, false, false).await;
    assert_eq!(w.holds(), vec![hold()]);
    let r = w.tick(DUE + 30 * MIN, false, false).await;
    assert_eq!(kinds(&r), vec!["backup:missed"]);
    assert!(w.holds().is_empty());
    assert_eq!(
        w.book().jobs[&JobId::new("backup")]
            .last
            .as_ref()
            .unwrap()
            .attempts,
        0,
        "a missed run uses no retry"
    );
}

#[tokio::test]
async fn downtime_over_several_weeks_runs_at_most_the_latest_catch_up_once() {
    let w = World::example();
    w.tick(SUN + H, true, false).await;
    // The service is down for three weeks and comes back at 05:00 on the third Sunday.
    let r = w.tick(DUE + 14 * D + 2 * H, true, false).await;
    assert_eq!(
        kinds(&r),
        vec!["backup:missed", "backup:started", "backup:caught_up"],
        "{r:?}"
    );
    assert_eq!(w.kube.made.lock().unwrap().len(), 1, "no burst of runs");
    assert_eq!(w.counter("backup", "caught_up"), "1");
}

#[tokio::test]
async fn a_run_due_while_the_previous_instance_runs_is_skipped_and_alerted() {
    let hourly = EXAMPLE_CONFIG.replace("cron: \"0 3 * * 0\"", "cron: \"0 * * * *\"");
    let mut w = World::new(&hourly);
    w.tick(SUN + H, true, false).await;
    let r = w.tick(SUN + 2 * H, true, false).await;
    assert_eq!(kinds(&r), vec!["backup:started"]);
    let r = w.tick(SUN + 3 * H, true, true).await;
    assert_eq!(kinds(&r), vec!["backup:skipped_overlap"], "{r:?}");
    assert_eq!(w.counter("backup", "skipped_overlap"), "1");
    assert_eq!(
        w.kube.made.lock().unwrap().len(),
        1,
        "never in parallel with itself"
    );
    assert_eq!(w.holds(), vec![hold()]);
    // It finishes; the next hour starts a new one, nothing queued in between.
    let name = w.kube.made.lock().unwrap()[0].clone();
    w.states.insert(name, JobRunState::Complete);
    let r = w.tick(SUN + 3 * H + 5 * MIN, true, true).await;
    assert_eq!(kinds(&r), vec!["backup:succeeded"]);
    let r = w.tick(SUN + 4 * H, true, true).await;
    assert_eq!(kinds(&r), vec!["backup:started"]);
    assert_eq!(w.kube.made.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn a_failure_is_retried_then_fails_for_good_alerts_and_releases_the_hold() {
    let w = {
        let mut w = World::example();
        w.tick(SUN + H, true, false).await;
        w.tick(DUE, true, false).await;
        w.states.insert(NAME.into(), JobRunState::Failed);
        w
    };
    let mut w = w;
    let r = w.tick(DUE + 2 * MIN, true, true).await;
    assert_eq!(kinds(&r), vec!["backup:retried"]);
    assert_eq!(
        w.holds(),
        vec![hold()],
        "the hold is kept through the backoff"
    );
    // The backoff (5m) ends: a second Job with its own name.
    let r = w.tick(DUE + 7 * MIN, true, true).await;
    assert_eq!(kinds(&r), vec!["backup:started"]);
    assert_eq!(w.kube.made.lock().unwrap()[1], "backup-1772334000-a2");
    w.states
        .insert("backup-1772334000-a2".into(), JobRunState::Failed);
    let r = w.tick(DUE + 10 * MIN, true, true).await;
    assert_eq!(kinds(&r), vec!["backup:failed"]);
    assert!(w.holds().is_empty(), "released after the retry limit");
    assert_eq!(w.counter("backup", "failed"), "1");
    assert_eq!(w.counter("backup", "retried"), "1");
    // Next Sunday starts afresh.
    let r = w.tick(DUE + 7 * D, true, true).await;
    assert_eq!(kinds(&r), vec!["backup:started"]);
    assert_eq!(
        w.kube.made.lock().unwrap().last().unwrap(),
        "backup-1772938800-a1"
    );
}

#[tokio::test]
async fn a_restart_mid_run_continues_from_the_stored_state_and_leaks_no_hold() {
    let mut w = World::example();
    w.tick(SUN + H, true, false).await;
    w.tick(DUE, true, false).await;
    assert_eq!(w.holds(), vec![hold()]);
    // The process restarts: empty cache, the stored job book and leases carry on. Without a
    // reading nothing is concluded.
    let r = w.tick(DUE + 10 * MIN, true, false).await;
    assert!(r.job_events.is_empty(), "{r:?}");
    assert_eq!(w.holds(), vec![hold()]);
    w.states.insert(NAME.into(), JobRunState::Complete);
    let r = w.tick(DUE + 11 * MIN, true, true).await;
    assert_eq!(kinds(&r), vec!["backup:succeeded"]);
    assert!(w.holds().is_empty());
}

#[tokio::test]
async fn a_crash_around_the_creation_makes_the_job_once() {
    let w = World::example();
    w.tick(SUN + H, true, false).await;
    // The Job is made but the invocation dies before the result is journaled.
    w.clock.set(Millis(DUE));
    w.push(
        "basestar-probe",
        vec![ObservedValue::ResourceUp {
            resource: "basestar-nfs".into(),
            up: true,
        }],
    );
    w.push(
        "cluster-pods",
        vec![ObservedValue::ResourceUp {
            resource: "cuda-central".into(),
            up: true,
        }],
    );
    w.ctx.crash_at("create_job", 1, Crash::After);
    assert!(object::tick(&w.shared, &w.ctx).await.is_err());
    assert_eq!(w.kube.calls.lock().unwrap().len(), 1);
    // The retry replays the decision and asks again: the Job exists, which is success.
    w.ctx.crash_and_retry();
    let r = object::tick(&w.shared, &w.ctx).await.unwrap();
    w.ctx.commit();
    assert_eq!(kinds(&r), vec!["backup:started"]);
    assert_eq!(w.kube.calls.lock().unwrap().len(), 2, "asked twice");
    assert_eq!(w.kube.made.lock().unwrap().len(), 1, "made once");
    assert_eq!(w.holds(), vec![hold()]);
}

#[tokio::test]
async fn a_job_deleted_externally_fails_the_run_and_releases_the_hold() {
    let w = World::example();
    w.tick(SUN + H, true, false).await;
    w.tick(DUE, true, false).await;
    // The observer reads after the Job was made and finds none.
    w.kube.made.lock().unwrap().clear();
    let r = w.tick(DUE + 2 * MIN, true, true).await;
    assert_eq!(kinds(&r), vec!["backup:failed"]);
    assert!(w.holds().is_empty());
    assert_eq!(
        w.book().jobs[&JobId::new("backup")]
            .last
            .as_ref()
            .unwrap()
            .reason
            .as_deref(),
        Some("job_deleted")
    );
}

#[tokio::test]
async fn a_stale_reading_never_finishes_a_job_and_the_deadline_alerts_without_deleting() {
    let w = World::example();
    w.tick(SUN + H, true, false).await;
    w.tick(DUE, true, false).await;
    // The Job completed, but the observer has gone quiet: stale readings conclude nothing.
    let mut w = w;
    w.states.insert(NAME.into(), JobRunState::Complete);
    for k in 1..=10 {
        let r = w.tick(DUE + k * 30 * MIN, true, false).await;
        assert!(r.job_events.is_empty(), "{k}: {r:?}");
        assert_eq!(w.holds(), vec![hold()]);
    }
    // 6h after creation (the default deadline): alerted and failed, the hold released, and
    // the Job is not touched (the backend has no way to delete).
    let r = w.tick(DUE + 6 * H, true, false).await;
    assert_eq!(kinds(&r), vec!["backup:failed"]);
    assert!(w.holds().is_empty());
    assert_eq!(
        w.book().jobs[&JobId::new("backup")].lingering.as_deref(),
        Some(NAME)
    );
    // A fresh reading that shows it complete frees the job.
    w.tick(DUE + 6 * H + MIN, true, true).await;
    assert_eq!(w.book().jobs[&JobId::new("backup")].lingering, None);
}

#[tokio::test]
async fn two_jobs_run_in_parallel_and_hold_nfs_together() {
    let mut w = World::new(&format!("{EXAMPLE_CONFIG}{BUDGET}"));
    w.tick(SUN + H, true, false).await;
    let r = w.tick(DUE, true, false).await;
    assert_eq!(kinds(&r), vec!["backup:started", "budget:started"]);
    assert_eq!(w.holds().len(), 2);
    // One finishes: only its own hold goes.
    w.states.insert(NAME.into(), JobRunState::Complete);
    let r = w.tick(DUE + 5 * MIN, true, true).await;
    assert_eq!(kinds(&r), vec!["backup:succeeded"]);
    assert_eq!(w.holds(), vec!["job:budget@basestar-nfs".to_string()]);
    let name = w.kube.made.lock().unwrap()[1].clone();
    w.states.insert(name, JobRunState::Complete);
    w.tick(DUE + 6 * MIN, true, true).await;
    assert!(w.holds().is_empty());
}

#[tokio::test]
async fn trigger_job_starts_a_run_now_refuses_overlap_and_unknown_jobs() {
    let w = World::example();
    w.tick(SUN + H, true, false).await;
    w.clock.set(Millis(SUN + 2 * H));
    let s = object::trigger_job(
        &w.shared,
        &w.ctx,
        TriggerJob {
            id: "backup".into(),
        },
    )
    .await
    .unwrap();
    w.ctx.commit();
    assert_eq!(s.run.unwrap().phase, "waiting");
    // The next tick takes the hold and creates the Job.
    let r = w.tick(SUN + 2 * H + MIN, true, false).await;
    assert_eq!(kinds(&r), vec!["backup:started"]);
    assert_eq!(w.holds(), vec![hold()]);
    // Again while it runs: refused (409), nothing queued.
    let e = object::trigger_job(
        &w.shared,
        &w.ctx,
        TriggerJob {
            id: "backup".into(),
        },
    )
    .await
    .unwrap_err();
    assert!(format!("{e:?}").contains("already has a run"), "{e:?}");
    assert_eq!(w.counter("backup", "skipped_overlap"), "1");
    let e = object::trigger_job(&w.shared, &w.ctx, TriggerJob { id: "nope".into() })
        .await
        .unwrap_err();
    assert!(format!("{e:?}").contains("unknown job"), "{e:?}");
    // The list shows the run.
    let raw = w.ctx.stored(persist::JOBS_KEY);
    let list = object::list_jobs(&w.shared, raw.as_deref()).unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(
        list[0].run.as_ref().unwrap().job_name.as_deref(),
        Some("backup-1772330400-a1")
    );
}

#[tokio::test]
async fn a_failed_creation_is_alerted_once_and_tried_again_and_a_stray_job_hold_is_released() {
    let w = World::example();
    w.tick(SUN + H, true, false).await;
    w.kube.refuse.store(true, Ordering::SeqCst);
    let r = w.tick(DUE, true, false).await;
    assert_eq!(kinds(&r), vec!["backup:create_failed"]);
    let r = w.tick(DUE + MIN, true, false).await;
    assert!(r.job_events.is_empty(), "alerted once: {r:?}");
    w.kube.refuse.store(false, Ordering::SeqCst);
    let r = w.tick(DUE + 2 * MIN, true, false).await;
    assert_eq!(kinds(&r), vec!["backup:started"]);
    // A hold of a job that has no run (left by a lost record, say) is released by the next
    // tick; a manual host lease is never touched.
    let mut book = persist::decode_leases(w.ctx.stored(persist::LEASES_KEY).as_deref()).unwrap();
    for (id, kind) in [
        (
            "stray",
            LeaseKind::SharedHold {
                holder: "job:gone".into(),
            },
        ),
        ("manual", LeaseKind::ManualHost),
    ] {
        book.leases.push(sched_model::Lease {
            id: id.into(),
            resource: "basestar-nfs".into(),
            kind,
            granted_at: Millis(0),
            origin: sched_model::LeaseOrigin::Requested,
        });
    }
    w.ctx
        .put(persist::LEASES_KEY, &persist::encode_leases(&book));
    w.tick(DUE + 3 * MIN, true, true).await;
    let ids: Vec<String> = persist::decode_leases(w.ctx.stored(persist::LEASES_KEY).as_deref())
        .unwrap()
        .leases
        .iter()
        .map(|l| l.id.to_string())
        .collect();
    assert!(!ids.contains(&"stray".to_string()), "{ids:?}");
    assert!(ids.contains(&"manual".to_string()), "{ids:?}");
    assert!(
        ids.contains(&"hold-job-backup-basestar-nfs".to_string()),
        "{ids:?}"
    );
}

#[tokio::test]
async fn an_unreadable_job_book_switches_only_the_jobs_off() {
    let w = World::example();
    w.tick(SUN + H, true, false).await;
    w.tick(DUE, true, false).await;
    w.ctx.put(persist::JOBS_KEY, b"not json");
    let r = w.tick(DUE + MIN, true, true).await;
    assert!(r.error.is_none(), "the tick still decides: {r:?}");
    assert_eq!(w.holds(), vec![hold()], "holds are left as they are");
    assert_eq!(
        w.ctx.stored(persist::JOBS_KEY).as_deref(),
        Some(&b"not json"[..])
    );
    assert!(
        w.shared.metrics.render().contains("job_book"),
        "counted as a state error"
    );
}

#[tokio::test]
async fn a_reading_received_while_the_job_was_being_made_never_proves_it_deleted() {
    let w = World::example();
    w.tick(SUN + H, true, false).await;
    // The tick decides at DUE; the call takes 10 s, and the observer's read, which cannot list
    // the Job yet, is received in the meantime.
    let (clock, shared) = (w.clock.clone(), w.shared.clone());
    *w.kube.on_create.lock().unwrap() = Some(Box::new(move || {
        clock.set(Millis(DUE + 10_000));
        shared.ingest_now(Report::Observed {
            source: "scheduled-jobs".into(),
            fidelity: Fidelity::Polled,
            values: vec![ObservedValue::JobRuns {
                job: "backup".into(),
                runs: vec![],
            }],
        });
    }));
    let r = w.tick(DUE, true, false).await;
    assert_eq!(kinds(&r), vec!["backup:started"]);
    *w.kube.on_create.lock().unwrap() = None;
    // That reading is still the newest at the next ticks (an unchanged reading wakes nothing).
    for k in 1..=2 {
        let r = w.tick(DUE + k * 30_000, true, false).await;
        assert!(r.job_events.is_empty(), "{k}: {r:?}");
        assert_eq!(w.holds(), vec![hold()]);
    }
    let rec = &w.book().jobs[&JobId::new("backup")];
    assert!(rec.run.is_some() && rec.last.is_none(), "{rec:?}");
    // The run is stamped with the time the call returned, not the time of the decision.
    assert!(
        matches!(
            rec.run.as_ref().unwrap().phase,
            sched_model::RunPhase::Running { created_at, .. } if created_at == Millis(DUE + 10_000)
        ),
        "{rec:?}"
    );
    // The Job is really there, and the next full read lists it.
    w.tick(DUE + 2 * MIN, true, true).await;
    assert!(w.book().jobs[&JobId::new("backup")].run.is_some());
}

#[tokio::test]
async fn a_creation_that_never_worked_leaves_the_job_marked_in_case_it_exists() {
    let skip = EXAMPLE_CONFIG.replace("missed_run: catch_up", "missed_run: skip");
    let w = World::new(&skip);
    w.tick(SUN + H, true, false).await;
    w.kube.refuse.store(true, Ordering::SeqCst);
    w.tick(DUE, true, false).await;
    let r = w.tick(DUE + 30 * MIN, true, false).await;
    assert_eq!(kinds(&r), vec!["backup:missed"]);
    assert!(w.holds().is_empty());
    let rec = &w.book().jobs[&JobId::new("backup")];
    assert_eq!(rec.lingering.as_deref(), Some(NAME));
    // A reading that does not list it frees the job once it is newer than the failed try.
    w.tick(DUE + 31 * MIN, true, true).await;
    assert_eq!(w.book().jobs[&JobId::new("backup")].lingering, None);
}

#[tokio::test]
async fn a_job_removed_from_the_config_while_its_job_runs_is_not_started_again_beside_it() {
    let two = format!("{EXAMPLE_CONFIG}{BUDGET}");
    let mut w = World::new(&two);
    w.tick(SUN + H, true, false).await;
    w.tick(DUE, true, false).await;
    assert_eq!(w.holds().len(), 2);
    let budget = "budget-1772334000-a1";
    // The job leaves the config: its run and hold end with it, but the Job may still be active.
    assert!(matches!(
        w.shared.config.apply_text(EXAMPLE_CONFIG),
        crate::config_source::ReloadOutcome::Applied(_)
    ));
    w.tick(DUE + MIN, true, true).await;
    assert_eq!(w.holds(), vec![hold()]);
    let tomb = &w.book().jobs[&JobId::new("budget")];
    assert!(tomb.run.is_none() && tomb.lingering.as_deref() == Some(budget));
    // Added back while the Job is still active: a run asked for now is refused.
    assert!(matches!(
        w.shared.config.apply_text(&two),
        crate::config_source::ReloadOutcome::Applied(_)
    ));
    w.clock.set(Millis(DUE + 2 * MIN));
    let e = object::trigger_job(
        &w.shared,
        &w.ctx,
        TriggerJob {
            id: "budget".into(),
        },
    )
    .await
    .unwrap_err();
    assert!(format!("{e:?}").contains("already has a run"), "{e:?}");
    w.ctx.commit();
    // Its Job finishes: the mark goes and a run may start.
    w.states.insert(budget.into(), JobRunState::Complete);
    w.tick(DUE + 3 * MIN, true, true).await;
    assert_eq!(w.book().jobs[&JobId::new("budget")].lingering, None);
    w.clock.set(Millis(DUE + 4 * MIN));
    object::trigger_job(
        &w.shared,
        &w.ctx,
        TriggerJob {
            id: "budget".into(),
        },
    )
    .await
    .unwrap();
    w.ctx.commit();
    // A job removed with nothing active leaves nothing behind.
    w.shared.config.apply_text(EXAMPLE_CONFIG);
    w.tick(DUE + 5 * MIN, true, true).await;
    w.tick(DUE + 6 * MIN, true, true).await;
    assert!(
        !w.book().jobs.contains_key(&JobId::new("budget")),
        "a run that was only waiting leaves no tombstone"
    );
}

#[tokio::test]
async fn a_changed_namespace_while_the_job_runs_is_not_read_as_the_job_being_deleted() {
    let w = World::example();
    w.tick(SUN + H, true, false).await;
    w.tick(DUE, true, false).await;
    let moved = EXAMPLE_CONFIG
        .replace("namespace: backups", "namespace: archive")
        .replace(
            "job_namespaces: [backups]",
            "job_namespaces: [backups, archive]",
        );
    assert_ne!(moved, EXAMPLE_CONFIG);
    assert!(matches!(
        w.shared.config.apply_text(&moved),
        crate::config_source::ReloadOutcome::Applied(_)
    ));
    // The observer now reads the new namespace and finds no such Job.
    w.kube.made.lock().unwrap().clear();
    for k in 1..=3 {
        let r = w.tick(DUE + k * 2 * MIN, true, true).await;
        assert!(r.job_events.is_empty(), "{k}: {r:?}");
        assert_eq!(w.holds(), vec![hold()]);
    }
}

#[tokio::test]
async fn an_unreadable_job_book_keeps_the_hold_in_the_planners_view_and_starts_no_shutdown() {
    let w = World::new(&example_yaml_managed());
    w.tick(SUN + H, false, false).await;
    w.tick(DUE - MIN, false, false).await;
    w.tick(DUE, false, false).await;
    // The host comes up and the Job runs, holding it.
    w.tick(DUE + MIN, true, false).await;
    let r = w.tick(DUE + 2 * MIN, true, false).await;
    assert!(
        r.job_events.iter().any(|e| e.kind.counter() == "started"),
        "{r:?}"
    );
    w.ctx.put(persist::JOBS_KEY, b"not json");
    // For as long as the book stays unreadable, the planner sees the hold (the job step
    // running on an empty book would release it and the idle host would be shut down).
    for k in 1..=40 {
        let r = w.tick(DUE + 2 * MIN + k * 5 * MIN, true, true).await;
        assert!(
            r.leases
                .iter()
                .any(|l| l.id.as_str() == "hold-job-backup-basestar-nfs"),
            "{k}: the hold is gone from the planner's view: {:?}",
            r.leases
        );
        assert!(
            !r.steps
                .iter()
                .any(|s| matches!(s, sched_model::Step::PowerOff { .. })),
            "{k}: {:?}",
            r.steps
        );
    }
    assert!(
        !w.ctx
            .powers
            .lock()
            .unwrap()
            .iter()
            .any(|(_, a)| *a == PowerAction::Off),
        "no shutdown was requested"
    );
}

/// A managed host that flowmanager is shutting down (the stored state says so), with
/// `min_off` of 45 minutes, and a job (`backup`, start window 30m) that is due a minute after
/// the host is seen down. `missed_run` is the job's policy.
async fn min_off_world(missed_run: &str) -> World {
    let yaml = example_yaml_managed()
        .replacen(
            "power_on_timeout:",
            "min_off: 45m\n      power_on_timeout:",
            2,
        )
        .replace("missed_run: catch_up", &format!("missed_run: {missed_run}"));
    let w = World::new(&yaml);
    let mut state = sched_model::SchedState::default();
    state.resources.insert(
        "basestar-nfs".into(),
        sched_model::ResourceState::new(sched_model::PowerPhase::ShuttingDown {
            since: Millis(DUE - 5 * MIN),
        }),
    );
    let core = persist::CoreState {
        state,
        ..Default::default()
    };
    w.ctx.put(persist::STATE_KEY, &persist::encode_state(&core));
    w
}

fn power_ons(w: &World) -> usize {
    w.ctx
        .powers
        .lock()
        .unwrap()
        .iter()
        .filter(|(r, a)| r == "basestar-nfs" && *a == PowerAction::On)
        .count()
}

#[tokio::test]
async fn a_job_hold_during_min_off_waits_and_a_catch_up_run_alerts_at_the_window_and_starts_late() {
    let w = min_off_world("catch_up").await;
    // The host is seen down a minute before the job is due: the minimum off time starts.
    let r = w.tick(DUE - MIN, false, false).await;
    let st = r.state.resource(&"basestar-nfs".into()).unwrap();
    assert_eq!(st.off_since, Some(Millis(DUE - MIN)));
    // Due: the hold exists, the host is wanted, and it is reported waiting, not silent.
    let r = w.tick(DUE, false, false).await;
    assert_eq!(w.holds(), vec![hold()]);
    assert!(
        r.steps.contains(&sched_model::Step::Wait {
            resource: "basestar-nfs".into(),
            reason: sched_model::WaitReason::MinOff
        }),
        "{:?}",
        r.steps
    );
    assert_eq!(r.min_off_waits().len(), 1, "{r:?}");
    assert_eq!(
        r.min_off_waits()[0].min_off.map(|m| m.remaining),
        Some(Millis(44 * MIN))
    );
    assert!(
        r.reason.contains("waiting for min_off, 2640s left"),
        "{}",
        r.reason
    );
    assert!(
        w.shared
            .metrics
            .render()
            .contains("sched_resource_min_off_remaining_seconds{resource=\"basestar-nfs\"} 2640"),
        "{}",
        w.shared.metrics.render()
    );
    assert_eq!(power_ons(&w), 0);
    // Through the start window (30m) and beyond: no power-on; catch_up alerts once at the
    // window's end and keeps waiting.
    let mut seen = vec![];
    for k in 1..44 {
        let r = w.tick(DUE + k * MIN, false, false).await;
        seen.extend(kinds(&r));
        assert_eq!(power_ons(&w), 0, "powered on early at +{k}m");
        assert_eq!(w.holds(), vec![hold()]);
    }
    assert_eq!(seen, vec!["backup:start_late"], "{seen:?}");
    // min_off after the host was seen down: powered on, once.
    w.tick(DUE + 44 * MIN, false, false).await;
    assert_eq!(power_ons(&w), 1);
    for k in 45..48 {
        w.tick(DUE + k * MIN, false, false).await;
    }
    assert_eq!(power_ons(&w), 1, "never retried");
    // It comes up; the late run goes ahead, once.
    w.tick(DUE + 50 * MIN, true, false).await;
    let r = w.tick(DUE + 51 * MIN, true, false).await;
    assert!(
        r.job_events.iter().any(|e| e.kind.counter() == "started"),
        "{r:?}"
    );
}

#[tokio::test]
async fn a_skip_run_that_waits_out_its_window_for_min_off_is_missed_and_the_host_stays_off() {
    let w = min_off_world("skip").await;
    w.tick(DUE - MIN, false, false).await;
    let r = w.tick(DUE, false, false).await;
    assert_eq!(w.holds(), vec![hold()]);
    assert_eq!(r.min_off_waits().len(), 1, "{r:?}");
    // The window (30m) ends inside the wait (45m): dropped as missed, the hold released.
    let r = w.tick(DUE + 30 * MIN, false, false).await;
    assert_eq!(kinds(&r), vec!["backup:missed"], "{r:?}");
    assert!(w.holds().is_empty());
    // Nothing wants the host any more: no wait is reported and it is not powered on.
    let r = w.tick(DUE + 31 * MIN, false, false).await;
    assert!(r.min_off_waits().is_empty(), "{r:?}");
    let r = w.tick(DUE + 50 * MIN, false, false).await;
    assert!(r.min_off_waits().is_empty(), "{r:?}");
    assert_eq!(power_ons(&w), 0);
    assert_eq!(w.counter("backup", "missed"), "1");
    assert!(w.kube.calls.lock().unwrap().is_empty());
}

impl World {
    /// Locks `basestar-nfs` out (or clears it) through the handlers, and commits.
    async fn lockout(&self, on: bool) {
        let req = || object::ResourceRequest {
            resource: "basestar-nfs".into(),
        };
        if on {
            object::lockout(&self.shared, &self.ctx, req())
                .await
                .unwrap();
        } else {
            object::clear_lockout(&self.shared, &self.ctx, req())
                .await
                .unwrap();
        }
        self.ctx.commit();
    }
}

#[tokio::test]
async fn a_due_job_does_not_start_on_a_locked_out_host_that_is_up_and_waits_as_for_a_down_one() {
    // catch_up (the example's backup): the host is up, but locked out.
    let w = World::example();
    w.tick(SUN + H, true, false).await;
    w.lockout(true).await;
    let r = w.tick(DUE, true, false).await;
    assert!(r.job_events.is_empty(), "{r:?}");
    assert!(w.kube.calls.lock().unwrap().is_empty(), "no Job is created");
    // Waiting: the run's own hold only, inert under the lockout (the planner wants no power
    // for a locked-out host and the lockout overrides holds).
    assert_eq!(w.holds(), vec![hold()]);
    for k in 1..=3 {
        let r = w.tick(DUE + k * MIN, true, false).await;
        assert!(r.job_events.is_empty(), "{r:?}");
    }
    // The catch-up rules apply as for a host that is not up: one alert at the end of the window.
    let r = w.tick(DUE + 30 * MIN, true, false).await;
    assert_eq!(kinds(&r), vec!["backup:start_late"]);
    assert!(w.kube.calls.lock().unwrap().is_empty());
    // Cleared: the catch-up run goes ahead (the host is up).
    w.lockout(false).await;
    let r = w.tick(DUE + 31 * MIN, true, false).await;
    assert_eq!(
        kinds(&r),
        vec!["backup:started", "backup:caught_up"],
        "{r:?}"
    );
    assert_eq!(w.kube.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_skip_job_on_a_locked_out_host_is_missed_at_the_end_of_its_window() {
    let skip = EXAMPLE_CONFIG.replace("missed_run: catch_up", "missed_run: skip");
    let w = World::new(&skip);
    w.tick(SUN + H, true, false).await;
    w.lockout(true).await;
    w.tick(DUE, true, false).await;
    assert_eq!(w.holds(), vec![hold()]);
    let r = w.tick(DUE + 30 * MIN, true, false).await;
    assert_eq!(kinds(&r), vec!["backup:missed"], "{r:?}");
    assert!(w.holds().is_empty());
    assert!(w.kube.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_job_already_running_when_the_lockout_arrives_keeps_running_and_keeps_its_hold() {
    let mut w = World::example();
    w.tick(SUN + H, true, false).await;
    let r = w.tick(DUE, true, false).await;
    assert_eq!(kinds(&r), vec!["backup:started"]);
    w.lockout(true).await;
    for k in 1..4 {
        let r = w.tick(DUE + k * MIN, true, true).await;
        assert!(r.job_events.is_empty(), "{r:?}");
        // Not killed, no Job deleted or recreated, and the hold stays until the run ends;
        // the lockout still shuts the host down (the veto waits for the job's pods).
        assert_eq!(w.holds(), vec![hold()]);
    }
    assert_eq!(w.kube.calls.lock().unwrap().len(), 1);
    w.states.insert(NAME.into(), JobRunState::Complete);
    let r = w.tick(DUE + 5 * MIN, true, true).await;
    assert_eq!(kinds(&r), vec!["backup:succeeded"]);
    assert!(w.holds().is_empty());
}
