use sched_model::{CronJobRef, JobBook, JobId, JobNeed, MissedRun, ResourceId, jobs::rfc3339_utc};

use super::*;

const MIN: u64 = 60_000;
const H: u64 = 60 * MIN;
const D: u64 = 24 * H;
/// 2026-03-01T00:00:00Z, a Sunday.
const SUN: u64 = 1_772_323_200_000;

fn spec(cron: &str, missed: MissedRun) -> JobSpec {
    JobSpec {
        id: JobId::new("backup"),
        cron: cron.into(),
        timezone: None,
        cronjob: CronJobRef {
            namespace: "backups".into(),
            name: "pg".into(),
        },
        needs: vec![JobNeed::Nfs {
            resource: ResourceId::new("basestar-nfs"),
        }],
        retries: None,
        retry_backoff: None,
        missed_run: missed,
        start_window: None,
        deadline: None,
    }
}

fn weekly(missed: MissedRun) -> JobSpec {
    spec("0 3 * * 0", missed)
}

/// A job and its clock.
struct Job {
    spec: JobSpec,
    rec: JobRecord,
    now: u64,
}

impl Job {
    fn new(spec: JobSpec, now: u64) -> Job {
        let mut j = Job {
            spec,
            rec: JobRecord::default(),
            now,
        };
        // First sight: nothing is invented for the past.
        let a = j.step(true, None);
        assert!(a.events.is_empty() && a.create.is_none(), "{a:?}");
        j
    }

    fn at(&mut self, now: u64) -> &mut Job {
        self.now = now;
        self
    }

    fn step(&mut self, needs_up: bool, reading: Option<(u64, &[JobRunReading])>) -> Advance {
        let i = Inputs {
            now: Millis(self.now),
            spec: &self.spec,
            needs_up,
            reading: reading.map(|(at, runs)| Reading {
                at: Millis(at),
                runs,
            }),
        };
        advance(&mut self.rec, &i)
    }

    fn made(&mut self) -> Vec<JobEvent> {
        created(&mut self.rec, &self.spec, Millis(self.now), Created::Made)
    }

    fn run(&self) -> &Run {
        self.rec.run.as_ref().expect("a run")
    }

    fn phase(&self) -> &'static str {
        self.rec.run.as_ref().map_or("none", |r| r.phase.as_str())
    }
}

fn kinds(events: &[JobEvent]) -> Vec<&'static str> {
    events.iter().map(|e| e.kind.counter()).collect()
}

fn reading(name: &str, state: JobRunState) -> Vec<JobRunReading> {
    vec![JobRunReading {
        name: name.to_string(),
        state,
    }]
}

const DUE: u64 = SUN + 3 * H;
const NAME1: &str = "backup-1772334000-a1";
const NAME2: &str = "backup-1772334000-a2";

/// A run that is due at `DUE`, its needs up: created and running.
fn running_job(missed: MissedRun) -> Job {
    let mut j = Job::new(weekly(missed), SUN);
    j.at(DUE);
    let a = j.step(true, None);
    assert_eq!(a.create.as_ref().map(|c| c.name.as_str()), Some(NAME1));
    assert_eq!(kinds(&j.made()), vec!["started"]);
    assert_eq!(j.phase(), "running");
    j
}

#[test]
fn a_job_seen_for_the_first_time_starts_from_now_and_invents_no_past() {
    // Mid-week, long after the last Sunday: no run, and the next is the coming Sunday.
    let j = Job::new(weekly(MissedRun::CatchUp), SUN + 3 * D);
    assert_eq!(j.rec.next_due, Some(Millis(DUE + 7 * D)));
    assert!(j.rec.run.is_none() && j.rec.last.is_none());
}

#[test]
fn a_normal_run_waits_for_the_clock_creates_the_job_and_succeeds() {
    let mut j = Job::new(weekly(MissedRun::Skip), SUN);
    // Before the time: nothing.
    j.at(DUE - 1);
    assert_eq!(j.step(true, None), Advance::default());
    // Due, the needs already up: the Job is asked for in this very tick, holding from now on.
    j.at(DUE);
    let a = j.step(true, None);
    assert!(a.events.is_empty());
    let c = a.create.expect("create");
    assert_eq!(
        (c.name.as_str(), c.attempt, c.scheduled_at),
        (NAME1, 1, Millis(DUE))
    );
    assert!(matches!(j.run().phase, RunPhase::Creating { .. }));
    assert_eq!(
        j.rec.next_due,
        Some(Millis(DUE + 7 * D)),
        "the next is set at once"
    );
    // Asked again on the next tick until its creation is recorded.
    j.at(DUE + 1000);
    assert_eq!(
        j.step(true, None).create.map(|c| c.name),
        Some(NAME1.to_string())
    );
    let ev = j.made();
    assert_eq!(kinds(&ev), vec!["started"]);
    assert_eq!(j.phase(), "running");
    // Running: an active Job changes nothing, however many ticks.
    for k in 1..4u64 {
        j.at(DUE + k * MIN);
        let r = reading(NAME1, JobRunState::Active);
        assert_eq!(j.step(true, Some((DUE + k * MIN, &r))), Advance::default());
    }
    // Complete: succeeded, nothing held, the success is remembered.
    j.at(DUE + 10 * MIN);
    let r = reading(NAME1, JobRunState::Complete);
    let a = j.step(true, Some((DUE + 10 * MIN, &r)));
    assert_eq!(kinds(&a.events), vec!["succeeded"]);
    assert!(j.rec.run.is_none());
    assert_eq!(j.rec.last_success_at, Some(Millis(DUE + 10 * MIN)));
    assert_eq!(j.rec.last.as_ref().unwrap().outcome, RunOutcome::Succeeded);
    let book = JobBook {
        jobs: [(JobId::new("backup"), j.rec.clone())].into(),
    };
    assert!(!book.holds(&JobId::new("backup")));
}

#[test]
fn nfs_that_comes_up_late_within_the_window_runs_the_job_without_a_catch_up() {
    for missed in [MissedRun::Skip, MissedRun::CatchUp] {
        let mut j = Job::new(weekly(missed), SUN);
        j.at(DUE);
        assert!(j.step(false, None).create.is_none());
        assert_eq!(j.phase(), "waiting");
        j.at(DUE + 20 * MIN);
        assert_eq!(j.step(false, None), Advance::default());
        j.at(DUE + 30 * MIN - 1);
        assert_eq!(
            j.step(true, None).create.map(|c| c.name),
            Some(NAME1.to_string())
        );
        assert_eq!(kinds(&j.made()), vec!["started"], "{missed:?}: not late");
    }
}

#[test]
fn nfs_that_never_comes_up_is_a_missed_run_and_uses_no_retry() {
    // skip: dropped at the end of the start window.
    let mut j = Job::new(weekly(MissedRun::Skip), SUN);
    j.at(DUE);
    j.step(false, None);
    j.at(DUE + 30 * MIN - 1);
    assert!(j.step(false, None).events.is_empty());
    j.at(DUE + 30 * MIN);
    let a = j.step(false, None);
    assert_eq!(
        a.events[0].kind,
        JobEventKind::Missed {
            reason: "start_window".into(),
            dropped: 0
        }
    );
    assert!(j.rec.run.is_none(), "the hold is released with the run");
    let last = j.rec.last.as_ref().unwrap();
    assert_eq!((last.outcome, last.attempts), (RunOutcome::Missed, 0));
    assert_eq!(
        j.rec.next_due,
        Some(Millis(DUE + 7 * D)),
        "the next run is not affected"
    );
}

#[test]
fn catch_up_keeps_waiting_alerts_once_and_runs_late_at_most_once_before_the_next_time() {
    let mut j = Job::new(weekly(MissedRun::CatchUp), SUN);
    j.at(DUE);
    j.step(false, None);
    // The window ends: one alert, the run stays.
    j.at(DUE + 30 * MIN);
    assert_eq!(kinds(&j.step(false, None).events), vec!["start_late"]);
    j.at(DUE + 31 * MIN);
    assert!(j.step(false, None).events.is_empty(), "alerted once");
    assert_eq!(j.phase(), "waiting");
    // Two days on, someone powers the host on: it runs, late, once.
    j.at(DUE + 2 * D);
    assert!(j.step(true, None).create.is_some());
    let ev = j.made();
    assert_eq!(kinds(&ev), vec!["started", "caught_up"]);
    assert_eq!(ev[1].kind, JobEventKind::CaughtUp { late_by_ms: 2 * D });
}

#[test]
fn the_next_window_arriving_while_still_waiting_drops_the_run_and_starts_no_burst() {
    for missed in [MissedRun::Skip, MissedRun::CatchUp] {
        let mut j = Job::new(weekly(missed), SUN);
        j.at(DUE);
        j.step(false, None);
        let mut seen = vec![];
        // The needs never come. A tick every ten minutes for the whole week and a bit.
        let mut t = DUE;
        while t < DUE + 7 * D + 10 * MIN {
            t += 10 * MIN;
            j.at(t);
            for e in j.step(false, None).events {
                seen.push((t - DUE, e.kind.counter(), e.scheduled_at.as_u64()));
            }
        }
        let missed_at = match missed {
            // skip gives up at the end of its window.
            MissedRun::Skip => 30 * MIN,
            // catch_up alerts at the end of the window and gives up when the next time comes.
            MissedRun::CatchUp => 7 * D,
        };
        let expect = match missed {
            MissedRun::Skip => vec![(missed_at, "missed", DUE)],
            MissedRun::CatchUp => vec![(30 * MIN, "start_late", DUE), (missed_at, "missed", DUE)],
        };
        assert_eq!(seen, expect, "{missed:?}");
        // Exactly one run exists afterwards: the new week's, waiting; nothing queued behind.
        assert_eq!(j.run().scheduled_at, Millis(DUE + 7 * D), "{missed:?}");
        assert_eq!(j.phase(), "waiting");
        assert_eq!(j.rec.next_due, Some(Millis(DUE + 14 * D)));
    }
}

#[test]
fn a_catch_up_run_missed_to_the_next_time_is_reported_with_that_reason() {
    let mut j = Job::new(weekly(MissedRun::CatchUp), SUN);
    j.at(DUE);
    j.step(false, None);
    j.at(DUE + 7 * D - 1);
    let early = j.step(false, None);
    assert!(!kinds(&early.events).contains(&"missed"));
    j.at(DUE + 7 * D);
    let a = j.step(false, None);
    assert_eq!(
        a.events[0].kind,
        JobEventKind::Missed {
            reason: "next_run_arrived".into(),
            dropped: 0
        }
    );
    assert_eq!(a.events[0].scheduled_at, Millis(DUE));
}

#[test]
fn downtime_over_several_times_drops_the_older_ones_and_runs_at_most_the_latest_late() {
    // Down from before the first Sunday until after the third: instants at DUE, +7d, +14d.
    let down_until = DUE + 14 * D + 2 * H;
    // catch_up: the latest runs late, once; the two before it are dropped in one event.
    let mut j = Job::new(weekly(MissedRun::CatchUp), SUN);
    j.at(down_until);
    let a = j.step(true, None);
    assert_eq!(
        a.events[0].kind,
        JobEventKind::Missed {
            reason: "downtime".into(),
            dropped: 2
        }
    );
    assert_eq!(a.events.len(), 1, "{a:?}");
    assert_eq!(j.run().scheduled_at, Millis(DUE + 14 * D));
    assert!(a.create.is_some());
    assert_eq!(kinds(&j.made()), vec!["started", "caught_up"]);
    assert_eq!(
        j.rec.next_due,
        Some(Millis(DUE + 21 * D)),
        "no burst of back-to-back runs"
    );
    // skip: all dropped, none run.
    let mut j = Job::new(weekly(MissedRun::Skip), SUN);
    j.at(down_until);
    let a = j.step(true, None);
    assert_eq!(kinds(&a.events), vec!["missed", "missed"]);
    assert!(j.rec.run.is_none() && a.create.is_none());
    // skip, back up within the window of the latest: it runs on time.
    let mut j = Job::new(weekly(MissedRun::Skip), SUN);
    j.at(DUE + 14 * D + 10 * MIN);
    let a = j.step(true, None);
    assert_eq!(
        a.events.len(),
        1,
        "only the two older ones were dropped: {a:?}"
    );
    assert!(a.create.is_some());
}

#[test]
fn a_very_frequent_schedule_after_a_long_outage_is_walked_with_a_bound() {
    let mut j = Job::new(spec("* * * * *", MissedRun::CatchUp), SUN);
    j.at(SUN + 20_000 * MIN + 30_000);
    let a = j.step(true, None);
    assert_eq!(
        a.events[0].kind,
        JobEventKind::Missed {
            reason: "downtime".into(),
            dropped: MAX_WALK
        }
    );
    assert!(
        j.rec.next_due.unwrap().as_u64() > j.now,
        "the next is in the future"
    );
}

#[test]
fn a_run_due_while_the_previous_instance_is_active_is_skipped_logged_and_never_queued() {
    // Hourly, so the next time arrives while the first is still running.
    let mut j = Job::new(spec("0 * * * *", MissedRun::Skip), SUN - 10 * MIN);
    j.at(SUN);
    j.step(true, None);
    j.made();
    let name = j.spec.job_name(Millis(SUN), 1);
    j.at(SUN + H);
    let r = reading(&name, JobRunState::Active);
    let a = j.step(true, Some((SUN + H, &r)));
    assert_eq!(kinds(&a.events), vec!["skipped_overlap"]);
    assert_eq!(a.events[0].scheduled_at, Millis(SUN + H));
    assert_eq!(
        j.run().scheduled_at,
        Millis(SUN),
        "the running instance is untouched"
    );
    assert_eq!(j.rec.next_due, Some(Millis(SUN + 2 * H)));
    // It finishes; the skipped run does not come back, the next time starts normally.
    j.at(SUN + H + 5 * MIN);
    let r = reading(&name, JobRunState::Complete);
    assert_eq!(
        kinds(&j.step(true, Some((SUN + H + 5 * MIN, &r))).events),
        vec!["succeeded"]
    );
    assert!(j.rec.run.is_none());
    j.at(SUN + 2 * H);
    let a = j.step(true, None);
    assert!(a.create.is_some() && a.events.is_empty(), "{a:?}");
    assert_eq!(j.run().scheduled_at, Millis(SUN + 2 * H));
}

#[test]
fn a_run_due_during_backoff_or_while_waiting_is_an_overlap_too() {
    let mut j = Job::new(spec("0 * * * *", MissedRun::CatchUp), SUN - 10 * MIN);
    j.at(SUN);
    j.step(false, None);
    j.at(SUN + H);
    // Waiting (needs not up): it is dropped at the next time by its own expiry, and the new
    // run takes over; never two.
    j.step(false, None);
    assert_eq!(j.run().scheduled_at, Millis(SUN + H));
    // Backoff: a failed Job waiting out its backoff still counts as the instance.
    let mut j = Job::new(spec("0 * * * *", MissedRun::Skip), SUN - 10 * MIN);
    j.at(SUN);
    j.step(true, None);
    j.made();
    let name = j.spec.job_name(Millis(SUN), 1);
    j.at(SUN + 5 * MIN);
    let r = reading(&name, JobRunState::Failed);
    assert_eq!(
        kinds(&j.step(true, Some((SUN + 5 * MIN, &r))).events),
        vec!["retried"]
    );
    j.at(SUN + H);
    let a = j.step(true, None);
    assert!(kinds(&a.events).contains(&"skipped_overlap"), "{a:?}");
}

#[test]
fn a_failed_job_is_retried_after_a_backoff_then_fails_and_releases_what_it_held() {
    let mut j = running_job(MissedRun::CatchUp);
    // The first attempt fails: retried after 5 minutes, the hold kept (the run still exists).
    j.at(DUE + 4 * MIN);
    let r = reading(NAME1, JobRunState::Failed);
    let a = j.step(true, Some((DUE + 4 * MIN, &r)));
    assert_eq!(
        a.events[0].kind,
        JobEventKind::Retry {
            next_attempt: 2,
            after_ms: 5 * MIN,
            reason: "job_failed".into()
        }
    );
    assert_eq!(j.phase(), "backoff");
    j.at(DUE + 9 * MIN - 1);
    assert_eq!(
        j.step(true, Some((DUE + 9 * MIN - 1, &r))),
        Advance::default()
    );
    // The backoff is over: attempt 2, with its own deterministic name; the old failed Job in
    // the reading does not matter.
    j.at(DUE + 9 * MIN);
    let a = j.step(true, Some((DUE + 9 * MIN, &r)));
    assert_eq!(
        a.create.map(|c| (c.name, c.attempt)),
        Some((NAME2.to_string(), 2))
    );
    assert_eq!(kinds(&j.made()), vec!["started"]);
    // The second attempt fails too: the retry limit is reached.
    j.at(DUE + 20 * MIN);
    let r2 = vec![
        JobRunReading {
            name: NAME1.into(),
            state: JobRunState::Failed,
        },
        JobRunReading {
            name: NAME2.into(),
            state: JobRunState::Failed,
        },
    ];
    let a = j.step(true, Some((DUE + 20 * MIN, &r2)));
    assert_eq!(
        a.events[0].kind,
        JobEventKind::Failed {
            attempts: 2,
            reason: "job_failed".into()
        }
    );
    assert!(j.rec.run.is_none(), "released");
    let last = j.rec.last.as_ref().unwrap();
    assert_eq!((last.outcome, last.attempts), (RunOutcome::Failed, 2));
    // The next scheduled time starts a fresh attempt 1.
    j.at(DUE + 7 * D);
    let a = j.step(true, None);
    assert_eq!(a.create.map(|c| c.attempt), Some(1));
}

#[test]
fn zero_retries_fail_at_once_and_backoff_doubles_with_more() {
    let mut s = weekly(MissedRun::Skip);
    s.retries = Some(0);
    let mut j = Job::new(s, SUN);
    j.at(DUE);
    j.step(true, None);
    j.made();
    j.at(DUE + MIN);
    let r = reading(NAME1, JobRunState::Failed);
    assert_eq!(
        kinds(&j.step(true, Some((DUE + MIN, &r))).events),
        vec!["failed"]
    );
    assert!(j.rec.run.is_none());

    let mut s = weekly(MissedRun::Skip);
    s.retries = Some(3);
    s.retry_backoff = Some(Millis(MIN));
    let mut j = Job::new(s, SUN);
    j.at(DUE);
    j.step(true, None);
    j.made();
    let mut waits = vec![];
    for attempt in 1..=3u32 {
        let name = j.spec.job_name(Millis(DUE), attempt);
        let r = reading(&name, JobRunState::Failed);
        let a = j.step(true, Some((j.now, &r)));
        let JobEventKind::Retry { after_ms, .. } = a.events[0].kind else {
            panic!("{a:?}");
        };
        waits.push(after_ms / MIN);
        j.at(j.now + after_ms);
        assert!(j.step(true, Some((j.now, &r))).create.is_some());
        j.made();
        j.at(j.now + 1);
    }
    assert_eq!(waits, vec![1, 2, 4]);
}

#[test]
fn a_restart_mid_run_continues_from_the_persisted_record() {
    let j = running_job(MissedRun::CatchUp);
    let book = JobBook {
        jobs: [(JobId::new("backup"), j.rec.clone())].into(),
    };
    // The process dies and comes back with only what was stored.
    let text = serde_json::to_string(&book).unwrap();
    let back: JobBook = serde_json::from_str(&text).unwrap();
    assert_eq!(back, book);
    let mut j = Job {
        spec: weekly(MissedRun::CatchUp),
        rec: back.jobs[&JobId::new("backup")].clone(),
        now: DUE + 40 * MIN,
    };
    assert_eq!(j.phase(), "running");
    // Until the observer has read again nothing is concluded (a restart has no reading yet).
    assert_eq!(j.step(true, None), Advance::default());
    let r = reading(NAME1, JobRunState::Complete);
    assert_eq!(
        kinds(&j.step(true, Some((j.now, &r))).events),
        vec!["succeeded"]
    );
    assert!(j.rec.run.is_none());
}

#[test]
fn a_job_deleted_externally_fails_the_run_without_a_retry_and_releases_the_hold() {
    let mut j = running_job(MissedRun::CatchUp);
    // A reading from before the Job was made proves nothing.
    j.at(DUE + MIN);
    assert_eq!(j.step(true, Some((DUE, &[]))), Advance::default());
    assert_eq!(j.phase(), "running");
    // A reading taken later that does not list it: it is gone.
    j.at(DUE + 2 * MIN);
    let a = j.step(true, Some((DUE + 2 * MIN, &[])));
    assert_eq!(
        a.events[0].kind,
        JobEventKind::Failed {
            attempts: 1,
            reason: "job_deleted".into()
        }
    );
    assert!(j.rec.run.is_none());
    assert!(j.rec.lingering.is_none());
}

#[test]
fn no_reading_is_never_finished_and_the_deadline_alerts_and_blocks_new_runs_until_it_ends() {
    let mut s = weekly(MissedRun::Skip);
    s.deadline = Some(Millis(2 * H));
    let mut j = Job::new(s, SUN);
    j.at(DUE);
    j.step(true, None);
    j.made();
    // Hours without any reading: still running, nothing released, however long.
    for k in 1..4u64 {
        j.at(DUE + k * 30 * MIN);
        assert_eq!(j.step(true, None), Advance::default(), "{k}");
    }
    assert_eq!(j.phase(), "running");
    // A stale-looking active reading changes nothing either.
    let active = reading(NAME1, JobRunState::Active);
    j.at(DUE + 2 * H - 1);
    assert_eq!(j.step(true, Some((DUE + H, &active))), Advance::default());
    // The deadline: alerted, failed (no retry), the hold released, the Job left alone.
    j.at(DUE + 2 * H);
    let a = j.step(true, Some((DUE + 2 * H, &active)));
    assert_eq!(
        a.events[0].kind,
        JobEventKind::Failed {
            attempts: 1,
            reason: "deadline".into()
        }
    );
    assert!(j.rec.run.is_none());
    assert_eq!(j.rec.lingering.as_deref(), Some(NAME1));
    assert!(a.create.is_none());
    // The next scheduled time while the Job is still active is an overlap, not a second instance.
    j.at(DUE + 7 * D);
    let a = j.step(true, Some((j.now, &active)));
    assert_eq!(kinds(&a.events), vec!["skipped_overlap"]);
    assert!(j.rec.run.is_none() && a.create.is_none());
    assert_eq!(j.rec.lingering.as_deref(), Some(NAME1));
    // A person asking for a run is refused the same way.
    assert!(trigger(&mut j.rec, &j.spec, Millis(j.now)).is_err());
    // Once the API shows it ended, the job is free again.
    let done = reading(NAME1, JobRunState::Complete);
    j.at(DUE + 7 * D + MIN);
    j.step(true, Some((j.now, &done)));
    assert!(j.rec.lingering.is_none());
    assert!(trigger(&mut j.rec, &j.spec, Millis(j.now)).is_ok());
    // An unknown reading never frees it.
}

#[test]
fn an_unknown_reading_never_clears_a_lingering_job() {
    let mut s = weekly(MissedRun::Skip);
    s.deadline = Some(Millis(H));
    let mut j = Job::new(s, SUN);
    j.at(DUE);
    j.step(true, None);
    j.made();
    j.at(DUE + H);
    j.step(true, None);
    assert!(j.rec.lingering.is_some());
    for k in 1..5u64 {
        j.at(DUE + H + k * D);
        j.step(true, None);
        assert!(j.rec.lingering.is_some(), "{k}");
    }
}

#[test]
fn a_manual_trigger_starts_a_run_now_subject_to_the_overlap_rule() {
    let mut j = Job::new(weekly(MissedRun::Skip), SUN);
    j.at(SUN + H);
    let ev = trigger(&mut j.rec, &j.spec, Millis(j.now)).unwrap();
    assert_eq!(ev.kind, JobEventKind::Triggered);
    assert_eq!(j.run().trigger, Trigger::Manual);
    // It goes through the same machine: waiting for the needs, then created.
    let a = j.step(false, None);
    assert!(a.create.is_none());
    j.at(SUN + H + 10 * MIN);
    let a = j.step(true, None);
    assert_eq!(
        a.create.map(|c| c.name),
        Some(j.spec.job_name(Millis(SUN + H), 1))
    );
    // A second request while it is pending or running is refused, and not queued.
    let err = trigger(&mut j.rec, &j.spec, Millis(j.now)).unwrap_err();
    assert_eq!(err.kind, JobEventKind::SkippedOverlap);
    assert_eq!(j.run().scheduled_at, Millis(SUN + H));
    // It does not disturb the schedule.
    assert_eq!(j.rec.next_due, Some(Millis(DUE)));
    // A manual run whose needs never come up is missed after the start window.
    let mut j = Job::new(weekly(MissedRun::CatchUp), SUN);
    j.at(SUN + H);
    trigger(&mut j.rec, &j.spec, Millis(j.now)).unwrap();
    j.step(false, None);
    j.at(SUN + H + 30 * MIN);
    assert_eq!(kinds(&j.step(false, None).events), vec!["missed"]);
}

#[test]
fn failing_to_create_the_job_is_alerted_once_retried_and_bounded_by_the_window() {
    let mut j = Job::new(weekly(MissedRun::Skip), SUN);
    j.at(DUE);
    j.step(true, None);
    let ev = created(
        &mut j.rec,
        &j.spec,
        Millis(j.now),
        Created::Failed("forbidden".into()),
    );
    assert_eq!(kinds(&ev), vec!["create_failed"]);
    assert_eq!(j.phase(), "creating");
    j.at(DUE + 30_000);
    assert!(j.step(true, None).create.is_some(), "asked for again");
    let ev = created(
        &mut j.rec,
        &j.spec,
        Millis(j.now),
        Created::Failed("forbidden".into()),
    );
    assert!(ev.is_empty(), "alerted once per attempt");
    // It starts to work: made, and not counted late.
    j.at(DUE + 60_000);
    j.step(true, None);
    assert_eq!(kinds(&j.made()), vec!["started"]);
    // Never works: the run is missed at the end of the window, releasing the hold.
    let mut j = Job::new(weekly(MissedRun::Skip), SUN);
    j.at(DUE);
    j.step(true, None);
    created(
        &mut j.rec,
        &j.spec,
        Millis(j.now),
        Created::Failed("x".into()),
    );
    j.at(DUE + 30 * MIN);
    let a = j.step(true, None);
    assert_eq!(kinds(&a.events), vec!["missed"]);
    assert!(j.rec.run.is_none() && a.create.is_none());
}

#[test]
fn needs_that_vanish_before_the_job_is_made_send_the_run_back_to_waiting() {
    let mut j = Job::new(weekly(MissedRun::Skip), SUN);
    j.at(DUE);
    j.step(true, None);
    assert_eq!(j.phase(), "creating");
    j.at(DUE + MIN);
    let a = j.step(false, None);
    assert!(a.create.is_none());
    assert_eq!(j.phase(), "waiting");
    j.at(DUE + 2 * MIN);
    assert!(j.step(true, None).create.is_some());
}

#[test]
fn a_retry_whose_needs_do_not_come_back_fails_the_run() {
    let mut j = running_job(MissedRun::Skip);
    j.at(DUE + MIN);
    let r = reading(NAME1, JobRunState::Failed);
    j.step(true, Some((j.now, &r)));
    // The backoff ends but the host is down for the whole window.
    j.at(DUE + 6 * MIN);
    j.step(false, Some((j.now, &r)));
    assert_eq!(j.phase(), "waiting");
    j.at(DUE + 6 * MIN + 30 * MIN);
    let a = j.step(false, Some((j.now, &r)));
    assert_eq!(
        a.events[0].kind,
        JobEventKind::Failed {
            attempts: 1,
            reason: "needs_lost".into()
        }
    );
    assert!(j.rec.run.is_none());
}

#[test]
fn a_changed_schedule_starts_from_now_and_two_jobs_are_independent() {
    let mut j = Job::new(weekly(MissedRun::CatchUp), SUN);
    j.spec.cron = "0 4 * * *".into();
    j.at(SUN + 5 * H);
    let a = j.step(true, None);
    assert!(
        a.events.is_empty() && a.create.is_none(),
        "no run for the past of a new schedule"
    );
    assert_eq!(rfc3339_utc(j.rec.next_due.unwrap()), "2026-03-02T04:00:00Z");
    // Two jobs holding the same resource at once: each has its own record and run.
    let mut a = running_job(MissedRun::Skip);
    let mut b = running_job(MissedRun::Skip);
    a.spec.id = JobId::new("pg");
    b.spec.id = JobId::new("budget");
    let book = JobBook {
        jobs: [
            (JobId::new("pg"), a.rec.clone()),
            (JobId::new("budget"), b.rec.clone()),
        ]
        .into(),
    };
    assert!(book.holds(&JobId::new("pg")) && book.holds(&JobId::new("budget")));
    let r = reading(NAME1, JobRunState::Complete);
    a.at(DUE + MIN);
    a.step(true, Some((a.now, &r)));
    assert!(
        a.rec.run.is_none() && b.rec.run.is_some(),
        "one finishing releases only its own"
    );
}

/// Creation takes time: the tick decided at `DUE`, the call returned at `DUE + 10 s`, and a
/// reading that lacks the Job was received in between, at `DUE + 5 s`.
#[test]
fn a_reading_received_before_the_creation_returned_never_proves_the_job_deleted() {
    let mut j = Job::new(weekly(MissedRun::CatchUp), SUN);
    j.at(DUE);
    assert!(j.step(true, None).create.is_some());
    j.at(DUE + 10_000);
    assert_eq!(kinds(&j.made()), vec!["started"]);
    // The stale reading is the newest one at the next ticks, however many there are.
    for k in 1..=3 {
        j.at(DUE + 10_000 + k * 30_000);
        let a = j.step(true, Some((DUE + 5_000, &[])));
        assert!(a.events.is_empty(), "{k}: {a:?}");
        assert_eq!(j.phase(), "running");
    }
    // A read that was in flight when the Job was made is received just after it.
    j.at(DUE + 60_000);
    assert!(j.step(true, Some((DUE + 25_000, &[]))).events.is_empty());
    assert_eq!(j.phase(), "running");
    // One received after that settles it.
    j.at(DUE + 90_000);
    let a = j.step(true, Some((DUE + 30_001, &[])));
    assert_eq!(kinds(&a.events), vec!["failed"]);
}

#[test]
fn a_creation_that_failed_may_have_made_the_job_so_giving_up_leaves_a_lingering_mark() {
    let mut j = Job::new(weekly(MissedRun::Skip), SUN);
    j.at(DUE);
    j.step(true, None);
    created(
        &mut j.rec,
        &j.spec,
        Millis(DUE + 5_000),
        Created::Failed("timed out".into()),
    );
    j.at(DUE + 30 * MIN);
    let a = j.step(true, None);
    assert_eq!(kinds(&a.events), vec!["missed"]);
    assert!(j.rec.run.is_none());
    assert_eq!(j.rec.lingering.as_deref(), Some(NAME1));
    // The next due run is skipped while the Job may exist.
    let next = DUE + 7 * D;
    j.at(next);
    let r = reading(NAME1, JobRunState::Active);
    assert_eq!(
        kinds(&j.step(true, Some((next - 1, &r))).events),
        vec!["skipped_overlap"]
    );
    // A reading from before the failed try ended proves nothing; a later one that lacks the
    // Job frees the job.
    j.at(DUE + 31 * MIN);
    j.step(true, Some((DUE + 5_000, &[])));
    assert!(
        j.rec.lingering.is_some(),
        "an older reading cannot clear it"
    );
    j.step(true, Some((DUE + 25_000, &[])));
    assert!(
        j.rec.lingering.is_some(),
        "one in flight at the failure neither"
    );
    j.step(true, Some((DUE + 25_001, &[])));
    assert_eq!(j.rec.lingering, None);
    assert_eq!(j.rec.lingering_after, None);
}

#[test]
fn a_creation_that_failed_and_then_lost_its_needs_still_leaves_the_mark_when_the_window_ends() {
    let mut j = Job::new(weekly(MissedRun::Skip), SUN);
    j.at(DUE);
    j.step(true, None);
    created(
        &mut j.rec,
        &j.spec,
        Millis(DUE),
        Created::Failed("timed out".into()),
    );
    j.at(DUE + MIN);
    j.step(false, None);
    assert_eq!(j.phase(), "waiting");
    j.at(DUE + 30 * MIN);
    assert_eq!(kinds(&j.step(false, None).events), vec!["missed"]);
    assert_eq!(j.rec.lingering.as_deref(), Some(NAME1));
}

#[test]
fn a_creation_failure_is_alerted_once_per_attempt_even_across_waiting_and_creating() {
    let mut j = Job::new(weekly(MissedRun::CatchUp), SUN);
    j.at(DUE);
    j.step(true, None);
    let ev = created(
        &mut j.rec,
        &j.spec,
        Millis(j.now),
        Created::Failed("x".into()),
    );
    assert_eq!(kinds(&ev), vec!["create_failed"]);
    // The needs go away and come back: creating again fails again, silently.
    j.at(DUE + MIN);
    j.step(false, None);
    assert_eq!(j.phase(), "waiting");
    j.at(DUE + 2 * MIN);
    assert!(j.step(true, None).create.is_some());
    let ev = created(
        &mut j.rec,
        &j.spec,
        Millis(j.now),
        Created::Failed("x".into()),
    );
    assert!(ev.is_empty(), "{ev:?}");
    // A later attempt is a new alert.
    j.at(DUE + 3 * MIN);
    j.step(true, None);
    assert_eq!(kinds(&j.made()), vec!["started"]);
    let r = reading(NAME1, JobRunState::Failed);
    j.at(DUE + 4 * MIN);
    j.step(true, Some((j.now, &r)));
    j.at(DUE + 10 * MIN);
    assert!(j.step(true, Some((j.now, &r))).create.is_some());
    let ev = created(
        &mut j.rec,
        &j.spec,
        Millis(j.now),
        Created::Failed("y".into()),
    );
    assert_eq!(kinds(&ev), vec!["create_failed"]);
}

#[test]
fn a_reading_in_another_namespace_than_the_live_job_was_made_in_says_nothing_about_it() {
    let mut j = running_job(MissedRun::CatchUp);
    assert_eq!(j.rec.namespace.as_deref(), Some("backups"));
    // The config now names another namespace: the observer reads there, and the Job is not.
    j.spec.cronjob.namespace = "archive".into();
    j.at(DUE + 5 * MIN);
    let a = j.step(true, Some((DUE + 5 * MIN, &[])));
    assert!(a.events.is_empty(), "{a:?}");
    assert_eq!(j.phase(), "running");
    // Nor does a reading finish it, or free a lingering Job.
    let done = reading(NAME1, JobRunState::Complete);
    j.at(DUE + 6 * MIN);
    assert!(j.step(true, Some((j.now, &done))).events.is_empty());
    assert_eq!(j.phase(), "running");
    j.at(DUE + 6 * H);
    assert_eq!(
        kinds(&j.step(true, Some((j.now, &[]))).events),
        vec!["failed"]
    );
    assert_eq!(j.rec.lingering.as_deref(), Some(NAME1));
    j.at(DUE + 7 * H);
    j.step(true, Some((j.now, &[])));
    assert_eq!(j.rec.lingering.as_deref(), Some(NAME1), "still unknown");
    // Back to the namespace it was made in, the reading counts again.
    j.spec.cronjob.namespace = "backups".into();
    j.at(DUE + 8 * H);
    j.step(true, Some((j.now, &[])));
    assert_eq!(j.rec.lingering, None);
}
