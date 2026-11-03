//! The state machine of scheduled jobs: when a run is due, whether it may start, what it
//! waits for, whether it succeeded, must be retried or has failed, and what is dropped.
//!
//! Pure: a job's [`JobRecord`], the config's [`JobSpec`], the time and two readings (are the
//! needs up, what does the Kubernetes API say about the job's Jobs) go in; the new record,
//! events and at most one request to create a Job come out. No clock, no I/O.
//!
//! The rules, which the tests below pin down one by one:
//!
//! - **Due.** The record keeps `next_due`, the next scheduled instant not yet handled. When
//!   the clock reaches it, every instant up to the latest one not after now is handled
//!   together: the earlier ones were missed (flowmanager or the cluster was down) and are
//!   dropped, counted in one `Missed` event, never run back to back. A job seen for the first
//!   time, or whose schedule changed, starts from now: nothing is invented for the past.
//! - **One instance.** A run that is due, or asked for by a person, while the previous
//!   instance is waiting, running or in backoff, or while a Job that outran its deadline may
//!   still be active, is skipped (`SkippedOverlap`, an alert). It is not queued.
//! - **Needs.** A run holds what the job needs from the moment it exists (waiting, running,
//!   backoff) until it ends, however it ends: the service derives the holds from the record,
//!   so a run that ends, is dropped or whose job leaves the config cannot leak one. The Job is
//!   created only while the needs are up.
//! - **Start window and missed runs.** Under `skip`, a run waits for its needs until
//!   `scheduled + start_window`, or until the next scheduled time if that comes first, and is
//!   then dropped as missed; a run only noticed after the window is dropped at once. Under
//!   `catch_up` it waits until the next scheduled time (alerting once at the end of the
//!   window), and runs late, at most once, if the needs come up before then. A run whose needs
//!   never came up is `Missed`, not failed: it uses no retries.
//! - **Retries.** A failed Job is retried after a backoff (doubling), up to `retries` times,
//!   keeping the hold. After the last the run is `Failed` (an alert) and the hold is released.
//!   The next scheduled time starts afresh. A Job that outran its `deadline`, or is gone, is
//!   not retried.
//! - **Readings.** A missing or stale reading of the Jobs is never taken as "finished", and
//!   a Job missing from a reading older than its creation (plus the time a read can be in
//!   flight) proves nothing. A creation that failed may still have made the Job, so a run that
//!   gives up while creating leaves a lingering mark until a later reading shows it is not there.

use sched_model::jobs::JOB_READ_SETTLE;
use sched_model::{
    Config, CreateJob, JobEvent, JobEventKind, JobRecord, JobRunReading, JobRunState, JobSpec,
    LastRun, Lease, Millis, Run, RunOutcome, RunPhase, SchedState, Snapshot, Trigger, host_is_up,
};

/// A fresh reading of a job's Jobs.
#[derive(Debug, Clone, Copy)]
pub struct Reading<'a> {
    /// When the scheduler received it.
    pub at: Millis,
    pub runs: &'a [JobRunReading],
}

/// What one tick tells the machine about one job.
#[derive(Debug, Clone, Copy)]
pub struct Inputs<'a> {
    pub now: Millis,
    pub spec: &'a JobSpec,
    /// Every need of the job is up. Vacuously true for a job that needs nothing.
    pub needs_up: bool,
    /// The job's Jobs as read; `None` when unknown (never read, stale, or failed).
    pub reading: Option<Reading<'a>>,
}

/// The machine's answer for one tick.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Advance {
    pub events: Vec<JobEvent>,
    /// The Job to create now (asked for on every tick until its creation is recorded).
    pub create: Option<CreateJob>,
}

/// How many instants one due check walks over before it jumps to the present, so a very
/// frequent schedule after a long outage cannot make a tick expensive.
const MAX_WALK: u32 = 10_000;

fn event(spec: &JobSpec, now: Millis, scheduled_at: Millis, kind: JobEventKind) -> JobEvent {
    JobEvent {
        job: spec.id.clone(),
        at: now,
        scheduled_at,
        kind,
    }
}

/// The key that says which schedule `next_due` was computed from.
pub fn schedule_key(spec: &JobSpec) -> String {
    format!("{} {}", spec.cron.trim(), spec.timezone())
}

/// Moves job `rec` forward to `i.now`.
pub fn advance(rec: &mut JobRecord, i: &Inputs) -> Advance {
    // The Jobs are read in the namespace the config names now. If the live Job was made in
    // another, the reading cannot show it: it says nothing (neither "gone" nor "finished").
    let moved = rec
        .namespace
        .as_deref()
        .is_some_and(|ns| ns != i.spec.cronjob.namespace);
    let masked = Inputs {
        reading: None,
        ..*i
    };
    let i = if moved { &masked } else { i };
    let (spec, now) = (i.spec, i.now);
    let mut out = Advance::default();

    let key = schedule_key(spec);
    if rec.schedule != key || rec.next_due.is_none() {
        rec.schedule = key;
        rec.next_due = spec.next_after(now).ok();
    }
    clear_lingering(rec, i);
    progress(rec, i, &mut out);

    if let Some(due) = rec.next_due
        && now >= due
    {
        let (latest, next, dropped) = walk(spec, due, now);
        rec.next_due = next;
        if dropped > 0 {
            out.events.push(event(
                spec,
                now,
                due,
                JobEventKind::Missed {
                    reason: "downtime".to_string(),
                    dropped,
                },
            ));
            note_last(
                rec,
                now,
                due,
                RunOutcome::Missed,
                0,
                Some("downtime".to_string()),
            );
        }
        due_run(rec, i, latest, &mut out);
        progress(rec, i, &mut out);
    }

    if let Some(Run {
        scheduled_at,
        attempt,
        phase: RunPhase::Creating { .. },
        ..
    }) = &rec.run
    {
        out.create = Some(CreateJob {
            job: spec.id.clone(),
            name: spec.job_name(*scheduled_at, *attempt),
            scheduled_at: *scheduled_at,
            attempt: *attempt,
        });
    }
    out
}

/// The latest scheduled instant at or before `now` found by stepping from `due` (the first
/// not yet handled), the first after `now`, and how many instants between were stepped over.
fn walk(spec: &JobSpec, due: Millis, now: Millis) -> (Millis, Option<Millis>, u32) {
    let mut latest = due;
    let mut dropped = 0;
    loop {
        match spec.next_after(latest) {
            Ok(n) if n <= now && dropped < MAX_WALK => {
                dropped += 1;
                latest = n;
            }
            Ok(n) if n <= now => return (latest, spec.next_after(now).ok(), dropped),
            Ok(n) => return (latest, Some(n), dropped),
            Err(_) => return (latest, None, dropped),
        }
    }
}

fn due_run(rec: &mut JobRecord, i: &Inputs, scheduled_at: Millis, out: &mut Advance) {
    let (spec, now) = (i.spec, i.now);
    if rec.run.is_some() || rec.lingering.is_some() {
        out.events
            .push(event(spec, now, scheduled_at, JobEventKind::SkippedOverlap));
        return;
    }
    let late = now.saturating_sub(scheduled_at);
    if spec.missed_run == sched_model::MissedRun::Skip && late > spec.start_window() {
        out.events.push(event(
            spec,
            now,
            scheduled_at,
            JobEventKind::Missed {
                reason: "start_window".to_string(),
                dropped: 0,
            },
        ));
        note_last(
            rec,
            now,
            scheduled_at,
            RunOutcome::Missed,
            0,
            Some("start_window".to_string()),
        );
        return;
    }
    rec.run = Some(Run {
        scheduled_at,
        trigger: Trigger::Schedule,
        attempt: 1,
        phase: RunPhase::Waiting {
            since: now,
            late_alerted: false,
        },
        create_failed_at: None,
    });
}

fn note_last(
    rec: &mut JobRecord,
    now: Millis,
    scheduled_at: Millis,
    outcome: RunOutcome,
    attempts: u32,
    reason: Option<String>,
) {
    rec.last = Some(LastRun {
        scheduled_at,
        ended_at: now,
        outcome,
        attempts,
        reason,
    });
    if outcome == RunOutcome::Succeeded {
        rec.last_success_at = Some(now);
    }
}

/// Ends the run: nothing is held any more.
fn end_run(
    rec: &mut JobRecord,
    now: Millis,
    outcome: RunOutcome,
    reason: Option<String>,
) -> Option<Run> {
    let run = rec.run.take()?;
    let attempts = if outcome == RunOutcome::Missed {
        0
    } else {
        run.attempt
    };
    note_last(rec, now, run.scheduled_at, outcome, attempts, reason);
    Some(run)
}

/// A Job that outran its deadline no longer blocks once the API shows it is not active.
fn clear_lingering(rec: &mut JobRecord, i: &Inputs) {
    let (Some(name), Some(r)) = (&rec.lingering, &i.reading) else {
        return;
    };
    // After a creation that failed ambiguously, only a reading received well after that
    // proves the Job is not there.
    if rec
        .lingering_after
        .is_some_and(|t| r.at <= t.saturating_add(JOB_READ_SETTLE))
    {
        return;
    }
    let active = r
        .runs
        .iter()
        .any(|x| &x.name == name && x.state == JobRunState::Active);
    if !active {
        rec.lingering = None;
        rec.lingering_after = None;
    }
}

/// The instant a run still waiting to start gives up, if it ever does.
fn start_expiry(run: &Run, spec: &JobSpec, since: Millis) -> Option<(Millis, &'static str)> {
    let window = spec.start_window();
    if run.trigger == Trigger::Schedule && run.attempt == 1 {
        let next = spec.next_after(run.scheduled_at).ok();
        let window_end = run.scheduled_at.saturating_add(window);
        return match (spec.missed_run, next) {
            (sched_model::MissedRun::CatchUp, next) => next.map(|n| (n, "next_run_arrived")),
            (sched_model::MissedRun::Skip, Some(n)) if n < window_end => {
                Some((n, "next_run_arrived"))
            }
            (sched_model::MissedRun::Skip, _) => Some((window_end, "start_window")),
        };
    }
    Some((since.saturating_add(window), "start_window"))
}

fn progress(rec: &mut JobRecord, i: &Inputs, out: &mut Advance) {
    let (spec, now) = (i.spec, i.now);
    // At most a few hops: backoff over, waiting, needs up, creating.
    for _ in 0..4 {
        let Some(run) = rec.run.clone() else {
            return;
        };
        match run.phase.clone() {
            RunPhase::Backoff { until } => {
                if now < until {
                    return;
                }
                set_phase(
                    rec,
                    RunPhase::Waiting {
                        since: now,
                        late_alerted: false,
                    },
                );
            }
            RunPhase::Waiting { since, .. } if i.needs_up => set_phase(
                rec,
                RunPhase::Creating {
                    since,
                    failures: 0,
                    last_error: None,
                },
            ),
            RunPhase::Waiting {
                since,
                late_alerted,
            } => {
                if expire(rec, &run, i, since, out) {
                    return;
                }
                if !late_alerted
                    && spec.missed_run == sched_model::MissedRun::CatchUp
                    && run.trigger == Trigger::Schedule
                    && run.attempt == 1
                    && now >= run.scheduled_at.saturating_add(spec.start_window())
                {
                    out.events
                        .push(event(spec, now, run.scheduled_at, JobEventKind::StartLate));
                    set_phase(
                        rec,
                        RunPhase::Waiting {
                            since,
                            late_alerted: true,
                        },
                    );
                }
                return;
            }
            RunPhase::Creating { since, .. } => {
                if expire(rec, &run, i, since, out) {
                    return;
                }
                if !i.needs_up {
                    // The needs went away again before the Job was made.
                    set_phase(
                        rec,
                        RunPhase::Waiting {
                            since,
                            late_alerted: false,
                        },
                    );
                }
                return;
            }
            RunPhase::Running { name, created_at } => {
                running(rec, &run, i, &name, created_at, out);
                return;
            }
        }
    }
}

fn set_phase(rec: &mut JobRecord, phase: RunPhase) {
    if let Some(run) = rec.run.as_mut() {
        run.phase = phase;
    }
}

/// Ends a run that has waited too long to start. True if it ended.
fn expire(rec: &mut JobRecord, run: &Run, i: &Inputs, since: Millis, out: &mut Advance) -> bool {
    let (spec, now) = (i.spec, i.now);
    let Some((at, why)) = start_expiry(run, spec, since) else {
        return false;
    };
    if now < at {
        return false;
    }
    if let Some(failed_at) = run.create_failed_at {
        // Creating the Job failed, but a call can fail after the API made the Job: it may
        // exist, so the next run must not start beside it until a reading shows it is not.
        rec.lingering = Some(spec.job_name(run.scheduled_at, run.attempt));
        rec.lingering_after = Some(failed_at);
    }
    if run.attempt == 1 {
        end_run(rec, now, RunOutcome::Missed, Some(why.to_string()));
        out.events.push(event(
            spec,
            now,
            run.scheduled_at,
            JobEventKind::Missed {
                reason: why.to_string(),
                dropped: 0,
            },
        ));
    } else {
        // A retry whose needs did not come back is a failure of the run, not a missed one:
        // an earlier attempt already ran.
        if let Some(r) = rec.run.as_mut() {
            r.attempt -= 1;
        }
        end_run(rec, now, RunOutcome::Failed, Some("needs_lost".to_string()));
        out.events.push(event(
            spec,
            now,
            run.scheduled_at,
            JobEventKind::Failed {
                attempts: run.attempt - 1,
                reason: "needs_lost".to_string(),
            },
        ));
    }
    true
}

fn running(
    rec: &mut JobRecord,
    run: &Run,
    i: &Inputs,
    name: &str,
    created_at: Millis,
    out: &mut Advance,
) {
    let (spec, now) = (i.spec, i.now);
    let seen = i
        .reading
        .and_then(|r| r.runs.iter().find(|x| x.name == name).map(|x| x.state));
    match seen {
        Some(JobRunState::Complete) => {
            end_run(rec, now, RunOutcome::Succeeded, None);
            out.events.push(event(
                spec,
                now,
                run.scheduled_at,
                JobEventKind::Succeeded {
                    attempts: run.attempt,
                },
            ));
            return;
        }
        Some(JobRunState::Failed) => {
            attempt_failed(rec, run, i, "job_failed", out);
            return;
        }
        Some(JobRunState::Active) => {}
        // Absence proves something only in a reading taken after the Job was made. `created_at`
        // is when the creation call returned, and a read in flight at that moment is received
        // after it, so the reading must also be received `JOB_READ_SETTLE` later.
        None if i
            .reading
            .is_some_and(|r| r.at > created_at.saturating_add(JOB_READ_SETTLE)) =>
        {
            end_run(
                rec,
                now,
                RunOutcome::Failed,
                Some("job_deleted".to_string()),
            );
            out.events.push(event(
                spec,
                now,
                run.scheduled_at,
                JobEventKind::Failed {
                    attempts: run.attempt,
                    reason: "job_deleted".to_string(),
                },
            ));
            return;
        }
        None => {}
    }
    if now >= created_at.saturating_add(spec.deadline()) {
        end_run(rec, now, RunOutcome::Failed, Some("deadline".to_string()));
        rec.lingering = Some(name.to_string());
        rec.lingering_after = None;
        out.events.push(event(
            spec,
            now,
            run.scheduled_at,
            JobEventKind::Failed {
                attempts: run.attempt,
                reason: "deadline".to_string(),
            },
        ));
    }
}

fn attempt_failed(rec: &mut JobRecord, run: &Run, i: &Inputs, why: &str, out: &mut Advance) {
    let (spec, now) = (i.spec, i.now);
    if run.attempt <= spec.retries() {
        let next = run.attempt + 1;
        let after = spec.backoff_before(next);
        if let Some(r) = rec.run.as_mut() {
            r.attempt = next;
            r.create_failed_at = None;
            r.phase = RunPhase::Backoff {
                until: now.saturating_add(after),
            };
        }
        out.events.push(event(
            spec,
            now,
            run.scheduled_at,
            JobEventKind::Retry {
                next_attempt: next,
                after_ms: after.as_u64(),
                reason: why.to_string(),
            },
        ));
    } else {
        end_run(rec, now, RunOutcome::Failed, Some(why.to_string()));
        out.events.push(event(
            spec,
            now,
            run.scheduled_at,
            JobEventKind::Failed {
                attempts: run.attempt,
                reason: why.to_string(),
            },
        ));
    }
}

/// What the service reports after trying to create the requested Job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Created {
    /// The Job exists (made now, or already there from an earlier try).
    Made,
    /// It could not be made; the run stays in `Creating` and the next tick asks again.
    Failed(String),
}

/// Records the outcome of creating the Job asked for by [`advance`]. `now` is when the call
/// returned (not when the tick decided to make the Job): a Job exists from then at the latest,
/// and only a reading received well after it can show the Job gone.
pub fn created(rec: &mut JobRecord, spec: &JobSpec, now: Millis, result: Created) -> Vec<JobEvent> {
    let Some(run) = rec.run.clone() else {
        return vec![];
    };
    let RunPhase::Creating {
        since,
        failures,
        last_error: _,
    } = run.phase
    else {
        return vec![];
    };
    let mut events = Vec::new();
    match result {
        Created::Made => {
            let name = spec.job_name(run.scheduled_at, run.attempt);
            rec.namespace = Some(spec.cronjob.namespace.clone());
            set_phase(
                rec,
                RunPhase::Running {
                    name: name.clone(),
                    created_at: now,
                },
            );
            events.push(event(
                spec,
                now,
                run.scheduled_at,
                JobEventKind::Started {
                    attempt: run.attempt,
                    job_name: name,
                },
            ));
            let late = now.saturating_sub(run.scheduled_at);
            if run.trigger == Trigger::Schedule && run.attempt == 1 && late > spec.start_window() {
                events.push(event(
                    spec,
                    now,
                    run.scheduled_at,
                    JobEventKind::CaughtUp {
                        late_by_ms: late.as_u64(),
                    },
                ));
            }
        }
        Created::Failed(error) => {
            // Alerted once per attempt: going back to waiting and creating again (the needs
            // came and went) does not alert again.
            if run.create_failed_at.is_none() {
                events.push(event(
                    spec,
                    now,
                    run.scheduled_at,
                    JobEventKind::CreateFailed {
                        error: error.clone(),
                    },
                ));
            }
            if let Some(r) = rec.run.as_mut() {
                r.create_failed_at = Some(now);
            }
            set_phase(
                rec,
                RunPhase::Creating {
                    since,
                    failures: failures + 1,
                    last_error: Some(error),
                },
            );
        }
    }
    events
}

/// A person asks for a run now. Refused, with the event to report, while the previous
/// instance is pending or running (or may still be): never run beside itself, never queued.
pub fn trigger(rec: &mut JobRecord, spec: &JobSpec, now: Millis) -> Result<JobEvent, JobEvent> {
    if rec.run.is_some() || rec.lingering.is_some() {
        let at = rec.run.as_ref().map_or(now, |r| r.scheduled_at);
        return Err(event(spec, now, at, JobEventKind::SkippedOverlap));
    }
    rec.run = Some(Run {
        scheduled_at: now,
        trigger: Trigger::Manual,
        attempt: 1,
        phase: RunPhase::Waiting {
            since: now,
            late_alerted: false,
        },
        create_failed_at: None,
    });
    Ok(event(spec, now, now, JobEventKind::Triggered))
}

/// Whether every need of `spec` is up as the sequencer sees it: the input `needs_up` of the
/// job machine ([`Inputs`]). A host whose power is failed, or whose shutdown flowmanager gave
/// up on, does not count: the job is missed or alerted per its policy and flowmanager does not
/// try to fix power. A host that is locked out (an active lockout lease in `leases`, on a host
/// with a power section) does not count either, whether it is up or not: nothing schedules a
/// locked-out host, so the job waits as for a host that is not up (its missed-run and
/// catch-up rules apply as usual) and takes no hold beyond the waiting run's own.
pub fn needs_up(
    config: &Config,
    snapshot: &Snapshot,
    state: &SchedState,
    leases: &[Lease],
    spec: &JobSpec,
) -> bool {
    spec.needs.iter().all(|n| {
        let r = n.resource();
        !config.locked_out(leases, r, snapshot.now)
            && host_is_up(config, snapshot, state, r)
            && !state
                .resource(r)
                .is_some_and(|s| s.shutdown.is_some_and(|x| x.stopped_since().is_some()))
    })
}

#[cfg(test)]
mod needs_up_tests;
#[cfg(test)]
mod tests;
