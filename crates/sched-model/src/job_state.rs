//! The durable state of scheduled jobs and the events their state machine reports. Plain
//! serializable data: the machine itself is `sched_sequencer::jobs`, the service persists the
//! [`JobBook`] and turns [`JobEvent`]s into logs and metrics.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::ids::JobId;
use crate::time::Millis;

/// What started a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Trigger {
    /// The job's cron schedule.
    Schedule,
    /// A person (the `trigger_job` handler).
    Manual,
}

/// Where the current run of a job is. While a run exists it holds what the job needs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunPhase {
    /// Waiting for the needs to come up. The hold is taken (it wakes a managed host).
    Waiting {
        since: Millis,
        /// A catch-up run that is still waiting after its start window has been alerted.
        #[serde(default)]
        late_alerted: bool,
    },
    /// The needs are up: the Job is to be created (asked for on every tick until it is).
    Creating {
        since: Millis,
        /// Failed attempts to create it so far in this phase.
        #[serde(default)]
        failures: u32,
        #[serde(default)]
        last_error: Option<String>,
    },
    /// The Job exists. `created_at` is a time at which it certainly existed: when the creation
    /// call returned, not when the tick decided to make it.
    Running { name: String, created_at: Millis },
    /// A failed attempt is waiting out its backoff; the hold is kept.
    Backoff { until: Millis },
}

impl RunPhase {
    /// The label used in handlers, logs and metrics.
    pub fn as_str(&self) -> &'static str {
        match self {
            RunPhase::Waiting { .. } => "waiting",
            RunPhase::Creating { .. } => "creating",
            RunPhase::Running { .. } => "running",
            RunPhase::Backoff { .. } => "backoff",
        }
    }
}

/// The run in progress. At most one per job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Run {
    /// The scheduled instant it is for (the moment of the request for a manual run).
    pub scheduled_at: Millis,
    pub trigger: Trigger,
    /// The number of the attempt in progress, or of the next one while in backoff (from 1).
    pub attempt: u32,
    pub phase: RunPhase,
    /// When creating the Job of this attempt last failed: alerted once per attempt (even if the
    /// run goes back to waiting and creating again). The Job may exist even so, because the
    /// call can fail after the API created it, so a run that gives up now is not plainly missed.
    #[serde(default)]
    pub create_failed_at: Option<Millis>,
}

/// How a run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunOutcome {
    Succeeded,
    /// The Job failed (after its retries), was deleted, or outran its deadline.
    Failed,
    /// It never started: its needs did not come up in time, or it was dropped.
    Missed,
}

impl RunOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            RunOutcome::Succeeded => "succeeded",
            RunOutcome::Failed => "failed",
            RunOutcome::Missed => "missed",
        }
    }
}

/// The last run that ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LastRun {
    pub scheduled_at: Millis,
    pub ended_at: Millis,
    pub outcome: RunOutcome,
    pub attempts: u32,
    #[serde(default)]
    pub reason: Option<String>,
}

/// Everything durable about one job.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobRecord {
    /// The `cron timezone` that `next_due` was computed from; a changed schedule recomputes it
    /// from now (a run is never invented for the past of a new schedule).
    #[serde(default)]
    pub schedule: String,
    /// The next scheduled instant not yet handled. `None` until the job is first seen.
    #[serde(default)]
    pub next_due: Option<Millis>,
    #[serde(default)]
    pub run: Option<Run>,
    /// The name of a Job that outran its deadline and may still be active. No new run starts
    /// while it is (or while that cannot be told), so a job never runs beside itself.
    #[serde(default)]
    pub lingering: Option<String>,
    /// The lingering Job can only be shown absent by a reading taken after this (a creation
    /// that failed ambiguously: the Job may have been made at any time up to then).
    #[serde(default)]
    pub lingering_after: Option<Millis>,
    /// The namespace of the CronJob the latest Job (running, or lingering) was made in. The
    /// Jobs are read where the config says; a changed namespace makes the reading say nothing
    /// about that Job.
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub last: Option<LastRun>,
    #[serde(default)]
    pub last_success_at: Option<Millis>,
}

/// The state of every job. Absent from the book means never seen.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobBook {
    #[serde(default)]
    pub jobs: BTreeMap<JobId, JobRecord>,
}

impl JobBook {
    /// Whether job `id` has a run that holds what it needs.
    pub fn holds(&self, id: &JobId) -> bool {
        self.jobs.get(id).is_some_and(|r| r.run.is_some())
    }
}

/// The Job a run asks the service to create. Created idempotently: the name is deterministic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateJob {
    pub job: JobId,
    pub name: String,
    pub scheduled_at: Millis,
    pub attempt: u32,
}

/// Something worth a log line, a counter and sometimes an alert.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum JobEventKind {
    /// A person asked for a run now.
    Triggered,
    /// The Job was created.
    Started {
        attempt: u32,
        job_name: String,
    },
    /// A catch-up run was created after its start window.
    CaughtUp {
        late_by_ms: u64,
    },
    /// A catch-up run is still waiting for its needs after its start window (alert).
    StartLate,
    /// An attempt failed and the next waits out its backoff.
    Retry {
        next_attempt: u32,
        after_ms: u64,
        reason: String,
    },
    Succeeded {
        attempts: u32,
    },
    /// The run failed for good (alert); what it held is released.
    Failed {
        attempts: u32,
        reason: String,
    },
    /// Runs that never started (alert). `dropped` counts scheduled times skipped over because
    /// flowmanager (or the cluster) was down, besides the one named by the event's time.
    Missed {
        reason: String,
        dropped: u32,
    },
    /// A run came due, or was asked for, while the previous instance was pending or running
    /// (alert); it is not run and not queued.
    SkippedOverlap,
    /// Creating the Job failed (alert on the first failure of an attempt); it is tried again.
    CreateFailed {
        error: String,
    },
}

impl JobEventKind {
    /// The label of the counter this event adds to (`sched_job_events_total{event}`).
    pub fn counter(&self) -> &'static str {
        match self {
            JobEventKind::Triggered => "triggered",
            JobEventKind::Started { .. } => "started",
            JobEventKind::CaughtUp { .. } => "caught_up",
            JobEventKind::StartLate => "start_late",
            JobEventKind::Retry { .. } => "retried",
            JobEventKind::Succeeded { .. } => "succeeded",
            JobEventKind::Failed { .. } => "failed",
            JobEventKind::Missed { .. } => "missed",
            JobEventKind::SkippedOverlap => "skipped_overlap",
            JobEventKind::CreateFailed { .. } => "create_failed",
        }
    }

    /// Whether a person should hear about it (logged as an alert).
    pub fn is_alert(&self) -> bool {
        matches!(
            self,
            JobEventKind::StartLate
                | JobEventKind::Failed { .. }
                | JobEventKind::Missed { .. }
                | JobEventKind::SkippedOverlap
                | JobEventKind::CreateFailed { .. }
        )
    }
}

/// One event of one job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobEvent {
    pub job: JobId,
    pub at: Millis,
    /// The scheduled instant of the run it concerns.
    pub scheduled_at: Millis,
    #[serde(flatten)]
    pub kind: JobEventKind,
}
