//! Scheduled jobs in the service: one step per tick that feeds the pure state machine
//! (`sched_sequencer::jobs`) with the clock, the sequencer's view of the hosts and the
//! reading of the jobs' Kubernetes Jobs, and keeps the holds in the lease book equal to the
//! runs that exist.
//!
//! The holds are derived, never granted on the side: after the machine has moved every job,
//! each run that exists holds each of its needs (a shared hold of holder `job:<id>`), and every
//! hold of that kind without a run is released. A run that ends however it ends, a job removed
//! from the config, or a lost job record therefore cannot leak a hold: the next tick releases
//! it. Holds of other kinds (manual leases) are never touched.
//!
//! Driven by the tick and the backstop rather than a Restate timer: the machine is a pure
//! function of the clock and the readings, a tick already runs on every relevant change and
//! at least every backstop interval (30 s by default), and a timer chain would be a second
//! thing to keep alive and to keep consistent with the durable record. The cost is that a time
//! is noticed up to one backstop interval late.

use std::collections::BTreeSet;

use sched_model::{
    Config, CreateJob, HolderId, JobBook, JobEvent, JobId, LastRun, Lease, LeaseId, LeaseKind,
    Millis, MountsVeto, Run, RunPhase, SchedState, Snapshot, Trigger, host_is_up,
};
use sched_sequencer::jobs::{Inputs, Reading, advance};
use serde::{Deserialize, Serialize};

/// The prefix of the holder of a job's holds.
pub const HOLDER_PREFIX: &str = "job:";

/// The holder of the holds of job `id`.
pub fn holder(id: &JobId) -> HolderId {
    HolderId::new(format!("{HOLDER_PREFIX}{id}"))
}

fn hold_id(id: &JobId, resource: &sched_model::ResourceId) -> LeaseId {
    LeaseId::new(format!("hold-job-{id}-{resource}"))
}

/// What one tick's job step produced.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JobsStep {
    pub book: JobBook,
    pub leases: Vec<Lease>,
    pub events: Vec<JobEvent>,
    /// Jobs to create, in job order.
    pub creates: Vec<CreateJob>,
}

/// Whether every need of `spec` is up as the sequencer sees it. A host whose power is failed,
/// or whose shutdown flowmanager gave up on, does not count: the job is missed or alerted per
/// its policy and flowmanager does not try to fix power.
pub fn needs_up(
    config: &Config,
    snapshot: &Snapshot,
    state: &SchedState,
    spec: &sched_model::JobSpec,
) -> bool {
    spec.needs.iter().all(|n| {
        let r = n.resource();
        host_is_up(config, snapshot, state, r)
            && !state
                .resource(r)
                .is_some_and(|s| matches!(s.mounts_veto, Some(MountsVeto::Stopped { .. })))
    })
}

/// Moves every configured job to `now` and reconciles the holds in `leases`.
pub fn step(
    config: &Config,
    snapshot: &Snapshot,
    state: &SchedState,
    book: &JobBook,
    leases: &[Lease],
    now: Millis,
) -> JobsStep {
    let mut out = JobsStep {
        book: book.clone(),
        leases: leases.to_vec(),
        ..Default::default()
    };
    out.book.jobs.retain(|id, _| config.job(id).is_some());
    let max_age = config.policy().host_reading_stale_after;
    for spec in config.jobs() {
        let reading = snapshot.fresh_job_runs(config, &spec.id, max_age);
        let rec = out.book.jobs.entry(spec.id.clone()).or_default();
        let adv = advance(
            rec,
            &Inputs {
                now,
                spec,
                needs_up: needs_up(config, snapshot, state, spec),
                reading: reading.as_ref().map(|(at, runs)| Reading { at: *at, runs }),
            },
        );
        out.events.extend(adv.events);
        out.creates.extend(adv.create);
    }
    reconcile_holds(config, &out.book, &mut out.leases, now);
    out
}

/// Makes the job holds in `leases` exactly those the runs in `book` call for.
pub fn reconcile_holds(config: &Config, book: &JobBook, leases: &mut Vec<Lease>, now: Millis) {
    let mut wanted: BTreeSet<LeaseId> = BTreeSet::new();
    let mut add = Vec::new();
    for spec in config.jobs() {
        if !book.holds(&spec.id) {
            continue;
        }
        for need in &spec.needs {
            let id = hold_id(&spec.id, need.resource());
            wanted.insert(id.clone());
            if !leases.iter().any(|l| l.id == id) {
                add.push(Lease {
                    id,
                    resource: need.resource().clone(),
                    kind: LeaseKind::SharedHold {
                        holder: holder(&spec.id),
                    },
                    granted_at: now,
                });
            }
        }
    }
    leases.retain(|l| match &l.kind {
        LeaseKind::SharedHold { holder } if holder.as_str().starts_with(HOLDER_PREFIX) => {
            wanted.contains(&l.id)
        }
        _ => true,
    });
    leases.extend(add);
}

/// The run in progress, for the `jobs` handler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSummary {
    pub scheduled_at: Millis,
    pub trigger: Trigger,
    pub attempt: u32,
    pub phase: String,
    /// The Job of the attempt, once created.
    pub job_name: Option<String>,
    /// When a backoff ends.
    pub retry_at: Option<Millis>,
}

impl From<&Run> for RunSummary {
    fn from(r: &Run) -> Self {
        RunSummary {
            scheduled_at: r.scheduled_at,
            trigger: r.trigger,
            attempt: r.attempt,
            phase: r.phase.as_str().to_string(),
            job_name: match &r.phase {
                RunPhase::Running { name, .. } => Some(name.clone()),
                _ => None,
            },
            retry_at: match &r.phase {
                RunPhase::Backoff { until } => Some(*until),
                _ => None,
            },
        }
    }
}

/// One job and its state, as `jobs` lists it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobSummary {
    pub id: JobId,
    pub cron: String,
    pub timezone: String,
    pub missed_run: String,
    pub retries: u32,
    pub next_due: Option<Millis>,
    pub run: Option<RunSummary>,
    /// A Job that outran its deadline and may still be active.
    pub lingering: Option<String>,
    pub last: Option<LastRun>,
    pub last_success_at: Option<Millis>,
}

/// Every configured job with its state (a job never seen has an empty one).
pub fn summaries(config: &Config, book: &JobBook) -> Vec<JobSummary> {
    config
        .jobs()
        .iter()
        .map(|spec| {
            let rec = book.jobs.get(&spec.id).cloned().unwrap_or_default();
            JobSummary {
                id: spec.id.clone(),
                cron: spec.cron.clone(),
                timezone: spec.timezone().to_string(),
                missed_run: spec.missed_run.as_str().to_string(),
                retries: spec.retries(),
                next_due: rec.next_due,
                run: rec.run.as_ref().map(RunSummary::from),
                lingering: rec.lingering,
                last: rec.last,
                last_success_at: rec.last_success_at,
            }
        })
        .collect()
}

#[cfg(test)]
mod scenarios;
