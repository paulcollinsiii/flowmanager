//! The state of the Kubernetes Jobs that flowmanager created for its scheduled jobs, read
//! from the API: complete, failed, active, or no longer there. Read-only: it lists Jobs by
//! label in the namespaces of the configured jobs, nothing else.
//!
//! A list that fails or times out is a failed read (the reading goes stale), never "no Jobs":
//! the scheduler must not take a missing reading for a Job that finished or vanished.

use std::time::Duration;

use k8s_openapi::api::batch::v1::Job;
use kube::api::ListParams;
use kube::{Api, ResourceExt};
use sched_model::jobs::{LABEL_JOB, LABEL_MANAGED_BY, MANAGED_BY_VALUE};
use sched_model::{
    Config, Fidelity, JobId, JobRunReading, JobRunState, KubeWatchParams, ObservedValue, SourceSpec,
};

use crate::kube_watch::{Timing, grow_backoff};
use crate::{Report, ReportTx};

/// How long one list may take before the reading is reported as failed.
pub const READ_TIMEOUT: Duration = Duration::from_secs(20);
/// The newest Jobs kept per scheduled job in a reading.
pub const MAX_RUNS: usize = 10;

/// A configured job and the namespace its Jobs live in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobTarget {
    pub id: JobId,
    pub namespace: String,
}

/// The targets of every job of `config`.
pub fn job_targets(config: &Config) -> Vec<JobTarget> {
    config
        .jobs()
        .iter()
        .map(|j| JobTarget {
            id: j.id.clone(),
            namespace: j.cronjob.namespace.clone(),
        })
        .collect()
}

/// What a Job's status says: `Complete` or `Failed` when that condition is true, otherwise
/// active (pending or running).
pub fn job_state(job: &Job) -> JobRunState {
    let has = |kind: &str| {
        job.status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .is_some_and(|cs| cs.iter().any(|c| c.type_ == kind && c.status == "True"))
    };
    if has("Complete") {
        JobRunState::Complete
    } else if has("Failed") {
        JobRunState::Failed
    } else {
        JobRunState::Active
    }
}

/// One `JobRuns` per target: its Jobs (the label names the job, the namespace must be the
/// target's), newest first, at most [`MAX_RUNS`]. A target with none gets an empty list: the
/// list was complete, so absence is a fact.
pub fn job_values(targets: &[JobTarget], jobs: &[Job]) -> Vec<ObservedValue> {
    targets
        .iter()
        .map(|t| {
            let mut mine: Vec<&Job> = jobs
                .iter()
                .filter(|j| {
                    j.namespace().as_deref() == Some(t.namespace.as_str())
                        && j.labels().get(LABEL_JOB).map(String::as_str) == Some(t.id.as_str())
                        && j.labels().get(LABEL_MANAGED_BY).map(String::as_str)
                            == Some(MANAGED_BY_VALUE)
                })
                .collect();
            mine.sort_by(|a, b| {
                b.metadata
                    .creation_timestamp
                    .cmp(&a.metadata.creation_timestamp)
                    .then_with(|| b.name_any().cmp(&a.name_any()))
            });
            ObservedValue::JobRuns {
                job: t.id.clone(),
                runs: mine
                    .into_iter()
                    .take(MAX_RUNS)
                    .map(|j| JobRunReading {
                        name: j.name_any(),
                        state: job_state(j),
                    })
                    .collect(),
            }
        })
        .collect()
}

async fn read(
    client: &kube::Client,
    targets: &[JobTarget],
    namespaces: &[String],
) -> Result<Vec<ObservedValue>, String> {
    let lp = ListParams::default().labels(&format!("{LABEL_MANAGED_BY}={MANAGED_BY_VALUE}"));
    let mut all = Vec::new();
    for ns in namespaces {
        let api: Api<Job> = Api::namespaced(client.clone(), ns);
        all.extend(
            api.list(&lp)
                .await
                .map_err(|e| format!("listing Jobs in namespace {ns}: {e}"))?
                .items,
        );
    }
    Ok(job_values(targets, &all))
}

/// The readings, or why they could not be read. Bounded by [`READ_TIMEOUT`].
pub async fn read_jobs(
    client: &kube::Client,
    targets: &[JobTarget],
    namespaces: &[String],
) -> Result<Vec<ObservedValue>, String> {
    tokio::time::timeout(READ_TIMEOUT, read(client, targets, namespaces))
        .await
        .map_err(|_| format!("reading Jobs took longer than {}s", READ_TIMEOUT.as_secs()))?
}

/// Reads with `client` every resync until the receiver is gone.
pub async fn run_with_client(
    spec: SourceSpec,
    params: KubeWatchParams,
    targets: Vec<JobTarget>,
    client: kube::Client,
    tx: ReportTx,
) {
    let resync = Duration::from_millis(spec.resync.as_u64().max(1));
    let mut ticker = tokio::time::interval(resync);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        let report = match read_jobs(&client, &targets, &params.job_namespaces).await {
            Ok(values) => Report::Observed {
                source: spec.id.clone(),
                fidelity: Fidelity::Polled,
                values,
            },
            Err(error) => Report::Failed {
                source: spec.id.clone(),
                error: format!("jobs: {error}"),
            },
        };
        if tx.send(report).await.is_err() {
            return;
        }
    }
}

/// As [`run_with_client`], creating the client from the environment; a client that cannot be
/// made is reported as a source failure and retried with a growing wait.
pub async fn run(
    spec: SourceSpec,
    params: KubeWatchParams,
    targets: Vec<JobTarget>,
    tx: ReportTx,
    timing: Timing,
) {
    let mut wait = timing.client_retry;
    let client = loop {
        match kube::Client::try_default().await {
            Ok(c) => break c,
            Err(e) => {
                let failed = Report::Failed {
                    source: spec.id.clone(),
                    error: format!("cannot create the Kubernetes client: {e}"),
                };
                if tx.send(failed).await.is_err() {
                    return;
                }
                tokio::time::sleep(wait).await;
                wait = grow_backoff(wait, timing.backoff_max);
            }
        }
    };
    run_with_client(spec, params, targets, client, tx).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::batch::v1::{JobCondition, JobStatus};
    use std::collections::BTreeMap;

    #[test]
    fn the_settle_time_the_state_machine_waits_covers_a_read_in_flight() {
        // A read taken before a Job was made can be received up to READ_TIMEOUT later.
        assert!(sched_model::jobs::JOB_READ_SETTLE.as_u64() >= READ_TIMEOUT.as_millis() as u64);
    }

    fn job(name: &str, ns: &str, id: Option<&str>, conds: &[(&str, &str)]) -> Job {
        let mut j = Job::default();
        j.metadata.name = Some(name.into());
        j.metadata.namespace = Some(ns.into());
        let mut labels = BTreeMap::new();
        labels.insert(LABEL_MANAGED_BY.to_string(), MANAGED_BY_VALUE.to_string());
        if let Some(id) = id {
            labels.insert(LABEL_JOB.to_string(), id.to_string());
        }
        j.metadata.labels = Some(labels);
        j.status = Some(JobStatus {
            conditions: Some(
                conds
                    .iter()
                    .map(|(t, s)| JobCondition {
                        type_: (*t).into(),
                        status: (*s).into(),
                        ..Default::default()
                    })
                    .collect(),
            ),
            ..Default::default()
        });
        j
    }

    fn target() -> Vec<JobTarget> {
        vec![JobTarget {
            id: "backup".into(),
            namespace: "backups".into(),
        }]
    }

    fn runs(v: &[ObservedValue]) -> Vec<(String, JobRunState)> {
        match &v[0] {
            ObservedValue::JobRuns { runs, .. } => {
                runs.iter().map(|r| (r.name.clone(), r.state)).collect()
            }
            o => panic!("{o:?}"),
        }
    }

    #[test]
    fn a_job_not_marked_as_made_by_flowmanager_is_ignored_even_if_the_selector_let_it_through() {
        // The server-side label selector already filters; this is the second line of defence.
        let mut foreign = job("backup-other", "backups", Some("backup"), &[]);
        foreign
            .metadata
            .labels
            .as_mut()
            .unwrap()
            .insert(LABEL_MANAGED_BY.to_string(), "someone-else".to_string());
        let mut unmarked = job("backup-unmarked", "backups", Some("backup"), &[]);
        unmarked
            .metadata
            .labels
            .as_mut()
            .unwrap()
            .remove(LABEL_MANAGED_BY);
        let ours = job("backup-1-a1", "backups", Some("backup"), &[]);
        let v = job_values(&target(), &[foreign, unmarked, ours]);
        assert_eq!(
            runs(&v),
            vec![("backup-1-a1".to_string(), JobRunState::Active)]
        );
    }

    #[test]
    fn the_conditions_decide_the_state_and_only_a_true_condition_counts() {
        assert_eq!(job_state(&job("a", "n", None, &[])), JobRunState::Active);
        assert_eq!(
            job_state(&job("a", "n", None, &[("Complete", "True")])),
            JobRunState::Complete
        );
        assert_eq!(
            job_state(&job("a", "n", None, &[("Failed", "True")])),
            JobRunState::Failed
        );
        assert_eq!(
            job_state(&job(
                "a",
                "n",
                None,
                &[("Failed", "False"), ("Complete", "Unknown")]
            )),
            JobRunState::Active
        );
        let mut none = job("a", "n", None, &[]);
        none.status = None;
        assert_eq!(job_state(&none), JobRunState::Active);
    }

    #[test]
    fn jobs_are_matched_by_label_and_namespace_and_a_job_with_none_reads_as_empty() {
        let jobs = [
            job(
                "backup-1-a1",
                "backups",
                Some("backup"),
                &[("Complete", "True")],
            ),
            job("other-1-a1", "backups", Some("other"), &[]),
            job("backup-1-a1", "elsewhere", Some("backup"), &[]),
            job("stray", "backups", None, &[]),
        ];
        assert_eq!(
            runs(&job_values(&target(), &jobs)),
            vec![("backup-1-a1".to_string(), JobRunState::Complete)]
        );
        assert!(runs(&job_values(&target(), &[])).is_empty());
    }

    #[test]
    fn the_newest_few_are_kept() {
        let jobs: Vec<Job> = (0..15)
            .map(|i| job(&format!("backup-{i:02}"), "backups", Some("backup"), &[]))
            .collect();
        let r = runs(&job_values(&target(), &jobs));
        assert_eq!(r.len(), MAX_RUNS);
        assert_eq!(
            r[0].0, "backup-14",
            "ties on time fall back to the name, newest first"
        );
    }
}
