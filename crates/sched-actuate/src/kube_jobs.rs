//! Creating a Job from a CronJob's template, the way `kubectl create job --from=cronjob/<name>`
//! does: the Job gets the template's labels, annotations and spec, an owner reference to the
//! CronJob and the annotation `cronjob.kubernetes.io/instantiate: manual`. flowmanager adds
//! its own marks (the `invoked-by` and scheduled-time annotations, the managed-by and job-id
//! labels). It does not store the definition, and never deletes anything.
//!
//! The Job's name is deterministic, so creation is idempotent: a Job that already exists under
//! that name is success (a replayed or retried creation finds what it made before).

use std::collections::BTreeMap;
use std::time::Duration;

use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::{Api, PostParams};
use sched_model::jobs::{
    ANNOTATION_INSTANTIATE, ANNOTATION_INVOKED_BY, ANNOTATION_SCHEDULED_AT, INVOKED_BY_VALUE,
    LABEL_JOB, LABEL_MANAGED_BY, MANAGED_BY_VALUE, rfc3339_utc,
};
use sched_model::{CronJobRef, JobId, Millis};

/// How long the two API calls may take in all.
pub const TIMEOUT: Duration = Duration::from_secs(15);

/// What creating the Job came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Created {
    /// The Job was made now.
    Made,
    /// A Job of that name already existed (an earlier try made it).
    AlreadyExisted,
}

/// The Job for run `name` of job `job`, scheduled at `scheduled_at`, from `cronjob`'s template.
/// Pure. Fails if the CronJob has no template spec or identity to own the Job.
pub fn build_job(
    cronjob: &CronJob,
    job: &JobId,
    name: &str,
    scheduled_at: Millis,
) -> Result<Job, String> {
    let template = &cronjob.spec.job_template;
    let spec = template
        .spec
        .clone()
        .ok_or("the CronJob's jobTemplate has no spec")?;
    let cj_name = cronjob
        .metadata
        .name
        .clone()
        .ok_or("the CronJob has no name")?;
    let uid = cronjob
        .metadata
        .uid
        .clone()
        .ok_or("the CronJob has no uid")?;
    let mut labels: BTreeMap<String, String> = template
        .metadata
        .as_ref()
        .and_then(|m| m.labels.clone())
        .unwrap_or_default();
    labels.insert(LABEL_MANAGED_BY.into(), MANAGED_BY_VALUE.into());
    labels.insert(LABEL_JOB.into(), job.as_str().into());
    let mut annotations: BTreeMap<String, String> = template
        .metadata
        .as_ref()
        .and_then(|m| m.annotations.clone())
        .unwrap_or_default();
    annotations.insert(ANNOTATION_INSTANTIATE.into(), "manual".into());
    annotations.insert(ANNOTATION_INVOKED_BY.into(), INVOKED_BY_VALUE.into());
    annotations.insert(ANNOTATION_SCHEDULED_AT.into(), rfc3339_utc(scheduled_at));
    let mut j = Job::default();
    j.metadata.name = Some(name.to_string());
    j.metadata.namespace = cronjob.metadata.namespace.clone();
    j.metadata.labels = Some(labels);
    j.metadata.annotations = Some(annotations);
    j.metadata.owner_references = Some(vec![OwnerReference {
        api_version: "batch/v1".into(),
        kind: "CronJob".into(),
        name: cj_name,
        uid,
        controller: Some(true),
        block_owner_deletion: None,
    }]);
    j.spec = Some(spec);
    Ok(j)
}

async fn create_inner(
    client: &kube::Client,
    cronjob: &CronJobRef,
    job: &JobId,
    name: &str,
    scheduled_at: Millis,
) -> Result<Created, String> {
    let cjs: Api<CronJob> = Api::namespaced(client.clone(), &cronjob.namespace);
    let cj = cjs.get(&cronjob.name).await.map_err(|e| {
        format!(
            "reading CronJob {}/{}: {e}",
            cronjob.namespace, cronjob.name
        )
    })?;
    let built = build_job(&cj, job, name, scheduled_at)?;
    let jobs: Api<Job> = Api::namespaced(client.clone(), &cronjob.namespace);
    match jobs.create(&PostParams::default(), &built).await {
        Ok(_) => Ok(Created::Made),
        Err(kube::Error::Api(status)) if status.code == 409 => Ok(Created::AlreadyExisted),
        Err(e) => Err(format!("creating Job {}/{name}: {e}", cronjob.namespace)),
    }
}

/// Creates the Job `name`, or finds it already there. Bounded by [`TIMEOUT`]; a failure is a
/// value for the caller to journal.
pub async fn create_from_cronjob(
    client: &kube::Client,
    cronjob: &CronJobRef,
    job: &JobId,
    name: &str,
    scheduled_at: Millis,
) -> Result<Created, String> {
    tokio::time::timeout(
        TIMEOUT,
        create_inner(client, cronjob, job, name, scheduled_at),
    )
    .await
    .map_err(|_| {
        format!(
            "creating Job {name} took longer than {}s",
            TIMEOUT.as_secs()
        )
    })?
}

#[cfg(test)]
mod tests {
    use super::*;
    use sched_observe::testserver::{Reply, TestServer};

    const CRONJOB: &str = r#"{"apiVersion":"batch/v1","kind":"CronJob","metadata":{"name":"pg","namespace":"backups","uid":"u-1"},"spec":{"schedule":"0 0 1 1 *","suspend":true,"jobTemplate":{"metadata":{"labels":{"app":"pg-backup"},"annotations":{"note":"x"}},"spec":{"backoffLimit":2,"template":{"spec":{"restartPolicy":"Never","containers":[{"name":"dump","image":"pg:1"}]}}}}}}"#;

    fn cj() -> CronJob {
        serde_json::from_str(CRONJOB).unwrap()
    }

    fn target() -> CronJobRef {
        CronJobRef {
            namespace: "backups".into(),
            name: "pg".into(),
        }
    }

    #[test]
    fn the_job_copies_the_template_and_carries_the_marks_and_the_owner() {
        let j = build_job(
            &cj(),
            &"backup".into(),
            "backup-1772334000-a1",
            Millis(1_772_334_000_000),
        )
        .unwrap();
        assert_eq!(j.metadata.name.as_deref(), Some("backup-1772334000-a1"));
        assert_eq!(j.metadata.namespace.as_deref(), Some("backups"));
        let labels = j.metadata.labels.unwrap();
        assert_eq!(labels["app"], "pg-backup", "the template's label is kept");
        assert_eq!(labels["app.kubernetes.io/managed-by"], "flowmanager");
        assert_eq!(labels["flowmanager/job"], "backup");
        let a = j.metadata.annotations.unwrap();
        assert_eq!(a["note"], "x");
        assert_eq!(a["cronjob.kubernetes.io/instantiate"], "manual");
        assert_eq!(a["invoked-by"], "flowmanager");
        assert_eq!(a["flowmanager/scheduled-at"], "2026-03-01T03:00:00Z");
        let o = &j.metadata.owner_references.unwrap()[0];
        assert_eq!(
            (o.kind.as_str(), o.name.as_str(), o.uid.as_str()),
            ("CronJob", "pg", "u-1")
        );
        assert_eq!(j.spec.unwrap().backoff_limit, Some(2));
    }

    #[test]
    fn a_cronjob_without_a_template_or_identity_is_refused() {
        let mut c = cj();
        c.spec.job_template.spec = None;
        assert!(build_job(&c, &"b".into(), "n", Millis(0)).is_err());
        let mut c = cj();
        c.metadata.uid = None;
        assert!(build_job(&c, &"b".into(), "n", Millis(0)).is_err());
    }

    fn client(server: &TestServer) -> kube::Client {
        kube::Client::try_from(kube::Config::new(server.url("").parse().unwrap())).unwrap()
    }

    #[tokio::test]
    async fn creation_is_idempotent_an_existing_job_is_success_and_errors_are_values() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU16, Ordering};
        let server = TestServer::start().await;
        server.set_dynamic("/apis/batch/v1/namespaces/backups/cronjobs/pg", |_| {
            Reply::json(CRONJOB)
        });
        // What the Job collection answers to a create: 200, then 409, then 403.
        let mode = Arc::new(AtomicU16::new(200));
        let m = mode.clone();
        server.set_dynamic("/apis/batch/v1/namespaces/backups/jobs", move |_| {
            match m.load(Ordering::SeqCst) {
                200 => Reply::json(r#"{"apiVersion":"batch/v1","kind":"Job","metadata":{"name":"n","namespace":"backups"}}"#),
                409 => Reply {
                    status: 409,
                    ..Reply::json(r#"{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"AlreadyExists","code":409,"message":"exists"}"#)
                },
                other => Reply::status(other),
            }
        });
        let c = client(&server);
        let made = create_from_cronjob(&c, &target(), &"backup".into(), "n", Millis(0)).await;
        assert_eq!(made, Ok(Created::Made));
        assert_eq!(server.methods().last().map(String::as_str), Some("POST"));
        // The same name again: the API says 409, which is success.
        mode.store(409, Ordering::SeqCst);
        let again = create_from_cronjob(&c, &target(), &"backup".into(), "n", Millis(0)).await;
        assert_eq!(again, Ok(Created::AlreadyExisted));
        // No such CronJob.
        let missing = CronJobRef {
            name: "gone".into(),
            ..target()
        };
        let e = create_from_cronjob(&c, &missing, &"backup".into(), "n", Millis(0))
            .await
            .unwrap_err();
        assert!(e.contains("reading CronJob backups/gone"), "{e}");
        // The API refuses (RBAC, say).
        mode.store(403, Ordering::SeqCst);
        let e = create_from_cronjob(&c, &target(), &"backup".into(), "n", Millis(0))
            .await
            .unwrap_err();
        assert!(e.contains("creating Job backups/n"), "{e}");
    }
}
