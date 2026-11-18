# Scheduled jobs

A scheduled job starts a Kubernetes Job on a cron schedule, after taking what it needs.
Terms are in [concepts.md](../concepts.md#scheduled-jobs).

## `jobs[]`

| Key | Default | Meaning and limits |
| --- | --- | --- |
| `id` | required | Unique. Lowercase letters, digits and `-`, starting and ending with a letter or digit, at most 40 characters: it becomes part of each Job's name |
| `cron` | required | A five-field crontab: minute, hour, day of month, month, day of week (`0` or `7` is Sunday; names, ranges, lists and steps allowed). It must match some time within four years |
| `timezone` | `UTC` | An IANA zone name, looked up in the host's time zone database (the container image carries one) |
| `cronjob` | required | `{ namespace, name }` of the CronJob whose template each run uses. Both are Kubernetes names (lowercase letters, digits, `-` and `.`) |
| `needs` | `[]` | A list of `{ type: nfs, resource: <id> }`, the only type. The resource must be `shared`, and listed once |
| `retries` | `1` | Retries after a failed run. `0` to `10` |
| `retry_backoff` | `5m` | The wait before the first retry; it doubles with each retry, up to `1h` (or the configured value, if larger). Above `0`, at most `7d` |
| `missed_run` | `skip` | What to do with a run that could not start on time: [`skip` or `catch_up`](#missed-runs) |
| `start_window` | `30m` | How long after its scheduled time a run may still start. Above `0`, at most `7d` |
| `deadline` | `6h` | How long a started run may take before it is alerted and counted as failed; the Job is not deleted. Above `0`, at most `7d` |

A `needs` entry is a shared hold while the run waits and runs ([concepts](../concepts.md#scheduled-jobs)).
A run whose needs never came up is a missed run, not a failed attempt, so it uses no retry.

Daylight saving follows wall-clock matching: a time inside the skipped spring hour does not
run that day, and one inside the repeated autumn hour runs on both occurrences. Use `UTC` for
a schedule that must run exactly once.

## Missed runs

| | `skip` | `catch_up` |
| --- | --- | --- |
| Needs not up within `start_window` of the scheduled time | The run is dropped, logged and alerted. So is a run that flowmanager only notices after the window | An alert is raised at the end of the window and the run keeps waiting for its needs |
| How late it may run | Never late | At most once, and only while the next scheduled time has not arrived; then it is dropped |

## The CronJob

The CronJob is only a template. flowmanager copies its `spec.jobTemplate` the way
`kubectl create job --from=cronjob/<name>` does: the Job gets the template's labels,
annotations and spec, an owner reference to the CronJob and the annotation
`cronjob.kubernetes.io/instantiate: manual`. It adds the labels
`app.kubernetes.io/managed-by: flowmanager` and `flowmanager/job: <id>`, and the annotations
`invoked-by: flowmanager` and `flowmanager/scheduled-at` (RFC 3339, UTC). The Job is named
`<id>-<scheduled epoch seconds>-a<attempt>`. flowmanager never deletes a Job or a CronJob
(`crates/sched-actuate/src/kube_jobs.rs`).

Write the CronJob in your own manifests with `suspend: true`, so Kubernetes does not also run
it on its own schedule; flowmanager does not check the field. The CronJob must have a
`jobTemplate` spec. Put preconditions in the Job itself, for example in init containers.

Two other parts of the config must agree:

- A `kubernetes_watch` source observing `jobs` must exist, and its `job_namespaces` must
  include the CronJob's namespace ([sources.md](sources.md#kubernetes_watch)).
- The service account needs `get` on the CronJob and `create` on Jobs in its namespace, and
  `list` on Jobs for the source ([install.md](../install.md#6-rbac)).

## Example

```yaml
jobs:
  - id: backup
    cron: "0 3 * * 0"            # Sundays at 03:00
    timezone: UTC
    cronjob: { namespace: backups, name: postgres-backup }
    needs:
      - { type: nfs, resource: basestar-nfs }
    retries: 1
    retry_backoff: 5m
    missed_run: catch_up
    start_window: 30m
    deadline: 6h
```
