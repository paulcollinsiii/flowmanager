//! Scheduled jobs: what the config says about a job that flowmanager starts on a cron
//! schedule by invoking a Kubernetes CronJob that is kept `suspend: true` as its template.
//!
//! The spec types are plain data and the cron evaluation is pure: [`JobSpec::next_after`]
//! maps an instant to the next scheduled instant, in the job's own timezone, so the state
//! machine that decides what to do with a job can be tested with a plain number as its clock.
//!
//! # Cron and time zones
//!
//! A job's `cron` is the standard five-field crontab (`minute hour day-of-month month
//! day-of-week`, with `0` or `7` for Sunday, names, ranges, lists and steps) and its
//! `timezone` an IANA name (default `UTC`). The expression is evaluated by the `cronexpr`
//! crate with the Vixie rules (day-of-month and day-of-week are a union unless one starts with
//! `*`). Named zones need the host's time zone database (`/usr/share/zoneinfo`); a zone the
//! database does not know is a config error, so a missing database fails closed at load.
//!
//! Daylight saving time follows the crate's wall-clock matching, which the tests below pin
//! down: a time that does not exist (the hour skipped in spring) does not run that day, and
//! a time that exists twice (the hour repeated in autumn) runs on both occurrences. Use `UTC`
//! (or avoid the 01:00 to 03:00 local hours) for a schedule that must run exactly once a day.

use serde::{Deserialize, Serialize};

use crate::ids::{JobId, ResourceId};
use crate::time::Millis;

/// Longest job id. A Job's name is `<id>-<scheduled epoch seconds>-a<attempt>`, and Kubernetes
/// keeps the `job-name` label of its pods to 63 characters.
pub const MAX_JOB_ID_LEN: usize = 40;
/// Default for [`JobSpec::retries`]: one retry after a failed run.
pub const DEFAULT_JOB_RETRIES: u32 = 1;
/// Most retries a job may be given.
pub const MAX_JOB_RETRIES: u32 = 10;
/// Default for [`JobSpec::retry_backoff`].
pub const DEFAULT_RETRY_BACKOFF: Millis = Millis(5 * 60_000);
/// The wait before a retry doubles with each retry up to this.
pub const MAX_RETRY_BACKOFF: Millis = Millis(60 * 60_000);
/// Default for [`JobSpec::start_window`].
pub const DEFAULT_START_WINDOW: Millis = Millis(30 * 60_000);
/// Default for [`JobSpec::deadline`].
pub const DEFAULT_DEADLINE: Millis = Millis(6 * 3_600_000);
/// Longest `start_window`, `deadline` and `retry_backoff` accepted.
pub const MAX_JOB_DURATION: Millis = Millis(7 * 86_400_000);

/// How long after a Job was made a reading still proves nothing about whether it exists: a
/// read of the Jobs can be in flight (up to the observer's read timeout, 20 s) when the Job is
/// made, and is then received after the creation although it was taken before it. Absence is
/// concluded only from a reading received later than the creation plus this.
pub const JOB_READ_SETTLE: Millis = Millis(20_000);

/// The label every Job flowmanager creates carries, with [`MANAGED_BY_VALUE`].
pub const LABEL_MANAGED_BY: &str = "app.kubernetes.io/managed-by";
pub const MANAGED_BY_VALUE: &str = "flowmanager";
/// The label naming the job id on every Job flowmanager creates.
pub const LABEL_JOB: &str = "flowmanager/job";
/// The annotation (value [`INVOKED_BY_VALUE`]) marking a Job as invoked by flowmanager.
pub const ANNOTATION_INVOKED_BY: &str = "invoked-by";
pub const INVOKED_BY_VALUE: &str = "flowmanager";
/// The annotation holding the scheduled time of the run, RFC3339 in UTC.
pub const ANNOTATION_SCHEDULED_AT: &str = "flowmanager/scheduled-at";
/// The annotation `kubectl create job --from=cronjob/<name>` adds (value `manual`).
pub const ANNOTATION_INSTANTIATE: &str = "cronjob.kubernetes.io/instantiate";

/// The Kubernetes CronJob a job invokes: the template holder, `suspend: true`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CronJobRef {
    pub namespace: String,
    pub name: String,
}

/// What a job needs while it runs. A tagged enum so that a need for a model (a GPU mode) can
/// be added later as another variant without changing how existing needs are written.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum JobNeed {
    /// A hold on a shared resource (an NFS host): it is kept up while the job waits for it
    /// and runs, and woken first if it is off and its power is managed.
    Nfs { resource: ResourceId },
}

impl JobNeed {
    /// The shared resource this need holds.
    pub fn resource(&self) -> &ResourceId {
        match self {
            JobNeed::Nfs { resource } => resource,
        }
    }
}

/// What to do with a run that could not be started on time.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissedRun {
    /// Drop it: dropped, logged and alerted (the default).
    #[default]
    Skip,
    /// Run it late, at most once, and only while the next scheduled time has not arrived.
    CatchUp,
}

impl MissedRun {
    pub fn as_str(self) -> &'static str {
        match self {
            MissedRun::Skip => "skip",
            MissedRun::CatchUp => "catch_up",
        }
    }
}

/// One scheduled job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobSpec {
    pub id: JobId,
    /// Five-field crontab, evaluated in `timezone`.
    pub cron: String,
    /// IANA time zone name; default `UTC`.
    #[serde(default)]
    pub timezone: Option<String>,
    /// The CronJob whose `spec.jobTemplate` each run is created from.
    pub cronjob: CronJobRef,
    /// What the job needs while it runs. May be empty.
    #[serde(default)]
    pub needs: Vec<JobNeed>,
    /// Retries after a failed run (0: none). Default [`DEFAULT_JOB_RETRIES`].
    #[serde(default)]
    pub retries: Option<u32>,
    /// The wait before the first retry; it doubles with each retry, up to
    /// [`MAX_RETRY_BACKOFF`]. Default [`DEFAULT_RETRY_BACKOFF`].
    #[serde(default)]
    pub retry_backoff: Option<Millis>,
    /// What to do with a run that missed its time. Default `skip`.
    #[serde(default)]
    pub missed_run: MissedRun,
    /// How long after its scheduled time a run may still start. Under `skip`, a run whose
    /// needs are not up by then, or that flowmanager only notices after it, is dropped; under
    /// `catch_up` an alert is raised then and the run keeps waiting until the next scheduled
    /// time. Default [`DEFAULT_START_WINDOW`].
    #[serde(default)]
    pub start_window: Option<Millis>,
    /// How long a started run may take before it is alerted and counted as failed. The Job
    /// is not deleted. Default [`DEFAULT_DEADLINE`].
    #[serde(default)]
    pub deadline: Option<Millis>,
}

impl JobSpec {
    pub fn retries(&self) -> u32 {
        self.retries.unwrap_or(DEFAULT_JOB_RETRIES)
    }

    pub fn retry_backoff(&self) -> Millis {
        self.retry_backoff.unwrap_or(DEFAULT_RETRY_BACKOFF)
    }

    pub fn start_window(&self) -> Millis {
        self.start_window.unwrap_or(DEFAULT_START_WINDOW)
    }

    pub fn deadline(&self) -> Millis {
        self.deadline.unwrap_or(DEFAULT_DEADLINE)
    }

    /// The time zone name, `UTC` unless set.
    pub fn timezone(&self) -> &str {
        self.timezone.as_deref().unwrap_or("UTC")
    }

    /// The wait before attempt `next_attempt` (2 for the first retry): `retry_backoff`,
    /// doubled for each retry after the first, never above [`MAX_RETRY_BACKOFF`] (or the
    /// configured backoff itself, if that is larger).
    pub fn backoff_before(&self, next_attempt: u32) -> Millis {
        let base = self.retry_backoff();
        let cap = Millis(MAX_RETRY_BACKOFF.as_u64().max(base.as_u64()));
        let doublings = next_attempt.saturating_sub(2).min(20);
        Millis(base.as_u64().saturating_mul(1u64 << doublings)).min(cap)
    }

    /// The deterministic name of the Job of attempt `attempt` (1-based) of the run scheduled
    /// at `scheduled_at`: the same inputs give the same name, so a replayed creation finds the
    /// Job it already made.
    pub fn job_name(&self, scheduled_at: Millis, attempt: u32) -> String {
        format!("{}-{}-a{attempt}", self.id, scheduled_at.as_u64() / 1000)
    }

    fn crontab(&self) -> Result<cronexpr::Crontab, String> {
        if self.cron.split_whitespace().count() != 5 {
            return Err(format!(
                "cron {:?} must have five fields (minute hour day-of-month month day-of-week); the time zone is the separate `timezone` key",
                self.cron
            ));
        }
        let zone = self.timezone();
        if zone.is_empty() || zone.contains(char::is_whitespace) {
            return Err(format!(
                "timezone {zone:?} must be an IANA name without spaces"
            ));
        }
        cronexpr::parse_crontab(&format!("{} {zone}", self.cron.trim()))
            .map_err(|e| format!("cron {:?} in {zone}: {e}", self.cron))
    }

    /// The first scheduled instant after `after` (strictly later), or the reason the
    /// schedule cannot be evaluated: it does not parse, the zone is unknown (or the host has
    /// no time zone database), or nothing matches within four years.
    pub fn next_after(&self, after: Millis) -> Result<Millis, String> {
        let tab = self.crontab()?;
        let ms = i64::try_from(after.as_u64()).map_err(|_| "time out of range".to_string())?;
        let from = cronexpr::MakeTimestamp::from_millisecond(ms).map_err(|e| e.to_string())?;
        let next = tab.find_next(from).map_err(|e| e.to_string())?;
        u64::try_from(next.timestamp().as_millisecond())
            .map(Millis)
            .map_err(|_| "time out of range".to_string())
    }

    /// Why this job's schedule is not valid, if it is not: the expression and the zone parse
    /// and something matches.
    pub fn schedule_problem(&self) -> Option<String> {
        self.next_after(Millis(1_800_000_000_000)).err()
    }
}

/// `at` as an RFC3339 UTC timestamp with second precision, for the scheduled-at annotation.
pub fn rfc3339_utc(at: Millis) -> String {
    let secs = at.as_u64() / 1000;
    let (days, rem) = (secs / 86_400, secs % 86_400);
    // Civil date from days since the epoch (Howard Hinnant's algorithm).
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(cron: &str, tz: Option<&str>) -> JobSpec {
        JobSpec {
            id: JobId::new("backup"),
            cron: cron.into(),
            timezone: tz.map(str::to_string),
            cronjob: CronJobRef {
                namespace: "backups".into(),
                name: "pg".into(),
            },
            needs: vec![],
            retries: None,
            retry_backoff: None,
            missed_run: MissedRun::Skip,
            start_window: None,
            deadline: None,
        }
    }

    /// 2026-03-01T00:00:00Z, a Sunday.
    const SUN: u64 = 1_772_323_200_000;
    const H: u64 = 3_600_000;
    const D: u64 = 24 * H;

    fn zoneinfo(name: &str) -> bool {
        std::path::Path::new("/usr/share/zoneinfo")
            .join(name)
            .exists()
    }

    #[test]
    fn rfc3339_formats_the_instant_in_utc() {
        assert_eq!(rfc3339_utc(Millis(0)), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339_utc(Millis(SUN)), "2026-03-01T00:00:00Z");
        assert_eq!(
            rfc3339_utc(Millis(SUN + 3 * H + 999)),
            "2026-03-01T03:00:00Z"
        );
        // A leap day, and the end of a year.
        assert_eq!(
            rfc3339_utc(Millis(1_709_164_800_000 + 5_000)),
            "2024-02-29T00:00:05Z"
        );
        assert_eq!(
            rfc3339_utc(Millis(1_798_761_599_000)),
            "2026-12-31T23:59:59Z"
        );
    }

    #[test]
    fn the_example_schedule_is_every_sunday_at_three_utc_and_the_next_is_strictly_later() {
        let j = job("0 3 * * 0", None);
        // From midnight Sunday: that morning.
        assert_eq!(j.next_after(Millis(SUN)), Ok(Millis(SUN + 3 * H)));
        // At exactly 03:00 the next is a week on, never the same instant.
        assert_eq!(
            j.next_after(Millis(SUN + 3 * H)),
            Ok(Millis(SUN + 3 * H + 7 * D))
        );
        assert_eq!(
            j.next_after(Millis(SUN + 3 * H - 1)),
            Ok(Millis(SUN + 3 * H))
        );
        // From mid-week.
        assert_eq!(
            j.next_after(Millis(SUN + 2 * D)),
            Ok(Millis(SUN + 3 * H + 7 * D))
        );
        // `7` is Sunday too.
        assert_eq!(
            job("0 3 * * 7", None).next_after(Millis(SUN)),
            Ok(Millis(SUN + 3 * H))
        );
    }

    #[test]
    fn a_zone_shifts_the_instant_and_an_invalid_schedule_says_why() {
        if !zoneinfo("Europe/Berlin") {
            eprintln!("skipped: no time zone database");
            return;
        }
        // 03:00 in Berlin (UTC+1 in winter) is 02:00 UTC.
        assert_eq!(
            job("0 3 * * 0", Some("Europe/Berlin")).next_after(Millis(SUN)),
            Ok(Millis(SUN + 2 * H))
        );
        let e = job("0 3 * * 0", Some("Nowhere/City")).next_after(Millis(SUN));
        assert!(e.is_err(), "{e:?}");
        for bad in [
            "",
            "0 3 * *",
            "0 3 * * 0 UTC",
            "61 3 * * *",
            "0 3 * * 9",
            "0 0 30 2 *",
        ] {
            assert!(job(bad, None).schedule_problem().is_some(), "{bad:?}");
        }
        assert!(
            job("*/15 1-5 * JAN,FEB MON-FRI", None)
                .schedule_problem()
                .is_none()
        );
    }

    #[test]
    fn dst_a_time_in_the_skipped_hour_does_not_run_and_the_next_day_does() {
        if !zoneinfo("America/New_York") {
            eprintln!("skipped: no time zone database");
            return;
        }
        // 2026-03-08: clocks in New York go from 02:00 to 03:00 (EST to EDT).
        let j = job("30 2 * * *", Some("America/New_York"));
        // 2026-03-07T12:00:00Z is 07:00 EST on the 7th. 02:30 EST on the 8th does not exist.
        let from = 1_772_884_800_000;
        let next = j.next_after(Millis(from)).unwrap().as_u64();
        // The first 02:30 after is the 9th (EDT, UTC-4): 2026-03-09T06:30:00Z.
        assert_eq!(rfc3339_utc(Millis(next)), "2026-03-09T06:30:00Z");
        // A time outside the gap is unaffected: 04:30 local on the 8th is 08:30Z.
        let j = job("30 4 * * *", Some("America/New_York"));
        assert_eq!(
            rfc3339_utc(j.next_after(Millis(from)).unwrap()),
            "2026-03-08T08:30:00Z"
        );
    }

    #[test]
    fn dst_a_time_in_the_repeated_hour_runs_at_both_occurrences() {
        if !zoneinfo("America/New_York") {
            eprintln!("skipped: no time zone database");
            return;
        }
        // 2026-11-01: clocks in New York go back from 02:00 EDT to 01:00 EST, so 01:30
        // happens at 05:30Z (EDT) and again at 06:30Z (EST).
        let j = job("30 1 * * *", Some("America/New_York"));
        let from = 1_793_500_000_000; // 2026-11-01T02:26:40Z
        assert_eq!(rfc3339_utc(Millis(from)), "2026-11-01T02:26:40Z");
        let first = j.next_after(Millis(from)).unwrap();
        assert_eq!(rfc3339_utc(first), "2026-11-01T05:30:00Z");
        let second = j.next_after(first).unwrap();
        assert_eq!(rfc3339_utc(second), "2026-11-01T06:30:00Z");
        let third = j.next_after(second).unwrap();
        assert_eq!(rfc3339_utc(third), "2026-11-02T06:30:00Z");
    }

    #[test]
    fn names_are_deterministic_and_backoff_doubles_to_a_cap() {
        let mut j = job("0 3 * * 0", None);
        assert_eq!(j.job_name(Millis(SUN + 3 * H), 1), "backup-1772334000-a1");
        assert_eq!(j.job_name(Millis(SUN + 3 * H), 2), "backup-1772334000-a2");
        assert_eq!(
            j.job_name(Millis(SUN + 3 * H + 999), 1),
            "backup-1772334000-a1"
        );
        j.retry_backoff = Some(Millis(60_000));
        assert_eq!(j.backoff_before(2), Millis(60_000));
        assert_eq!(j.backoff_before(3), Millis(120_000));
        assert_eq!(j.backoff_before(4), Millis(240_000));
        assert_eq!(j.backoff_before(11), MAX_RETRY_BACKOFF);
        j.retry_backoff = Some(Millis(2 * 3_600_000));
        assert_eq!(
            j.backoff_before(2),
            Millis(2 * 3_600_000),
            "a configured wait above the cap is kept"
        );
        assert_eq!(j.backoff_before(5), Millis(2 * 3_600_000));
    }

    #[test]
    fn defaults_are_the_documented_ones() {
        let j = job("0 3 * * 0", None);
        assert_eq!(j.retries(), 1);
        assert_eq!(j.missed_run, MissedRun::Skip);
        assert_eq!(j.timezone(), "UTC");
        assert_eq!(j.start_window(), DEFAULT_START_WINDOW);
        assert_eq!(j.deadline(), DEFAULT_DEADLINE);
        assert_eq!(j.retry_backoff(), DEFAULT_RETRY_BACKOFF);
    }
}
