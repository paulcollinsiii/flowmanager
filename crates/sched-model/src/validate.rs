//! Whole-config validation. Problems are collected, not short-circuited, so one load
//! reports everything that is wrong.

use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::config::{Capability, Config, MAX_LEASE_DURATION, ResourceKind, SourceKind};
use crate::time::Millis;

/// One validation failure: where it is and what is wrong.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Problem {
    pub path: String,
    pub message: String,
}

impl fmt::Display for Problem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.path, self.message)
    }
}

pub(crate) struct Problems(pub Vec<Problem>);

impl Problems {
    pub(crate) fn add(&mut self, path: impl Into<String>, message: impl Into<String>) {
        self.0.push(Problem {
            path: path.into(),
            message: message.into(),
        });
    }
}

fn check_ids<'a>(p: &mut Problems, section: &str, ids: impl Iterator<Item = &'a str>) {
    let mut seen = HashSet::new();
    for id in ids {
        if id.trim().is_empty() {
            p.add(section, "entry has an empty id");
        } else if id != id.trim() {
            p.add(
                format!("{section}[{id:?}]"),
                "id has leading or trailing whitespace",
            );
        } else if !seen.insert(id) {
            p.add(format!("{section}[{id}]"), format!("duplicate id {id:?}"));
        }
    }
}

/// Longest model or class id. A deliberately conservative rule, not a Kubernetes
/// requirement: the scaler builds a metric name from an id (`flowmanager-model-<id>`), and
/// Kubernetes validates an external metric name only as a path segment (no length limit),
/// while the real 63 character limit of KEDA applies to ScaledObject and HPA names. The rule
/// keeps ids usable in resource-style names (ScaledObject names, labels, URLs).
pub(crate) const MAX_SCALER_ID_LEN: usize = 50;

/// Why `id` cannot name a model or a class, or `None` if it can: lowercase letters, digits
/// and `-`, starting and ending with a letter or digit, at most [`MAX_SCALER_ID_LEN`]
/// characters. A free-form id would end up in a metric name and in ScaledObject names.
pub(crate) fn scaler_id_problem(id: &str) -> Option<String> {
    let alnum = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit();
    let rule = "an id may use only lowercase letters, digits and '-', must start and end with a letter or digit, and be at most 50 characters (a conservative rule that keeps ids usable in resource-style names and in the scaler's metric names; Kubernetes itself would accept more)";
    if id.is_empty() || id != id.trim() {
        return None; // reported by check_ids
    }
    if let Some(bad) = id.chars().find(|c| !alnum(*c) && *c != '-') {
        return Some(format!("id {id:?} has the character {bad:?}; {rule}"));
    }
    if !id.starts_with(alnum) || !id.ends_with(alnum) {
        return Some(format!(
            "id {id:?} must start and end with a letter or digit; {rule}"
        ));
    }
    if id.chars().count() > MAX_SCALER_ID_LEN {
        return Some(format!(
            "id {id:?} is {} characters, more than {MAX_SCALER_ID_LEN}; {rule}",
            id.chars().count()
        ));
    }
    None
}

fn check_scaler_ids<'a>(p: &mut Problems, section: &str, ids: impl Iterator<Item = &'a str>) {
    for id in ids {
        if let Some(why) = scaler_id_problem(id) {
            p.add(format!("{section}[{id}]"), why);
        }
    }
}

pub(crate) fn validate(c: &Config) -> Vec<Problem> {
    let mut p = Problems(Vec::new());
    policy(c, &mut p);
    check_ids(
        &mut p,
        "resources",
        c.resources().iter().map(|r| r.id.as_str()),
    );
    check_ids(&mut p, "models", c.models().iter().map(|m| m.id.as_str()));
    check_ids(&mut p, "classes", c.classes().iter().map(|k| k.id.as_str()));
    check_ids(&mut p, "sources", c.sources().iter().map(|s| s.id.as_str()));
    check_scaler_ids(&mut p, "models", c.models().iter().map(|m| m.id.as_str()));
    check_scaler_ids(&mut p, "classes", c.classes().iter().map(|k| k.id.as_str()));
    models(c, &mut p);
    classes(c, &mut p);
    sources(c, &mut p);
    jobs(c, &mut p);
    p.0
}

/// Why `id` cannot name a scheduled job, or `None` if it can: lowercase letters, digits and
/// `-`, starting and ending with a letter or digit, at most [`MAX_JOB_ID_LEN`] characters. It
/// becomes part of a Kubernetes Job name and a label value.
fn job_id_problem(id: &str) -> Option<String> {
    use crate::jobs::MAX_JOB_ID_LEN;
    let alnum = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit();
    let rule = format!(
        "a job id may use only lowercase letters, digits and '-', must start and end with a letter or digit, and be at most {MAX_JOB_ID_LEN} characters (it becomes part of the Job's name)"
    );
    if id.is_empty() || id != id.trim() {
        return None; // reported by check_ids
    }
    if let Some(bad) = id.chars().find(|c| !alnum(*c) && *c != '-') {
        return Some(format!("id {id:?} has the character {bad:?}; {rule}"));
    }
    if !id.starts_with(alnum) || !id.ends_with(alnum) {
        return Some(format!(
            "id {id:?} must start and end with a letter or digit; {rule}"
        ));
    }
    if id.chars().count() > MAX_JOB_ID_LEN {
        return Some(format!(
            "id {id:?} is {} characters, more than {MAX_JOB_ID_LEN}; {rule}",
            id.chars().count()
        ));
    }
    None
}

/// Why `name` cannot be a Kubernetes namespace or CronJob name (a DNS-1123 subdomain: at most
/// 253 characters of lowercase letters, digits, '-' and '.', starting and ending with a
/// letter or digit), or `None`.
fn kube_name_problem(what: &str, name: &str) -> Option<String> {
    let alnum = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit();
    let ok = !name.is_empty()
        && name.len() <= 253
        && name.chars().all(|c| alnum(c) || c == '-' || c == '.')
        && name.starts_with(alnum)
        && name.ends_with(alnum);
    (!ok).then(|| {
        format!("{what} {name:?} must be a Kubernetes name: lowercase letters, digits, '-' and '.', starting and ending with a letter or digit")
    })
}

fn jobs(c: &Config, p: &mut Problems) {
    use crate::jobs::{JobNeed, MAX_JOB_DURATION, MAX_JOB_RETRIES};
    check_ids(p, "jobs", c.jobs().iter().map(|j| j.id.as_str()));
    let sources_with_jobs: Vec<_> = c
        .sources()
        .iter()
        .filter(|s| s.observe.contains(&Capability::Jobs))
        .collect();
    if sources_with_jobs.len() > 1 {
        p.add(
            "sources",
            format!(
                "more than one source declares the jobs capability ({}); one reads the state of every scheduled job's Kubernetes Jobs",
                sources_with_jobs
                    .iter()
                    .map(|s| s.id.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        );
    }
    for s in &sources_with_jobs {
        if s.observe.len() > 1 {
            p.add(
                format!("sources[{}]", s.id),
                "a source that observes jobs must observe nothing else (give pods, reachability and mounts their own source)",
            );
        }
    }
    // The namespaces the jobs source lists; a job whose CronJob is elsewhere could never be
    // observed, so its hold would never be released on completion.
    let listed: Option<Vec<String>> = sources_with_jobs
        .first()
        .and_then(|s| s.kube_watch_params().ok().flatten())
        .map(|k| k.job_namespaces);
    if !c.jobs().is_empty() && sources_with_jobs.is_empty() {
        p.add(
            "jobs",
            "scheduled jobs are configured but no source declares the jobs capability (observe: [jobs] on the kubernetes_watch source, with job_namespaces), so their Jobs could never be observed",
        );
    }
    for j in c.jobs() {
        let at = format!("jobs[{}]", j.id);
        if let Some(why) = job_id_problem(j.id.as_str()) {
            p.add(&at, why);
        }
        if let Some(why) = j.schedule_problem() {
            p.add(format!("{at}.cron"), why);
        }
        for (what, value) in [
            ("namespace", &j.cronjob.namespace),
            ("name", &j.cronjob.name),
        ] {
            if let Some(why) = kube_name_problem(&format!("cronjob {what}"), value) {
                p.add(format!("{at}.cronjob.{what}"), why);
            }
        }
        if let Some(listed) = &listed
            && !listed.contains(&j.cronjob.namespace)
        {
            p.add(
                format!("{at}.cronjob.namespace"),
                format!(
                    "namespace {:?} is not in the jobs source's job_namespaces, so its Jobs would never be listed",
                    j.cronjob.namespace
                ),
            );
        }
        let mut seen = HashSet::new();
        for (i, need) in j.needs.iter().enumerate() {
            let at = format!("{at}.needs[{i}]");
            let JobNeed::Nfs { resource } = need;
            match c.resource(resource) {
                None => p.add(&at, format!("unknown resource \"{resource}\"")),
                Some(r) if r.kind != ResourceKind::Shared => p.add(
                    &at,
                    format!(
                        "resource \"{resource}\" is not a shared resource; an nfs need is a hold on a shared resource"
                    ),
                ),
                Some(_) => {}
            }
            if !seen.insert(resource) {
                p.add(&at, format!("resource \"{resource}\" is needed twice"));
            }
        }
        if j.retries() > MAX_JOB_RETRIES {
            p.add(
                format!("{at}.retries"),
                format!(
                    "{} is more than the largest allowed, {MAX_JOB_RETRIES}",
                    j.retries()
                ),
            );
        }
        for (name, value) in [
            ("retry_backoff", j.retry_backoff()),
            ("start_window", j.start_window()),
            ("deadline", j.deadline()),
        ] {
            if value == Millis::ZERO || value > MAX_JOB_DURATION {
                p.add(
                    format!("{at}.{name}"),
                    format!("{value} must be above zero and at most {MAX_JOB_DURATION} (7 days)"),
                );
            }
        }
    }
}

fn policy(c: &Config, p: &mut Problems) {
    let pol = c.policy();
    if pol.strategy.trim().is_empty() {
        p.add("policy.strategy", "must not be empty");
    }
    if pol.min_dwell > pol.max_dwell {
        p.add(
            "policy",
            format!(
                "min_dwell ({}) is greater than max_dwell ({})",
                pol.min_dwell, pol.max_dwell
            ),
        );
    }
    let positive = [
        ("max_dwell", pol.max_dwell),
        ("age_threshold", pol.age_threshold),
        ("queue_reading_stale_after", pol.queue_reading_stale_after),
        ("host_reading_stale_after", pol.host_reading_stale_after),
        ("interactive_ttl", pol.interactive_ttl),
        ("drain_timeout", pol.drain_timeout),
    ];
    for (name, value) in [
        ("interactive_ttl", pol.interactive_ttl),
        ("drain_timeout", pol.drain_timeout),
    ]
    .into_iter()
    .chain(pol.lockout_timeout.map(|t| ("lockout_timeout", t)))
    {
        if value > MAX_LEASE_DURATION {
            p.add(
                format!("policy.{name}"),
                format!("{value} exceeds the maximum of {MAX_LEASE_DURATION} (1 day)"),
            );
        }
    }
    if pol.lockout_timeout == Some(Millis::ZERO) {
        p.add("policy.lockout_timeout", "must be greater than zero");
    }
    for (name, value) in positive {
        if value == Millis::ZERO {
            p.add(format!("policy.{name}"), "must be greater than zero");
        }
    }
    if pol.host_reading_stale_after > pol.queue_reading_stale_after {
        p.add(
            "policy",
            format!(
                "host_reading_stale_after ({}) is greater than queue_reading_stale_after ({}); safety-critical data must be at least as fresh as scheduling inputs",
                pol.host_reading_stale_after, pol.queue_reading_stale_after
            ),
        );
    }
}

fn models(c: &Config, p: &mut Problems) {
    let mut owners: std::collections::HashMap<&str, &crate::ids::ModelId> = Default::default();
    for m in c.models() {
        let at = format!("models[{}]", m.id);
        match c.resource(&m.resource) {
            None => p.add(
                &at,
                format!("references unknown resource {:?}", m.resource.as_str()),
            ),
            Some(r) if r.kind != ResourceKind::Exclusive => p.add(
                &at,
                format!(
                    "resource {:?} is not exclusive; only exclusive resources have models",
                    m.resource.as_str()
                ),
            ),
            Some(_) => {}
        }
        if m.workloads.is_empty() {
            p.add(&at, "must list at least one workload");
        }
        if m.workloads.iter().any(|w| w.trim().is_empty()) {
            p.add(&at, "workload names must not be empty");
        }
        if m.load_time == Millis::ZERO {
            p.add(
                &at,
                "load_time must be greater than zero; it is the cost of switching to this model",
            );
        }
        for w in &m.workloads {
            match owners.get(w.as_str()) {
                Some(prev) if **prev == m.id => {
                    p.add(&at, format!("workload {w:?} is listed more than once"))
                }
                Some(prev) => p.add(
                    &at,
                    format!("workload {w:?} is already scaled by model {:?}; a workload may belong to only one model", prev.as_str()),
                ),
                None => {
                    owners.insert(w.as_str(), &m.id);
                }
            }
        }
    }
    for r in c.resources() {
        if r.kind == ResourceKind::Exclusive && c.models_of(&r.id).next().is_none() {
            p.add(
                format!("resources[{}]", r.id),
                "exclusive resource has no models",
            );
        }
        if let Some(pw) = &r.power {
            // Under manual control the timeouts are never applied, so only managed resources
            // are cross-checked. A timeout no longer than the staleness bound could expire before one fresh
            // reading can show the host's state.
            let stale = c.policy().host_reading_stale_after;
            for (name, value) in [
                ("power_on_timeout", pw.power_on_timeout()),
                ("shutdown_timeout", pw.shutdown_timeout()),
                ("veto_timeout", pw.veto_timeout()),
            ] {
                if c.power_managed(&r.id) && value <= stale {
                    p.add(
                        format!("resources[{}].power", r.id),
                        format!(
                            "{name} ({value}) must be greater than policy.host_reading_stale_after ({stale})"
                        ),
                    );
                }
            }
            for problem in pw.problems() {
                p.add(format!("resources[{}].power", r.id), problem);
            }
        }
    }
    // Two resources must not be powered through the same host: one would switch the other.
    let mut macs = HashMap::new();
    let mut targets = HashMap::new();
    for r in c.resources() {
        let Some(pw) = &r.power else { continue };
        let at = format!("resources[{}].power", r.id);
        if let Some(w) = &pw.wake_on_lan
            && let Some(prev) = macs.insert(w.mac.0, &r.id)
        {
            p.add(
                &at,
                format!(
                    "wake_on_lan.mac {} is also used by resource {:?}",
                    w.mac,
                    prev.as_str()
                ),
            );
        }
        if let Some(s) = &pw.shutdown
            && let Some(prev) = targets.insert((s.host.to_ascii_lowercase(), s.port), &r.id)
        {
            p.add(
                &at,
                format!(
                    "shutdown host {}:{} is also used by resource {:?}",
                    s.host,
                    s.port,
                    prev.as_str()
                ),
            );
        }
    }
}

fn classes(c: &Config, p: &mut Problems) {
    for k in c.classes() {
        let at = format!("classes[{}]", k.id);
        if k.requires.is_empty() {
            p.add(&at, "must require at least one resource");
        }
        let mut seen = HashSet::new();
        let mut any_power = false;
        for (i, req) in k.requires.iter().enumerate() {
            let rat = format!("{at}.requires[{i}]");
            if !seen.insert(&req.resource) {
                p.add(
                    &rat,
                    format!(
                        "resource {:?} is required more than once",
                        req.resource.as_str()
                    ),
                );
            }
            let Some(res) = c.resource(&req.resource) else {
                p.add(
                    &rat,
                    format!("references unknown resource {:?}", req.resource.as_str()),
                );
                continue;
            };
            any_power |= res.power.is_some();
            match (res.kind, &req.model) {
                // A gate on the host being up: the class must not have demand of its own,
                // so it can never influence which model is chosen.
                (ResourceKind::Exclusive, None) => {
                    if k.demand.is_some() {
                        p.add(
                            &rat,
                            format!(
                                "resource {:?} is exclusive and no model is given, which makes this a gate class (host up only); a gate class must not have a demand source",
                                req.resource.as_str()
                            ),
                        );
                    }
                }
                (ResourceKind::Shared, Some(m)) => p.add(
                    &rat,
                    format!(
                        "resource {:?} is shared; model {:?} must not be given",
                        req.resource.as_str(),
                        m.as_str()
                    ),
                ),
                (ResourceKind::Exclusive, Some(m)) => match c.model(m) {
                    None => p.add(&rat, format!("references unknown model {:?}", m.as_str())),
                    Some(spec) if spec.resource != req.resource => p.add(
                        &rat,
                        format!(
                            "model {:?} belongs to resource {:?}, not {:?}",
                            m.as_str(),
                            spec.resource.as_str(),
                            req.resource.as_str()
                        ),
                    ),
                    Some(_) => {}
                },
                (ResourceKind::Shared, None) => {}
            }
        }
        if k.may_change_power && !any_power {
            p.add(
                &at,
                "may_change_power is set but none of its required resources has a power section",
            );
        }
        if let Some(d) = &k.demand {
            match c.source(&d.source) {
                None => p.add(
                    format!("{at}.demand"),
                    format!("references unknown source {:?}", d.source.as_str()),
                ),
                Some(s) if !s.observe.contains(&Capability::Demand) => p.add(
                    format!("{at}.demand"),
                    format!(
                        "source {:?} does not declare the demand capability (observe: [demand])",
                        d.source.as_str()
                    ),
                ),
                Some(_) => {}
            }
        }
        if let Some(sw) = &k.switch {
            if k.demand.is_some() {
                p.add(
                    format!("{at}.switch"),
                    "a class has demand or a switch, not both",
                );
            }
            if k.requires.iter().any(|r| r.model.is_some()) {
                p.add(
                    format!("{at}.switch"),
                    "a switched class names no model on any requirement",
                );
            }
            match c.source(&sw.source) {
                None => p.add(
                    format!("{at}.switch"),
                    format!("references unknown source {:?}", sw.source.as_str()),
                ),
                Some(s) if !s.observe.contains(&Capability::Switch) => p.add(
                    format!("{at}.switch"),
                    format!(
                        "source {:?} does not declare the switch capability (observe: [switch])",
                        sw.source.as_str()
                    ),
                ),
                Some(_) => {}
            }
        }
        if let Some(t) = k.kill_timeout
            && (t == Millis::ZERO || t > MAX_LEASE_DURATION)
        {
            p.add(
                format!("{at}.kill_timeout"),
                format!("{t} must be greater than zero and at most {MAX_LEASE_DURATION}"),
            );
        }
        if let Some(r) = &k.recovery {
            for problem in r.problems() {
                p.add(format!("{at}.recovery"), problem);
            }
            if let Some(src) = &r.source
                && c.source(src).is_none()
            {
                p.add(
                    format!("{at}.recovery"),
                    format!("references unknown source {:?}", src.as_str()),
                );
            }
            if k.preemptibility != crate::config::Preemptibility::Kill {
                p.add(
                    format!("{at}.recovery"),
                    "a recovery action only applies to classes with preemptibility \"kill\"",
                );
            }
        }
    }
}

fn sources(c: &Config, p: &mut Problems) {
    let pol = c.policy();
    let declared = |cap: Capability| c.sources().iter().any(|s| s.observe.contains(&cap));
    // Without a declared source the safety-critical readings would never exist and every
    // decision on the host would wait on stale observations, so refuse such a config at
    // load.
    for r in c.resources().iter().filter(|r| r.power.is_some()) {
        if !declared(Capability::Reachability) {
            p.add(
                format!("resources[{}]", r.id),
                "has a power section but no source declares the reachability capability (observe: [reachability]), so its host state could never be observed",
            );
        }
    }
    // A managed shared host is powered off by the scheduler, and a pod may still be using it:
    // refuse a config that gives no way to know.
    for r in c.resources() {
        if r.kind == ResourceKind::Shared && c.power_managed(&r.id) && !c.mounts_checked(&r.id) {
            p.add(
                format!("resources[{}]", r.id),
                "control is managed but no source declares the mounts capability (observe: [mounts]) with nfs_servers for it, so the scheduler could not tell whether pods still mount it before powering it off",
            );
        }
    }
    // The pods veto of a managed exclusive host trusts a model's pod count only where a pods
    // source watches the namespaces of the model's workloads: refuse a config that leaves one
    // uncovered, since the host could then never be powered off (the veto would stop it).
    for r in c.resources() {
        if r.kind == ResourceKind::Exclusive && c.power_managed(&r.id) && c.pods_checked(&r.id) {
            for m in c.models_of(&r.id).filter(|m| !c.model_pods_covered(m)) {
                p.add(
                    format!("models[{}]", m.id),
                    format!(
                        "resource {:?} is managed and a pods source exists, so the model's pods are checked before the host is powered off, but namespaces is empty or names a namespace no pods source watches (sources[].params.namespaces), so its workloads could not be seen",
                        r.id.as_str()
                    ),
                );
            }
        }
    }
    for s in c.sources() {
        let at = format!("sources[{}]", s.id);
        if let Some(from) = &s.reads_from {
            if !s.observe.contains(&Capability::Demand) {
                p.add(
                    format!("{at}.reads_from"),
                    "only a source that declares the demand capability (observe: [demand]) can read from a switched class",
                );
            }
            match c.class(from) {
                None => p.add(
                    format!("{at}.reads_from"),
                    format!("references unknown class {:?}", from.as_str()),
                ),
                Some(k) if k.switch.is_none() => p.add(
                    format!("{at}.reads_from"),
                    format!(
                        "class {:?} is not a switched class (it has no switch:)",
                        from.as_str()
                    ),
                ),
                Some(_) => {}
            }
        }
        if s.resync == Millis::ZERO {
            p.add(&at, "resync must be greater than zero");
            continue;
        }
        if s.resync > pol.queue_reading_stale_after {
            p.add(
                &at,
                format!(
                    "resync ({}) exceeds policy.queue_reading_stale_after ({}); its observations would always be stale",
                    s.resync, pol.queue_reading_stale_after
                ),
            );
        }
        for message in crate::source_params::problems(s) {
            p.add(format!("{at}.params"), message);
        }
        for r in s
            .kube_watch_params()
            .ok()
            .flatten()
            .iter()
            .flat_map(|k| k.nodes.keys())
        {
            if c.resource(r).is_none() {
                p.add(
                    format!("{at}.params.nodes"),
                    format!("unknown resource \"{r}\""),
                );
            }
        }
        for r in s
            .kube_watch_params()
            .ok()
            .flatten()
            .iter()
            .flat_map(|k| k.nfs_servers.keys())
        {
            if c.resource(r).is_none() {
                p.add(
                    format!("{at}.params.nfs_servers"),
                    format!("unknown resource \"{r}\""),
                );
            }
        }
        let probed = match s.kind {
            SourceKind::Prometheus => s.prometheus_params().ok().flatten().map(|p| p.resource),
            SourceKind::TcpProbe => s.tcp_probe_params().ok().flatten().map(|p| p.resource),
            _ => None,
        };
        if let Some(resource) = probed
            && c.resource(&resource).is_none()
        {
            p.add(
                format!("{at}.params.resource"),
                format!("unknown resource \"{resource}\""),
            );
        }
        for cap in &s.observe {
            if !s.kind.can_supply(*cap) {
                p.add(
                    &at,
                    format!(
                        "kind {:?} cannot supply the {cap} capability listed in observe",
                        s.kind
                    ),
                );
            }
        }
        if (s.observe.contains(&Capability::Pods)
            || s.observe.contains(&Capability::Reachability)
            || s.observe.contains(&Capability::Mounts)
            || s.observe.contains(&Capability::Jobs))
            && s.resync > pol.host_reading_stale_after
        {
            p.add(
                &at,
                format!(
                    "resync ({}) exceeds policy.host_reading_stale_after ({}); its pods/reachability/mounts/jobs observations would always be stale",
                    s.resync, pol.host_reading_stale_after
                ),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{Config, ConfigError};

    const VALID: &str = r#"
policy:
  strategy: fifo-drain
  min_dwell: 1m
  max_dwell: 1h
  idle_grace: 5m
  age_threshold: 30m
  queue_reading_stale_after: 5m
  host_reading_stale_after: 30s
  interactive_ttl: 10m
  drain_timeout: 5m
resources:
  - { id: gpu, kind: exclusive, power: { control: manual } }
  - { id: nfs, kind: shared, power: { control: manual } }
  - { id: gpu2, kind: exclusive }
models:
  - { id: m1, resource: gpu, load_time: 60s, workloads: [w1], namespaces: [ai] }
  - { id: m2, resource: gpu2, load_time: 60s, workloads: [w2] }
classes:
  - id: c1
    requires: [ { resource: gpu, model: m1 }, { resource: nfs } ]
    priority: 10
    preemptibility: kill
    demand: { source: s1 }
    recovery: { action: requeue_failed, params: { list: { url: 'http://x/l', items: /i, id: /id }, requeue: { url: 'http://x/{id}' } } }
sources:
  - { id: s1, kind: http_json, resync: 30s, observe: [demand] }
  - { id: k8s, kind: kubernetes_watch, resync: 10s, observe: [pods, reachability], params: { namespaces: [ai], nodes: { gpu: node1 } } }
  - { id: hook, kind: webhook, resync: 30s }
"#;

    fn problems(yaml: &str) -> Vec<String> {
        match Config::from_yaml(yaml) {
            Err(ConfigError::Invalid(ps)) => ps.iter().map(|p| p.to_string()).collect(),
            Err(e) => panic!("expected validation problems, got {e}"),
            Ok(_) => panic!("expected invalid config"),
        }
    }

    fn has(ps: &[String], needle: &str) -> bool {
        ps.iter().any(|p| p.contains(needle))
    }

    #[test]
    fn valid_config_loads() {
        let c = Config::from_yaml(VALID).unwrap();
        assert_eq!(c.classes().len(), 1);
        assert_eq!(c.models_of(&"gpu".into()).count(), 1);
    }

    #[test]
    fn rejects_dangling_references() {
        let y = VALID
            .replace("model: m1 }", "model: nope }")
            .replace("demand: { source: s1 }", "demand: { source: ghost }")
            .replace("resource: gpu, load_time", "resource: nogpu, load_time");
        let ps = problems(&y);
        assert!(
            has(
                &ps,
                "classes[c1].requires[0]: references unknown model \"nope\""
            ),
            "{ps:?}"
        );
        assert!(
            has(
                &ps,
                "classes[c1].demand: references unknown source \"ghost\""
            ),
            "{ps:?}"
        );
        assert!(
            has(&ps, "models[m1]: references unknown resource \"nogpu\""),
            "{ps:?}"
        );
    }

    #[test]
    fn rejects_duplicate_names_in_every_section() {
        let y = VALID
            .replace(
                "  - { id: nfs, kind: shared, power: { control: manual } }",
                "  - { id: nfs, kind: shared }\n  - { id: nfs, kind: shared }",
            )
            .replace(
                "models:\n",
                "models:\n  - { id: m1, resource: gpu, load_time: 1s, workloads: [w] }\n",
            )
            .replace(
                "\nsources:\n",
                "\nsources:\n  - { id: s1, kind: sql, resync: 1s }\n",
            );
        let y = format!("{y}\n");
        let ps = problems(&y);
        assert!(has(&ps, "resources[nfs]: duplicate id"), "{ps:?}");
        assert!(has(&ps, "models[m1]: duplicate id"), "{ps:?}");
        assert!(has(&ps, "sources[s1]: duplicate id"), "{ps:?}");
    }

    #[test]
    fn model_and_class_ids_follow_the_conservative_resource_style_rule() {
        let fifty = "a".repeat(50);
        for good in ["a", "mineru", "immich-ml", "0a-1", "x--y", fifty.as_str()] {
            for (what, y) in [
                ("model", VALID.replace("m1", good)),
                ("class", VALID.replace("c1", good)),
            ] {
                if let Err(e) = Config::from_yaml(&y) {
                    panic!("{what} id {good:?} should be accepted: {e}");
                }
            }
        }
        let fifty_one = "a".repeat(51);
        let bad = [
            ("Upper", "character 'U'"),
            ("under_score", "character '_'"),
            ("dot.id", "character '.'"),
            ("sp ace", "character ' '"),
            ("caf\u{e9}", "character '\u{e9}'"),
            ("-lead", "must start and end"),
            ("trail-", "must start and end"),
            ("-", "must start and end"),
            (fifty_one.as_str(), "51 characters, more than 50"),
        ];
        for (id, why) in bad {
            for (section, y) in [
                ("models", VALID.replace("m1", &format!("\"{id}\""))),
                ("classes", VALID.replace("c1", &format!("\"{id}\""))),
            ] {
                let ps = problems(&y);
                let at = format!("{section}[{id}]: ");
                assert!(
                    ps.iter().any(|p| p.starts_with(&at) && p.contains(why)),
                    "{section} {id:?}: {ps:?}"
                );
                assert!(
                    ps.iter()
                        .any(|p| p.contains("lowercase letters, digits and '-'")),
                    "the message names the rule: {ps:?}"
                );
            }
        }
    }

    #[test]
    fn rejects_inconsistent_policy_and_reports_all() {
        let y = VALID
            .replace("min_dwell: 1m", "min_dwell: 2h")
            .replace(
                "host_reading_stale_after: 30s",
                "host_reading_stale_after: 10m",
            )
            .replace("drain_timeout: 5m", "drain_timeout: 0");
        let ps = problems(&y);
        assert!(
            has(
                &ps,
                "min_dwell (7200000ms) is greater than max_dwell (3600000ms)"
            ),
            "{ps:?}"
        );
        assert!(has(&ps, "host_reading_stale_after"), "{ps:?}");
        assert!(
            has(&ps, "policy.drain_timeout: must be greater than zero"),
            "{ps:?}"
        );
        assert!(ps.len() >= 3);
    }

    #[test]
    fn rejects_requirement_mode_mismatches() {
        let y = VALID.replace(
            "{ resource: gpu, model: m1 }, { resource: nfs }",
            "{ resource: gpu }, { resource: nfs, model: m1 }",
        );
        let ps = problems(&y);
        // A model-less requirement on an exclusive resource is a gate on the host, which a
        // class with a demand source must not be.
        assert!(
            has(
                &ps,
                "classes[c1].requires[0]: resource \"gpu\" is exclusive and no model is given, which makes this a gate class"
            ),
            "{ps:?}"
        );
        assert!(
            has(&ps, "is shared; model \"m1\" must not be given"),
            "{ps:?}"
        );
    }

    #[test]
    fn a_gate_class_requires_hosts_without_a_model_and_has_no_demand() {
        let gate = "  - id: gate\n    requires: [ { resource: gpu }, { resource: nfs } ]\n    priority: 0\n    preemptibility: none\n";
        let y = VALID.replace("\nsources:\n", &format!("\n{gate}sources:\n"));
        assert_ne!(y, VALID);
        let c = Config::from_yaml(&y).unwrap_or_else(|e| panic!("{e}"));
        let g = c.class(&"gate".into()).unwrap();
        assert!(g.demand.is_none() && g.requires[0].model.is_none());
        // With a demand source it is refused: it would hold the host without naming a model.
        let with_demand = y.replace(
            "    priority: 0\n    preemptibility: none\n",
            "    priority: 0\n    preemptibility: none\n    demand: { source: s1 }\n",
        );
        assert_ne!(with_demand, y);
        let ps = problems(&with_demand);
        assert!(
            has(
                &ps,
                "classes[gate].requires[0]: resource \"gpu\" is exclusive and no model is given"
            ),
            "{ps:?}"
        );
    }

    #[test]
    fn rejects_recovery_without_kill_and_power_without_power_section() {
        let y = VALID
            .replace(
                "preemptibility: kill",
                "preemptibility: none\n    may_change_power: true",
            )
            .replace(
                "{ id: gpu, kind: exclusive, power: { control: manual } }",
                "{ id: gpu, kind: exclusive }",
            )
            .replace(
                "{ id: nfs, kind: shared, power: { control: manual } }",
                "{ id: nfs, kind: shared }",
            );
        let ps = problems(&y);
        assert!(
            has(&ps, "recovery: a recovery action only applies"),
            "{ps:?}"
        );
        assert!(has(&ps, "may_change_power is set but"), "{ps:?}");
    }

    #[test]
    fn rejects_model_on_shared_resource_and_stale_resync() {
        let y = VALID
            .replace("{ id: m1, resource: gpu,", "{ id: m1, resource: nfs,")
            .replace("resync: 30s }", "resync: 10m }");
        let ps = problems(&y);
        assert!(has(&ps, "is not exclusive"), "{ps:?}");
        assert!(
            has(&ps, "exceeds policy.queue_reading_stale_after"),
            "{ps:?}"
        );
    }

    #[test]
    fn rejects_unknown_fields_and_bad_durations() {
        assert!(matches!(
            Config::from_yaml(&VALID.replace("priority: 10", "priorty: 10")),
            Err(ConfigError::Parse(_))
        ));
        assert!(matches!(
            Config::from_yaml(&VALID.replace("min_dwell: 1m", "min_dwell: soon")),
            Err(ConfigError::Parse(_))
        ));
        assert!(matches!(
            Config::from_yaml("not: [valid"),
            Err(ConfigError::Parse(_))
        ));
    }

    #[test]
    fn error_message_lists_every_problem() {
        let y = VALID
            .replace("min_dwell: 1m", "min_dwell: 2h")
            .replace("source: s1", "source: zz");
        let msg = Config::from_yaml(&y).unwrap_err().to_string();
        assert!(msg.contains("2 problem(s)"), "{msg}");
        assert!(msg.contains("min_dwell") && msg.contains("zz"), "{msg}");
    }

    #[test]
    fn rejects_model_from_another_exclusive_resource() {
        let y = VALID.replace(
            "{ resource: gpu, model: m1 }",
            "{ resource: gpu, model: m2 }",
        );
        let ps = problems(&y);
        assert!(
            has(
                &ps,
                "model \"m2\" belongs to resource \"gpu2\", not \"gpu\""
            ),
            "{ps:?}"
        );
    }

    #[test]
    fn rejects_recovery_params_and_actions_this_build_cannot_run() {
        let bad = VALID.replace(
            "requeue: { url: 'http://x/{id}' }",
            "requeue: { url: 'http://x/again' }",
        );
        assert_ne!(bad, VALID);
        let ps = problems(&bad);
        assert!(
            has(
                &ps,
                "classes[c1].recovery: requeue.url must contain {id} exactly once"
            ),
            "{ps:?}"
        );
        let unknown = VALID.replace("action: requeue_failed", "action: requeue_faild");
        let ps = problems(&unknown);
        assert!(
            has(
                &ps,
                "classes[c1].recovery: unknown action \"requeue_faild\""
            ),
            "{ps:?}"
        );
    }

    #[test]
    fn rejects_recovery_source_that_dangles() {
        let y = VALID.replace(
            "recovery: { action: requeue_failed, params: { list: { url: 'http://x/l', items: /i, id: /id }, requeue: { url: 'http://x/{id}' } } }",
            "recovery: { action: requeue_failed, source: ghost, params: { list: { url: 'http://x/l', items: /i, id: /id }, requeue: { url: 'http://x/{id}' } } }",
        );
        assert!(has(
            &problems(&y),
            "classes[c1].recovery: references unknown source \"ghost\""
        ));
        let ok = VALID.replace(
            "recovery: { action: requeue_failed, params: { list: { url: 'http://x/l', items: /i, id: /id }, requeue: { url: 'http://x/{id}' } } }",
            "recovery: { action: requeue_failed, source: s1, params: { list: { url: 'http://x/l', items: /i, id: /id }, requeue: { url: 'http://x/{id}' } } }",
        );
        assert!(Config::from_yaml(&ok).is_ok());
        let empty = VALID.replace("action: requeue_failed, params", "action: \"\", params");
        assert!(has(&problems(&empty), "action must not be empty"));
    }

    #[test]
    fn a_managed_gpu_host_needs_its_models_namespaces_watched_by_a_pods_source() {
        let managed = crate::test_support::example_yaml_managed();
        let c = Config::from_yaml(&managed).unwrap();
        let gpu: crate::ResourceId = "cuda-central".into();
        assert!(c.models_of(&gpu).all(|m| c.model_pods_covered(m)));
        // A model without namespaces, or in one no pods source watches, is refused.
        for (from, to) in [
            (
                "workloads: [mineru], namespaces: [ai]",
                "workloads: [mineru]",
            ),
            (
                "workloads: [mineru], namespaces: [ai]",
                "workloads: [mineru], namespaces: [ai, elsewhere]",
            ),
            (
                "workloads: [mineru], namespaces: [ai]",
                "workloads: [mineru], namespaces: [elsewhere]",
            ),
        ] {
            let ps = problems(&managed.replace(from, to));
            assert!(
                has(
                    &ps,
                    "models[mineru]: resource \"cuda-central\" is managed and a pods source exists"
                ),
                "{to}: {ps:?}"
            );
        }
        // Under manual control nothing is powered off, so nothing is refused (the model just
        // reads as unknown to a veto that never runs); and without a pods source there is no
        // pods veto to cover.
        let manual = crate::test_support::EXAMPLE_CONFIG.replace(
            "workloads: [mineru], namespaces: [ai]",
            "workloads: [mineru]",
        );
        let c = Config::from_yaml(&manual).unwrap();
        let m = c
            .models()
            .iter()
            .find(|m| m.id.as_str() == "mineru")
            .unwrap();
        assert!(!c.model_pods_covered(m));
    }

    #[test]
    fn a_config_where_no_source_declares_pods_is_accepted() {
        // No allow waits on pod counts (only recovery readiness and the GPU veto read them), so a
        // config without a pods source is valid: its hosts simply have no GPU veto.
        let y = VALID.replace("observe: [pods, reachability]", "observe: [reachability]");
        assert_ne!(y, VALID);
        assert!(Config::from_yaml(&y).is_ok(), "{:?}", problems(&y));
    }

    #[test]
    fn rejects_a_config_where_no_source_declares_reachability() {
        let y = VALID.replace("observe: [pods, reachability]", "observe: [pods]");
        let ps = problems(&y);
        assert!(
            has(
                &ps,
                "resources[gpu]: has a power section but no source declares the reachability capability"
            ),
            "{ps:?}"
        );
        assert!(has(&ps, "resources[nfs]: has a power section"), "{ps:?}");
        assert!(!has(&ps, "resources[gpu2]"), "{ps:?}");
    }

    #[test]
    fn kinds_can_supply_exactly_the_documented_capabilities() {
        use crate::config::{Capability::*, SourceKind::*};
        let table = [
            (KubernetesWatch, [true, true, false]),
            (HttpJson, [false, true, true]),
            (Prometheus, [false, true, true]),
            (TcpProbe, [false, true, false]),
            (Sql, [false, false, true]),
            (Session, [false, false, true]),
            (Webhook, [false, false, false]),
        ];
        for (kind, [pods, reach, demand]) in table {
            assert_eq!(kind.can_supply(Pods), pods, "{kind:?} pods");
            assert_eq!(
                kind.can_supply(Reachability),
                reach,
                "{kind:?} reachability"
            );
            assert_eq!(kind.can_supply(Demand), demand, "{kind:?} demand");
            // Only an http_json source can read a switch (it maps a string to a state).
            assert_eq!(kind.can_supply(Switch), kind == HttpJson, "{kind:?} switch");
        }
    }

    #[test]
    fn rejects_declaring_capabilities_through_a_config() {
        // kubernetes_watch cannot declare demand; http_json and prometheus cannot declare pods.
        let ps = problems(&VALID.replace(
            "kind: kubernetes_watch, resync: 10s, observe: [pods, reachability]",
            "kind: kubernetes_watch, resync: 10s, observe: [pods, reachability, demand]",
        ));
        assert!(
            has(
                &ps,
                "sources[k8s]: kind KubernetesWatch cannot supply the demand capability"
            ),
            "{ps:?}"
        );
        for kind in ["http_json", "prometheus"] {
            let ps = problems(&VALID.replace(
                "{ id: s1, kind: http_json, resync: 30s, observe: [demand] }",
                &format!("{{ id: s1, kind: {kind}, resync: 30s, observe: [demand, pods] }}"),
            ));
            assert!(
                has(&ps, "cannot supply the pods capability"),
                "{kind}: {ps:?}"
            );
        }
    }

    #[test]
    fn the_safety_resync_limit_applies_to_pods_and_to_reachability_sources_alike() {
        // host_reading_stale_after is 30s in VALID; 45s is within queue_reading_stale_after.
        let probe = |observe: &str, resync: &str| {
            let kind = if observe == "pods" {
                "kubernetes_watch"
            } else {
                "prometheus"
            };
            VALID.replace(
                "  - { id: hook,",
                &format!("  - {{ id: probe, kind: {kind}, resync: {resync}, observe: [{observe}] }}\n  - {{ id: hook,"),
            )
        };
        for observe in ["reachability", "pods"] {
            let ok = probe(observe, "30s");
            assert!(Config::from_yaml(&ok).is_ok(), "{observe}");
            let ps = problems(&probe(observe, "45s"));
            assert!(
                has(
                    &ps,
                    "sources[probe]: resync (45000ms) exceeds policy.host_reading_stale_after"
                ),
                "{observe}: {ps:?}"
            );
        }
        // A demand-only source is held only to queue_reading_stale_after.
        assert!(Config::from_yaml(&probe("demand", "45s")).is_ok());
    }

    #[test]
    fn observe_is_a_set_so_duplicates_are_harmless() {
        let y = VALID.replace(
            "observe: [pods, reachability]",
            "observe: [pods, reachability, pods, pods]",
        );
        let c = Config::from_yaml(&y).unwrap();
        let k8s = c.source(&"k8s".into()).unwrap();
        assert_eq!(k8s.observe.len(), 2);
    }

    #[test]
    fn reachability_may_come_from_a_non_kubernetes_source() {
        let y = VALID
            .replace("observe: [pods, reachability]", "observe: [pods]")
            .replace(
                "  - { id: hook,",
                "  - { id: probe, kind: http_json, resync: 10s, observe: [reachability] }\n  - { id: hook,",
            );
        assert!(Config::from_yaml(&y).is_ok());
        let y = y.replace("id: probe, kind: http_json", "id: probe, kind: prometheus");
        assert!(Config::from_yaml(&y).is_ok());
        let y = y.replace("id: probe, kind: prometheus", "id: probe, kind: tcp_probe");
        assert!(Config::from_yaml(&y).is_ok());
    }

    #[test]
    fn rejects_capabilities_the_kind_cannot_supply() {
        let ps = problems(&VALID.replace(
            "id: hook, kind: webhook, resync: 30s",
            "id: hook, kind: webhook, resync: 30s, observe: [demand]",
        ));
        assert!(
            has(
                &ps,
                "sources[hook]: kind Webhook cannot supply the demand capability"
            ),
            "{ps:?}"
        );
        let ps = problems(&VALID.replace(
            "{ id: s1, kind: http_json, resync: 30s, observe: [demand] }",
            "{ id: s1, kind: sql, resync: 30s, observe: [demand, pods] }",
        ));
        assert!(
            has(
                &ps,
                "sources[s1]: kind Sql cannot supply the pods capability"
            ),
            "{ps:?}"
        );
    }

    #[test]
    fn rejects_demand_from_sources_that_cannot_supply_it() {
        for id in ["k8s", "hook"] {
            let y = VALID.replace(
                "demand: { source: s1 }",
                &format!("demand: {{ source: {id} }}"),
            );
            let ps = problems(&y);
            assert!(
                has(
                    &ps,
                    &format!("source \"{id}\" does not declare the demand capability")
                ),
                "{ps:?}"
            );
        }
        for kind in ["sql", "prometheus", "session"] {
            let y = VALID.replace(
                "{ id: s1, kind: http_json",
                &format!("{{ id: s1, kind: {kind}"),
            );
            assert!(Config::from_yaml(&y).is_ok(), "{kind}");
        }
    }

    #[test]
    fn rejects_shared_or_repeated_workloads() {
        let ps = problems(&VALID.replace("workloads: [w2]", "workloads: [w1]"));
        assert!(
            has(
                &ps,
                "models[m2]: workload \"w1\" is already scaled by model \"m1\""
            ),
            "{ps:?}"
        );
        let ps = problems(&VALID.replace("workloads: [w1]", "workloads: [w1, w1]"));
        assert!(
            has(&ps, "models[m1]: workload \"w1\" is listed more than once"),
            "{ps:?}"
        );
        assert!(has(
            &problems(&VALID.replace("workloads: [w1]", "workloads: []")),
            "at least one workload"
        ));
        assert!(has(
            &problems(&VALID.replace("workloads: [w1]", "workloads: [\" \"]")),
            "workload names must not be empty"
        ));
    }

    #[test]
    fn rejects_exclusive_resource_without_models() {
        let y = VALID.replace(
            "  - { id: m2, resource: gpu2, load_time: 60s, workloads: [w2] }\n",
            "",
        );
        assert!(has(
            &problems(&y),
            "resources[gpu2]: exclusive resource has no models"
        ));
    }

    #[test]
    fn rejects_zero_load_time() {
        let ps = problems(&VALID.replace(
            "load_time: 60s, workloads: [w1]",
            "load_time: 0, workloads: [w1]",
        ));
        assert!(
            has(&ps, "models[m1]: load_time must be greater than zero"),
            "{ps:?}"
        );
    }

    const POWER: &str = "wake_on_lan: { router_url: 'https://r.example', mac: 'aa:bb:cc:dd:ee:01' }, shutdown: { host: h1, user: u, host_key: 'SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' }";

    #[test]
    fn power_timeouts_are_bounded_and_longer_than_the_staleness_bound() {
        let with = |extra: &str| {
            VALID.replace(
                "{ id: gpu, kind: exclusive, power: { control: manual } }",
                &format!(
                    "{{ id: gpu, kind: exclusive, power: {{ control: managed, {POWER}, {extra} }} }}"
                ),
            )
        };
        let c = Config::from_yaml(&with("power_on_timeout: 3m, shutdown_timeout: 1h")).unwrap();
        let pw = c.resource(&"gpu".into()).unwrap().power.as_ref().unwrap();
        assert_eq!(pw.power_on_timeout(), crate::Millis(180_000));
        let ps = problems(&with("power_on_timeout: 0"));
        assert!(
            has(
                &ps,
                "resources[gpu].power: power_on_timeout must be greater than zero"
            ),
            "{ps:?}"
        );
        let ps = problems(&with("shutdown_timeout: 2d"));
        assert!(
            has(
                &ps,
                "resources[gpu].power: shutdown_timeout 172800000ms exceeds"
            ),
            "{ps:?}"
        );
        let ps = problems(&with("power_on_timeout: 1s"));
        assert!(
            has(
                &ps,
                "power_on_timeout (1000ms) must be greater than policy.host_reading_stale_after"
            ),
            "{ps:?}"
        );
    }

    #[test]
    fn the_staleness_cross_check_applies_only_to_managed_resources() {
        // Manual control: the timeouts have no effect, so a large staleness bound (and the
        // defaults, and a short explicit timeout) are fine; zero is still rejected.
        let manual = |stale: &str, extra: &str| {
            VALID
                .replace("host_reading_stale_after: 30s", stale)
                .replace(
                    "{ id: gpu, kind: exclusive, power: { control: manual } }",
                    &format!("{{ id: gpu, kind: exclusive, power: {{ control: manual{extra} }} }}"),
                )
        };
        let ps = problems(&manual("host_reading_stale_after: 20m", ""));
        assert!(
            !has(&ps, "must be greater than policy.host_reading_stale_after"),
            "{ps:?}"
        );
        let ps = problems(&manual(
            "host_reading_stale_after: 20m",
            ", power_on_timeout: 1s, shutdown_timeout: 1s",
        ));
        assert!(
            !has(&ps, "must be greater than policy.host_reading_stale_after"),
            "{ps:?}"
        );
        let ps = problems(&manual(
            "host_reading_stale_after: 30s",
            ", power_on_timeout: 0",
        ));
        assert!(
            has(&ps, "power_on_timeout must be greater than zero"),
            "{ps:?}"
        );
        let ps = problems(&manual(
            "host_reading_stale_after: 30s",
            ", shutdown_timeout: 2d",
        ));
        assert!(has(&ps, "shutdown_timeout 172800000ms exceeds"), "{ps:?}");
    }

    #[test]
    fn a_managed_resource_needs_its_actuators() {
        let ps = problems(&VALID.replace(
            "{ id: gpu, kind: exclusive, power: { control: manual } }",
            "{ id: gpu, kind: exclusive, power: { control: managed } }",
        ));
        assert!(
            has(
                &ps,
                "resources[gpu].power: control is managed but there is no wake_on_lan"
            ),
            "{ps:?}"
        );
        assert!(
            has(
                &ps,
                "resources[gpu].power: control is managed but there is no shutdown"
            ),
            "{ps:?}"
        );
        let ok = VALID.replace(
            "{ id: gpu, kind: exclusive, power: { control: manual } }",
            &format!("{{ id: gpu, kind: exclusive, power: {{ control: managed, {POWER} }} }}"),
        );
        Config::from_yaml(&ok).unwrap();
        // A malformed section is refused even for a manual resource.
        let bad = ok
            .replace("control: managed", "control: manual")
            .replace("https://r.example", "http://r.example");
        assert!(
            has(&problems(&bad), "wake_on_lan.router_url"),
            "{:?}",
            problems(&bad)
        );
        // The placeholder shape this replaced is refused with a parse error.
        let old = VALID.replace(
            "power: { control: manual } }\n  - { id: nfs",
            "power: { actuator: wol } }\n  - { id: nfs",
        );
        assert_ne!(old, VALID);
        assert!(matches!(
            Config::from_yaml(&old),
            Err(ConfigError::Parse(_))
        ));
    }

    #[test]
    fn two_resources_must_not_share_a_power_target() {
        let both = VALID
            .replace(
                "{ id: gpu, kind: exclusive, power: { control: manual } }",
                &format!("{{ id: gpu, kind: exclusive, power: {{ control: managed, {POWER} }} }}"),
            )
            .replace(
                "{ id: nfs, kind: shared, power: { control: manual } }",
                &format!("{{ id: nfs, kind: shared, power: {{ control: managed, {POWER} }} }}"),
            );
        let ps = problems(&both);
        assert!(
            has(
                &ps,
                "resources[nfs].power: wake_on_lan.mac aa:bb:cc:dd:ee:01 is also used by resource \"gpu\""
            ),
            "{ps:?}"
        );
        assert!(
            has(
                &ps,
                "resources[nfs].power: shutdown host h1:22 is also used by resource \"gpu\""
            ),
            "{ps:?}"
        );
    }

    #[test]
    fn rejects_whitespace_padded_and_empty_ids() {
        let ps = problems(&VALID.replace("- id: c1", "- id: \" c1\""));
        assert!(has(&ps, "leading or trailing whitespace"), "{ps:?}");
        let ps =
            problems(&VALID.replace("{ id: s1, kind: http_json", "{ id: \"\", kind: http_json"));
        assert!(has(&ps, "sources: entry has an empty id"), "{ps:?}");
    }

    #[test]
    fn rejects_absurd_ttl_and_empty_strategy() {
        let ps = problems(&VALID.replace("interactive_ttl: 10m", "interactive_ttl: 400d"));
        assert!(
            has(
                &ps,
                "policy.interactive_ttl: 34560000000ms exceeds the maximum"
            ),
            "{ps:?}"
        );
        let ps = problems(&VALID.replace("drain_timeout: 5m", "drain_timeout: 2d"));
        assert!(has(&ps, "policy.drain_timeout"), "{ps:?}");
        assert!(has(
            &problems(&VALID.replace("strategy: fifo-drain", "strategy: \" \"")),
            "policy.strategy: must not be empty"
        ));
    }

    #[test]
    fn permissive_values_are_accepted() {
        // Zero grace/dwell, priority 0 with demand, and max_dwell below age_threshold are
        // legitimate: no hysteresis, lowest priority, and independent triggers.
        let y = VALID
            .replace("min_dwell: 1m", "min_dwell: 0")
            .replace("idle_grace: 5m", "idle_grace: 0")
            .replace("priority: 10", "priority: 0")
            .replace("max_dwell: 1h", "max_dwell: 10m");
        assert!(Config::from_yaml(&y).is_ok());
    }

    #[test]
    fn rejects_requirement_structure_errors() {
        let ps = problems(&VALID.replace(
            "{ resource: gpu, model: m1 }, { resource: nfs }",
            "{ resource: nfs }, { resource: nfs }",
        ));
        assert!(has(&ps, "required more than once"), "{ps:?}");
        let ps = problems(&VALID.replace("{ resource: nfs } ]", "{ resource: tape } ]"));
        assert!(has(&ps, "references unknown resource \"tape\""), "{ps:?}");
        assert!(has(
            &problems(&VALID.replace(
                "requires: [ { resource: gpu, model: m1 }, { resource: nfs } ]",
                "requires: []"
            )),
            "at least one resource"
        ));
    }

    #[test]
    fn rejects_resync_problems() {
        assert!(has(
            &problems(&VALID.replace(
                "resync: 30s, observe: [demand] }",
                "resync: 0, observe: [demand] }"
            )),
            "resync must be greater than zero"
        ));
        let ps = problems(&VALID.replace(
            "kind: kubernetes_watch, resync: 10s",
            "kind: kubernetes_watch, resync: 1m",
        ));
        assert!(
            has(&ps, "exceeds policy.host_reading_stale_after"),
            "{ps:?}"
        );
    }

    #[test]
    fn duration_errors_name_the_key() {
        for (bad, key) in [
            ("true", "min_dwell"),
            ("1.5", "min_dwell"),
            ("-3", "min_dwell"),
            ("null", "min_dwell"),
            ("5 m", "min_dwell"),
            ("1_000", "min_dwell"),
        ] {
            let y = VALID
                .replace("min_dwell: 1m", &format!("min_dwell: {bad:?}"))
                .replace(&format!("{bad:?}"), bad);
            let msg = Config::from_yaml(&y).unwrap_err().to_string();
            assert!(msg.contains(key), "{bad}: {msg}");
            assert!(
                !msg.contains("untagged") && !msg.contains("Raw"),
                "{bad}: {msg}"
            );
        }
    }

    #[test]
    fn lease_duration_boundary() {
        let ok = VALID.replace("interactive_ttl: 10m", "interactive_ttl: 1d");
        assert!(Config::from_yaml(&ok).is_ok());
        let over = VALID.replace("interactive_ttl: 10m", "interactive_ttl: 86400001");
        assert!(has(&problems(&over), "policy.interactive_ttl"));
        let ok = VALID.replace(
            "drain_timeout: 5m",
            "drain_timeout: 5m\n  lockout_timeout: 1d",
        );
        assert!(Config::from_yaml(&ok).is_ok());
    }

    #[test]
    fn lockout_and_kill_timeouts_are_validated() {
        let y = VALID.replace(
            "drain_timeout: 5m",
            "drain_timeout: 5m\n  lockout_timeout: 0",
        );
        assert!(has(
            &problems(&y),
            "policy.lockout_timeout: must be greater than zero"
        ));
        let y = VALID.replace(
            "drain_timeout: 5m",
            "drain_timeout: 5m\n  lockout_timeout: 2d",
        );
        assert!(has(&problems(&y), "policy.lockout_timeout"));
        let y = VALID.replace("    priority: 10", "    kill_timeout: 0\n    priority: 10");
        assert!(has(&problems(&y), "classes[c1].kill_timeout"));
        let y = VALID.replace(
            "    priority: 10",
            "    kill_timeout: 30s\n    priority: 10",
        );
        let c = Config::from_yaml(&y).unwrap();
        assert_eq!(
            c.class(&"c1".into()).unwrap().kill_timeout,
            Some(crate::Millis(30_000))
        );
        assert_eq!(c.policy().lockout_timeout(), c.policy().drain_timeout);
    }

    #[test]
    fn generation_is_stamped() {
        assert_eq!(Config::from_yaml(VALID).unwrap().generation(), 0);
        assert_eq!(
            Config::from_yaml_with_generation(VALID, 9)
                .unwrap()
                .generation(),
            9
        );
    }

    /// The managed example, with its `basestar-mounts` source changed by `f`.
    fn managed_example_with(from: &str, to: &str) -> String {
        let y = crate::test_support::example_yaml_managed();
        assert!(y.contains(from), "the example changed: {from}");
        y.replace(from, to)
    }

    #[test]
    fn a_managed_shared_host_needs_a_mounts_source_naming_it() {
        let c = Config::from_yaml(&crate::test_support::example_yaml_managed()).unwrap();
        assert!(c.mounts_checked(&"basestar-nfs".into()));
        assert!(!c.mounts_checked(&"cuda-central".into()));
        let ps = problems(&managed_example_with(
            "nfs_servers: { basestar-nfs: [basestar] }",
            "nfs_servers: { cuda-central: [basestar] }",
        ));
        assert!(
            has(
                &ps,
                "resources[basestar-nfs]: control is managed but no source declares the mounts capability"
            ),
            "{ps:?}"
        );
        // Under manual control nothing is shut down, so nothing is required.
        let manual = crate::test_support::EXAMPLE_CONFIG.replace(
            "nfs_servers: { basestar-nfs: [basestar] }",
            "nfs_servers: { cuda-central: [basestar] }",
        );
        assert!(Config::from_yaml(&manual).is_ok());
    }

    #[test]
    fn mounts_params_are_checked() {
        let ps = problems(&managed_example_with(
            "nfs_servers: { basestar-nfs: [basestar] }",
            "nfs_servers: { nowhere: [basestar] }",
        ));
        assert!(
            has(
                &ps,
                "sources[basestar-mounts].params.nfs_servers: unknown resource \"nowhere\""
            ),
            "{ps:?}"
        );
        let ps = problems(&managed_example_with(
            "nfs_servers: { basestar-nfs: [basestar] }",
            "nfs_servers: { basestar-nfs: [] }",
        ));
        assert!(
            has(&ps, "nfs_servers[basestar-nfs] must not be empty"),
            "{ps:?}"
        );
        let ps = problems(&managed_example_with(
            "nfs_servers: { basestar-nfs: [basestar] }",
            "nfs_servers: { basestar-nfs: ['a b'] }",
        ));
        assert!(has(&ps, "without whitespace"), "{ps:?}");
        let ps = problems(&managed_example_with(
            "    observe: [mounts]\n    resync: 10s",
            "    observe: [mounts]\n    resync: 1m",
        ));
        assert!(
            has(&ps, "exceeds policy.host_reading_stale_after"),
            "{ps:?}"
        );
    }

    #[test]
    fn the_veto_timeout_is_bounded_and_longer_than_the_staleness_bound() {
        let ps = problems(&managed_example_with(
            "veto_timeout: 30m",
            "veto_timeout: 1s",
        ));
        assert!(
            has(
                &ps,
                "veto_timeout (1000ms) must be greater than policy.host_reading_stale_after"
            ),
            "{ps:?}"
        );
        let ps = problems(&managed_example_with(
            "veto_timeout: 30m",
            "veto_timeout: 2d",
        ));
        assert!(has(&ps, "veto_timeout 172800000ms exceeds"), "{ps:?}");
        let ps = problems(&managed_example_with(
            "veto_timeout: 30m",
            "veto_timeout: 0",
        ));
        assert!(has(&ps, "veto_timeout must be greater than zero"), "{ps:?}");
    }

    #[test]
    fn min_off_is_zero_by_default_and_bounded_in_a_whole_config() {
        let c = Config::from_yaml(&crate::test_support::example_yaml_managed()).unwrap();
        assert_eq!(
            c.resource(&"basestar-nfs".into())
                .and_then(|r| r.power.as_ref())
                .map(|p| p.min_off()),
            Some(crate::Millis::ZERO)
        );
        let ok = managed_example_with("veto_timeout: 30m", "veto_timeout: 30m\n      min_off: 10m");
        let c = Config::from_yaml(&ok).unwrap();
        assert_eq!(
            c.resource(&"basestar-nfs".into())
                .and_then(|r| r.power.as_ref())
                .map(|p| p.min_off()),
            Some(crate::Millis(600_000))
        );
        let ps = problems(&managed_example_with("veto_timeout: 30m", "min_off: 2d"));
        assert!(has(&ps, "min_off 172800000ms exceeds"), "{ps:?}");
        assert!(
            Config::from_yaml(&managed_example_with("veto_timeout: 30m", "min_off: 0")).is_ok()
        );
    }

    // Scheduled jobs.

    fn example_with(from: &str, to: &str) -> String {
        let text = crate::test_support::EXAMPLE_CONFIG;
        assert!(text.contains(from), "the example has no {from:?}");
        text.replacen(from, to, 1)
    }

    #[test]
    fn the_example_job_is_valid_and_typed() {
        let c = Config::from_yaml(crate::test_support::EXAMPLE_CONFIG).unwrap();
        let j = c.job(&"backup".into()).unwrap();
        assert_eq!(j.missed_run, crate::jobs::MissedRun::CatchUp);
        assert_eq!(j.retries(), 1);
        assert_eq!(j.needs.len(), 1);
        assert_eq!(j.needs[0].resource().as_str(), "basestar-nfs");
        assert_eq!(c.jobs_source().unwrap().id.as_str(), "scheduled-jobs");
        // Omitted keys take the documented defaults, and a config without jobs is valid.
        let bare = example_with("    retries: 1\n    retry_backoff: 5m\n", "");
        let c = Config::from_yaml(&bare).unwrap();
        assert_eq!(c.jobs()[0].retries(), 1);
        assert_eq!(
            c.jobs()[0].retry_backoff(),
            crate::jobs::DEFAULT_RETRY_BACKOFF
        );
    }

    #[test]
    fn a_job_that_is_malformed_is_refused_with_the_path_and_the_reason() {
        let ps = problems(&example_with(
            "  - id: backup\n    # Five-field",
            "  - id: Back_up\n    # Five-field",
        ));
        assert!(has(&ps, "jobs[Back_up]"), "{ps:?}");
        assert!(has(&ps, "lowercase letters"), "{ps:?}");
        let long = "a".repeat(41);
        let ps = problems(&example_with(
            "  - id: backup\n    # Five-field",
            &format!("  - id: {long}\n    # Five-field"),
        ));
        assert!(has(&ps, "more than 40"), "{ps:?}");
        for (cron, why) in [
            ("0 3 * *", "five fields"),
            ("0 3 * * 0 UTC", "five fields"),
            ("61 3 * * 0", "cron"),
            ("0 0 30 2 *", "cron"),
        ] {
            let ps = problems(&example_with(
                "cron: \"0 3 * * 0\"",
                &format!("cron: \"{cron}\""),
            ));
            assert!(has(&ps, "jobs[backup].cron"), "{cron}: {ps:?}");
            assert!(has(&ps, why), "{cron}: {ps:?}");
        }
        let ps = problems(&example_with("timezone: UTC", "timezone: Nowhere/City"));
        assert!(has(&ps, "jobs[backup].cron"), "{ps:?}");
        assert!(has(&ps, "Nowhere/City"), "{ps:?}");
    }

    #[test]
    fn a_jobs_needs_cronjob_and_numbers_are_checked() {
        let ps = problems(&example_with(
            "{ type: nfs, resource: basestar-nfs }",
            "{ type: nfs, resource: nowhere }",
        ));
        assert!(
            has(&ps, "jobs[backup].needs[0]") && has(&ps, "unknown resource \"nowhere\""),
            "{ps:?}"
        );
        let ps = problems(&example_with(
            "{ type: nfs, resource: basestar-nfs }",
            "{ type: nfs, resource: cuda-central }",
        ));
        assert!(has(&ps, "is not a shared resource"), "{ps:?}");
        let ps = problems(&example_with(
            "      - { type: nfs, resource: basestar-nfs }\n    # Retries",
            "      - { type: nfs, resource: basestar-nfs }\n      - { type: nfs, resource: basestar-nfs }\n    # Retries",
        ));
        assert!(has(&ps, "needed twice"), "{ps:?}");
        let e = Config::from_yaml(&example_with(
            "{ type: nfs, resource: basestar-nfs }",
            "{ type: gpu, model: llm }",
        ))
        .unwrap_err();
        assert!(e.to_string().contains("unknown variant `gpu`"), "{e}");
        let ps = problems(&example_with(
            "cronjob: { namespace: backups, name: postgres-backup }",
            "cronjob: { namespace: Backups, name: pg_backup }",
        ));
        assert!(
            has(&ps, "jobs[backup].cronjob.namespace") && has(&ps, "jobs[backup].cronjob.name"),
            "{ps:?}"
        );
        let ps = problems(&example_with("retries: 1\n", "retries: 11\n"));
        assert!(has(&ps, "jobs[backup].retries"), "{ps:?}");
        for key in ["retry_backoff: 5m", "start_window: 30m", "deadline: 6h"] {
            let name = key.split(':').next().unwrap();
            for bad in ["0", "8d"] {
                let ps = problems(&example_with(key, &format!("{name}: {bad}")));
                assert!(
                    has(&ps, &format!("jobs[backup].{name}")),
                    "{key} {bad}: {ps:?}"
                );
            }
        }
        let e = Config::from_yaml(&example_with("missed_run: catch_up", "missed_run: later"))
            .unwrap_err();
        assert!(e.to_string().contains("unknown variant `later`"), "{e}");
    }

    #[test]
    fn jobs_need_a_source_that_lists_their_jobs() {
        // No source declares the capability.
        let ps = problems(&example_with(
            "    observe: [jobs]\n",
            "    observe: [mounts]\n",
        ));
        assert!(has(&ps, "no source declares the jobs capability"), "{ps:?}");
        // The CronJob's namespace is not listed.
        let ps = problems(&example_with(
            "job_namespaces: [backups]",
            "job_namespaces: [other]",
        ));
        assert!(
            has(&ps, "not in the jobs source's job_namespaces"),
            "{ps:?}"
        );
        // A jobs source must name namespaces.
        let ps = problems(&example_with(
            "job_namespaces: [backups]",
            "job_namespaces: []",
        ));
        assert!(
            has(&ps, "job_namespaces must name at least one namespace"),
            "{ps:?}"
        );
        // Only one source may declare it.
        let two = example_with(
            "  - id: scheduled-jobs\n",
            "  - id: more-jobs\n    kind: kubernetes_watch\n    observe: [jobs]\n    resync: 10s\n    params:\n      job_namespaces: [backups]\n\n  - id: scheduled-jobs\n",
        );
        let ps = problems(&two);
        assert!(
            has(&ps, "more than one source declares the jobs capability"),
            "{ps:?}"
        );
        // Its resync is held to the host-reading bound: a stale reading would block the hold.
        let ps = problems(&example_with(
            "    observe: [jobs]\n    resync: 10s\n",
            "    observe: [jobs]\n    resync: 5m\n",
        ));
        assert!(
            has(&ps, "exceeds policy.host_reading_stale_after"),
            "{ps:?}"
        );
        // A kind that cannot supply it.
        let ps = problems(&example_with(
            "  - id: scheduled-jobs\n    kind: kubernetes_watch\n",
            "  - id: scheduled-jobs\n    kind: tcp_probe\n",
        ));
        assert!(has(&ps, "cannot supply the jobs capability"), "{ps:?}");
        // No jobs at all needs no source.
        let none = example_with(
            "  - id: scheduled-jobs\n    kind: kubernetes_watch\n    observe: [jobs]\n    resync: 10s\n    params:\n      job_namespaces: [backups]\n",
            "",
        );
        let none = none.split("\n# Scheduled jobs").next().unwrap().to_string();
        assert!(Config::from_yaml(&none).unwrap().jobs().is_empty());
    }

    // ---- switched classes -------------------------------------------------------------

    const SWITCH_CLASS: &str = "  - id: app\n    requires: [ { resource: nfs } ]\n    priority: 0\n    preemptibility: kill\n    switch: { source: sw }\n";
    const SWITCH_MAP: &str = "map: { \"on\": on, \"off\": off, \"off-now\": off_now }";
    const SWITCH_SOURCE: &str = "  - { id: sw, kind: http_json, resync: 30s, observe: [switch], params: { requests: [ { url: 'http://x/s', value: /state, map: { \"on\": on, \"off\": off, \"off-now\": off_now } } ] } }\n";

    /// [`VALID`] with the switched class `app` and its source `sw` added.
    fn with_switch() -> String {
        VALID.replace(
            "\nsources:\n",
            &format!("\n{SWITCH_CLASS}sources:\n{SWITCH_SOURCE}"),
        )
    }

    /// [`with_switch`] with one piece of text replaced.
    fn switch_with(from: &str, to: &str) -> String {
        let y = with_switch();
        assert!(y.contains(from), "{from}");
        y.replace(from, to)
    }

    #[test]
    fn a_switched_class_with_a_switch_source_loads() {
        let c = Config::from_yaml(&with_switch()).unwrap_or_else(|e| panic!("{e}"));
        let k = c.class(&"app".into()).unwrap();
        assert_eq!(k.switch.as_ref().unwrap().source.as_str(), "sw");
        assert!(k.demand.is_none());
        assert!(c.source_observes(&"sw".into(), crate::Capability::Switch));
        // It needs no resource with a power section: a shared resource without one will do.
        let y = switch_with("resource: nfs } ]", "resource: blade } ]").replace(
            "resources:\n",
            "resources:\n  - { id: blade, kind: shared }\n",
        );
        assert!(Config::from_yaml(&y).is_ok(), "{:?}", problems(&y));
    }

    #[test]
    fn rejects_a_class_with_both_demand_and_switch() {
        let ps = problems(&switch_with(
            "    switch: { source: sw }\n",
            "    switch: { source: sw }\n    demand: { source: s1 }\n",
        ));
        assert!(
            has(
                &ps,
                "classes[app].switch: a class has demand or a switch, not both"
            ),
            "{ps:?}"
        );
    }

    #[test]
    fn rejects_a_switched_class_that_names_a_model_or_requires_nothing() {
        let ps = problems(&switch_with(
            "requires: [ { resource: nfs } ]",
            "requires: [ { resource: gpu, model: m1 } ]",
        ));
        assert!(
            has(&ps, "a switched class names no model on any requirement"),
            "{ps:?}"
        );
        let ps = problems(&switch_with(
            "requires: [ { resource: nfs } ]",
            "requires: []",
        ));
        assert!(
            has(&ps, "classes[app]: must require at least one resource"),
            "{ps:?}"
        );
    }

    #[test]
    fn rejects_a_switch_source_that_is_unknown_or_does_not_declare_switch() {
        let ps = problems(&switch_with(
            "switch: { source: sw }",
            "switch: { source: ghost }",
        ));
        assert!(
            has(
                &ps,
                "classes[app].switch: references unknown source \"ghost\""
            ),
            "{ps:?}"
        );
        // A demand source is not a switch source.
        let ps = problems(&switch_with(
            "switch: { source: sw }",
            "switch: { source: s1 }",
        ));
        assert!(
            has(&ps, "source \"s1\" does not declare the switch capability"),
            "{ps:?}"
        );
    }

    #[test]
    fn reads_from_names_a_switched_class_on_a_demand_source() {
        let c = Config::from_yaml(&switch_with(
            "{ id: s1, kind: http_json, resync: 30s, observe: [demand] }",
            "{ id: s1, kind: http_json, resync: 30s, observe: [demand], reads_from: app }",
        ))
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            c.source(&"s1".into()).unwrap().reads_from,
            Some("app".into())
        );
        // An unknown class.
        let ps = problems(&switch_with(
            "{ id: s1, kind: http_json, resync: 30s, observe: [demand] }",
            "{ id: s1, kind: http_json, resync: 30s, observe: [demand], reads_from: ghost }",
        ));
        assert!(
            has(
                &ps,
                "sources[s1].reads_from: references unknown class \"ghost\""
            ),
            "{ps:?}"
        );
        // A class that is not switched.
        let ps = problems(&switch_with(
            "{ id: s1, kind: http_json, resync: 30s, observe: [demand] }",
            "{ id: s1, kind: http_json, resync: 30s, observe: [demand], reads_from: c1 }",
        ));
        assert!(has(&ps, "class \"c1\" is not a switched class"), "{ps:?}");
        // A source that does not declare demand.
        let ps = problems(&switch_with(
            "{ id: hook, kind: webhook, resync: 30s }",
            "{ id: hook, kind: webhook, resync: 30s, reads_from: app }",
        ));
        assert!(
            has(
                &ps,
                "sources[hook].reads_from: only a source that declares the demand"
            ),
            "{ps:?}"
        );
    }

    #[test]
    fn only_http_json_can_supply_switch() {
        for kind in [
            "prometheus",
            "tcp_probe",
            "sql",
            "session",
            "kubernetes_watch",
        ] {
            let ps = problems(&switch_with(
                "{ id: sw, kind: http_json,",
                &format!("{{ id: sw, kind: {kind},"),
            ));
            assert!(
                has(&ps, "cannot supply the switch capability"),
                "{kind}: {ps:?}"
            );
        }
    }

    #[test]
    fn rejects_a_switch_source_that_is_not_one_value_read_through_a_map() {
        // Another capability beside switch.
        let ps = problems(&switch_with(
            "observe: [switch]",
            "observe: [switch, demand]",
        ));
        assert!(
            has(&ps, "must observe nothing else (it also declares demand)"),
            "{ps:?}"
        );
        // Two requests.
        let one = format!("{{ url: 'http://x/s', value: /state, {SWITCH_MAP} }}");
        let two = format!("{{ url: 'http://x/t', value: /state, {SWITCH_MAP} }}");
        let ps = problems(&switch_with(&one, &format!("{one}, {two}")));
        assert!(has(&ps, "exactly one request (it has 2)"), "{ps:?}");
        // No request at all.
        let ps = problems(&switch_with(&format!("[ {one} ]"), "[]"));
        assert!(has(&ps, "exactly one request (it has 0)"), "{ps:?}");
        // A count instead of value and map, and a value without a map.
        let ps = problems(&switch_with(
            &format!("value: /state, {SWITCH_MAP}"),
            "count: /n",
        ));
        assert!(has(&ps, "needs value and map"), "{ps:?}");
        let ps = problems(&switch_with(&format!(", {SWITCH_MAP}"), ""));
        assert!(has(&ps, "needs value and map"), "{ps:?}");
        assert!(has(&ps, "value needs a map"), "{ps:?}");
        // No params at all.
        let ps = problems(&switch_with(
            &format!(", params: {{ requests: [ {one} ] }}"),
            "",
        ));
        assert!(has(&ps, "needs params with exactly one request"), "{ps:?}");
        // Other request options do not combine with value.
        let ps = problems(&switch_with(
            "value: /state,",
            "value: /state, count: /n, when: { field: /a, equals: true },",
        ));
        assert!(has(&ps, "takes only url, value and map"), "{ps:?}");
        // A bad pointer.
        let ps = problems(&switch_with("value: /state", "value: state"));
        assert!(has(&ps, "must be empty or start with '/'"), "{ps:?}");
    }

    #[test]
    fn value_is_only_for_a_source_that_observes_switch() {
        let ps = problems(&VALID.replace(
            "{ id: s1, kind: http_json, resync: 30s, observe: [demand] }",
            "{ id: s1, kind: http_json, resync: 30s, observe: [demand], params: { requests: [ { url: 'http://x/s', count: /n, value: /state, map: { \"on\": on } } ] } }",
        ));
        assert!(
            has(&ps, "value is only valid on a source that observes switch"),
            "{ps:?}"
        );
    }

    #[test]
    fn map_values_must_be_switch_states_or_numbers_as_the_request_needs() {
        // Switch source: a number, an unknown word.
        let ps = problems(&switch_with("\"on\": on,", "\"on\": 1,"));
        assert!(
            has(
                &ps,
                "map[\"on\"] must be a switch state (on, off or off_now), not a number"
            ),
            "{ps:?}"
        );
        for bad in ["maybe", "off-now", "true"] {
            let ps = problems(&switch_with("\"on\": on,", &format!("\"on\": {bad},")));
            assert!(has(&ps, "invalid params"), "{bad}: {ps:?}");
        }
        let ps = problems(&switch_with(SWITCH_MAP, "map: {}"));
        assert!(has(&ps, "map must not be empty"), "{ps:?}");

        // Demand source: numbers are fine; a switch state, a negative, a fraction are not.
        let demand = |map: &str| {
            VALID.replace(
                "{ id: s1, kind: http_json, resync: 30s, observe: [demand] }",
                &format!("{{ id: s1, kind: http_json, resync: 30s, observe: [demand], params: {{ requests: [ {{ url: 'http://x/s', count: /on, map: {map}, also: [ {{ count: /q, map: {map} }} ] }} ] }} }}"),
            )
        };
        let ok = demand("{ \"true\": 1, \"false\": 0 }");
        assert!(Config::from_yaml(&ok).is_ok(), "{:?}", problems(&ok));
        let ps = problems(&demand("{ \"true\": on }"));
        assert!(
            has(
                &ps,
                "requests[0].map[\"true\"] must be a non-negative number, not a switch state"
            ),
            "{ps:?}"
        );
        assert!(
            has(&ps, "also[0].map[\"true\"] must be a non-negative number"),
            "{ps:?}"
        );
        let ps = problems(&demand("{ \"true\": -1 }"));
        assert!(has(&ps, "-1 is negative"), "{ps:?}");
        let ps = problems(&demand("{ \"true\": 1.5 }"));
        assert!(has(&ps, "invalid type"), "{ps:?}");
        // A map needs a count (or value) to apply to.
        let ps = problems(&VALID.replace(
            "{ id: s1, kind: http_json, resync: 30s, observe: [demand] }",
            "{ id: s1, kind: http_json, resync: 30s, observe: [demand], params: { requests: [ { url: 'http://x/s', length: /l, map: { \"a\": 1 } } ] } }",
        ));
        assert!(has(&ps, "map is only valid with count or value"), "{ps:?}");
    }
}
