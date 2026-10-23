//! Whole-config validation. Problems are collected, not short-circuited, so one load
//! reports everything that is wrong.

use std::collections::HashSet;
use std::fmt;

use crate::config::{Config, ResourceKind, SourceKind};
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
        } else if !seen.insert(id) {
            p.add(format!("{section}[{id}]"), format!("duplicate id {id:?}"));
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
    models(c, &mut p);
    classes(c, &mut p);
    sources(c, &mut p);
    p.0
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
        ("demand_max_age", pol.demand_max_age),
        ("safety_max_age", pol.safety_max_age),
        ("interactive_ttl", pol.interactive_ttl),
        ("drain_timeout", pol.drain_timeout),
    ];
    for (name, value) in positive {
        if value == Millis::ZERO {
            p.add(format!("policy.{name}"), "must be greater than zero");
        }
    }
    if pol.safety_max_age > pol.demand_max_age {
        p.add(
            "policy",
            format!(
                "safety_max_age ({}) is greater than demand_max_age ({}); safety-critical data must be at least as fresh as scheduling inputs",
                pol.safety_max_age, pol.demand_max_age
            ),
        );
    }
}

fn models(c: &Config, p: &mut Problems) {
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
                (ResourceKind::Exclusive, None) => p.add(
                    &rat,
                    format!(
                        "resource {:?} is exclusive; a model is required",
                        req.resource.as_str()
                    ),
                ),
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
        if let Some(d) = &k.demand
            && c.source(&d.source).is_none()
        {
            p.add(
                format!("{at}.demand"),
                format!("references unknown source {:?}", d.source.as_str()),
            );
        }
        if let Some(r) = &k.recovery {
            if r.action.trim().is_empty() {
                p.add(format!("{at}.recovery"), "action must not be empty");
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
    for s in c.sources() {
        let at = format!("sources[{}]", s.id);
        if s.resync == Millis::ZERO {
            p.add(&at, "resync must be greater than zero");
            continue;
        }
        if s.resync > pol.demand_max_age {
            p.add(
                &at,
                format!(
                    "resync ({}) exceeds policy.demand_max_age ({}); its observations would always be stale",
                    s.resync, pol.demand_max_age
                ),
            );
        }
        if s.kind == SourceKind::KubernetesWatch && s.resync > pol.safety_max_age {
            p.add(
                &at,
                format!(
                    "resync ({}) exceeds policy.safety_max_age ({}); safety-critical observations would always be stale",
                    s.resync, pol.safety_max_age
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
  demand_max_age: 5m
  safety_max_age: 30s
  interactive_ttl: 10m
  drain_timeout: 5m
resources:
  - { id: gpu, kind: exclusive, power: { actuator: wol } }
  - { id: nfs, kind: shared, power: { actuator: ssh } }
models:
  - { id: m1, resource: gpu, load_time: 60s, workloads: [w1] }
classes:
  - id: c1
    requires: [ { resource: gpu, model: m1 }, { resource: nfs } ]
    priority: 10
    preemptibility: kill
    demand: { source: s1 }
    recovery: { action: requeue }
sources:
  - { id: s1, kind: http_json, resync: 30s }
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
                "  - { id: nfs, kind: shared, power: { actuator: ssh } }",
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
    fn rejects_inconsistent_policy_and_reports_all() {
        let y = VALID
            .replace("min_dwell: 1m", "min_dwell: 2h")
            .replace("safety_max_age: 30s", "safety_max_age: 10m")
            .replace("drain_timeout: 5m", "drain_timeout: 0");
        let ps = problems(&y);
        assert!(
            has(
                &ps,
                "min_dwell (7200000ms) is greater than max_dwell (3600000ms)"
            ),
            "{ps:?}"
        );
        assert!(has(&ps, "safety_max_age"), "{ps:?}");
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
        assert!(has(&ps, "exclusive; a model is required"), "{ps:?}");
        assert!(
            has(&ps, "is shared; model \"m1\" must not be given"),
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
                "{ id: gpu, kind: exclusive, power: { actuator: wol } }",
                "{ id: gpu, kind: exclusive }",
            )
            .replace(
                "{ id: nfs, kind: shared, power: { actuator: ssh } }",
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
        assert!(has(&ps, "exceeds policy.demand_max_age"), "{ps:?}");
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
}
