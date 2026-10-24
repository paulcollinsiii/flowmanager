//! Whole-config validation. Problems are collected, not short-circuited, so one load
//! reports everything that is wrong.

use std::collections::HashSet;
use std::fmt;

use crate::config::{Capability, Config, MAX_LEASE_DURATION, ResourceKind};
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
    for (name, value) in [
        ("interactive_ttl", pol.interactive_ttl),
        ("drain_timeout", pol.drain_timeout),
    ]
    .into_iter()
    .chain(pol.reclaim_timeout.map(|t| ("reclaim_timeout", t)))
    {
        if value > MAX_LEASE_DURATION {
            p.add(
                format!("policy.{name}"),
                format!("{value} exceeds the maximum of {MAX_LEASE_DURATION} (1 day)"),
            );
        }
    }
    if pol.reclaim_timeout == Some(Millis::ZERO) {
        p.add("policy.reclaim_timeout", "must be greater than zero");
    }
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
        if let Some(pw) = &r.power
            && pw.actuator.trim().is_empty()
        {
            p.add(
                format!("resources[{}].power", r.id),
                "actuator must not be empty",
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
        if let Some(t) = k.kill_timeout
            && (t == Millis::ZERO || t > MAX_LEASE_DURATION)
        {
            p.add(
                format!("{at}.kill_timeout"),
                format!("{t} must be greater than zero and at most {MAX_LEASE_DURATION}"),
            );
        }
        if let Some(r) = &k.recovery {
            if r.action.trim().is_empty() {
                p.add(format!("{at}.recovery"), "action must not be empty");
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
    // load. Pod counts are not checked: no decision reads them.
    for r in c.resources().iter().filter(|r| r.power.is_some()) {
        if !declared(Capability::Reachability) {
            p.add(
                format!("resources[{}]", r.id),
                "has a power section but no source declares the reachability capability (observe: [reachability]), so its host state could never be observed",
            );
        }
    }
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
        if let Ok(Some(prom)) = s.prometheus_params()
            && c.resource(&prom.resource).is_none()
        {
            p.add(
                format!("{at}.params.resource"),
                format!("unknown resource \"{}\"", prom.resource),
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
        if (s.observe.contains(&Capability::Pods) || s.observe.contains(&Capability::Reachability))
            && s.resync > pol.safety_max_age
        {
            p.add(
                &at,
                format!(
                    "resync ({}) exceeds policy.safety_max_age ({}); its pods/reachability observations would always be stale",
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
  - { id: gpu2, kind: exclusive }
models:
  - { id: m1, resource: gpu, load_time: 60s, workloads: [w1] }
  - { id: m2, resource: gpu2, load_time: 60s, workloads: [w2] }
classes:
  - id: c1
    requires: [ { resource: gpu, model: m1 }, { resource: nfs } ]
    priority: 10
    preemptibility: kill
    demand: { source: s1 }
    recovery: { action: requeue }
sources:
  - { id: s1, kind: http_json, resync: 30s, observe: [demand] }
  - { id: k8s, kind: kubernetes_watch, resync: 10s, observe: [pods, reachability] }
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
    fn rejects_recovery_source_that_dangles() {
        let y = VALID.replace(
            "recovery: { action: requeue }",
            "recovery: { action: requeue, source: ghost }",
        );
        assert!(has(
            &problems(&y),
            "classes[c1].recovery: references unknown source \"ghost\""
        ));
        let ok = VALID.replace(
            "recovery: { action: requeue }",
            "recovery: { action: requeue, source: s1 }",
        );
        assert!(Config::from_yaml(&ok).is_ok());
        let empty = VALID.replace("{ action: requeue }", "{ action: \"\" }");
        assert!(has(&problems(&empty), "action must not be empty"));
    }

    #[test]
    fn a_config_where_no_source_declares_pods_is_accepted() {
        // Pod counts are information only: no decision waits for them, so a config without
        // a pods source is valid (it used to be refused, since every allow waited on them).
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
            (Sql, [false, false, true]),
            (Schedule, [false, false, true]),
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
        // safety_max_age is 30s in VALID; 45s is within demand_max_age.
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
                    "sources[probe]: resync (45000ms) exceeds policy.safety_max_age"
                ),
                "{observe}: {ps:?}"
            );
        }
        // A demand-only source is held only to demand_max_age.
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
        for kind in ["sql", "prometheus", "schedule", "session"] {
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
    fn rejects_zero_load_time_and_empty_actuator() {
        let ps = problems(&VALID.replace(
            "load_time: 60s, workloads: [w1]",
            "load_time: 0, workloads: [w1]",
        ));
        assert!(
            has(&ps, "models[m1]: load_time must be greater than zero"),
            "{ps:?}"
        );
        let ps = problems(&VALID.replace("actuator: wol", "actuator: \"\""));
        assert!(
            has(&ps, "resources[gpu].power: actuator must not be empty"),
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
        assert!(has(&ps, "exceeds policy.safety_max_age"), "{ps:?}");
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
            "drain_timeout: 5m\n  reclaim_timeout: 1d",
        );
        assert!(Config::from_yaml(&ok).is_ok());
    }

    #[test]
    fn reclaim_and_kill_timeouts_are_validated() {
        let y = VALID.replace(
            "drain_timeout: 5m",
            "drain_timeout: 5m\n  reclaim_timeout: 0",
        );
        assert!(has(
            &problems(&y),
            "policy.reclaim_timeout: must be greater than zero"
        ));
        let y = VALID.replace(
            "drain_timeout: 5m",
            "drain_timeout: 5m\n  reclaim_timeout: 2d",
        );
        assert!(has(&problems(&y), "policy.reclaim_timeout"));
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
        assert_eq!(c.policy().reclaim_timeout(), c.policy().drain_timeout);
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
}
