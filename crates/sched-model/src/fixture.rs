//! Snapshot fixtures: recorded or synthetic snapshots in YAML, for replaying through the
//! decision core. Pure and string-based; reading files is left to the caller (or to the
//! `test-support` feature).
//!
//! A checked fixture is consistent with its config: every reference resolves, observations
//! come from the source the config says supplies them, and the values agree with each
//! other. At most one observation is allowed per source and subject (class, model or
//! resource), so which one wins is never a question; the recorder keeps only the latest.

use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::config::{Config, ResourceKind};
use crate::ids::{ResourceId, SourceId};
use crate::lease::LeaseKind;
use crate::observation::{ObservedValue, Snapshot};
use crate::validate::Problem;

#[derive(Debug, thiserror::Error)]
pub enum FixtureError {
    #[error("cannot parse fixture: {0}")]
    Parse(#[from] serde_yaml_ng::Error),
    #[error("{}", format_problems(.0))]
    Inconsistent(Vec<Problem>),
}

fn format_problems(problems: &[Problem]) -> String {
    let mut out = format!(
        "fixture disagrees with its config ({} problem(s)):",
        problems.len()
    );
    for p in problems {
        out.push_str("\n  - ");
        out.push_str(&p.to_string());
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fixture {
    /// What situation the snapshot captures.
    pub description: String,
    pub snapshot: Snapshot,
}

impl Fixture {
    /// Parses a fixture without checking it against a config.
    pub fn from_yaml(text: &str) -> Result<Fixture, FixtureError> {
        Ok(serde_yaml_ng::from_str(text)?)
    }

    /// Parses a fixture and checks it against `config`.
    pub fn from_yaml_checked(text: &str, config: &Config) -> Result<Fixture, FixtureError> {
        let fixture = Self::from_yaml(text)?;
        let problems = fixture.check(config);
        if problems.is_empty() {
            Ok(fixture)
        } else {
            Err(FixtureError::Inconsistent(problems))
        }
    }

    /// Every way this fixture disagrees with `config`.
    pub fn check(&self, config: &Config) -> Vec<Problem> {
        let mut out = Vec::new();
        let mut add = |path: &str, message: String| {
            out.push(Problem {
                path: path.to_string(),
                message,
            })
        };
        let snap = &self.snapshot;

        let mut seen: HashSet<(&SourceId, String)> = HashSet::new();
        for (i, o) in snap.observations.iter().enumerate() {
            let at = format!("snapshot.observations[{i}]");
            let source = config.source(&o.source);
            if source.is_none() {
                add(&at, format!("unknown source {:?}", o.source.as_str()));
            }
            if o.observed_at > snap.now {
                add(
                    &at,
                    format!(
                        "observed_at ({}) is after now ({})",
                        o.observed_at, snap.now
                    ),
                );
            }
            let subject = match &o.value {
                ObservedValue::Demand { class, .. } => format!("demand:{class}"),
                ObservedValue::ModelPods { model, .. } => format!("model_pods:{model}"),
                ObservedValue::ResourceUp { resource, .. } => format!("resource_up:{resource}"),
            };
            if !seen.insert((&o.source, subject.clone())) {
                add(
                    &at,
                    format!(
                        "duplicate observation of {subject} from source {:?}",
                        o.source.as_str()
                    ),
                );
            }
            match &o.value {
                ObservedValue::Demand {
                    class,
                    ready,
                    oldest_ready_at,
                    ..
                } => {
                    match config.class(class) {
                        None => add(&at, format!("unknown class {:?}", class.as_str())),
                        Some(spec) => match &spec.demand {
                            None => add(
                                &at,
                                format!(
                                    "class {:?} has no demand source, so it cannot have demand observations",
                                    class.as_str()
                                ),
                            ),
                            Some(d) if d.source != o.source => add(
                                &at,
                                format!(
                                    "class {:?} takes demand from source {:?}, not {:?}",
                                    class.as_str(),
                                    d.source.as_str(),
                                    o.source.as_str()
                                ),
                            ),
                            Some(_) => {}
                        },
                    }
                    if let Some(s) = source
                        && !s.kind.can_supply_demand()
                    {
                        add(
                            &at,
                            format!("source {:?} cannot supply demand", o.source.as_str()),
                        );
                    }
                    if *ready == 0 && oldest_ready_at.is_some() {
                        add(
                            &at,
                            "oldest_ready_at is set but nothing is ready".to_string(),
                        );
                    }
                    if let Some(t) = oldest_ready_at
                        && *t > o.observed_at
                    {
                        add(
                            &at,
                            format!(
                                "oldest_ready_at ({t}) is after observed_at ({})",
                                o.observed_at
                            ),
                        );
                    }
                }
                ObservedValue::ModelPods {
                    model,
                    pods,
                    ready_pods,
                } => {
                    if config.model(model).is_none() {
                        add(&at, format!("unknown model {:?}", model.as_str()));
                    }
                    if ready_pods > pods {
                        add(
                            &at,
                            format!("ready_pods ({ready_pods}) exceeds pods ({pods})"),
                        );
                    }
                }
                ObservedValue::ResourceUp { resource, .. } => match config.resource(resource) {
                    None => add(&at, format!("unknown resource {:?}", resource.as_str())),
                    Some(r) if r.power.is_none() => add(
                        &at,
                        format!(
                            "resource {:?} has no power section (always on), so its up state is not observed",
                            resource.as_str()
                        ),
                    ),
                    Some(_) => {}
                },
            }
        }

        let mut ids = HashSet::new();
        let mut manual_models: BTreeMap<&ResourceId, &crate::ids::ModelId> = BTreeMap::new();
        for (i, l) in snap.leases.iter().enumerate() {
            let at = format!("snapshot.leases[{i}]");
            if !ids.insert(&l.id) {
                add(&at, format!("duplicate lease id {:?}", l.id.as_str()));
            }
            if l.granted_at > snap.now {
                add(
                    &at,
                    format!("granted_at ({}) is after now ({})", l.granted_at, snap.now),
                );
            }
            let Some(res) = config.resource(&l.resource) else {
                add(&at, format!("unknown resource {:?}", l.resource.as_str()));
                continue;
            };
            match &l.kind {
                LeaseKind::SharedHold { .. } if res.kind != ResourceKind::Shared => add(
                    &at,
                    format!(
                        "shared_hold on non-shared resource {:?}",
                        l.resource.as_str()
                    ),
                ),
                LeaseKind::ManualHost if res.power.is_none() => add(
                    &at,
                    format!(
                        "manual_host on resource {:?} that has no power section",
                        l.resource.as_str()
                    ),
                ),
                LeaseKind::ManualModel { model } | LeaseKind::Interactive { model, .. } => {
                    match config.model(model) {
                        None => add(&at, format!("unknown model {:?}", model.as_str())),
                        Some(m) if m.resource != l.resource => add(
                            &at,
                            format!(
                                "model {:?} does not belong to resource {:?}",
                                model.as_str(),
                                l.resource.as_str()
                            ),
                        ),
                        Some(_) => {}
                    }
                    if let LeaseKind::Interactive { expires_at, .. } = &l.kind
                        && *expires_at <= l.granted_at
                    {
                        add(
                            &at,
                            format!(
                                "expires_at ({expires_at}) is not after granted_at ({})",
                                l.granted_at
                            ),
                        );
                    }
                    if let LeaseKind::ManualModel { model } = &l.kind {
                        match manual_models.get(&l.resource) {
                            Some(prev) if *prev != model => add(
                                &at,
                                format!(
                                    "manual_model leases for different models ({:?} and {:?}) on resource {:?} conflict",
                                    prev.as_str(),
                                    model.as_str(),
                                    l.resource.as_str()
                                ),
                            ),
                            _ => {
                                manual_models.insert(&l.resource, model);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        out
    }
}
