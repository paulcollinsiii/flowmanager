//! Snapshot fixtures: recorded or synthetic snapshots in YAML, for replaying through the
//! decision core. Pure and string-based; reading files is left to the caller (or to the
//! `test-support` feature).

use serde::{Deserialize, Serialize};

use crate::config::{Config, ConfigError, ResourceKind};
use crate::lease::LeaseKind;
use crate::observation::{ObservedValue, Snapshot};
use crate::validate::Problem;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fixture {
    /// What situation the snapshot captures.
    pub description: String,
    pub snapshot: Snapshot,
}

impl Fixture {
    /// Parses a fixture without checking it against a config.
    pub fn from_yaml(text: &str) -> Result<Fixture, ConfigError> {
        Ok(serde_yaml_ng::from_str(text)?)
    }

    /// Parses a fixture and checks every reference against `config`.
    pub fn from_yaml_checked(text: &str, config: &Config) -> Result<Fixture, ConfigError> {
        let fixture = Self::from_yaml(text)?;
        let problems = fixture.check(config);
        if problems.is_empty() {
            Ok(fixture)
        } else {
            Err(ConfigError::Invalid(problems))
        }
    }

    /// Every way this fixture disagrees with `config`.
    pub fn check(&self, config: &Config) -> Vec<Problem> {
        let mut out = Vec::new();
        let mut add = |path: String, message: String| out.push(Problem { path, message });
        let snap = &self.snapshot;

        for (i, o) in snap.observations.iter().enumerate() {
            let at = format!("snapshot.observations[{i}]");
            if config.source(&o.source).is_none() {
                add(
                    at.clone(),
                    format!("unknown source {:?}", o.source.as_str()),
                );
            }
            if o.observed_at > snap.now {
                add(
                    at.clone(),
                    format!(
                        "observed_at ({}) is after now ({})",
                        o.observed_at, snap.now
                    ),
                );
            }
            match &o.value {
                ObservedValue::Demand { class, .. } if config.class(class).is_none() => {
                    add(at, format!("unknown class {:?}", class.as_str()))
                }
                ObservedValue::ModelPods { model, .. } if config.model(model).is_none() => {
                    add(at, format!("unknown model {:?}", model.as_str()))
                }
                ObservedValue::ResourceUp { resource, .. }
                    if config.resource(resource).is_none() =>
                {
                    add(at, format!("unknown resource {:?}", resource.as_str()))
                }
                _ => {}
            }
        }

        let mut ids = std::collections::HashSet::new();
        for (i, l) in snap.leases.iter().enumerate() {
            let at = format!("snapshot.leases[{i}]");
            if !ids.insert(&l.id) {
                add(
                    at.clone(),
                    format!("duplicate lease id {:?}", l.id.as_str()),
                );
            }
            let Some(res) = config.resource(&l.resource) else {
                add(at, format!("unknown resource {:?}", l.resource.as_str()));
                continue;
            };
            match &l.kind {
                LeaseKind::SharedHold { .. } if res.kind != ResourceKind::Shared => add(
                    at,
                    format!(
                        "shared_hold on non-shared resource {:?}",
                        l.resource.as_str()
                    ),
                ),
                LeaseKind::ManualHost if res.power.is_none() => add(
                    at,
                    format!(
                        "manual_host on resource {:?} that has no power section",
                        l.resource.as_str()
                    ),
                ),
                LeaseKind::ManualModel { model } | LeaseKind::Interactive { model, .. } => {
                    match config.model(model) {
                        None => add(at, format!("unknown model {:?}", model.as_str())),
                        Some(m) if m.resource != l.resource => add(
                            at,
                            format!(
                                "model {:?} does not belong to resource {:?}",
                                model.as_str(),
                                l.resource.as_str()
                            ),
                        ),
                        Some(_) => {}
                    }
                }
                _ => {}
            }
        }
        out
    }
}
