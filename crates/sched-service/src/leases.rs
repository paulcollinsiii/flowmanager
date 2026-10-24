//! Manual model leases as the user requests them: acquired for a model, released by id,
//! listed. The book is plain data that the `Scheduler` object keeps in its Restate state;
//! the rules here are pure, so they can be tested without Restate.
//!
//! Rules:
//! - The model must be in the active config; the lease is on the model's resource.
//! - At most one manual model lease is held per exclusive resource. Asking again for the
//!   model already leased returns that lease; asking for another model of the same
//!   resource is refused until the first is released.
//! - Lease ids come from a counter kept in the book, so they are deterministic under
//!   Restate's replay.
//!
//! Granting a lease only records the request. What it does (cutting batch work short and
//! pinning the model) is the planner's and sequencer's decision on the next tick.

use std::fmt;

use sched_model::{Config, Lease, LeaseId, LeaseKind, Millis, ModelId, ResourceKind};
use serde::{Deserialize, Serialize};

/// Prefix of the ids of manual model leases.
pub const MANUAL_MODEL_PREFIX: &str = "manual-model-";

/// Every lease the scheduler holds, and the counter its ids are drawn from.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseBook {
    /// The number of the next lease id.
    #[serde(default)]
    pub next: u64,
    #[serde(default)]
    pub leases: Vec<Lease>,
}

/// The answer to an acquire request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseGrant {
    pub lease: Lease,
    /// False if the same model was already leased and that lease is returned.
    pub created: bool,
}

/// Why a lease request was refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LeaseError {
    /// The model is not in the active config.
    UnknownModel { model: ModelId, known: Vec<ModelId> },
    /// The model's resource is not an exclusive resource.
    NotExclusive { model: ModelId },
    /// Another model of the same resource already has a manual lease.
    Conflict { requested: ModelId, held: Lease },
    /// No lease has this id.
    UnknownLease { id: LeaseId },
}

impl LeaseError {
    /// The HTTP status the ingress answers with.
    pub fn status(&self) -> u16 {
        match self {
            LeaseError::UnknownModel { .. } | LeaseError::NotExclusive { .. } => 400,
            LeaseError::Conflict { .. } => 409,
            LeaseError::UnknownLease { .. } => 404,
        }
    }
}

impl fmt::Display for LeaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LeaseError::UnknownModel { model, known } => {
                let known: Vec<&str> = known.iter().map(ModelId::as_str).collect();
                write!(
                    f,
                    "unknown model {:?}: the active config has no such model (configured: {})",
                    model.as_str(),
                    known.join(", ")
                )
            }
            LeaseError::NotExclusive { model } => write!(
                f,
                "model {:?} does not belong to an exclusive resource",
                model.as_str()
            ),
            LeaseError::Conflict { requested, held } => {
                let held_model = match &held.kind {
                    LeaseKind::ManualModel { model } => model.as_str(),
                    _ => "?",
                };
                write!(
                    f,
                    "cannot lease {:?}: {} already has manual model lease {:?} for {:?}; release it first",
                    requested.as_str(),
                    held.resource,
                    held.id.as_str(),
                    held_model
                )
            }
            LeaseError::UnknownLease { id } => {
                write!(f, "no lease with id {:?}", id.as_str())
            }
        }
    }
}

impl std::error::Error for LeaseError {}

impl LeaseBook {
    /// Acquires a manual model lease on `model`, granted at `now`.
    pub fn acquire_manual_model(
        &mut self,
        config: &Config,
        model: &ModelId,
        now: Millis,
    ) -> Result<LeaseGrant, LeaseError> {
        let Some(spec) = config.model(model) else {
            return Err(LeaseError::UnknownModel {
                model: model.clone(),
                known: config.models().iter().map(|m| m.id.clone()).collect(),
            });
        };
        if config
            .resource(&spec.resource)
            .is_none_or(|r| r.kind != ResourceKind::Exclusive)
        {
            return Err(LeaseError::NotExclusive {
                model: model.clone(),
            });
        }
        let held = self.leases.iter().find(|l| {
            l.resource == spec.resource && matches!(l.kind, LeaseKind::ManualModel { .. })
        });
        if let Some(held) = held {
            return match &held.kind {
                LeaseKind::ManualModel { model: m } if m == model => Ok(LeaseGrant {
                    lease: held.clone(),
                    created: false,
                }),
                _ => Err(LeaseError::Conflict {
                    requested: model.clone(),
                    held: held.clone(),
                }),
            };
        }
        self.next += 1;
        let lease = Lease {
            id: LeaseId::new(format!("{MANUAL_MODEL_PREFIX}{}", self.next)),
            resource: spec.resource.clone(),
            kind: LeaseKind::ManualModel {
                model: model.clone(),
            },
            granted_at: now,
        };
        self.leases.push(lease.clone());
        Ok(LeaseGrant {
            lease,
            created: true,
        })
    }

    /// Releases the lease with `id` and returns it.
    pub fn release(&mut self, id: &LeaseId) -> Result<Lease, LeaseError> {
        let Some(at) = self.leases.iter().position(|l| &l.id == id) else {
            return Err(LeaseError::UnknownLease { id: id.clone() });
        };
        Ok(self.leases.remove(at))
    }
}

/// Splits `leases` into those the core is given under `config` and the ids of those it
/// ignores because their resource or model is no longer configured (or the model no
/// longer belongs to the lease's resource).
pub fn effective(config: &Config, leases: &[Lease]) -> (Vec<Lease>, Vec<LeaseId>) {
    let mut keep = Vec::new();
    let mut ignored = Vec::new();
    for l in leases {
        let resource_ok = config.resource(&l.resource).is_some();
        let model_ok = match &l.kind {
            LeaseKind::ManualModel { model } | LeaseKind::Interactive { model, .. } => config
                .model(model)
                .is_some_and(|m| m.resource == l.resource),
            _ => true,
        };
        if resource_ok && model_ok {
            keep.push(l.clone());
        } else {
            ignored.push(l.id.clone());
        }
    }
    (keep, ignored)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sched_model::test_support::{EXAMPLE_CONFIG, example_config};

    #[test]
    fn a_lease_is_granted_for_a_configured_model_on_its_resource_with_a_counter_id() {
        let c = example_config();
        let mut b = LeaseBook::default();
        let g = b
            .acquire_manual_model(&c, &"llm".into(), Millis(7))
            .unwrap();
        assert!(g.created);
        assert_eq!(g.lease.id.as_str(), "manual-model-1");
        assert_eq!(g.lease.resource.as_str(), "cuda-central");
        assert_eq!(g.lease.granted_at, Millis(7));
        assert_eq!(
            g.lease.kind,
            LeaseKind::ManualModel {
                model: "llm".into()
            }
        );
        assert_eq!(b.leases, vec![g.lease.clone()]);
        // The same id comes out of the same book every time: no randomness or clock.
        let mut again = LeaseBook::default();
        assert_eq!(
            again
                .acquire_manual_model(&c, &"llm".into(), Millis(7))
                .unwrap(),
            g
        );
    }

    #[test]
    fn the_same_model_again_returns_the_existing_lease() {
        let c = example_config();
        let mut b = LeaseBook::default();
        let first = b
            .acquire_manual_model(&c, &"llm".into(), Millis(1))
            .unwrap();
        let second = b
            .acquire_manual_model(&c, &"llm".into(), Millis(2))
            .unwrap();
        assert!(!second.created);
        assert_eq!(second.lease, first.lease);
        assert_eq!(b.leases.len(), 1);
        assert_eq!(b.next, 1);
    }

    #[test]
    fn another_model_on_the_same_resource_is_refused_until_the_first_is_released() {
        let c = example_config();
        let mut b = LeaseBook::default();
        let first = b
            .acquire_manual_model(&c, &"llm".into(), Millis(1))
            .unwrap();
        let e = b
            .acquire_manual_model(&c, &"whisperx".into(), Millis(2))
            .unwrap_err();
        assert_eq!(e.status(), 409);
        let text = e.to_string();
        assert!(
            text.contains("manual-model-1")
                && text.contains("\"llm\"")
                && text.contains("release it first"),
            "{text}"
        );
        assert_eq!(b.leases.len(), 1);
        assert_eq!(b.release(&first.lease.id).unwrap(), first.lease);
        let g = b
            .acquire_manual_model(&c, &"whisperx".into(), Millis(3))
            .unwrap();
        assert_eq!(g.lease.id.as_str(), "manual-model-2");
    }

    #[test]
    fn unknown_models_and_lease_ids_are_refused_with_clear_errors() {
        let c = example_config();
        let mut b = LeaseBook::default();
        let e = b
            .acquire_manual_model(&c, &"gpt-17".into(), Millis(1))
            .unwrap_err();
        assert_eq!(e.status(), 400);
        let text = e.to_string();
        assert!(
            text.contains("unknown model \"gpt-17\"") && text.contains("mineru, llm"),
            "{text}"
        );
        assert!(b.leases.is_empty() && b.next == 0);
        let e = b.release(&"nope".into()).unwrap_err();
        assert_eq!(e.status(), 404);
        assert_eq!(e.to_string(), "no lease with id \"nope\"");
    }

    #[test]
    fn leases_whose_model_left_the_config_are_ignored_not_given_to_the_core() {
        let c = example_config();
        let mut b = LeaseBook::default();
        b.acquire_manual_model(&c, &"whisperx".into(), Millis(1))
            .unwrap();
        let (keep, ignored) = effective(&c, &b.leases);
        assert_eq!((keep.len(), ignored.len()), (1, 0));
        // A config without whisperx (and without the class that needs it).
        let y = EXAMPLE_CONFIG
            .replace(
                "  - { id: whisperx, resource: cuda-central, load_time: 60s, workloads: [whisperx] }\n",
                "",
            )
            .replace("model: whisperx }", "model: llm }");
        assert_ne!(y, EXAMPLE_CONFIG);
        let c2 = Config::from_yaml(&y).unwrap();
        let (keep, ignored) = effective(&c2, &b.leases);
        assert!(keep.is_empty());
        assert_eq!(ignored, vec![LeaseId::new("manual-model-1")]);
    }

    #[test]
    fn the_book_round_trips_through_json_as_restate_stores_it() {
        let c = example_config();
        let mut b = LeaseBook::default();
        b.acquire_manual_model(&c, &"mineru".into(), Millis(5))
            .unwrap();
        let text = serde_json::to_string(&b).unwrap();
        let back: LeaseBook = serde_json::from_str(&text).unwrap();
        assert_eq!(back, b);
    }
}
