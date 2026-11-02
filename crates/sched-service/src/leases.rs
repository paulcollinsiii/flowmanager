//! Manual leases as the user requests them: on a model, on a shared host, a lockout of an
//! exclusive resource; released by id (or by resource for a host lease and a lockout),
//! listed. The book is plain data that the `Scheduler` object keeps in its Restate state;
//! the rules here are pure, so they can be tested without Restate.
//!
//! Rules:
//! - The model must be in the active config; the lease is on the model's resource.
//! - At most one manual model lease is held per exclusive resource. Asking again for the
//!   model already leased returns that lease; asking for another model of the same
//!   resource is refused until the first is released.
//! - A manual host lease is for a resource (shared or exclusive) with a power section, a lockout for an
//!   exclusive resource with a power section (`Config::supports_lockout`); each is held at
//!   most once per resource, and asking again returns the lease held.
//! - Lease ids come from a counter kept in the book, so they are deterministic under
//!   Restate's replay.
//!
//! Granting a lease only records the request. What it does (cutting batch work short and
//! pinning the model) is the planner's and sequencer's decision on the next tick.

use std::fmt;

use sched_model::{Config, Lease, LeaseId, LeaseKind, Millis, ModelId, ResourceId, ResourceKind};
use serde::{Deserialize, Serialize};

/// Prefix of the ids of manual model leases.
pub const MANUAL_MODEL_PREFIX: &str = "manual-model-";
/// Prefix of the ids of manual host leases.
pub const MANUAL_HOST_PREFIX: &str = "manual-host-";
/// Prefix of the ids of lockout leases.
pub const LOCKOUT_PREFIX: &str = "lockout-";

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
    /// The resource is not in the active config.
    UnknownResource {
        resource: ResourceId,
        known: Vec<ResourceId>,
    },
    /// A drain is for a shared resource; an exclusive host is taken down by releasing its
    /// host lease.
    NotShared { resource: ResourceId },
    /// The resource has no power section: it is always on, so a lease on it would do nothing.
    NoPowerSection { resource: ResourceId },
    /// A lockout names a resource that cannot be locked out: only an exclusive resource with a
    /// power section can be taken back and shut down.
    NotLockable { resource: ResourceId },
    /// The resource holds no lease of the kind that was asked to be released.
    NoLeaseOnResource { resource: ResourceId, kind: String },
    /// No lease has this id.
    UnknownLease { id: LeaseId },
}

impl LeaseError {
    /// The HTTP status the ingress answers with.
    pub fn status(&self) -> u16 {
        match self {
            LeaseError::UnknownModel { .. }
            | LeaseError::NotExclusive { .. }
            | LeaseError::UnknownResource { .. }
            | LeaseError::NotShared { .. }
            | LeaseError::NoPowerSection { .. }
            | LeaseError::NotLockable { .. } => 400,
            LeaseError::Conflict { .. } => 409,
            LeaseError::UnknownLease { .. } | LeaseError::NoLeaseOnResource { .. } => 404,
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
            LeaseError::UnknownResource { resource, known } => {
                let known: Vec<&str> = known.iter().map(ResourceId::as_str).collect();
                write!(
                    f,
                    "unknown resource {:?}: the active config has no such resource (configured: {})",
                    resource.as_str(),
                    known.join(", ")
                )
            }
            LeaseError::NotShared { resource } => write!(
                f,
                "resource {:?} is not a shared resource; a drain is for shared hosts such as an NFS server (an exclusive host is taken down by releasing its host lease, or by a lockout)",
                resource.as_str()
            ),
            LeaseError::NoPowerSection { resource } => write!(
                f,
                "resource {:?} has no power section: it is always on, so a lease on it would do nothing",
                resource.as_str()
            ),
            LeaseError::NotLockable { resource } => write!(
                f,
                "resource {:?} cannot be locked out: only an exclusive resource with a power section can be taken back and shut down",
                resource.as_str()
            ),
            LeaseError::NoLeaseOnResource { resource, kind } => {
                write!(f, "resource {:?} holds no {kind} lease", resource.as_str())
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

    /// Whether `resource` can have a manual host lease: a resource (shared or exclusive) with
    /// a power section.
    pub fn check_host(config: &Config, resource: &ResourceId) -> Result<(), LeaseError> {
        let spec = known_resource(config, resource)?;
        if spec.power.is_none() {
            return Err(LeaseError::NoPowerSection {
                resource: resource.clone(),
            });
        }
        Ok(())
    }

    /// Whether `resource` can be drained: a shared resource with a power section. An
    /// exclusive host is taken down by releasing its host lease (a plain release) or by a
    /// lockout, never by a drain.
    pub fn check_drain(config: &Config, resource: &ResourceId) -> Result<(), LeaseError> {
        Self::check_host(config, resource)?;
        if known_resource(config, resource)?.kind != ResourceKind::Shared {
            return Err(LeaseError::NotShared {
                resource: resource.clone(),
            });
        }
        Ok(())
    }

    /// Acquires a manual lease on a host with a power section (an NFS server, or an exclusive
    /// GPU host): the host is powered on if it is off (when its power is managed) and kept up
    /// until the lease is released. Asking again returns the lease already held.
    pub fn acquire_manual_host(
        &mut self,
        config: &Config,
        resource: &ResourceId,
        now: Millis,
    ) -> Result<LeaseGrant, LeaseError> {
        Self::check_host(config, resource)?;
        Ok(self.acquire_unique(resource, LeaseKind::ManualHost, MANUAL_HOST_PREFIX, now))
    }

    /// Lockouts an exclusive resource with a power section: the user takes the host back
    /// despite pending work (see [`LeaseKind::Lockout`]). Asking again returns the lease
    /// already held.
    pub fn acquire_lockout(
        &mut self,
        config: &Config,
        resource: &ResourceId,
        now: Millis,
    ) -> Result<LeaseGrant, LeaseError> {
        known_resource(config, resource)?;
        if !config.supports_lockout(resource) {
            return Err(LeaseError::NotLockable {
                resource: resource.clone(),
            });
        }
        Ok(self.acquire_unique(resource, LeaseKind::Lockout, LOCKOUT_PREFIX, now))
    }

    /// The lease of `kind` on `resource`, if one is held, else a new one from the counter.
    fn acquire_unique(
        &mut self,
        resource: &ResourceId,
        kind: LeaseKind,
        prefix: &str,
        now: Millis,
    ) -> LeaseGrant {
        if let Some(held) = self
            .leases
            .iter()
            .find(|l| &l.resource == resource && l.kind == kind)
        {
            return LeaseGrant {
                lease: held.clone(),
                created: false,
            };
        }
        self.next += 1;
        let lease = Lease {
            id: LeaseId::new(format!("{prefix}{}", self.next)),
            resource: resource.clone(),
            kind,
            granted_at: now,
        };
        self.leases.push(lease.clone());
        LeaseGrant {
            lease,
            created: true,
        }
    }

    /// Releases the manual host lease on `resource` and returns it.
    pub fn release_manual_host(&mut self, resource: &ResourceId) -> Result<Lease, LeaseError> {
        self.release_kind(resource, "manual host", |k| {
            matches!(k, LeaseKind::ManualHost)
        })
    }

    /// Releases the lockout on `resource` and returns it.
    pub fn release_lockout(&mut self, resource: &ResourceId) -> Result<Lease, LeaseError> {
        self.release_kind(resource, "lockout", |k| matches!(k, LeaseKind::Lockout))
    }

    fn release_kind(
        &mut self,
        resource: &ResourceId,
        what: &str,
        is: impl Fn(&LeaseKind) -> bool,
    ) -> Result<Lease, LeaseError> {
        let Some(at) = self
            .leases
            .iter()
            .position(|l| &l.resource == resource && is(&l.kind))
        else {
            return Err(LeaseError::NoLeaseOnResource {
                resource: resource.clone(),
                kind: what.to_string(),
            });
        };
        Ok(self.leases.remove(at))
    }

    /// Releases the lease with `id` and returns it.
    pub fn release(&mut self, id: &LeaseId) -> Result<Lease, LeaseError> {
        let Some(at) = self.leases.iter().position(|l| &l.id == id) else {
            return Err(LeaseError::UnknownLease { id: id.clone() });
        };
        Ok(self.leases.remove(at))
    }
}

/// The resource `id` in the active config, or the error listing what is configured.
fn known_resource<'a>(
    config: &'a Config,
    id: &ResourceId,
) -> Result<&'a sched_model::ResourceSpec, LeaseError> {
    config
        .resource(id)
        .ok_or_else(|| LeaseError::UnknownResource {
            resource: id.clone(),
            known: config.resources().iter().map(|r| r.id.clone()).collect(),
        })
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
                "  - { id: whisperx, resource: cuda-central, load_time: 60s, workloads: [whisperx], namespaces: [ai] }\n",
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
    fn a_host_lease_is_for_a_shared_powered_resource_and_is_unique_per_resource() {
        let c = example_config();
        let mut b = LeaseBook::default();
        let g = b
            .acquire_manual_host(&c, &"basestar-nfs".into(), Millis(3))
            .unwrap();
        assert!(g.created);
        assert_eq!(g.lease.id.as_str(), "manual-host-1");
        assert_eq!(g.lease.kind, LeaseKind::ManualHost);
        let again = b
            .acquire_manual_host(&c, &"basestar-nfs".into(), Millis(9))
            .unwrap();
        assert!(!again.created);
        assert_eq!(
            (again.lease, b.next, b.leases.len()),
            (g.lease.clone(), 1, 1)
        );
        // The counter is shared with the other kinds, so ids stay unique.
        let m = b
            .acquire_manual_model(&c, &"llm".into(), Millis(4))
            .unwrap();
        assert_eq!(m.lease.id.as_str(), "manual-model-2");
        // An exclusive host with a power section can be leased too, and listed; a drain of it
        // is refused.
        let x = b
            .acquire_manual_host(&c, &"cuda-central".into(), Millis(1))
            .unwrap();
        assert!(x.created);
        assert_eq!(x.lease.kind, LeaseKind::ManualHost);
        assert_eq!(x.lease.resource.as_str(), "cuda-central");
        assert!(
            !b.acquire_manual_host(&c, &"cuda-central".into(), Millis(2))
                .unwrap()
                .created
        );
        assert_eq!(
            b.release_manual_host(&"cuda-central".into()).unwrap(),
            x.lease
        );
        let e = LeaseBook::check_drain(&c, &"cuda-central".into()).unwrap_err();
        assert_eq!(e.status(), 400);
        assert!(e.to_string().contains("not a shared resource"), "{e}");
        assert!(LeaseBook::check_drain(&c, &"basestar-nfs".into()).is_ok());
        let e = b
            .acquire_manual_host(&c, &"nowhere".into(), Millis(1))
            .unwrap_err();
        assert!(
            e.to_string().contains("unknown resource \"nowhere\"")
                && e.to_string().contains("cuda-central, basestar-nfs"),
            "{e}"
        );
        assert_eq!(
            b.release_manual_host(&"basestar-nfs".into()).unwrap(),
            g.lease
        );
        let e = b.release_manual_host(&"basestar-nfs".into()).unwrap_err();
        assert_eq!(e.status(), 404);
        assert_eq!(
            e.to_string(),
            "resource \"basestar-nfs\" holds no manual host lease"
        );
    }

    #[test]
    fn a_shared_resource_without_a_power_section_cannot_be_leased() {
        let y = EXAMPLE_CONFIG
            .replace(
                "    power:\n      wake_on_lan: { router_url: \"https://router.example.internal\", interface: lan, mac: \"02:00:00:00:00:02\" }\n      shutdown: { host: basestar, port: 22, user: poweroff, host_key: \"SHA256:BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB\" }\n      control: manual\n      # Set per resource: the NFS host boots and stops faster than the GPU host.\n      power_on_timeout: 5m\n      shutdown_timeout: 10m\n      # Under managed control, how long a shutdown may wait for pods to stop mounting this\n      # host (see the basestar-mounts source) before the scheduler alerts and stops, without\n      # shutting it down and without retrying (default 30m, at most 1d).\n      veto_timeout: 30m\n",
                "",
            )
            .replace("    may_change_power: true\n", "");
        let c = Config::from_yaml(&y).unwrap();
        let e = LeaseBook::default()
            .acquire_manual_host(&c, &"basestar-nfs".into(), Millis(1))
            .unwrap_err();
        assert_eq!(e.status(), 400);
        assert!(e.to_string().contains("no power section"), "{e}");
    }

    #[test]
    fn a_lockout_is_for_an_exclusive_powered_resource_and_is_unique_per_resource() {
        let c = example_config();
        let mut b = LeaseBook::default();
        let g = b
            .acquire_lockout(&c, &"cuda-central".into(), Millis(3))
            .unwrap();
        assert!(g.created);
        assert_eq!(g.lease.id.as_str(), "lockout-1");
        assert_eq!(g.lease.kind, LeaseKind::Lockout);
        assert!(
            !b.acquire_lockout(&c, &"cuda-central".into(), Millis(4))
                .unwrap()
                .created
        );
        let e = b
            .acquire_lockout(&c, &"basestar-nfs".into(), Millis(1))
            .unwrap_err();
        assert_eq!(e.status(), 400);
        assert!(e.to_string().contains("cannot be locked out"), "{e}");
        assert_eq!(b.leases.len(), 1);
        // It does not disturb a model lease on the same resource, and each is released alone.
        let m = b
            .acquire_manual_model(&c, &"llm".into(), Millis(5))
            .unwrap();
        assert_eq!(b.release_lockout(&"cuda-central".into()).unwrap(), g.lease);
        assert_eq!(b.leases, vec![m.lease]);
        assert_eq!(
            b.release_lockout(&"cuda-central".into())
                .unwrap_err()
                .status(),
            404
        );
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
