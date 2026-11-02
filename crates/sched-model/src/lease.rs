//! Leases: durable records that a holder is using a shared resource or has a claim on a
//! model. Granting is the planner's and sequencer's decision; this module only describes
//! them and answers questions about a set of them.

use serde::{Deserialize, Serialize};

use crate::ids::{HolderId, LeaseId, ModelId, ResourceId};
use crate::time::Millis;

/// Prefix of the id of a manual host lease the scheduler minted for a host it found running
/// (an "adopted" lease); the rest of the id is the resource.
pub const ADOPTED_HOST_PREFIX: &str = "adopted-host-";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lease {
    pub id: LeaseId,
    pub resource: ResourceId,
    pub kind: LeaseKind,
    pub granted_at: Millis,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum LeaseKind {
    /// A reference-counted hold on a shared resource by a running consumer.
    SharedHold { holder: HolderId },
    /// A user's manual power-on of a host; held until the user releases it.
    ManualHost,
    /// A manual lease on a model: cuts batch work short and keeps the model loaded
    /// until released.
    ManualModel { model: ModelId },
    /// An interactive session's claim on a model. Expires unless renewed.
    Interactive { model: ModelId, expires_at: Millis },
    /// The user takes a host (shared or exclusive, with a power section) back despite pending
    /// work. Until released, intake stops, the host is shut down once and it is not woken.
    Lockout,
}

impl Lease {
    pub fn is_expired(&self, now: Millis) -> bool {
        matches!(&self.kind, LeaseKind::Interactive { expires_at, .. } if now >= *expires_at)
    }

    pub fn is_active(&self, now: Millis) -> bool {
        !self.is_expired(now)
    }

    /// The model this lease pins against batch eviction, if it is active.
    pub fn pinned_model(&self, now: Millis) -> Option<&ModelId> {
        if !self.is_active(now) {
            return None;
        }
        match &self.kind {
            LeaseKind::ManualModel { model } | LeaseKind::Interactive { model, .. } => Some(model),
            _ => None,
        }
    }

    /// Whether this lease forces batch work on its resource to stop early.
    pub fn cuts_batch_short(&self) -> bool {
        matches!(self.kind, LeaseKind::ManualModel { .. })
    }

    /// Whether this lease holds the host up (counts against powering it down). Every
    /// active lease does except a lockout, which asks for the opposite.
    pub fn holds_host(&self, now: Millis) -> bool {
        self.is_active(now) && !matches!(self.kind, LeaseKind::Lockout)
    }

    /// Whether this is a manual host lease the scheduler minted for a host found running
    /// (see [`ADOPTED_HOST_PREFIX`]) rather than one a person asked for.
    pub fn is_adopted_host(&self) -> bool {
        matches!(self.kind, LeaseKind::ManualHost)
            && self.id.as_str().starts_with(ADOPTED_HOST_PREFIX)
    }

    pub fn is_lockout(&self) -> bool {
        matches!(self.kind, LeaseKind::Lockout)
    }

    /// An interactive lease renewed to expire `ttl` after `now`; `None` for other kinds.
    pub fn renewed(&self, now: Millis, ttl: Millis) -> Option<Lease> {
        match &self.kind {
            LeaseKind::Interactive { model, .. } => Some(Lease {
                kind: LeaseKind::Interactive {
                    model: model.clone(),
                    expires_at: now.saturating_add(ttl),
                },
                ..self.clone()
            }),
            _ => None,
        }
    }
}

/// Number of active leases holding `resource` up.
pub fn host_hold_count(leases: &[Lease], resource: &ResourceId, now: Millis) -> usize {
    leases
        .iter()
        .filter(|l| &l.resource == resource && l.holds_host(now))
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lease(id: &str, res: &str, kind: LeaseKind) -> Lease {
        Lease {
            id: LeaseId::new(id),
            resource: ResourceId::new(res),
            kind,
            granted_at: Millis(0),
        }
    }

    #[test]
    fn interactive_expires_and_stops_pinning() {
        let l = lease(
            "l1",
            "gpu",
            LeaseKind::Interactive {
                model: ModelId::new("m"),
                expires_at: Millis(100),
            },
        );
        assert_eq!(l.pinned_model(Millis(99)), Some(&ModelId::new("m")));
        assert!(l.is_expired(Millis(100)));
        assert_eq!(l.pinned_model(Millis(100)), None);
    }

    #[test]
    fn renewal_extends_only_interactive() {
        let i = lease(
            "l1",
            "gpu",
            LeaseKind::Interactive {
                model: ModelId::new("m"),
                expires_at: Millis(100),
            },
        );
        let r = i.renewed(Millis(90), Millis(50)).unwrap();
        assert_eq!(
            r.kind,
            LeaseKind::Interactive {
                model: ModelId::new("m"),
                expires_at: Millis(140)
            }
        );
        assert!(
            lease("l2", "nfs", LeaseKind::ManualHost)
                .renewed(Millis(0), Millis(1))
                .is_none()
        );
    }

    #[test]
    fn host_hold_count_counts_shared_and_manual() {
        let nfs = ResourceId::new("nfs");
        let leases = vec![
            lease(
                "a",
                "nfs",
                LeaseKind::SharedHold {
                    holder: HolderId::new("x"),
                },
            ),
            lease(
                "b",
                "nfs",
                LeaseKind::SharedHold {
                    holder: HolderId::new("y"),
                },
            ),
            lease("c", "nfs", LeaseKind::ManualHost),
            lease("d", "gpu", LeaseKind::ManualHost),
        ];
        assert_eq!(host_hold_count(&leases, &nfs, Millis(0)), 3);
    }

    #[test]
    fn only_manual_model_cuts_batch_short() {
        let m = lease(
            "a",
            "gpu",
            LeaseKind::ManualModel {
                model: ModelId::new("m"),
            },
        );
        let i = lease(
            "b",
            "gpu",
            LeaseKind::Interactive {
                model: ModelId::new("m"),
                expires_at: Millis(5),
            },
        );
        assert!(m.cuts_batch_short());
        assert!(!i.cuts_batch_short());
    }
}
