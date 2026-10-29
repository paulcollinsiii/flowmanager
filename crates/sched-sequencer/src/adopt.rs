//! Adoption of a host found running: which hosts the user is taken to hold up, and which
//! earlier adoptions are over.
//!
//! The sequencer is pure and leases live in the service, so it does not mint anything here:
//! these two functions only say, from the previous state and a fresh reading, which hosts to
//! adopt and which adopted leases to drop. The service applies them before the tick, so the
//! tick's planner and sequencer see the result and a replayed tick sees the same.
//!
//! A host is found running when it is shared, has a power section under `control: managed`,
//! a fresh reading says it is up, and flowmanager did not power it on: the previous state
//! has no entry for it (first start, discarded state), has it off (a power button, a manual
//! Wake-on-LAN, a host that went down and came back), or has it in a failed power-on that
//! the reading has now cleared (whatever brought it up, the sequencer treats it as found
//! running). A host in `powering_on` that is seen up is flowmanager's own, a host that was
//! already `on` is not found again (a restart with persisted state adopts nothing), and under
//! `control: manual` nothing is ever adopted.

use sched_model::{
    ADOPTED_HOST_PREFIX, Config, Lease, LeaseId, LeaseKind, Millis, PowerPhase, ResourceId,
    ResourceKind, SchedState, Snapshot,
};

/// The id of the adopted lease of `resource`.
pub fn adopted_id(resource: &ResourceId) -> LeaseId {
    LeaseId::new(format!("{ADOPTED_HOST_PREFIX}{resource}"))
}

/// The hosts to adopt at `snapshot.now`: found running (see the module) and holding no
/// manual host lease yet.
pub fn found_running(config: &Config, snapshot: &Snapshot, prev: &SchedState) -> Vec<ResourceId> {
    let max_age = config.policy().host_reading_stale_after;
    config
        .resources()
        .iter()
        .filter(|r| r.kind == ResourceKind::Shared && r.power.is_some())
        .filter(|r| config.power_managed(&r.id))
        .filter(|r| snapshot.fresh_resource_up(config, &r.id, max_age) == Some(true))
        .filter(|r| match prev.resource(&r.id).map(|s| s.power) {
            None | Some(PowerPhase::Off) => true,
            Some(PowerPhase::Failed {
                during: sched_model::PowerTransition::PoweringOn,
                ..
            }) => true,
            Some(_) => false,
        })
        .filter(|r| {
            !snapshot
                .leases
                .iter()
                .any(|l| l.resource == r.id && l.kind == LeaseKind::ManualHost)
        })
        .map(|r| r.id.clone())
        .collect()
}

/// The adopted lease of `resource`, granted at `now`.
pub fn adopted_lease(resource: &ResourceId, now: Millis) -> Lease {
    Lease {
        id: adopted_id(resource),
        resource: resource.clone(),
        kind: LeaseKind::ManualHost,
        granted_at: now,
    }
}

/// The adopted leases whose host a fresh reading shows down: the user (or a power cut) took
/// it down, so the claim is over and the lease must not wake it again. Leases a person asked
/// for are never listed.
pub fn gone_down(config: &Config, snapshot: &Snapshot, leases: &[Lease]) -> Vec<LeaseId> {
    let max_age = config.policy().host_reading_stale_after;
    leases
        .iter()
        .filter(|l| l.is_adopted_host())
        .filter(|l| snapshot.fresh_resource_up(config, &l.resource, max_age) == Some(false))
        .map(|l| l.id.clone())
        .collect()
}
