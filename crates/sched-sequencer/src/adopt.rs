//! Adoption of a host found running: which hosts the user is taken to hold up, and which
//! earlier adoptions are over.
//!
//! The sequencer is pure and leases live in the service, so it does not mint anything here:
//! these two functions only say, from the previous state and a fresh reading, which hosts to
//! adopt and which adopted leases to drop. The service applies them before the tick, so the
//! tick's planner and sequencer see the result and a replayed tick sees the same.
//!
//! A host is found running when it (shared or exclusive) has a power section under `control: managed`,
//! a fresh reading says it is up, and flowmanager did not power it on: the previous state
//! has no entry for it (first start, discarded state), has it off (a power button, a manual
//! Wake-on-LAN, a host that went down and came back), has it in a failed power-on that
//! the reading has now cleared (whatever brought it up, the sequencer treats it as found
//! running), or has it `on` but not managed (stored by a build that did not adopt, or kept
//! so by a tick that skipped adoption), unless the scheduler left it so on purpose: a stopped
//! mount veto, a drain, or a job's hold. A host in `powering_on` that is seen up is
//! flowmanager's own and an `on` host it manages is not found again (a restart with persisted
//! state adopts nothing), and under `control: manual` nothing is ever adopted.

use sched_model::{
    ADOPTED_HOST_PREFIX, Config, Lease, LeaseId, LeaseKind, Millis, PowerPhase, ResourceId,
    SchedState, Snapshot,
};

/// The id of the adopted lease of `resource`.
pub fn adopted_id(resource: &ResourceId) -> LeaseId {
    LeaseId::new(format!("{ADOPTED_HOST_PREFIX}{resource}"))
}

/// The hosts to adopt at `snapshot.now`: found running (see the module) and holding no
/// manual host lease yet (`leases` is the book the tick is about to use).
pub fn found_running(
    config: &Config,
    snapshot: &Snapshot,
    prev: &SchedState,
    leases: &[Lease],
) -> Vec<ResourceId> {
    let max_age = config.policy().host_reading_stale_after;
    config
        .resources()
        .iter()
        .filter(|r| r.power.is_some())
        .filter(|r| config.power_managed(&r.id))
        .filter(|r| snapshot.fresh_resource_up(config, &r.id, max_age) == Some(true))
        .filter(|r| match prev.resource(&r.id) {
            None => true,
            Some(st) => match st.power {
                // Off after a shutdown we started or a failed power-on is not managed; off with
                // the flag still set is a host we powered on that went down for a moment
                // (only `power_off` clears it), and it is not found running when it is back.
                PowerPhase::Off => !st.managed,
                PowerPhase::Failed {
                    during: sched_model::PowerTransition::PoweringOn,
                    ..
                } => true,
                // Up, not ours, and nothing says the scheduler left it so on purpose: a state
                // an older build stored for a host found running, or one whose adoption was
                // skipped (an unreadable job book). Not a host we powered on (managed), one
                // whose shutdown is under way or was stopped by mounting pods (a shutdown), or
                // one a job holds (a hold never takes a host over; it is adopted once the
                // hold ends).
                PowerPhase::On { .. } => {
                    !st.managed
                        && st.shutdown.is_none()
                        && !leases.iter().any(|l| {
                            l.resource == r.id && matches!(l.kind, LeaseKind::SharedHold { .. })
                        })
                }
                _ => false,
            },
        })
        .filter(|r| {
            !leases
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
