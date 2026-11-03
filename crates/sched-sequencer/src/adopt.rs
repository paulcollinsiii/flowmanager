//! Adoption of a host found running: the ownership rule of `planning/power-model.md` (2.1b)
//! as one pure function, [`adopt`].
//!
//! A host flowmanager powers on is flowmanager's (`ours`, on `PowerPhase::Off` and `On`) and
//! goes down when nothing holds it. A host something else started is found running and
//! adopted: the user is taken to hold it up, through a manual host lease with origin
//! `adopted` that the service stores like any lease. The sequencer is pure and leases live in
//! the service, so nothing is minted here: [`adopt`] says which leases to use, which previous
//! state to decide from, and what was adopted and ended. The service applies the result
//! before the tick, so the tick's planner and sequencer see it and a replayed tick sees the
//! same.
//!
//! A host is found running ([`found_running`]) when it has a power section under
//! `control: managed`, a fresh reading says it is up, and it is not ours: the previous state
//! has no entry for it (first start, discarded state), has it off or up without the marker (a
//! power button, a manual Wake-on-LAN, a host that went down after the person took it down, a
//! host switched from manual to managed control while up, one whose stopped shutdown a person
//! cleared with `resume`), or has it in a failed power-on that the reading has now cleared.
//! It is never found when
//!
//! - it is `powering_on` or `ours` (a restart with persisted state adopts nothing, and a short
//!   blip of a host flowmanager woke is not a new start),
//! - its shutdown was stopped by the veto (a person decides: `resume`),
//! - a manual host lease already holds it,
//! - it is locked out, on either kind of host: a locked-out host that boots is left alone,
//!   and once the lockout is cleared a host that is up is found running like any other.
//!
//! A job's hold plays no part: a host found running while a job holds it is adopted at once
//! (D6), and one the job woke itself is ours and never found.
//!
//! An adopted lease ends when a fresh reading shows its host down ([`gone_down`]): the
//! person (or a power cut) took it down, so the claim is over and the host is no longer ours,
//! and their next power-on is adopted afresh. A host flowmanager woke that merely blips keeps
//! `ours` and has no adopted lease to lose.

use sched_model::{
    Config, Lease, LeaseId, LeaseKind, LeaseOrigin, Millis, PowerPhase, PowerTransition,
    ResourceId, SchedState, Snapshot, manual_host_lease,
};

/// The result of [`adopt`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Adoption {
    /// The lease book the tick uses and the service stores: the input without the adopted
    /// leases that ended, plus an adopted lease for each host adopted. Nothing else differs.
    pub leases: Vec<Lease>,
    /// The state the tick decides from: the input, with every host whose adopted lease ended
    /// no longer ours and its idle clock cleared. Nothing else differs.
    pub prev: SchedState,
    /// Hosts adopted this tick.
    pub adopted: Vec<ResourceId>,
    /// Hosts whose adopted lease was dropped this tick because the host was seen down.
    pub ended: Vec<ResourceId>,
}

/// Decides adoption at `now` (the time of `snapshot`) from the fresh readings, the previous
/// state and the lease book the tick is about to use. Pure and deterministic.
pub fn adopt(
    config: &Config,
    snapshot: &Snapshot,
    prev: &SchedState,
    leases: &[Lease],
    now: Millis,
) -> Adoption {
    let gone = gone_down(config, snapshot, leases);
    let ended: Vec<ResourceId> = leases
        .iter()
        .filter(|l| gone.contains(&l.id))
        .map(|l| l.resource.clone())
        .collect();
    let mut leases: Vec<Lease> = leases
        .iter()
        .filter(|l| !gone.contains(&l.id))
        .cloned()
        .collect();
    let mut prev = prev.clone();
    for r in &ended {
        if let Some(st) = prev.resources.get_mut(r) {
            st.set_ours(false);
            st.idle_since = None;
        }
    }
    let adopted = found_running(config, snapshot, &prev, &leases, now);
    leases.extend(adopted.iter().map(|r| adopted_lease(r, now)));
    Adoption {
        leases,
        prev,
        adopted,
        ended,
    }
}

/// The hosts found running at `now` (see the module) in the lease book `leases`.
///
/// Only a shutdown stopped by the veto blocks adoption of a not-ours host that is up; any
/// other shutdown does not. The reachable case: the tick a lockout lease disappears while a
/// lockout shutdown is under way (the lockout's shutdown is still in the previous state, the
/// host is not ours). The host is adopted at that tick, where the build before 4b waited one
/// tick (the sequencer drops the lockout's shutdown first). The adopted lease then cancels the
/// leftover shutdown as any manual want does.
pub fn found_running(
    config: &Config,
    snapshot: &Snapshot,
    prev: &SchedState,
    leases: &[Lease],
    now: Millis,
) -> Vec<ResourceId> {
    let max_age = config.policy().host_reading_stale_after;
    config
        .resources()
        .iter()
        .filter(|r| r.power.is_some() && config.power_managed(&r.id))
        .filter(|r| snapshot.fresh_resource_up(config, &r.id, max_age) == Some(true))
        .filter(|r| match prev.resource(&r.id) {
            None => true,
            Some(st) => {
                let not_ours_and_up_or_off = matches!(
                    st.power,
                    PowerPhase::Off { ours: false }
                        | PowerPhase::On { ours: false, .. }
                        | PowerPhase::Failed {
                            during: PowerTransition::PoweringOn,
                            ..
                        }
                );
                not_ours_and_up_or_off && !st.shutdown.is_some_and(|s| s.stopped_since().is_some())
            }
        })
        .filter(|r| manual_host_lease(leases, &r.id).is_none())
        .filter(|r| !config.locked_out(leases, &r.id, now))
        .map(|r| r.id.clone())
        .collect()
}

/// The id of the adopted lease of `resource`. Only a name: whether a lease is adopted is its
/// `origin`.
pub fn adopted_id(resource: &ResourceId) -> LeaseId {
    LeaseId::new(format!("adopted-host-{resource}"))
}

/// The adopted lease of `resource`, granted at `now`.
pub fn adopted_lease(resource: &ResourceId, now: Millis) -> Lease {
    Lease {
        id: adopted_id(resource),
        resource: resource.clone(),
        kind: LeaseKind::ManualHost,
        granted_at: now,
        origin: LeaseOrigin::Adopted,
    }
}

/// The adopted leases whose host a fresh reading shows down. Leases a person asked for are
/// never listed.
pub fn gone_down(config: &Config, snapshot: &Snapshot, leases: &[Lease]) -> Vec<LeaseId> {
    let max_age = config.policy().host_reading_stale_after;
    leases
        .iter()
        .filter(|l| l.is_adopted_host())
        .filter(|l| snapshot.fresh_resource_up(config, &l.resource, max_age) == Some(false))
        .map(|l| l.id.clone())
        .collect()
}
