//! `resume`: a person acts on one resource flowmanager stopped at, after fixing the host by
//! hand. Two things stop flowmanager and leave it waiting for a person: a power transition
//! that timed out (`PowerPhase::Failed`) and a shutdown that waited for the veto to clear until
//! `veto_timeout` ran out (`Veto::Stopped`).
//!
//! Resuming forgets the resource's state, so the sequencer starts it afresh from what is
//! observed (the same as a resource it has never seen): found running means up and not
//! managed (it is never powered off by flowmanager on account of an earlier action; a shared
//! host (shared or exclusive) under `control: managed` is then adopted at the next tick, like any host found
//! running, so a person holds it until they switch it off), found
//! down means off. Nothing is powered on or off by the request itself, and no other
//! resource's state, nor the lease book, is touched. What the resource's own state held
//! besides its power (its mode, which is idle while it is not up, and the classes still
//! awaiting a recovery action after a kill, which no action in this build waits for) is
//! dropped with it.
//!
//! The rules are pure, so they are tested without Restate; `object::resume` journals them.

use std::fmt;

use sched_model::{Config, PowerPhase, PowerTransition, ResourceId};
use serde::{Deserialize, Serialize};

use crate::persist::CoreState;

/// `resume` request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resume {
    pub resource: ResourceId,
}

/// What a resume cleared.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeAck {
    pub resource: ResourceId,
    /// What the resource was stopped at: `power_on_failed`, `shutdown_failed` or
    /// `shutdown_stopped_by_veto`.
    pub cleared: String,
}

/// Why a resume was refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResumeError {
    /// The resource is not in the active config.
    UnknownResource {
        resource: ResourceId,
        known: Vec<ResourceId>,
    },
    /// The resource has no power section, so nothing about its power can have stopped.
    NoPowerSection { resource: ResourceId },
    /// The resource is not stopped: there is nothing to resume.
    NotStopped { resource: ResourceId, state: String },
}

impl ResumeError {
    /// The HTTP status the ingress answers with.
    pub fn status(&self) -> u16 {
        match self {
            ResumeError::UnknownResource { .. } | ResumeError::NoPowerSection { .. } => 400,
            ResumeError::NotStopped { .. } => 409,
        }
    }
}

impl fmt::Display for ResumeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResumeError::UnknownResource { resource, known } => {
                let known: Vec<&str> = known.iter().map(ResourceId::as_str).collect();
                write!(
                    f,
                    "unknown resource {:?}: the active config has no such resource (configured: {})",
                    resource.as_str(),
                    known.join(", ")
                )
            }
            ResumeError::NoPowerSection { resource } => write!(
                f,
                "resource {:?} has no power section, so there is no power state to resume",
                resource.as_str()
            ),
            ResumeError::NotStopped { resource, state } => write!(
                f,
                "resource {:?} is not stopped ({state}); resume only applies to a failed power transition or a shutdown stopped by mounting pods",
                resource.as_str()
            ),
        }
    }
}

impl std::error::Error for ResumeError {}

/// Forgets the state of `resource` if flowmanager stopped at it, so it follows observation
/// again. Refused for an unknown resource, one without a power section, and one that is not
/// stopped (a host that is merely powering on or off, or has never been seen, included).
pub fn resume(
    core: &mut CoreState,
    config: &Config,
    resource: &ResourceId,
) -> Result<ResumeAck, ResumeError> {
    let Some(spec) = config.resource(resource) else {
        return Err(ResumeError::UnknownResource {
            resource: resource.clone(),
            known: config.resources().iter().map(|r| r.id.clone()).collect(),
        });
    };
    if spec.power.is_none() {
        return Err(ResumeError::NoPowerSection {
            resource: resource.clone(),
        });
    }
    let not_stopped = |state: String| ResumeError::NotStopped {
        resource: resource.clone(),
        state,
    };
    let Some(st) = core.state.resources.get(resource) else {
        return Err(not_stopped("it has no state yet".to_string()));
    };
    let cleared = match (st.power, st.shutdown.and_then(|s| s.stopped_since())) {
        (
            PowerPhase::Failed {
                during: PowerTransition::PoweringOn,
                ..
            },
            _,
        ) => "power_on_failed",
        (
            PowerPhase::Failed {
                during: PowerTransition::ShuttingDown,
                ..
            },
            _,
        ) => "shutdown_failed",
        (_, Some(_)) => "shutdown_stopped_by_veto",
        (power, _) => return Err(not_stopped(format!("its power phase is {}", phase(power)))),
    };
    core.state.resources.remove(resource);
    Ok(ResumeAck {
        resource: resource.clone(),
        cleared: cleared.to_string(),
    })
}

fn phase(p: PowerPhase) -> &'static str {
    match p {
        PowerPhase::Off { .. } => "off",
        PowerPhase::PoweringOn { .. } => "powering on",
        PowerPhase::On { .. } => "on",
        PowerPhase::ShuttingDown { .. } => "shutting down",
        PowerPhase::Failed { .. } => "failed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sched_model::test_support::example_config_managed;
    use sched_model::{Millis, ResourceState, Shutdown};

    /// The idle shutdown of a shared host, held back by the veto since `at`.
    fn vetoing(at: u64) -> Shutdown {
        Shutdown::idle_withdrawn(Millis(1)).vetoing(Millis(at))
    }

    fn core(entries: &[(&str, ResourceState)]) -> CoreState {
        let mut c = CoreState::default();
        for (id, st) in entries {
            c.state.resources.insert(ResourceId::new(*id), st.clone());
        }
        c.reported.insert("cluster-pods".into());
        c
    }

    fn failed(during: PowerTransition) -> ResourceState {
        ResourceState::new(PowerPhase::Failed {
            since: Millis(5),
            during,
        })
    }

    fn on() -> ResourceState {
        ResourceState::new(PowerPhase::On {
            since: Millis(1),
            ours: false,
        })
    }

    #[test]
    fn a_failed_power_on_or_shutdown_is_forgotten_for_that_resource_only() {
        let c = example_config_managed();
        for (during, label) in [
            (PowerTransition::PoweringOn, "power_on_failed"),
            (PowerTransition::ShuttingDown, "shutdown_failed"),
        ] {
            let mut s = core(&[("cuda-central", failed(during)), ("basestar-nfs", on())]);
            let before = s.clone();
            let ack = resume(&mut s, &c, &"cuda-central".into()).unwrap();
            assert_eq!(ack.cleared, label);
            // The resource follows observation again; the other and `reported` are as they were.
            assert!(
                !s.state
                    .resources
                    .contains_key(&ResourceId::new("cuda-central"))
            );
            assert_eq!(
                s.state.resources.get(&ResourceId::new("basestar-nfs")),
                before.state.resources.get(&ResourceId::new("basestar-nfs"))
            );
            assert_eq!(s.reported, before.reported);
        }
    }

    #[test]
    fn a_shutdown_stopped_by_mounting_pods_is_forgotten_too() {
        let c = example_config_managed();
        let mut st = on();
        st.shutdown = Some(vetoing(5).stopped(Millis(9)));
        let mut s = core(&[("basestar-nfs", st)]);
        let ack = resume(&mut s, &c, &"basestar-nfs".into()).unwrap();
        assert_eq!(ack.cleared, "shutdown_stopped_by_veto");
        assert!(s.state.resources.is_empty());
    }

    #[test]
    fn a_resource_that_is_not_stopped_is_refused_and_left_alone() {
        let c = example_config_managed();
        let mut waiting = on();
        waiting.shutdown = Some(vetoing(9));
        for (st, text) in [
            (on(), "its power phase is on"),
            (
                ResourceState::new(PowerPhase::Off { ours: false }),
                "its power phase is off",
            ),
            (
                ResourceState::new(PowerPhase::PoweringOn { since: Millis(1) }),
                "powering on",
            ),
            (waiting, "its power phase is on"),
        ] {
            let mut s = core(&[("basestar-nfs", st)]);
            let before = s.clone();
            let e = resume(&mut s, &c, &"basestar-nfs".into()).unwrap_err();
            assert_eq!(e.status(), 409);
            assert!(e.to_string().contains(text), "{e}");
            assert_eq!(s, before);
        }
        let mut s = CoreState::default();
        let e = resume(&mut s, &c, &"basestar-nfs".into()).unwrap_err();
        assert!(e.to_string().contains("no state yet"), "{e}");
    }

    #[test]
    fn unknown_resources_and_resources_without_power_are_refused_clearly() {
        let c = example_config_managed();
        let mut s = CoreState::default();
        let e = resume(&mut s, &c, &"nowhere".into()).unwrap_err();
        assert_eq!(e.status(), 400);
        let text = e.to_string();
        assert!(
            text.contains("unknown resource \"nowhere\"")
                && text.contains("cuda-central, basestar-nfs"),
            "{text}"
        );
        // A config where basestar has no power section.
        let y = sched_model::test_support::EXAMPLE_CONFIG
            .replace(
                "    power:\n      wake_on_lan: { router_url: \"https://router.example.internal\", interface: lan, mac: \"02:00:00:00:00:02\" }\n      shutdown: { host: basestar, port: 22, user: poweroff, host_key: \"SHA256:BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB\" }\n      control: manual\n      # Set per resource: the NFS host boots and stops faster than the GPU host.\n      power_on_timeout: 5m\n      shutdown_timeout: 10m\n      # Under managed control, how long a shutdown may wait for pods to stop mounting this\n      # host (see the basestar-mounts source) before the scheduler alerts and stops, without\n      # shutting it down and without retrying (default 30m, at most 1d).\n      veto_timeout: 30m\n",
                "",
            )
            .replace("    may_change_power: true\n", "");
        let c2 = Config::from_yaml(&y).unwrap();
        let e = resume(&mut s, &c2, &"basestar-nfs".into()).unwrap_err();
        assert_eq!(e.status(), 400);
        assert!(e.to_string().contains("no power section"), "{e}");
    }
}
