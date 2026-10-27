//! The sequencer's own state: plain serializable data, carried from one tick to the next
//! (the service persists it between ticks). The planner reads it but never changes it.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::ids::{ClassId, ModelId, ResourceId};
use crate::time::Millis;

/// The power transition a host was in when it timed out (see [`PowerPhase::Failed`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PowerTransition {
    /// The host was powering on and was not observed up in time.
    PoweringOn,
    /// The host was shutting down and was not observed down in time.
    ShuttingDown,
}

impl PowerTransition {
    /// The label used in logs and metrics.
    pub fn as_str(self) -> &'static str {
        match self {
            PowerTransition::PoweringOn => "powering_on",
            PowerTransition::ShuttingDown => "shutting_down",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum PowerPhase {
    Off,
    PoweringOn {
        since: Millis,
    },
    On {
        since: Millis,
    },
    ShuttingDown {
        since: Millis,
    },
    /// A power transition did not complete within its configured timeout. The scheduler
    /// stops here: it raises an alert, allows nothing on the host, and never issues another
    /// power action for it by itself (a host that does not come up or go down is a matter
    /// for a person, and a retry risks flapping it). `since` is when the timeout expired.
    /// It clears only when a fresh observation shows the state the transition was after
    /// (up for `PoweringOn`, down for `ShuttingDown`), including because a person fixed the
    /// host, or when the resource is set to `control: manual`.
    Failed {
        since: Millis,
        during: PowerTransition,
    },
}

/// A serving model that is being switched away from: its classes have left the allowed
/// set and the sequencer is waiting for the in-flight work its queues report to end or for
/// each class's timeout, whichever comes first (forced or not: no wind-down is unbounded
/// except by a busy class that cannot be preempted, which prevents it from starting). Nothing here is about pods: the model's
/// pods stay allowed until the switch, and Kubernetes keeps the next model's pod pending
/// until they have released the GPU.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WindDown {
    pub since: Millis,
    /// In-flight work gets this long from `since` (unless a class declares its own
    /// `kill_timeout`) and is then cut off. The sequencer always sets it; `None` is only
    /// read from a state persisted before unforced wind-downs had a timeout.
    #[serde(default)]
    pub timeout: Option<Millis>,
    /// Whether the cause is a reclaim (rather than a manual lease or none), so that the
    /// cause flipping later restarts the clock with the new cause's timeout.
    #[serde(default)]
    pub reclaim: bool,
    /// Classes already stopped. Each class is stopped once, when its own timeout expires.
    #[serde(default)]
    pub killed: BTreeSet<ClassId>,
}

/// The mode of an exclusive resource. The scheduler decides it from queues, leases and
/// policy alone, never from pod state: GPU exclusivity is enforced by Kubernetes (every GPU
/// workload requests the one GPU the node exposes), so a model is in the allowed set from
/// the tick it is allowed until the tick the next one replaces it, and the two never
/// overlap in the set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "phase",
    rename_all = "snake_case",
    deny_unknown_fields,
    from = "ModePhaseRepr"
)]
pub enum ModePhase {
    /// No model is allowed.
    Idle,
    /// The model is allowed. `since` is the tick it was allowed: dwell is measured from
    /// there, so `min_dwell` includes the time its pods take to load (and to wait for the
    /// previous model's pods to release the GPU).
    Serving {
        model: ModelId,
        since: Millis,
        /// When the model last had no work; cleared as soon as work appears.
        #[serde(default)]
        idle_since: Option<Millis>,
        #[serde(default)]
        wind_down: Option<WindDown>,
    },
}

/// What a persisted mode may hold. Besides the current phases it accepts the two that
/// builds before the queue-only change wrote, which gated on pod observations: `loading`
/// (allowed, pods not yet ready) and `draining` (withdrawn, pods not yet observed at zero).
/// They are read, never written, so the persisted document keeps its version: everything
/// this build writes is also valid for the build before it.
#[derive(Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
enum ModePhaseRepr {
    Idle,
    /// Legacy: becomes `Serving` since the same instant (the model was allowed then).
    Loading {
        model: ModelId,
        since: Millis,
    },
    Serving {
        model: ModelId,
        since: Millis,
        #[serde(default)]
        idle_since: Option<Millis>,
        #[serde(default)]
        wind_down: Option<WindDown>,
    },
    /// Legacy: becomes `Idle`. The model had already left the allowed set; nothing waits
    /// for its pods any more.
    Draining {
        #[serde(rename = "model")]
        _model: ModelId,
        #[serde(rename = "since")]
        _since: Millis,
    },
}

impl From<ModePhaseRepr> for ModePhase {
    fn from(r: ModePhaseRepr) -> ModePhase {
        match r {
            ModePhaseRepr::Idle | ModePhaseRepr::Draining { .. } => ModePhase::Idle,
            ModePhaseRepr::Loading { model, since } => ModePhase::Serving {
                model,
                since,
                idle_since: None,
                wind_down: None,
            },
            ModePhaseRepr::Serving {
                model,
                since,
                idle_since,
                wind_down,
            } => ModePhase::Serving {
                model,
                since,
                idle_since,
                wind_down,
            },
        }
    }
}

impl ModePhase {
    /// The model currently allowed (serving or winding down), if any.
    pub fn allowed_model(&self) -> Option<&ModelId> {
        match self {
            ModePhase::Serving { model, .. } => Some(model),
            ModePhase::Idle => None,
        }
    }
}

/// The veto on shutting a host down while pods may still mount it (see the `mounts`
/// capability). Only an On host that the scheduler is about to power off has one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum MountsVeto {
    /// The shutdown is held back: pods mount the host, or the count is unknown (a missing,
    /// stale or failed reading is never read as "no mounts"). `since` is when the wait began.
    Waiting { since: Millis },
    /// The wait outlasted the resource's `mounts_timeout`. The scheduler stops: no shutdown,
    /// an alert, no retry, and the host is no longer managed (as after a failed power
    /// transition). It clears only when the host is seen down or its state is reset by a
    /// person (`resume`), or when a new manual request takes the host over again. `since` is
    /// when the scheduler gave up.
    Stopped { since: Millis },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceState {
    pub power: PowerPhase,
    pub mode: ModePhase,
    /// Whether the scheduler may power this host down: set when it woke the host or took
    /// over a manual request, never for a host merely found running.
    #[serde(default)]
    pub managed: bool,
    /// When the host last had nothing holding it.
    #[serde(default)]
    pub host_idle_since: Option<Millis>,
    /// Classes whose in-flight work was stopped and whose recovery action has not run.
    #[serde(default)]
    pub pending_recovery: BTreeSet<ClassId>,
    /// Whether a shutdown of the host is held back by pods that may mount it. Absent from the
    /// stored state while there is no veto, so a state without one reads and writes as before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mounts_veto: Option<MountsVeto>,
    /// When the host was observed down after flowmanager shut it down (the instant the phase
    /// became `off` from `shutting_down`, or from a failed shutdown). It starts the `min_off`
    /// wait before the next automatic power-on. Absent for a host found down, one that went down
    /// by itself, and once the phase leaves `off`; absent from the stored state then, so a state
    /// without it reads and writes as before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shutdown_confirmed_at: Option<Millis>,
}

impl ResourceState {
    pub fn new(power: PowerPhase) -> Self {
        ResourceState {
            power,
            mode: ModePhase::Idle,
            managed: false,
            host_idle_since: None,
            pending_recovery: BTreeSet::new(),
            mounts_veto: None,
            shutdown_confirmed_at: None,
        }
    }

    pub fn is_on(&self) -> bool {
        matches!(self.power, PowerPhase::On { .. })
    }

    /// How much of `min_off` is left to wait at `now` before the host may be powered on
    /// again by itself: zero when the host is not off after a shutdown flowmanager started
    /// (found down, gone down by itself, or no shutdown stamp), when `min_off` is zero or when
    /// it has passed. A stamp in the future (the clock went back) counts as just made, so the
    /// wait never exceeds `min_off`.
    pub fn min_off_remaining(&self, min_off: Millis, now: Millis) -> Millis {
        match (self.power, self.shutdown_confirmed_at) {
            (PowerPhase::Off, Some(at)) => min_off.saturating_sub(now.saturating_sub(at)),
            _ => Millis::ZERO,
        }
    }
}

/// State of every resource. A resource absent from the map has not been seen yet; the
/// sequencer initialises it from observation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedState {
    #[serde(default)]
    pub resources: BTreeMap<ResourceId, ResourceState>,
}

impl SchedState {
    pub fn resource(&self, id: &ResourceId) -> Option<&ResourceState> {
        self.resources.get(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mode(text: &str) -> Result<ModePhase, serde_yaml_ng::Error> {
        serde_yaml_ng::from_str(text)
    }

    #[test]
    fn modes_written_before_the_queue_only_change_are_read_and_mapped() {
        // As the previous build stored them (JSON is read the same way).
        assert_eq!(
            mode(r#"{"phase":"loading","model":"mineru","since":7}"#).unwrap(),
            ModePhase::Serving {
                model: "mineru".into(),
                since: Millis(7),
                idle_since: None,
                wind_down: None,
            }
        );
        assert_eq!(
            mode(r#"{"phase":"draining","model":"mineru","since":7}"#).unwrap(),
            ModePhase::Idle
        );
        let wd = mode(
            r#"{"phase":"serving","model":"llm","since":1,"idle_since":5,"wind_down":{"since":3,"timeout":600000,"reclaim":false,"killed":["c"]}}"#,
        )
        .unwrap();
        let ModePhase::Serving {
            wind_down,
            idle_since,
            ..
        } = &wd
        else {
            panic!("{wd:?}");
        };
        assert_eq!(*idle_since, Some(Millis(5)));
        assert_eq!(wind_down.as_ref().unwrap().killed.len(), 1);
        // Still strict: unknown phases and fields are refused, also on the legacy phases.
        assert!(mode(r#"{"phase":"warming","model":"m","since":1}"#).is_err());
        assert!(mode(r#"{"phase":"draining","model":"m","since":1,"x":1}"#).is_err());
        assert!(mode(r#"{"phase":"loading","model":"m"}"#).is_err());
    }

    #[test]
    fn only_idle_and_serving_are_ever_written_and_they_read_back() {
        for m in [
            ModePhase::Idle,
            ModePhase::Serving {
                model: "llm".into(),
                since: Millis(9),
                idle_since: Some(Millis(10)),
                wind_down: Some(WindDown {
                    since: Millis(11),
                    timeout: None,
                    reclaim: false,
                    killed: Default::default(),
                }),
            },
        ] {
            let text = serde_yaml_ng::to_string(&m).unwrap();
            assert!(
                !text.contains("loading") && !text.contains("draining"),
                "{text}"
            );
            assert_eq!(mode(&text).unwrap(), m);
        }
    }

    #[test]
    fn the_failed_power_phase_reads_and_writes_as_documented() {
        let p: PowerPhase =
            serde_yaml_ng::from_str(r#"{"phase":"failed","since":9,"during":"shutting_down"}"#)
                .unwrap();
        assert_eq!(
            p,
            PowerPhase::Failed {
                since: Millis(9),
                during: PowerTransition::ShuttingDown
            }
        );
        assert_eq!(
            serde_yaml_ng::from_str::<PowerPhase>(&serde_yaml_ng::to_string(&p).unwrap()).unwrap(),
            p
        );
        // Not a resource the sequencer treats as on.
        assert!(!ResourceState::new(p).is_on());
        assert!(serde_yaml_ng::from_str::<PowerPhase>(r#"{"phase":"failed","since":9}"#).is_err());
    }
}
