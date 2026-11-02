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
    /// Whether the cause is a lockout (rather than a manual lease or none), so that the
    /// cause flipping later restarts the clock with the new cause's timeout.
    #[serde(default)]
    pub lockout: bool,
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

/// How a drain treats in-flight work (see [`ShutdownCause::Requested`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DrainMode {
    /// In-flight work of the classes that need the host runs for at most `policy.drain_timeout`
    /// (less if it finishes) before their consumers are scaled down.
    Graceful,
    /// The consumers are scaled down at once.
    Immediate,
}

impl DrainMode {
    /// The label used in logs, reports and requests.
    pub fn as_str(self) -> &'static str {
        match self {
            DrainMode::Graceful => "graceful",
            DrainMode::Immediate => "immediate",
        }
    }
}

/// Why a host is being shut down (see [`Shutdown`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ShutdownCause {
    /// The scheduler's own: nothing holds the host and `idle_grace` is over. Any hold arriving
    /// cancels it. On a shared host the consumers are scaled down first (the shutdown starts
    /// with its withdrawal already [`Withdrawal::Done`], as an immediate drain); an exclusive
    /// host has already withdrawn its model, so its shutdown starts at the veto with nothing
    /// to withdraw.
    Idle,
    /// A person asked (a manual host lease released with a drain, or `drain_host`). It
    /// ignores queued and in-flight work, never a lease, and is cancelled only by a new manual
    /// host lease (or the host no longer being up). `mode` is how in-flight work is treated
    /// before the consumers are withdrawn: scaled down on a shared host, wound down by the
    /// mode machine on an exclusive one. Both kinds take it through the API.
    Requested { mode: DrainMode },
    /// A lockout lease forced the shutdown. It overrides holds, leases and the grace period.
    /// On a shared host the consumers are withdrawn first (in-flight work gets
    /// `lockout_timeout`); on an exclusive host the mode machine winds the model down first
    /// (see `ModePhase`), so the shutdown starts with nothing to withdraw.
    Lockout,
}

impl ShutdownCause {
    /// The label used in logs and reports for who started a drain: `requested` or `idle`.
    /// A lockout is not a drain and has no label.
    pub fn origin(self) -> Option<&'static str> {
        match self {
            ShutdownCause::Requested { .. } => Some("requested"),
            ShutdownCause::Idle => Some("idle"),
            ShutdownCause::Lockout => None,
        }
    }
}

/// How far the withdrawal has got: taking the host's consumers away. On a shared host the
/// classes that need it leave the allowed set (their consumers are scaled down); on an
/// exclusive host the mode machine winds the model down.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Withdrawal {
    /// Under way: the consumers still run; in-flight work is given its timeout (a graceful
    /// shutdown's `drain_timeout` from the shutdown's `since`; at once for an immediate one).
    Pending,
    /// The consumers are out. They stay out for as long as the shutdown stands.
    Done,
    /// Nothing is kept out: an exclusive host's idle or lockout shutdown begins after its
    /// model has gone, and an idle shutdown that stopped lets its consumers come back
    /// (nobody asked for them to stay down).
    NotNeeded,
}

/// The veto on powering off: a fresh reading must show nothing still uses the host (see
/// `Shutdown`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum Veto {
    /// Not (yet) waiting on the readings.
    None,
    /// The shutdown is held back: something uses the host, or the reading is unknown (a
    /// missing, stale or failed reading is never read as "unused"). `since` is when the wait
    /// began.
    Vetoing { since: Millis },
    /// The wait outlasted the resource's `veto_timeout`. The scheduler stops: no shutdown,
    /// an alert, no retry, and the host is no longer managed (as after a failed power
    /// transition). It clears only when the host is seen down or its state is reset by a
    /// person (`resume`), or when a new manual request takes the host over again. `since` is
    /// when the scheduler gave up.
    Stopped { since: Millis },
}

/// A shutdown of an up host under managed control, from the decision to take it down to the
/// power-off, as one value: absent while the host is not on its way down. The whole value is
/// cleared at once when the host is no longer up, when it is cancelled (see
/// [`ShutdownCause`]), or when a new manual host lease takes the host over.
///
/// Two axes advance independently of what caused the shutdown: the [`Withdrawal`] of the
/// consumers, then the [`Veto`] on the power-off. The fields are private and every
/// transition is a method, so a combination that cannot happen cannot be built (nor read
/// back from a stored state, which is validated). The combinations that exist:
///
/// | cause | withdrawal | veto |
/// | --- | --- | --- |
/// | idle | done | none, vetoing |
/// | idle | not needed | any |
/// | requested | pending | none |
/// | requested | done | any |
/// | lockout | pending | none |
/// | lockout | done | any |
/// | lockout | not needed | any |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ShutdownRepr")]
pub struct Shutdown {
    cause: ShutdownCause,
    /// When the shutdown was requested (or, for an idle one, started). For a graceful
    /// requested shutdown, on a shared and on an exclusive host alike, in-flight work gets
    /// exactly `drain_timeout` from here: no class `kill_timeout` extends it and no earlier
    /// wind-down's start shortens it (a lockout present keeps its own `lockout_timeout`).
    since: Millis,
    withdrawal: Withdrawal,
    veto: Veto,
}

/// What a stored shutdown holds, checked on reading.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ShutdownRepr {
    cause: ShutdownCause,
    since: Millis,
    withdrawal: Withdrawal,
    veto: Veto,
}

impl TryFrom<ShutdownRepr> for Shutdown {
    type Error = String;

    fn try_from(r: ShutdownRepr) -> Result<Shutdown, String> {
        let s = Shutdown {
            cause: r.cause,
            since: r.since,
            withdrawal: r.withdrawal,
            veto: r.veto,
        };
        if s.is_possible() {
            Ok(s)
        } else {
            Err(format!(
                "a shutdown with cause {:?}, withdrawal {:?} and veto {:?} cannot exist",
                r.cause, r.withdrawal, r.veto
            ))
        }
    }
}

impl Shutdown {
    /// The table in the type's documentation.
    fn is_possible(&self) -> bool {
        match (self.cause, self.withdrawal) {
            (ShutdownCause::Requested { .. }, Withdrawal::Pending) => self.veto == Veto::None,
            (ShutdownCause::Requested { .. }, Withdrawal::Done) => true,
            (ShutdownCause::Requested { .. }, Withdrawal::NotNeeded) => false,
            (ShutdownCause::Idle, Withdrawal::Pending) => false,
            (ShutdownCause::Idle, Withdrawal::Done) => !matches!(self.veto, Veto::Stopped { .. }),
            (ShutdownCause::Idle, Withdrawal::NotNeeded) => true,
            (ShutdownCause::Lockout, Withdrawal::Pending) => self.veto == Veto::None,
            (ShutdownCause::Lockout, Withdrawal::Done | Withdrawal::NotNeeded) => true,
        }
    }

    /// An idle shutdown of a shared host: its consumers leave the allowed set in the same
    /// tick, so the withdrawal is done from the start.
    pub fn idle_withdrawn(since: Millis) -> Shutdown {
        Shutdown {
            cause: ShutdownCause::Idle,
            since,
            withdrawal: Withdrawal::Done,
            veto: Veto::None,
        }
    }

    /// An idle shutdown with nothing to withdraw (an exclusive host: its model is gone).
    pub fn idle_without_consumers(since: Millis) -> Shutdown {
        Shutdown {
            cause: ShutdownCause::Idle,
            since,
            withdrawal: Withdrawal::NotNeeded,
            veto: Veto::None,
        }
    }

    /// A shutdown a person asked for, with its withdrawal still to do.
    pub fn requested(mode: DrainMode, since: Millis) -> Shutdown {
        Shutdown {
            cause: ShutdownCause::Requested { mode },
            since,
            withdrawal: Withdrawal::Pending,
            veto: Veto::None,
        }
    }

    /// The shutdown a lockout forces on an exclusive host (the model was wound down first:
    /// nothing to withdraw).
    pub fn lockout_without_consumers(since: Millis) -> Shutdown {
        Shutdown {
            cause: ShutdownCause::Lockout,
            since,
            withdrawal: Withdrawal::NotNeeded,
            veto: Veto::None,
        }
    }

    /// The shutdown a lockout forces on a shared host, with the withdrawal of its consumers
    /// still to do (in-flight work gets `lockout_timeout`).
    pub fn lockout_withdrawing(since: Millis) -> Shutdown {
        Shutdown {
            cause: ShutdownCause::Lockout,
            since,
            withdrawal: Withdrawal::Pending,
            veto: Veto::None,
        }
    }

    /// Whether a lockout forced this shutdown.
    pub fn is_lockout(&self) -> bool {
        self.cause == ShutdownCause::Lockout
    }

    pub fn cause(&self) -> ShutdownCause {
        self.cause
    }

    /// When the shutdown was requested (or, for an idle one, started).
    pub fn since(&self) -> Millis {
        self.since
    }

    pub fn withdrawal(&self) -> Withdrawal {
        self.withdrawal
    }

    pub fn veto(&self) -> Veto {
        self.veto
    }

    /// Whether the consumers that need the host are out (withdrawn).
    pub fn consumers_withdrawn(&self) -> bool {
        self.withdrawal == Withdrawal::Done
    }

    /// Whether this shutdown has a withdrawal to track and keep: asked for by a person, or
    /// withdrawing the consumers of an idle shared host. (The shutdown of an exclusive host
    /// after its model went, or a stopped idle one whose consumers came back, has none.)
    pub fn is_drain(&self) -> bool {
        self.withdrawal != Withdrawal::NotNeeded
    }

    /// Whether a person asked for this shutdown.
    pub fn is_requested(&self) -> bool {
        matches!(self.cause, ShutdownCause::Requested { .. })
    }

    /// When the veto wait began, while it is waiting.
    pub fn vetoing_since(&self) -> Option<Millis> {
        match self.veto {
            Veto::Vetoing { since } => Some(since),
            _ => None,
        }
    }

    /// When the scheduler gave up, once it has stopped.
    pub fn stopped_since(&self) -> Option<Millis> {
        match self.veto {
            Veto::Stopped { since } => Some(since),
            _ => None,
        }
    }

    /// The withdrawal done. Only a requested shutdown has one pending; others are unchanged.
    pub fn withdrawn(self) -> Shutdown {
        match self.withdrawal {
            Withdrawal::Pending => Shutdown {
                withdrawal: Withdrawal::Done,
                ..self
            },
            _ => self,
        }
    }

    /// The veto waiting since `since`. A shutdown still withdrawing has no veto yet and is
    /// returned unchanged.
    pub fn vetoing(self, since: Millis) -> Shutdown {
        match self.withdrawal {
            Withdrawal::Pending => self,
            _ => Shutdown {
                veto: Veto::Vetoing { since },
                ..self
            },
        }
    }

    /// The veto given up at `at`. Nobody asked for the consumers of an idle shutdown to stay
    /// down, so they come back (nothing is withdrawn any more); a requested shutdown keeps its
    /// consumers out. A shutdown still withdrawing has no veto yet and is returned unchanged.
    pub fn stopped(self, at: Millis) -> Shutdown {
        match (self.withdrawal, self.cause) {
            (Withdrawal::Pending, _) => self,
            (_, ShutdownCause::Idle) => Shutdown {
                withdrawal: Withdrawal::NotNeeded,
                veto: Veto::Stopped { since: at },
                ..self
            },
            _ => Shutdown {
                veto: Veto::Stopped { since: at },
                ..self
            },
        }
    }

    /// The shutdown after a veto wait that is over (the host is unused) or no longer due:
    /// back to waiting before the veto if consumers were withdrawn, and no shutdown at all
    /// for one that never withdrew any (an exclusive host's). A veto that is not waiting
    /// (none, or stopped) leaves the shutdown unchanged.
    pub fn without_veto(self) -> Option<Shutdown> {
        match self.veto {
            Veto::Vetoing { .. } => (self.withdrawal == Withdrawal::Done).then_some(Shutdown {
                veto: Veto::None,
                ..self
            }),
            _ => Some(self),
        }
    }

    /// A person's request takes over a shutdown that has a withdrawal (see [`is_drain`]):
    /// it is a requested one from now on, in `mode`, with its start and withdrawal kept and a
    /// stop tried afresh. `None` for one with nothing to withdraw (it starts anew instead).
    ///
    /// [`is_drain`]: Shutdown::is_drain
    pub fn taken_over(self, mode: DrainMode) -> Option<Shutdown> {
        self.is_drain().then_some(Shutdown {
            cause: ShutdownCause::Requested { mode },
            veto: match self.veto {
                Veto::Stopped { .. } => Veto::None,
                v => v,
            },
            ..self
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceState {
    pub power: PowerPhase,
    pub mode: ModePhase,
    /// Whether the scheduler may power this host down: set when it woke the host or took
    /// over a manual request, never for a host merely found running. (Under `control: managed`
    /// the service turns a shared host found running into a manual request by minting a manual
    /// host lease for it, see `sched_sequencer::adopt`; the sequencer itself still never owns
    /// a host it only found.)
    #[serde(default)]
    pub managed: bool,
    /// When the host last had nothing holding it (the start of the `idle_grace` clock).
    pub idle_since: Option<Millis>,
    /// Classes whose in-flight work was stopped and whose recovery action has not run.
    #[serde(default)]
    pub pending_recovery: BTreeSet<ClassId>,
    /// The host's shutdown, while it is up and on its way down (or stopped on the way).
    pub shutdown: Option<Shutdown>,
    /// When the host was observed down after flowmanager shut it down (the instant the phase
    /// became `off` from `shutting_down`, or from a failed shutdown). It starts the `min_off`
    /// wait before the next automatic power-on. Absent for a host found down, one that went down
    /// by itself, once the phase leaves `off`, and for a resource without a `min_off`.
    pub off_since: Option<Millis>,
    /// Whether the host was seen off while it was locked out: the lockout has done its one
    /// shutdown. From then on flowmanager leaves the host alone until the lockout is
    /// cleared: a later boot (another OS, physical work) is not powered off, not adopted and
    /// not given work. Set when the host is seen off under a lockout (also one taken while it
    /// was already off); cleared as soon as no lockout applies. Never set under
    /// `control: manual`.
    #[serde(default)]
    pub locked_down: bool,
}

impl ResourceState {
    pub fn new(power: PowerPhase) -> Self {
        ResourceState {
            power,
            mode: ModePhase::Idle,
            managed: false,
            idle_since: None,
            pending_recovery: BTreeSet::new(),
            shutdown: None,
            off_since: None,
            locked_down: false,
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
        match (self.power, self.off_since) {
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
            r#"{"phase":"serving","model":"llm","since":1,"idle_since":5,"wind_down":{"since":3,"timeout":600000,"lockout":false,"killed":["c"]}}"#,
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
                    lockout: false,
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

    const BASE: &str = r#"{"power":{"phase":"on","since":1},"mode":{"phase":"idle"},"managed":true,"idle_since":null,"pending_recovery":[],"shutdown":SHUTDOWN,"off_since":null}"#;

    fn resource_with(shutdown: &str) -> Result<ResourceState, serde_json::Error> {
        serde_json::from_str(&BASE.replace("SHUTDOWN", shutdown))
    }

    /// Every shutdown the transitions can reach from the constructors.
    fn reachable() -> Vec<Shutdown> {
        let mut seen: Vec<Shutdown> = vec![
            Shutdown::idle_withdrawn(Millis(1)),
            Shutdown::idle_without_consumers(Millis(1)),
            Shutdown::requested(DrainMode::Graceful, Millis(1)),
            Shutdown::requested(DrainMode::Immediate, Millis(1)),
            Shutdown::lockout_without_consumers(Millis(1)),
            Shutdown::lockout_withdrawing(Millis(1)),
        ];
        let mut i = 0;
        while i < seen.len() {
            let s = seen[i];
            i += 1;
            let mut next = vec![s.withdrawn(), s.vetoing(Millis(5)), s.stopped(Millis(6))];
            next.extend(s.without_veto());
            next.extend(s.taken_over(DrainMode::Graceful));
            next.extend(s.taken_over(DrainMode::Immediate));
            for n in next {
                if !seen.contains(&n) {
                    seen.push(n);
                }
            }
        }
        seen
    }

    #[test]
    fn every_shutdown_the_transitions_reach_is_a_possible_one_and_reads_back() {
        let all = reachable();
        // 3 causes' worth of states: idle {done: none, vetoing; not needed: none, vetoing,
        // stopped}, requested x2 modes {pending; done: none, vetoing, stopped}, lockout
        // {pending; done: none, vetoing, stopped; not needed: none, vetoing, stopped}.
        assert_eq!(all.len(), 5 + 2 * 4 + 7, "{all:#?}");
        for s in all {
            assert!(s.is_possible(), "{s:?}");
            let mut st = resource_with("null").unwrap();
            st.shutdown = Some(s);
            let text = serde_json::to_string(&st).unwrap();
            assert_eq!(serde_json::from_str::<ResourceState>(&text).unwrap(), st);
        }
    }

    #[test]
    fn a_shutdown_that_cannot_exist_is_refused_when_read() {
        let cause = |c: &str| match c {
            "idle" => r#"{"kind":"idle"}"#,
            "requested" => r#"{"kind":"requested","mode":"graceful"}"#,
            _ => r#"{"kind":"lockout"}"#,
        };
        let text = |c: &str, withdrawal: &str, veto: &str| {
            format!(
                r#"{{"cause":{},"since":2,"withdrawal":"{withdrawal}","veto":{veto}}}"#,
                cause(c)
            )
        };
        let none = r#"{"phase":"none"}"#;
        let vetoing = r#"{"phase":"vetoing","since":5}"#;
        let stopped = r#"{"phase":"stopped","since":5}"#;
        // The combinations the type does not have.
        for (c, w, v) in [
            ("idle", "pending", none),
            ("lockout", "pending", vetoing),
            ("lockout", "pending", stopped),
            ("idle", "done", stopped),
            ("requested", "not_needed", vetoing),
            ("requested", "not_needed", stopped),
            ("requested", "pending", vetoing),
            ("requested", "pending", stopped),
        ] {
            let err = resource_with(&text(c, w, v)).unwrap_err().to_string();
            assert!(err.contains("cannot exist"), "{c} {w} {v}: {err}");
        }
        // Some that it does.
        for (c, w, v) in [
            ("idle", "done", none),
            ("idle", "done", vetoing),
            ("idle", "not_needed", stopped),
            ("requested", "pending", none),
            ("requested", "done", stopped),
            ("lockout", "not_needed", vetoing),
            ("lockout", "pending", none),
            ("lockout", "done", none),
            ("lockout", "done", stopped),
        ] {
            assert!(resource_with(&text(c, w, v)).is_ok(), "{c} {w} {v}");
        }
        // Still strict.
        let extra = text("idle", "done", none).replace(r#""since":2"#, r#""since":2,"x":1"#);
        assert!(resource_with(&extra).is_err());
        assert!(resource_with(&text("idle", "gone", none)).is_err());
    }

    #[test]
    fn a_stored_requested_shutdown_reads_with_its_accessors() {
        let st = resource_with(
            r#"{"cause":{"kind":"requested","mode":"immediate"},"since":2,"withdrawal":"done","veto":{"phase":"vetoing","since":5}}"#,
        )
        .unwrap();
        let sh = st.shutdown.unwrap();
        assert!(sh.is_drain() && sh.is_requested() && sh.consumers_withdrawn());
        assert_eq!(sh.vetoing_since(), Some(Millis(5)));
        assert_eq!(sh.since(), Millis(2));
    }

    #[test]
    fn ending_a_veto_keeps_what_was_withdrawn_and_drops_what_never_was() {
        let waiting = Shutdown::idle_withdrawn(Millis(2)).vetoing(Millis(5));
        assert_eq!(
            waiting.without_veto(),
            Some(Shutdown::idle_withdrawn(Millis(2)))
        );
        // An exclusive host's shutdown never withdrew anything: nothing is left of it.
        for s in [
            Shutdown::idle_without_consumers(Millis(2)),
            Shutdown::lockout_without_consumers(Millis(2)),
        ] {
            assert_eq!(s.vetoing(Millis(5)).without_veto(), None);
        }
        // A shared host's lockout keeps its consumers out, and stays one even when stopped.
        let l = Shutdown::lockout_withdrawing(Millis(2)).withdrawn();
        assert!(l.is_lockout() && l.consumers_withdrawn());
        assert!(l.stopped(Millis(7)).consumers_withdrawn());
        let stopped = waiting.stopped(Millis(7));
        assert_eq!(stopped.without_veto(), Some(stopped));
        assert!(!stopped.is_drain());
    }

    #[test]
    fn stopping_returns_the_consumers_of_an_idle_shutdown_and_keeps_a_requested_ones_out() {
        let idle = Shutdown::idle_withdrawn(Millis(2))
            .vetoing(Millis(5))
            .stopped(Millis(7));
        assert!(!idle.consumers_withdrawn());
        assert_eq!(idle.withdrawal(), Withdrawal::NotNeeded);
        assert_eq!(idle.stopped_since(), Some(Millis(7)));
        let requested = Shutdown::requested(DrainMode::Graceful, Millis(2))
            .withdrawn()
            .vetoing(Millis(5))
            .stopped(Millis(7));
        assert!(requested.consumers_withdrawn() && requested.is_drain());
        // Taking over a stop tries again; one with no withdrawal starts anew instead.
        let again = requested.taken_over(DrainMode::Immediate).unwrap();
        assert_eq!((again.veto(), again.since()), (Veto::None, Millis(2)));
        assert_eq!(idle.taken_over(DrainMode::Immediate), None);
    }

    #[test]
    fn a_shutdown_still_withdrawing_has_no_veto() {
        let s = Shutdown::requested(DrainMode::Graceful, Millis(2));
        assert_eq!(s.vetoing(Millis(5)), s);
        assert_eq!(s.stopped(Millis(5)), s);
        assert_eq!(s.withdrawn().withdrawal(), Withdrawal::Done);
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
