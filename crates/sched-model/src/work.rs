//! Queries over a config and a snapshot that both the planner and the sequencer need, kept
//! here so the two cannot disagree about what counts as work.

use crate::config::{Allowed, ClassSpec, Config};
use std::collections::BTreeMap;

use crate::ids::{ClassId, ModelId};
use crate::observation::{ObservedValue, Snapshot, SwitchState};
use crate::state::{SchedState, SwitchOff, Withdrawal};
use crate::time::Millis;

/// The demand for one class as the scheduler may rely on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClassWork {
    /// Items whose upstream steps are done. Zero when the demand is not known.
    pub ready: u64,
    pub in_flight: u64,
    /// Items that failed and wait to be re-queued by the class's recovery action. A separate
    /// bucket: failed items are not `ready`, never contribute to `oldest_ready_at` (so they
    /// never age into starvation priority) and do not make the class `busy` (they hold no
    /// model, host or lease). They do make the class's model *wanted* (see
    /// [`ClassWork::wanted`]), so the model is brought in to re-queue them.
    pub failed: u64,
    pub oldest_ready_at: Option<Millis>,
    /// False when the class has demand but its observation is missing or older than
    /// `queue_reading_stale_after`, while every host it requires may be up. Unknown work must
    /// be assumed possibly in flight.
    pub known: bool,
    /// The observation is missing or stale but a host the class requires is freshly seen down:
    /// known to have no work. Derived on every read, never stored; only reports tell it apart
    /// from an empty queue.
    ///
    /// Also set when the switched class the demand source `reads_from` is off and its
    /// graceful wait is over (`switched_off` says which cause).
    pub offline: bool,
    /// `offline` because the switched class the demand source reads from is switched off, not
    /// because a host is down.
    pub switched_off: bool,
    /// Ready items not counted in `ready` because the switched class the demand source reads
    /// from is waiting out a graceful off: they want no model and wake no host.
    pub withheld: u64,
}

impl ClassWork {
    const NONE: ClassWork = ClassWork {
        ready: 0,
        in_flight: 0,
        failed: 0,
        oldest_ready_at: None,
        known: true,
        offline: false,
        switched_off: false,
        withheld: 0,
    };

    /// Ready or in-flight work is known to exist.
    pub fn busy(&self) -> bool {
        self.ready > 0 || self.in_flight > 0
    }

    /// Work that makes the class's model worth loading: ready, in-flight or failed items
    /// (failed ones are waiting for the model to be allowed so they can be re-queued).
    /// Starvation never looks at failed items; only `ready` ages.
    pub fn wanted(&self) -> bool {
        self.busy() || self.failed > 0
    }

    /// Work that keeps an `allowed: with_work` class allowed: [`ClassWork::wanted`], or a count
    /// that is not known. Offline is none.
    pub fn has_work(&self) -> bool {
        !self.offline && (self.wanted() || !self.known)
    }

    /// Age of the oldest ready item; zero if it has no timestamp.
    pub fn oldest_age(&self, now: Millis) -> Millis {
        self.oldest_ready_at
            .map_or(Millis::ZERO, |t| now.saturating_sub(t))
    }
}

/// The current work of `class`. Classes without a demand source never have work, nor do
/// classes whose source is listed in `Snapshot::unobservable` and has not reported, nor
/// offline ones (see [`ClassWork::offline`]).
///
/// `state` is the sequencer's state as the tick began. A class whose demand source
/// `reads_from` a switched class is offline once that class is off and its graceful wait is
/// over, and has no ready work while the wait runs (see [`switch_phase`], the one rule the
/// planner and the sequencer both read).
pub fn class_work(
    config: &Config,
    snapshot: &Snapshot,
    state: &SchedState,
    class: &ClassSpec,
) -> ClassWork {
    let work = demand_work(config, snapshot, class);
    let Some(switched) = reads_from_class(config, class) else {
        return work;
    };
    match switch_phase(config, snapshot, state, switched) {
        // The person turned the app off and the wait is over: offline, whatever a fresh
        // demand reading says (it does not win over a switch-off, unlike a host-offline).
        Some(SwitchPhase::Off) => ClassWork {
            offline: true,
            switched_off: true,
            ..ClassWork::NONE
        },
        // The wait: in-flight work is still honoured, ready work wants nothing.
        Some(SwitchPhase::Waiting { .. }) => ClassWork {
            ready: 0,
            oldest_ready_at: None,
            withheld: work.ready,
            ..work
        },
        Some(SwitchPhase::On) | None => work,
    }
}

/// The switched class the demand source of `class` reads from, if any.
pub fn reads_from_class<'a>(config: &'a Config, class: &ClassSpec) -> Option<&'a ClassSpec> {
    let source = config.source(&class.demand.as_ref()?.source)?;
    config.class(source.reads_from.as_ref()?)
}

/// The classes whose demand source declares `reads_from` the switched class `class`, in
/// configuration order.
pub fn dependents<'a>(
    config: &'a Config,
    class: &'a ClassId,
) -> impl Iterator<Item = &'a ClassSpec> {
    config
        .classes()
        .iter()
        .filter(move |k| reads_from_class(config, k).is_some_and(|s| &s.id == class))
}

/// The demand a class's source reports, before any switch-off is applied.
fn demand_work(config: &Config, snapshot: &Snapshot, class: &ClassSpec) -> ClassWork {
    let Some(demand) = &class.demand else {
        return ClassWork::NONE;
    };
    // Observations are filtered by class AND the class's configured demand source before
    // the newest timestamp is chosen, so an observation of this class from another source
    // is ignored and cannot make the demand unknown. Only the newest timestamp then counts;
    // observations tied at it are merged conservatively (most ready, most in flight, oldest
    // item), so snapshot order never matters. A newest observation that is stale or
    // future-dated makes the class's demand unknown.
    let group = snapshot.demand_group(&demand.source, &class.id);
    // No usable reading. With a host the class requires freshly seen down its work cannot run
    // and its source has good reason to be silent, so it is offline; otherwise it is unknown.
    // A fresh reading always wins, offline or not: in-flight work it reports is still waited for.
    let max_age = config.policy().host_reading_stale_after;
    let unknown = if class
        .requires
        .iter()
        .any(|r| snapshot.fresh_resource_up(config, &r.resource, max_age) == Some(false))
    {
        ClassWork {
            offline: true,
            ..ClassWork::NONE
        }
    } else {
        ClassWork {
            known: false,
            ..ClassWork::NONE
        }
    };
    let Some(first) = group.first() else {
        // A source that can never report and never has: no demand can ever arrive, so
        // there is none (not "unknown", which would hold models and hosts forever).
        if snapshot.unobservable.contains(&demand.source) {
            return ClassWork::NONE;
        }
        return unknown;
    };
    if !first.is_fresh(snapshot.now, config.policy().max_age_for(&first.value)) {
        return unknown;
    }
    let mut work = ClassWork::NONE;
    for o in &group {
        if let ObservedValue::Demand {
            ready,
            in_flight,
            failed,
            oldest_ready_at,
            ..
        } = o.value
        {
            work.ready = work.ready.max(ready);
            work.in_flight = work.in_flight.max(in_flight);
            work.failed = work.failed.max(failed);
            work.oldest_ready_at = match (work.oldest_ready_at, oldest_ready_at) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
        }
    }
    work
}

/// What a switched class's switch allows. The one place the rule lives: the planner (wakes),
/// the sequencer (allowed set, holds), the status text and the invariant check all read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClassSwitch {
    /// The newest reading's state, or `OffNow` when there is no usable reading (fail closed).
    pub state: SwitchState,
    /// False when `state` is `OffNow` only because the reading is missing, stale or
    /// future-dated, not because a person pressed it.
    pub read: bool,
}

impl ClassSwitch {
    /// The switch itself reads `on`. A class may also run while a graceful `off` waits, which
    /// is not this switch's say-so but [`switch_phase`]'s; use that to decide whether a class
    /// is allowed.
    pub fn on(&self) -> bool {
        self.state == SwitchState::On
    }
}

/// The switch of `class`, or `None` for a class that is not switched. Fail closed: a missing,
/// stale or future-dated reading is `off-now`. Observations are filtered by class AND the
/// class's configured switch source before the newest timestamp is chosen; ties at it
/// resolve to the most restrictive state, so snapshot order never matters.
pub fn class_switch(
    config: &Config,
    snapshot: &Snapshot,
    class: &ClassSpec,
) -> Option<ClassSwitch> {
    let switch = class.switch.as_ref()?;
    let closed = ClassSwitch {
        state: SwitchState::OffNow,
        read: false,
    };
    let group = snapshot.switch_group(&switch.source, &class.id);
    let Some(first) = group.first() else {
        return Some(closed);
    };
    if !first.is_fresh(snapshot.now, config.policy().max_age_for(&first.value)) {
        return Some(closed);
    }
    let state = group
        .iter()
        .filter_map(|o| match o.value {
            ObservedValue::Switch { state, .. } => Some(state),
            _ => None,
        })
        .max()
        .unwrap_or(SwitchState::OffNow);
    Some(ClassSwitch { state, read: true })
}

/// Where a switched class stands: the one decision "is this class off, its wait over?".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwitchPhase {
    /// The switch reads `on`.
    On,
    /// The switch reads `off` and a graceful wait is running: some dependent has in-flight or
    /// unknown work and `drain_timeout` has not passed since `off` was first read, at
    /// `until` minus the timeout. The class stays allowed.
    Waiting { until: Millis },
    /// Off with the wait over (or none needed): `off-now`, an unusable reading, a timed-out or
    /// finished wait, or an `off` read while no dependent has work.
    Off,
}

impl SwitchPhase {
    /// A class in this phase may run (switch `on`, or still waiting).
    pub fn allows(&self) -> bool {
        !matches!(self, SwitchPhase::Off)
    }
}

/// Whether a dependent of a switched class has work in flight, or unknown demand (which
/// counts as in flight). Reads the demand as reported, before any switch-off is applied.
fn dependents_unsettled(config: &Config, snapshot: &Snapshot, class: &ClassId) -> bool {
    dependents(config, class).any(|d| {
        let w = demand_work(config, snapshot, d);
        w.in_flight > 0 || !w.known
    })
}

/// The phase of the switched class `class`, or `None` for a class that is not switched. The
/// one rule for whether a switched class is off (graceful wait over), read by the planner
/// (wakes, dependents' ready work), the sequencer (allowed set, holds, offline dependents),
/// the status text and the invariant check, from the switch observation and the sequencer's
/// state as the tick began (`SchedState::classes`).
///
/// - `on`: [`SwitchPhase::On`].
/// - `off-now`, or a reading that is missing, stale or unmapped: `Off` at once.
/// - `off`: the wait starts at the stored `since`; with no entry yet it starts now, if every
///   resource the class requires is up (a class that was not running has nothing to wait for).
///   It ends, as `Off`, at `since + drain_timeout` or when no dependent has work in flight or
///   unknown; a stored `Done` ends it for good. Otherwise `Waiting`.
pub fn switch_phase(
    config: &Config,
    snapshot: &Snapshot,
    state: &SchedState,
    class: &ClassSpec,
) -> Option<SwitchPhase> {
    let switch = class_switch(config, snapshot, class)?;
    match switch.state {
        SwitchState::On => return Some(SwitchPhase::On),
        SwitchState::OffNow => return Some(SwitchPhase::Off),
        SwitchState::Off => {}
    }
    let since = match state.classes.get(&class.id) {
        Some(SwitchOff {
            withdrawal: Withdrawal::Pending,
            since,
        }) => *since,
        Some(_) => return Some(SwitchPhase::Off),
        None if class
            .requires
            .iter()
            .all(|r| host_is_up(config, snapshot, state, &r.resource)) =>
        {
            snapshot.now
        }
        None => return Some(SwitchPhase::Off),
    };
    let until = since.saturating_add(config.policy().drain_timeout);
    if snapshot.now >= until || !dependents_unsettled(config, snapshot, &class.id) {
        return Some(SwitchPhase::Off);
    }
    Some(SwitchPhase::Waiting { until })
}

/// The `SchedState::classes` after a tick that began with `state`: an entry for every
/// switched class whose switch does not read `on`, `Pending` while its wait runs and `Done`
/// once the class has left, each keeping its original `since`; none for a class that reads
/// `on` or is no longer configured. A pure transition, applied by the sequencer.
pub fn switch_offs_next(
    config: &Config,
    snapshot: &Snapshot,
    state: &SchedState,
) -> BTreeMap<ClassId, SwitchOff> {
    let mut next = BTreeMap::new();
    for class in config.classes() {
        let Some(phase) = switch_phase(config, snapshot, state, class) else {
            continue;
        };
        let since = state
            .classes
            .get(&class.id)
            .map_or(snapshot.now, |prev| prev.since);
        let withdrawal = match phase {
            SwitchPhase::On => continue,
            SwitchPhase::Waiting { .. } => Withdrawal::Pending,
            SwitchPhase::Off => Withdrawal::Done,
        };
        next.insert(class.id.clone(), SwitchOff { since, withdrawal });
    }
    next
}

/// Whether `class` is a demand class that declares `allowed: with_work`.
pub fn gated_by_work(class: &ClassSpec) -> bool {
    class.allowed == Allowed::WithWork && class.demand.is_some()
}

/// Whether a stamp made at `stamp` is still within `policy.idle_grace` at `now`. A stamp in
/// the future (the clock went back) counts as just made.
pub fn work_grace_holds(config: &Config, stamp: Millis, now: Millis) -> bool {
    now.saturating_sub(stamp) < config.policy().idle_grace
}

/// Whether a class whose last stamp is `stamp` may be allowed at `now`: the stamp is this tick's
/// (it has work now, whatever `idle_grace` is, even 0) or still within `idle_grace`.
pub fn work_stamp_allows(config: &Config, stamp: Millis, now: Millis) -> bool {
    stamp >= now || work_grace_holds(config, stamp, now)
}

/// Whether `class` has work in the sense of `allowed: with_work`: `ClassWork::has_work` of
/// the class as `state` (the one the tick began with) reads it.
pub fn class_has_work(
    config: &Config,
    snapshot: &Snapshot,
    state: &SchedState,
    class: &ClassSpec,
) -> bool {
    class_work(config, snapshot, state, class).has_work()
}

/// The `SchedState::work_seen` after a tick that began with `state`: for every class that
/// declares `allowed: with_work`, `now` if it has work, else the earlier stamp while it is
/// within `policy.idle_grace`. Written whether or not the class's requirements hold. None for
/// a class without the key or no longer configured. A pure transition, applied by the
/// sequencer.
pub fn work_seen_next(
    config: &Config,
    snapshot: &Snapshot,
    state: &SchedState,
) -> BTreeMap<ClassId, Millis> {
    let mut next = BTreeMap::new();
    for class in config.classes().iter().filter(|c| gated_by_work(c)) {
        if class_has_work(config, snapshot, state, class) {
            next.insert(class.id.clone(), snapshot.now);
        } else if let Some(&stamp) = state.work_seen.get(&class.id)
            && work_grace_holds(config, stamp, snapshot.now)
        {
            next.insert(class.id.clone(), stamp);
        }
    }
    next
}

/// Classes that require `model`, in configuration order.
pub fn classes_of_model<'a>(
    config: &'a Config,
    model: &'a ModelId,
) -> impl Iterator<Item = &'a ClassSpec> {
    config
        .classes()
        .iter()
        .filter(move |c| c.requires.iter().any(|r| r.model.as_ref() == Some(model)))
}

/// Whether `resource`'s host is up as far as the scheduler knows: resources without a
/// power section are always up; otherwise the sequencer's state decides, falling back to a
/// fresh observation for a resource it has not seen yet.
pub fn host_is_up(
    config: &Config,
    snapshot: &Snapshot,
    state: &crate::state::SchedState,
    resource: &crate::ids::ResourceId,
) -> bool {
    let Some(spec) = config.resource(resource) else {
        return false;
    };
    if spec.power.is_none() {
        return true;
    }
    match state.resource(resource) {
        Some(st) => st.is_on(),
        None => snapshot
            .fresh_resource_up(config, resource, config.policy().host_reading_stale_after)
            .unwrap_or(false),
    }
}

#[cfg(test)]
mod graceful_off_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observation::{Fidelity, Observation};
    use crate::state::SchedState;
    use crate::test_support::example_config;

    #[test]
    fn failed_items_are_wanted_but_not_busy_and_never_give_an_age() {
        let config = example_config();
        let class = config.class(&"speakr-transcription".into()).unwrap();
        let snap = |failed: u64| Snapshot {
            now: Millis(1_000_000),
            observations: vec![Observation {
                source: "speakr-pending".into(),
                observed_at: Millis(999_000),
                fidelity: Fidelity::Polled,
                value: ObservedValue::Demand {
                    class: "speakr-transcription".into(),
                    ready: 0,
                    in_flight: 0,
                    failed,
                    oldest_ready_at: None,
                },
            }],
            ..Snapshot::default()
        };
        let w = class_work(&config, &snap(3), &SchedState::default(), class);
        assert_eq!(w.failed, 3);
        assert!(w.wanted() && !w.busy() && w.oldest_ready_at.is_none());
        assert_eq!(w.oldest_age(Millis(1_000_000)), Millis::ZERO);
        let w = class_work(&config, &snap(0), &SchedState::default(), class);
        assert!(!w.wanted());
    }

    #[test]
    fn no_reading_is_offline_only_while_a_required_host_is_freshly_seen_down() {
        let config = example_config();
        let class = config.class(&"paperless-ocr".into()).unwrap();
        let now = 1_000_000;
        let nfs = |up: bool, at: u64| Observation {
            source: "cluster-pods".into(),
            observed_at: Millis(at),
            fidelity: Fidelity::Event,
            value: ObservedValue::ResourceUp {
                resource: "basestar-nfs".into(),
                up,
            },
        };
        let work = |observations: Vec<Observation>| {
            let snap = Snapshot {
                now: Millis(now),
                observations,
                ..Snapshot::default()
            };
            class_work(&config, &snap, &SchedState::default(), class)
        };
        let w = work(vec![nfs(false, now)]);
        assert!(w.offline && w.known && !w.busy(), "fresh down: {w:?}");
        for (name, obs) in [
            ("no host reading", vec![]),
            ("host up", vec![nfs(true, now)]),
            ("stale down", vec![nfs(false, 1)]),
        ] {
            let w = work(obs);
            assert!(!w.offline && !w.known, "{name}: {w:?}");
        }
        // A fresh demand reading wins: reported work counts with the host down.
        let w = work(vec![
            nfs(false, now),
            Observation {
                source: "paperless-ocr-pending".into(),
                observed_at: Millis(now),
                fidelity: Fidelity::Polled,
                value: ObservedValue::Demand {
                    class: "paperless-ocr".into(),
                    ready: 0,
                    in_flight: 2,
                    failed: 0,
                    oldest_ready_at: None,
                },
            },
        ]);
        assert!(!w.offline && w.known && w.in_flight == 2, "{w:?}");
    }

    #[test]
    fn a_source_that_can_never_report_is_no_demand_until_it_reports() {
        let config = example_config();
        let class = config.class(&"interactive-llm".into()).unwrap();
        let mut snap = Snapshot {
            now: Millis(1_000_000),
            ..Snapshot::default()
        };
        // Missing reading of an ordinary source: unknown.
        assert!(!class_work(&config, &snap, &SchedState::default(), class).known);
        // Listed as unobservable: known, no work.
        snap.unobservable.insert("llm-sessions".into());
        let w = class_work(&config, &snap, &SchedState::default(), class);
        assert!(w.known && !w.busy());
        // A reading always wins, and a stale one is unknown again.
        snap.observations.push(Observation {
            source: "llm-sessions".into(),
            observed_at: Millis(1),
            fidelity: Fidelity::Event,
            value: ObservedValue::Demand {
                class: "interactive-llm".into(),
                ready: 1,
                in_flight: 0,
                failed: 0,
                oldest_ready_at: None,
            },
        });
        assert!(!class_work(&config, &snap, &SchedState::default(), class).known);
        snap.observations[0].observed_at = Millis(999_000);
        let w = class_work(&config, &snap, &SchedState::default(), class);
        assert!(w.known && w.busy());
    }

    fn switch_obs(source: &str, state: SwitchState, at: u64) -> Observation {
        Observation {
            source: source.into(),
            observed_at: Millis(at),
            fidelity: Fidelity::Polled,
            value: ObservedValue::Switch {
                class: "wikijs".into(),
                state,
            },
        }
    }

    #[test]
    fn a_switch_reads_its_newest_fresh_reading_and_fails_closed() {
        let config = example_config();
        let class = config.class(&"wikijs".into()).unwrap();
        let now = 1_000_000;
        let read = |observations: Vec<Observation>| {
            let snap = Snapshot {
                now: Millis(now),
                observations,
                ..Snapshot::default()
            };
            class_switch(&config, &snap, class).unwrap()
        };
        let closed = ClassSwitch {
            state: SwitchState::OffNow,
            read: false,
        };
        for (state, on) in [
            (SwitchState::On, true),
            (SwitchState::Off, false),
            (SwitchState::OffNow, false),
        ] {
            let s = read(vec![switch_obs("ha-wikijs", state, now)]);
            assert_eq!((s.state, s.read, s.on()), (state, true, on));
        }
        assert_eq!(read(vec![]), closed, "missing");
        let stale = now - 10 * 60_000 - 1;
        assert_eq!(
            read(vec![switch_obs("ha-wikijs", SwitchState::On, stale)]),
            closed,
            "stale"
        );
        assert_eq!(
            read(vec![switch_obs("ha-wikijs", SwitchState::On, now + 1)]),
            closed,
            "future-dated"
        );
        assert_eq!(
            read(vec![switch_obs("speakr-pending", SwitchState::On, now)]),
            closed,
            "wrong source"
        );
        // The newest reading wins; a wrong-source one never hides it, and neither does a
        // reading of another class.
        let mut other = switch_obs("ha-wikijs", SwitchState::On, now);
        other.value = ObservedValue::Switch {
            class: "other".into(),
            state: SwitchState::On,
        };
        let s = read(vec![
            switch_obs("ha-wikijs", SwitchState::On, now - 5),
            switch_obs("ha-wikijs", SwitchState::Off, now),
            switch_obs("speakr-pending", SwitchState::On, now),
        ]);
        assert_eq!((s.state, s.read), (SwitchState::Off, true));
        assert_eq!(read(vec![other]), closed, "another class");
        // Tied readings resolve to the most restrictive, in any order.
        let tied = vec![
            switch_obs("ha-wikijs", SwitchState::On, now),
            switch_obs("ha-wikijs", SwitchState::OffNow, now),
        ];
        let rev: Vec<_> = tied.iter().cloned().rev().collect();
        assert_eq!(read(tied).state, SwitchState::OffNow);
        assert_eq!(read(rev).state, SwitchState::OffNow);
        // A class that is not switched has no switch.
        let snap = Snapshot::default();
        let plain = config.class(&"paperless-ngx".into()).unwrap();
        assert_eq!(class_switch(&config, &snap, plain), None);
    }
}
