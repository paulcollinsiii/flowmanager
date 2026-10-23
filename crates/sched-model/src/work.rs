//! Queries over a config and a snapshot that both the planner and the sequencer need, kept
//! here so the two cannot disagree about what counts as work.

use crate::config::{ClassSpec, Config};
use crate::ids::ModelId;
use crate::observation::{ObservedValue, Snapshot};
use crate::time::Millis;

/// The demand for one class as the scheduler may rely on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClassWork {
    /// Items whose upstream steps are done. Zero when the demand is not known.
    pub ready: u64,
    pub in_flight: u64,
    pub oldest_ready_at: Option<Millis>,
    /// False when the class has demand but its observation is missing or older than
    /// `demand_max_age`. Unknown work must be assumed possibly in flight.
    pub known: bool,
}

impl ClassWork {
    const NONE: ClassWork = ClassWork {
        ready: 0,
        in_flight: 0,
        oldest_ready_at: None,
        known: true,
    };

    /// Ready or in-flight work is known to exist.
    pub fn busy(&self) -> bool {
        self.ready > 0 || self.in_flight > 0
    }

    /// Age of the oldest ready item; zero if it has no timestamp.
    pub fn oldest_age(&self, now: Millis) -> Millis {
        self.oldest_ready_at
            .map_or(Millis::ZERO, |t| now.saturating_sub(t))
    }
}

/// The current work of `class`. Classes without a demand source never have work.
pub fn class_work(config: &Config, snapshot: &Snapshot, class: &ClassSpec) -> ClassWork {
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
    let unknown = ClassWork {
        known: false,
        ..ClassWork::NONE
    };
    let Some(first) = group.first() else {
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
            oldest_ready_at,
            ..
        } = o.value
        {
            work.ready = work.ready.max(ready);
            work.in_flight = work.in_flight.max(in_flight);
            work.oldest_ready_at = match (work.oldest_ready_at, oldest_ready_at) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
        }
    }
    work
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
            .fresh_resource_up(config, resource, config.policy().safety_max_age)
            .unwrap_or(false),
    }
}
