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
    let obs = snapshot
        .observations
        .iter()
        .filter(|o| o.source == demand.source)
        .filter(|o| matches!(&o.value, ObservedValue::Demand { class: c, .. } if c == &class.id))
        .max_by_key(|o| o.observed_at);
    match obs {
        Some(o) if o.age(snapshot.now) <= config.policy().max_age_for(&o.value) => match o.value {
            ObservedValue::Demand {
                ready,
                in_flight,
                oldest_ready_at,
                ..
            } => ClassWork {
                ready,
                in_flight,
                oldest_ready_at,
                known: true,
            },
            _ => ClassWork::NONE,
        },
        _ => ClassWork {
            known: false,
            ..ClassWork::NONE
        },
    }
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
            .fresh_resource_up(resource, config.policy().safety_max_age)
            .unwrap_or(false),
    }
}
