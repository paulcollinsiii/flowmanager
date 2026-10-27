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
    /// Items that failed and wait to be re-queued by the class's recovery action. A separate
    /// bucket: failed items are not `ready`, never contribute to `oldest_ready_at` (so they
    /// never age into starvation priority) and do not make the class `busy` (they hold no
    /// model, host or lease). They do make the class's model *wanted* (see
    /// [`ClassWork::wanted`]), so the model is brought in to re-queue them.
    pub failed: u64,
    pub oldest_ready_at: Option<Millis>,
    /// False when the class has demand but its observation is missing or older than
    /// `queue_reading_stale_after`. Unknown work must be assumed possibly in flight.
    pub known: bool,
}

impl ClassWork {
    const NONE: ClassWork = ClassWork {
        ready: 0,
        in_flight: 0,
        failed: 0,
        oldest_ready_at: None,
        known: true,
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

    /// Age of the oldest ready item; zero if it has no timestamp.
    pub fn oldest_age(&self, now: Millis) -> Millis {
        self.oldest_ready_at
            .map_or(Millis::ZERO, |t| now.saturating_sub(t))
    }
}

/// The current work of `class`. Classes without a demand source never have work, nor do
/// classes whose source is listed in `Snapshot::unobservable` and has not reported.
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
mod tests {
    use super::*;
    use crate::observation::{Fidelity, Observation};
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
        let w = class_work(&config, &snap(3), class);
        assert_eq!(w.failed, 3);
        assert!(w.wanted() && !w.busy() && w.oldest_ready_at.is_none());
        assert_eq!(w.oldest_age(Millis(1_000_000)), Millis::ZERO);
        let w = class_work(&config, &snap(0), class);
        assert!(!w.wanted());
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
        assert!(!class_work(&config, &snap, class).known);
        // Listed as unobservable: known, no work.
        snap.unobservable.insert("llm-sessions".into());
        let w = class_work(&config, &snap, class);
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
        assert!(!class_work(&config, &snap, class).known);
        snap.observations[0].observed_at = Millis(999_000);
        let w = class_work(&config, &snap, class);
        assert!(w.known && w.busy());
    }
}
