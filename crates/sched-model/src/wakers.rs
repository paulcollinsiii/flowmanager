//! The wakers of a host (`planning/power-model.md`, 2.1d): the classes that would power it
//! back on if it went down now. One pure rule, read by the planner (the wake for a class), the
//! sequencer (a graceful requested shutdown is cancelled by a waker), the status the report
//! carries per host, and the refusal of a graceful `off`, so they cannot disagree.

use serde::{Deserialize, Serialize};

use crate::config::{ClassSpec, Config};
use crate::ids::{ClassId, ResourceId};
use crate::observation::Snapshot;
use crate::state::SchedState;
use crate::work::{class_switch, class_work};

/// Why a class wants its host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WakeReason {
    /// A switched class whose switch reads `on` (a usable reading).
    SwitchedOn,
    /// A class with queued or in-flight work.
    Work,
}

impl WakeReason {
    pub fn as_str(self) -> &'static str {
        match self {
            WakeReason::SwitchedOn => "switched_on",
            WakeReason::Work => "work",
        }
    }

    /// The reason in words, for a message to a person.
    pub fn describe(self) -> &'static str {
        match self {
            WakeReason::SwitchedOn => "switched on",
            WakeReason::Work => "queued or in-flight work",
        }
    }
}

/// A class that would power a host back on, and why.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Waker {
    pub class: ClassId,
    pub why: WakeReason,
}

/// Why `class` wants the hosts it requires, if it does: it may change power, and it is a
/// switched class reading `on` or, if it is not switched, has busy work. A switched class has
/// no demand of its own: its switch is the want. Unknown demand wants nothing (it wakes no
/// host), nor does a switched class without a usable `on`.
fn wake_reason(
    config: &Config,
    snapshot: &Snapshot,
    state: &SchedState,
    class: &ClassSpec,
) -> Option<WakeReason> {
    if !class.may_change_power {
        return None;
    }
    match class_switch(config, snapshot, class) {
        Some(switch) => switch.on().then_some(WakeReason::SwitchedOn),
        None => class_work(config, snapshot, state, class)
            .busy()
            .then_some(WakeReason::Work),
    }
}

/// The wakers of `resource`, in configuration order of the classes: each class that may change
/// power, requires the host and wants it (see [`WakeReason`]). Empty for a resource without a
/// power section and for a locked-out host (nothing wakes it). A job's hold is not a waker: it
/// is a lease, which a graceful drain already waits for. `state` is the sequencer's state as the
/// tick began, as for [`class_work`].
pub fn wakers(
    config: &Config,
    snapshot: &Snapshot,
    state: &SchedState,
    resource: &ResourceId,
) -> Vec<Waker> {
    if config.resource(resource).is_none_or(|r| r.power.is_none())
        || config.locked_out(&snapshot.leases, resource, snapshot.now)
    {
        return Vec::new();
    }
    config
        .classes()
        .iter()
        .filter(|c| c.requires.iter().any(|r| &r.resource == resource))
        .filter_map(|c| {
            wake_reason(config, snapshot, state, c).map(|why| Waker {
                class: c.id.clone(),
                why,
            })
        })
        .collect()
}

/// The wakers in words: `class (why), ...`.
pub fn wakers_label(wakers: &[Waker]) -> String {
    wakers
        .iter()
        .map(|w| format!("{} ({})", w.class, w.why.describe()))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::lease::{Lease, LeaseKind, LeaseOrigin};
    use crate::observation::{Fidelity, Observation, ObservedValue, SwitchState};
    use crate::test_support::{example_yaml_switched_dependents, with_nfs_batch};
    use crate::time::Millis;

    const NOW: u64 = 100_000_000;
    const NFS: &str = "basestar-nfs";

    /// The managed example with `nfs-app` (switched, may change power), `nfs-batch` (demand,
    /// may change power), `quiet-app` (switched, may NOT change power) and the example's own
    /// `paperless-ocr` (demand, may not change power), all on the NFS host.
    fn config() -> Config {
        let quiet = "  - id: quiet-app\n    requires:\n      - { resource: basestar-nfs }\n    priority: 0\n    preemptibility: kill\n    switch: { source: ha-wikijs }\n\nsources:\n";
        let yaml = with_nfs_batch(&example_yaml_switched_dependents()).replacen(
            "\nsources:\n",
            &format!("\n{quiet}"),
            1,
        );
        Config::from_yaml(&yaml).unwrap_or_else(|e| panic!("{e}"))
    }

    fn obs(source: &str, at: u64, value: ObservedValue) -> Observation {
        Observation {
            source: source.into(),
            observed_at: Millis(at),
            fidelity: Fidelity::Polled,
            value,
        }
    }

    fn switch(class: &str, state: SwitchState, at: u64) -> Observation {
        obs(
            "ha-wikijs",
            at,
            ObservedValue::Switch {
                class: class.into(),
                state,
            },
        )
    }

    fn demand(class: &str, source: &str, ready: u64, in_flight: u64) -> Observation {
        obs(
            source,
            NOW,
            ObservedValue::Demand {
                class: class.into(),
                ready,
                in_flight,
                failed: 0,
                oldest_ready_at: (ready > 0).then_some(Millis(NOW - 1000)),
            },
        )
    }

    fn batch(ready: u64, in_flight: u64) -> Observation {
        demand("nfs-batch", "nfs-batch-queue", ready, in_flight)
    }

    fn snap(mut observations: Vec<Observation>, leases: Vec<Lease>) -> Snapshot {
        observations.push(obs(
            "cluster-pods",
            NOW,
            ObservedValue::ResourceUp {
                resource: NFS.into(),
                up: true,
            },
        ));
        Snapshot {
            now: Millis(NOW),
            observations,
            leases,
            ..Snapshot::default()
        }
    }

    fn lease(kind: LeaseKind) -> Lease {
        Lease {
            id: "l1".into(),
            resource: NFS.into(),
            kind,
            granted_at: Millis(NOW - 1),
            origin: LeaseOrigin::default(),
        }
    }

    fn list(observations: Vec<Observation>, leases: Vec<Lease>) -> Vec<(String, WakeReason)> {
        wakers(
            &config(),
            &snap(observations, leases),
            &SchedState::default(),
            &NFS.into(),
        )
        .into_iter()
        .map(|w| (w.class.as_str().to_string(), w.why))
        .collect()
    }

    #[test]
    fn a_host_with_nothing_busy_or_on_has_no_wakers() {
        assert_eq!(list(vec![], vec![]), vec![]);
        assert_eq!(list(vec![batch(0, 0)], vec![]), vec![]);
    }

    #[test]
    fn a_busy_class_that_may_change_power_is_a_waker_for_queued_or_in_flight_work() {
        let work = vec![("nfs-batch".to_string(), WakeReason::Work)];
        assert_eq!(list(vec![batch(3, 0)], vec![]), work);
        assert_eq!(list(vec![batch(0, 1)], vec![]), work);
    }

    #[test]
    fn a_switched_class_is_a_waker_while_its_switch_reads_on_and_not_otherwise() {
        let on = vec![("nfs-app".to_string(), WakeReason::SwitchedOn)];
        assert_eq!(
            list(vec![switch("nfs-app", SwitchState::On, NOW)], vec![]),
            on
        );
        for (name, o) in [
            ("off", Some(switch("nfs-app", SwitchState::Off, NOW))),
            ("off_now", Some(switch("nfs-app", SwitchState::OffNow, NOW))),
            ("missing", None),
            (
                "stale",
                Some(switch("nfs-app", SwitchState::On, NOW - 10 * 60_000 - 1)),
            ),
            ("future", Some(switch("nfs-app", SwitchState::On, NOW + 1))),
        ] {
            assert_eq!(list(o.into_iter().collect(), vec![]), vec![], "{name}");
        }
    }

    #[test]
    fn wakers_are_listed_in_configuration_order_each_with_its_reason() {
        let got = list(
            vec![batch(1, 0), switch("nfs-app", SwitchState::On, NOW)],
            vec![],
        );
        let config = config();
        let order: Vec<&str> = config
            .classes()
            .iter()
            .map(|c| c.id.as_str())
            .filter(|c| ["nfs-app", "nfs-batch"].contains(c))
            .collect();
        let names: Vec<&str> = got.iter().map(|(c, _)| c.as_str()).collect();
        assert_eq!(names, order);
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn what_is_not_a_waker() {
        // Unknown demand: nfs-batch has no reading at all.
        assert_eq!(list(vec![], vec![]), vec![], "unknown demand");
        // A busy class that may not change power, and a switched one reading on.
        let quiet = list(
            vec![
                demand("paperless-ocr", "paperless-ocr-pending", 5, 2),
                switch("quiet-app", SwitchState::On, NOW),
            ],
            vec![],
        );
        assert_eq!(quiet, vec![], "classes without may_change_power");
        // A job's hold, and a manual lease, are leases, not wakers.
        let hold = LeaseKind::SharedHold {
            holder: "job".into(),
        };
        assert_eq!(list(vec![], vec![lease(hold)]), vec![], "a job's hold");
        assert_eq!(
            list(vec![], vec![lease(LeaseKind::ManualHost)]),
            vec![],
            "a manual lease"
        );
    }

    #[test]
    fn a_locked_out_host_has_no_wakers() {
        let busy = vec![batch(2, 2), switch("nfs-app", SwitchState::On, NOW)];
        assert_eq!(list(busy.clone(), vec![]).len(), 2);
        assert_eq!(list(busy, vec![lease(LeaseKind::Lockout)]), vec![]);
    }

    #[test]
    fn an_unknown_resource_has_none() {
        let s = snap(vec![batch(2, 2)], vec![]);
        let st = SchedState::default();
        assert!(wakers(&config(), &s, &st, &"nowhere".into()).is_empty());
    }

    #[test]
    fn the_label_names_each_waker_and_why() {
        let w = vec![
            Waker {
                class: "a".into(),
                why: WakeReason::SwitchedOn,
            },
            Waker {
                class: "b".into(),
                why: WakeReason::Work,
            },
        ];
        assert_eq!(
            wakers_label(&w),
            "a (switched on), b (queued or in-flight work)"
        );
    }
}
