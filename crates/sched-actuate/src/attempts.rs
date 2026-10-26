//! The bookkeeping that bounds a re-queue action: how often each item was re-queued, when
//! the action last ran, and which items a run may touch. Pure data and rules; the service
//! stores it durably and records an attempt before the request for it is sent.

use std::collections::{BTreeMap, BTreeSet};

use sched_model::{Millis, RequeueFailedParams};
use serde::{Deserialize, Serialize};

/// Re-queue attempts of one item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attempts {
    /// Attempts recorded, each before its request was sent (so a crash can only over-count).
    pub count: u32,
    /// When the latest was recorded.
    pub last_at: Millis,
}

/// What a run may do, decided from the list and the attempts so far.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Plan {
    /// Items to re-queue now, in list order, at most `max_per_run`.
    pub send: Vec<String>,
    /// Listed items that have used all their attempts (they are left failed).
    pub capped: usize,
    /// Items with attempts left that this run's `max_per_run` does not reach.
    pub deferred: usize,
}

/// Attempts per item id, and the last run.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptBook {
    /// When a run last started (it spaces runs by `min_interval`, including failed ones).
    #[serde(default)]
    pub last_run_at: Option<Millis>,
    #[serde(default)]
    pub attempts: BTreeMap<String, Attempts>,
}

impl AttemptBook {
    /// Whether a run may start at `now`: `min_interval` has passed since the last started.
    /// A clock that stepped back does not lock the action out for ever: it runs.
    pub fn run_due(&self, now: Millis, min_interval: Millis) -> bool {
        match self.last_run_at {
            Some(t) if t <= now => now.saturating_sub(t) >= min_interval,
            _ => true,
        }
    }

    pub fn mark_run(&mut self, now: Millis) {
        self.last_run_at = Some(now);
    }

    pub fn count(&self, id: &str) -> u32 {
        self.attempts.get(id).map_or(0, |a| a.count)
    }

    /// Records one more attempt for `id` at `now`. Call it, and persist the book, BEFORE
    /// sending the request.
    pub fn record(&mut self, id: &str, now: Millis) {
        let e = self.attempts.entry(id.to_string()).or_insert(Attempts {
            count: 0,
            last_at: now,
        });
        e.count = e.count.saturating_add(1);
        e.last_at = now;
    }

    /// Forgets items whose latest attempt is older than `window` AND that are not in the
    /// list: an item still listed is still failed, and forgetting it would give it fresh
    /// attempts. Returns how many were forgotten. Call it only with a list that was read
    /// successfully (a failed read says nothing about what is listed).
    pub fn prune(&mut self, now: Millis, window: Millis, listed: &[String]) -> usize {
        let listed: BTreeSet<&str> = listed.iter().map(String::as_str).collect();
        let before = self.attempts.len();
        self.attempts.retain(|id, a| {
            listed.contains(id.as_str())
                || a.last_at > now
                || now.saturating_sub(a.last_at) <= window
        });
        before - self.attempts.len()
    }

    /// Which of `listed` to re-queue now: those with attempts left, in list order and
    /// without repeats, at most `max_per_run`.
    pub fn plan(&self, params: &RequeueFailedParams, listed: &[String]) -> Plan {
        let mut plan = Plan::default();
        let mut seen = BTreeSet::new();
        for id in listed {
            if !seen.insert(id.as_str()) {
                continue;
            }
            if self.count(id) >= params.max_attempts {
                plan.capped += 1;
            } else if plan.send.len() >= params.max_per_run as usize {
                plan.deferred += 1;
            } else {
                plan.send.push(id.clone());
            }
        }
        plan
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sched_model::RecoverySpec;

    fn params(extra: &str) -> RequeueFailedParams {
        let spec: RecoverySpec = serde_yaml_ng_from(&format!(
            "action: requeue_failed\nparams:\n  list: {{ url: 'http://x/l', items: /i, id: /id }}\n  requeue: {{ url: 'http://x/{{id}}' }}\n{extra}"
        ));
        spec.requeue_failed_params().unwrap()
    }

    fn serde_yaml_ng_from(text: &str) -> RecoverySpec {
        // Through the public config loader's own types: RecoverySpec is deserializable.
        sched_model::test_support::recovery_spec(text)
    }

    fn ids(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn at_most_max_attempts_per_item_and_max_per_run_per_run() {
        let p = params("  max_per_run: 2\n");
        let mut b = AttemptBook::default();
        let listed = ids(&["a", "b", "c", "a"]);
        let plan = b.plan(&p, &listed);
        assert_eq!(plan.send, ids(&["a", "b"]));
        assert_eq!((plan.capped, plan.deferred), (0, 1));
        b.record("a", Millis(1));
        b.record("a", Millis(2));
        let plan = b.plan(&p, &listed);
        assert_eq!(plan.send, ids(&["b", "c"]));
        assert_eq!((plan.capped, plan.deferred), (1, 0));
    }

    #[test]
    fn runs_are_spaced_and_a_clock_stepping_back_never_locks_the_action_out() {
        let mut b = AttemptBook::default();
        assert!(b.run_due(Millis(0), Millis(60_000)));
        b.mark_run(Millis(1_000_000));
        assert!(!b.run_due(Millis(1_059_999), Millis(60_000)));
        assert!(b.run_due(Millis(1_060_000), Millis(60_000)));
        assert!(b.run_due(Millis(5), Millis(60_000)));
    }

    #[test]
    fn pruning_keeps_listed_items_and_recent_attempts() {
        let w = Millis(15 * 60_000);
        let mut b = AttemptBook::default();
        b.record("old-listed", Millis(0));
        b.record("old-gone", Millis(0));
        b.record("recent-gone", Millis(1_000_000));
        let now = Millis(1_000_000 + 60_000);
        assert_eq!(b.prune(now, w, &ids(&["old-listed"])), 1);
        assert!(b.attempts.contains_key("old-listed"));
        assert!(b.attempts.contains_key("recent-gone"));
        assert!(!b.attempts.contains_key("old-gone"));
        // Exactly at the window it is kept; after it, forgotten.
        let listed = ids(&["old-listed"]);
        assert_eq!(b.prune(Millis(1_000_000 + 900_000), w, &listed), 0);
        assert_eq!(b.prune(Millis(1_000_000 + 900_001), w, &listed), 1);
    }
}
