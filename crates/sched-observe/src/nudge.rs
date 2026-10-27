//! "Poll now" requests for polling observers.
//!
//! An observer that can be nudged owns a [`Nudge`]: a [`Notify`] shared with whoever asks
//! (the webhook listener) and the least time between two of its polls. The notify lives in
//! a [`Nudges`] registry that outlives the observer task, so a restart caused by a config
//! reload keeps the same handle and a request that arrives meanwhile is not lost.
//!
//! A nudge only brings the next poll forward. The poll's result is reported exactly like a
//! scheduled one, and the source's `resync` stays the safety net.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sched_model::SourceId;
use tokio::sync::Notify;
use tokio::time::{Instant, MissedTickBehavior};

/// The least time between two polls of a nudged source unless the caller says otherwise.
pub const DEFAULT_NUDGE_MIN_INTERVAL: Duration = Duration::from_secs(10);

/// What a nudged observer waits on.
#[derive(Clone, Debug)]
pub struct Nudge {
    pub notify: Arc<Notify>,
    /// A poll starts at least this long after the previous one started, however many
    /// nudges arrive.
    pub min_interval: Duration,
}

/// The nudge handles of the sources that take them, by source.
#[derive(Clone, Debug, Default)]
pub struct Nudges(Arc<Mutex<BTreeMap<SourceId, Arc<Notify>>>>);

impl Nudges {
    fn map(&self) -> std::sync::MutexGuard<'_, BTreeMap<SourceId, Arc<Notify>>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The handle of `source`, created on first use and the same one afterwards.
    pub fn handle(&self, source: &SourceId) -> Arc<Notify> {
        Arc::clone(self.map().entry(source.clone()).or_default())
    }

    /// Asks `source` to poll as soon as it may. Requests that arrive before it does are
    /// merged into one. `false` if no observer takes nudges for that source.
    pub fn nudge(&self, source: &SourceId) -> bool {
        match self.map().get(source) {
            Some(n) => {
                n.notify_one();
                true
            }
            None => false,
        }
    }

    /// Whether `source` takes nudges.
    pub fn contains(&self, source: &SourceId) -> bool {
        self.map().contains_key(source)
    }

    /// Forgets every source not in `keep`.
    pub(crate) fn retain_only(&self, keep: &BTreeSet<SourceId>) {
        self.map().retain(|id, _| keep.contains(id));
    }
}

/// Runs `poll` every `resync`, and also when `nudge` fires, until `poll` returns `false`.
///
/// A nudged poll waits until `min_interval` has passed since the previous poll started, and
/// takes in the nudges that arrived while it waited. A nudge that arrives during a poll
/// causes one more poll after the minimum interval, because the poll may have read before
/// the change. A nudged poll restarts the `resync` clock.
pub async fn poll_loop<F, Fut>(resync: Duration, nudge: Option<Nudge>, mut poll: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let mut ticker = tokio::time::interval(resync);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut last_start: Option<Instant> = None;
    loop {
        let nudged = match &nudge {
            Some(n) => {
                tokio::select! {
                    _ = ticker.tick() => false,
                    _ = n.notify.notified() => true,
                }
            }
            None => {
                ticker.tick().await;
                false
            }
        };
        if let (true, Some(n)) = (nudged, &nudge) {
            if let Some(last) = last_start {
                tokio::time::sleep_until(last + n.min_interval).await;
            }
            // Nudges that came in while waiting are served by the poll about to start.
            let _ = tokio::time::timeout(Duration::ZERO, n.notify.notified()).await;
            ticker.reset();
        }
        last_start = Some(Instant::now());
        if !poll().await {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    const RESYNC: Duration = Duration::from_secs(60);
    const MIN: Duration = Duration::from_secs(10);

    fn counted() -> (Arc<AtomicU32>, impl FnMut() -> std::future::Ready<bool>) {
        let n = Arc::new(AtomicU32::new(0));
        let n2 = Arc::clone(&n);
        (n, move || {
            n2.fetch_add(1, Ordering::SeqCst);
            std::future::ready(true)
        })
    }

    async fn settle() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_nudge_polls_at_once_and_a_flood_costs_one_poll_per_minimum_interval() {
        let notify = Arc::new(Notify::new());
        let nudge = Nudge {
            notify: Arc::clone(&notify),
            min_interval: MIN,
        };
        let (n, poll) = counted();
        let task = tokio::spawn(poll_loop(RESYNC, Some(nudge), poll));
        settle().await;
        assert_eq!(n.load(Ordering::SeqCst), 1, "the first poll is immediate");

        // Well after the first poll, one nudge polls without waiting.
        tokio::time::advance(Duration::from_secs(20)).await;
        notify.notify_one();
        settle().await;
        assert_eq!(n.load(Ordering::SeqCst), 2);

        // A flood right after: nothing runs before the interval ends, then exactly one.
        for _ in 0..1000 {
            notify.notify_one();
        }
        settle().await;
        tokio::time::advance(MIN - Duration::from_secs(1)).await;
        settle().await;
        assert_eq!(n.load(Ordering::SeqCst), 2, "held back by the interval");
        tokio::time::advance(Duration::from_secs(1)).await;
        settle().await;
        assert_eq!(n.load(Ordering::SeqCst), 3, "one poll for the whole flood");
        tokio::time::advance(MIN * 3).await;
        settle().await;
        assert_eq!(n.load(Ordering::SeqCst), 3, "nothing is left over");
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn resync_still_polls_and_a_nudge_restarts_its_clock() {
        let notify = Arc::new(Notify::new());
        let nudge = Nudge {
            notify: Arc::clone(&notify),
            min_interval: MIN,
        };
        let (n, poll) = counted();
        let task = tokio::spawn(poll_loop(RESYNC, Some(nudge), poll));
        settle().await;
        tokio::time::advance(Duration::from_secs(50)).await;
        notify.notify_one();
        settle().await;
        assert_eq!(n.load(Ordering::SeqCst), 2);
        // The old schedule (t=60) is gone; the next resync poll is a full period later.
        tokio::time::advance(Duration::from_secs(20)).await;
        settle().await;
        assert_eq!(n.load(Ordering::SeqCst), 2);
        tokio::time::advance(Duration::from_secs(40)).await;
        settle().await;
        assert_eq!(n.load(Ordering::SeqCst), 3);
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn without_a_nudge_handle_the_loop_is_the_plain_resync_ticker() {
        let (n, poll) = counted();
        let task = tokio::spawn(poll_loop(RESYNC, None, poll));
        settle().await;
        assert_eq!(n.load(Ordering::SeqCst), 1);
        tokio::time::advance(RESYNC).await;
        settle().await;
        assert_eq!(n.load(Ordering::SeqCst), 2);
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn the_loop_ends_when_the_poll_says_stop() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = Arc::clone(&calls);
        let task = tokio::spawn(poll_loop(RESYNC, None, move || {
            std::future::ready(c.fetch_add(1, Ordering::SeqCst) < 1)
        }));
        settle().await;
        tokio::time::advance(RESYNC).await;
        settle().await;
        assert!(task.is_finished());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn the_registry_hands_out_one_handle_per_source_and_forgets_the_rest() {
        let nudges = Nudges::default();
        let (a, b): (SourceId, SourceId) = ("a".into(), "b".into());
        assert!(!nudges.nudge(&a), "unknown source");
        let h1 = nudges.handle(&a);
        assert!(Arc::ptr_eq(&h1, &nudges.handle(&a)));
        nudges.handle(&b);
        assert!(nudges.nudge(&a) && nudges.contains(&b));
        nudges.retain_only(&BTreeSet::from([a.clone()]));
        assert!(nudges.contains(&a) && !nudges.contains(&b));
        // The handle survives: what was kept is the same Notify.
        assert!(Arc::ptr_eq(&h1, &nudges.handle(&a)));
    }
}
