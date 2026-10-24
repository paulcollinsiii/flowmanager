//! The tasks around the scheduler object: taking in observer reports, keeping observers in
//! step with the config, asking Restate for ticks, and arming the backstop.

use std::sync::Arc;
use std::time::Duration;

use sched_model::Millis;
use sched_observe::{Observers, Report, ReportTx, SpawnOptions};
use tokio::sync::mpsc;

use crate::scheduler::KEY;
use crate::tick::Shared;

/// Name of the Restate virtual object (the `Scheduler` type).
pub const OBJECT: &str = "Scheduler";

/// Capacity of the channel between observers and the cache. Observers wait when it is full;
/// the receiver only takes a lock to store a value, so it is rarely anywhere near full.
pub const REPORT_BUFFER: usize = 1024;

pub fn report_channel() -> (ReportTx, mpsc::Receiver<Report>) {
    mpsc::channel(REPORT_BUFFER)
}

/// Stores observer reports as they arrive, dating each with the scheduler's clock at the
/// moment of receipt.
pub async fn pump(shared: Arc<Shared>, mut rx: mpsc::Receiver<Report>) {
    while let Some(report) = rx.recv().await {
        shared.ingest_now(report);
    }
}

/// Runs the observers for the active config and, on a reload, restarts only those whose
/// own inputs changed (the rest keep running, with their state).
pub async fn supervise_observers(shared: Arc<Shared>, tx: ReportTx) {
    let mut rx = shared.config.subscribe();
    let opts = SpawnOptions::default();
    let mut observers = Observers::default();
    let config = Arc::clone(&rx.borrow_and_update());
    observers.update(&config, &tx, &opts);
    tracing::info!(
        count = observers.len(),
        generation = config.generation(),
        "observers started"
    );
    while rx.changed().await.is_ok() {
        let next = Arc::clone(&rx.borrow_and_update());
        let restarted = observers.update(&next, &tx, &opts);
        tracing::info!(
            running = observers.len(),
            restarted = ?restarted.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            generation = next.generation(),
            "observers updated"
        );
        shared.prune();
        // A new generation changes decisions even if no observation did.
        shared.changed.notify_one();
    }
}

/// `POST {ingress}/Scheduler/main/{handler}/send` with an empty body.
pub async fn send(client: &reqwest::Client, ingress: &str, handler: &str) -> Result<(), String> {
    let url = format!(
        "{}/{OBJECT}/{KEY}/{handler}/send",
        ingress.trim_end_matches('/')
    );
    let resp = client
        .post(&url)
        .send()
        .await
        .map_err(|e| e.without_url().to_string())?;
    if resp.status().is_success() {
        Ok(())
    } else {
        Err(format!("{url}: HTTP {}", resp.status()))
    }
}

/// Asks Restate for a tick whenever observations or the config changed, at most once per
/// `debounce`. Failure only costs promptness: the backstop still ticks.
pub async fn trigger_ticks(shared: Arc<Shared>, ingress: String, debounce: Duration) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("client builds");
    loop {
        shared.changed.notified().await;
        tokio::time::sleep(debounce).await;
        if let Err(e) = send(&client, &ingress, "tick").await {
            shared.metrics.trigger_failed();
            tracing::warn!("could not ask Restate for a tick: {e}");
        }
    }
}

/// Starts the backstop chain, retrying until Restate accepts (the service may not be
/// registered yet when it starts).
pub async fn arm_backstop(ingress: String, retry: Duration) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("client builds");
    let mut warned = false;
    loop {
        match send(&client, &ingress, "arm_backstop").await {
            Ok(()) => {
                tracing::info!("backstop armed");
                return;
            }
            Err(e) => {
                if !warned {
                    tracing::warn!(
                        "could not arm the backstop yet (is the service registered with Restate?): {e}"
                    );
                    warned = true;
                }
                tokio::time::sleep(retry).await;
            }
        }
    }
}

/// Whether the backstop chain looks dead: it has not run for well over its interval. The
/// chain lives in Restate's state, so wiping that state, or a lost delayed send, would
/// otherwise stop it for good.
pub fn backstop_overdue(now: Millis, last: Millis, started_at: Millis, every: Duration) -> bool {
    let since = if last == Millis::ZERO {
        started_at
    } else {
        last
    };
    let allowed = (every.as_millis() as u64)
        .saturating_mul(3)
        .saturating_add(10_000);
    now.saturating_sub(since).as_u64() > allowed
}

/// Checks every `check_every` whether the backstop is overdue and, if so, arms it again.
pub async fn watch_backstop(
    shared: Arc<Shared>,
    ingress: String,
    every: Duration,
    check_every: Duration,
) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("client builds");
    let mut ticker = tokio::time::interval(check_every);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let now = shared.clock.now();
        let last = Millis(
            shared
                .last_backstop
                .load(std::sync::atomic::Ordering::SeqCst),
        );
        if !backstop_overdue(now, last, shared.started_at, every) {
            continue;
        }
        tracing::warn!("the backstop has not run for a while; arming it again");
        match send(&client, &ingress, "arm_backstop").await {
            // Give the new chain a full interval before judging it.
            Ok(()) => shared.note_backstop(now),
            Err(e) => tracing::warn!("could not re-arm the backstop: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ManualClock;
    use crate::config_source::ConfigHolder;
    use crate::metrics::Metrics;
    use sched_model::test_support::EXAMPLE_CONFIG;
    use sched_model::{Fidelity, Millis, ObservedValue};
    use sched_observe::testserver::{Reply, TestServer};

    fn shared(clock: Arc<ManualClock>) -> Arc<Shared> {
        let holder = Arc::new(ConfigHolder::new(EXAMPLE_CONFIG).unwrap());
        Arc::new(Shared::new(holder, Arc::new(Metrics::new()), clock))
    }

    #[tokio::test]
    async fn the_pump_dates_reports_on_receipt_with_the_scheduler_clock() {
        let clock = Arc::new(ManualClock::new(Millis(5_000)));
        let s = shared(clock.clone());
        let (tx, rx) = report_channel();
        let task = tokio::spawn(pump(Arc::clone(&s), rx));
        tx.send(Report::Observed {
            source: "paperless-ocr-pending".into(),
            fidelity: Fidelity::Polled,
            values: vec![ObservedValue::Demand {
                class: "paperless-ocr".into(),
                ready: 2,
                in_flight: 0,
                // A remote clock a day ahead.
                oldest_ready_at: Some(Millis(5_000 + 86_400_000)),
            }],
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), s.changed.notified())
            .await
            .unwrap();
        let config = s.config.current();
        let snap = s.cache.lock().unwrap().snapshot(&config, Millis(5_100));
        assert_eq!(snap.observations[0].observed_at, Millis(5_000));
        task.abort();
    }

    #[tokio::test]
    async fn a_change_asks_for_a_tick_and_failure_to_do_so_is_counted_not_fatal() {
        let server = TestServer::start().await;
        server.set("/Scheduler/main/tick/send", Reply::status(200));
        let s = shared(Arc::new(ManualClock::new(Millis(1))));
        let ingress = format!("http://{}", server.url("").trim_start_matches("http://"));
        let task = tokio::spawn(trigger_ticks(
            Arc::clone(&s),
            ingress,
            Duration::from_millis(10),
        ));
        s.changed.notify_one();
        for _ in 0..100 {
            if !server.requests().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(server.requests()[0].0, "/Scheduler/main/tick/send");
        task.abort();

        // An unreachable ingress is counted.
        let dead = TestServer::start().await;
        let dead_ingress = dead.url("");
        drop(dead);
        let task = tokio::spawn(trigger_ticks(
            Arc::clone(&s),
            dead_ingress,
            Duration::from_millis(10),
        ));
        s.changed.notify_one();
        for _ in 0..100 {
            if s.metrics
                .render()
                .contains("sched_tick_trigger_failures_total 1")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            s.metrics
                .render()
                .contains("sched_tick_trigger_failures_total 1")
        );
        task.abort();
    }

    #[tokio::test]
    async fn the_backstop_is_armed_once_restate_accepts() {
        let server = TestServer::start().await;
        let ingress = server.url("");
        let task = tokio::spawn(arm_backstop(ingress, Duration::from_millis(20)));
        // Not registered yet: 404s, retried.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!task.is_finished());
        server.set("/Scheduler/main/arm_backstop/send", Reply::status(202));
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[test]
    fn the_backstop_is_overdue_only_well_past_its_interval() {
        let every = Duration::from_secs(30);
        let (start, ms) = (Millis(1_000), Millis);
        assert!(!backstop_overdue(ms(60_000), ms(0), start, every));
        assert!(backstop_overdue(ms(1_000 + 100_001), ms(0), start, every));
        assert!(!backstop_overdue(ms(200_000), ms(150_000), start, every));
        assert!(backstop_overdue(ms(300_000), ms(150_000), start, every));
    }

    #[tokio::test]
    async fn a_dead_backstop_chain_is_armed_again_and_a_live_one_is_left_alone() {
        let server = TestServer::start().await;
        server.set("/Scheduler/main/arm_backstop/send", Reply::status(202));
        let clock = Arc::new(ManualClock::new(Millis(1_000)));
        let s = shared(clock.clone());
        let task = tokio::spawn(watch_backstop(
            Arc::clone(&s),
            server.url(""),
            Duration::from_secs(30),
            Duration::from_millis(20),
        ));
        // Recently started and recently run: nothing to do.
        s.note_backstop(Millis(1_000));
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(server.requests().is_empty());
        // The chain stops running while the clock moves on.
        clock.set(Millis(1_000 + 200_000));
        for _ in 0..100 {
            if !server.requests().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(server.requests()[0].0, "/Scheduler/main/arm_backstop/send");
        // And it is then given time rather than armed over and over.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(server.requests().len(), 1);
        task.abort();
    }
}
