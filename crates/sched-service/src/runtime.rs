//! The tasks around the scheduler object: taking in observer reports, keeping observers in
//! step with the config, asking Restate for ticks, and arming the backstop.

use std::sync::Arc;
use std::time::Duration;

use sched_model::Config;
use sched_observe::{Report, ReportTx, spawn_observers};
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

/// Whether observers built for `old` would differ from those for `new`.
pub fn observers_differ(old: &Config, new: &Config) -> bool {
    old.sources() != new.sources() || old.classes() != new.classes() || old.models() != new.models()
}

/// Runs the observers for the active config and restarts them when a reload changes what
/// they depend on.
pub async fn supervise_observers(shared: Arc<Shared>, tx: ReportTx) {
    let mut rx = shared.config.subscribe();
    let mut config = Arc::clone(&rx.borrow_and_update());
    let mut observers = spawn_observers(&config, &tx);
    tracing::info!(
        count = observers.len(),
        generation = config.generation(),
        "observers started"
    );
    while rx.changed().await.is_ok() {
        let next = Arc::clone(&rx.borrow_and_update());
        if observers_differ(&config, &next) {
            drop(observers);
            observers = spawn_observers(&next, &tx);
            tracing::info!(
                count = observers.len(),
                generation = next.generation(),
                "observers restarted"
            );
        }
        shared.prune();
        // A new generation changes decisions even if no observation did.
        shared.changed.notify_one();
        config = next;
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

    #[test]
    fn observers_restart_only_when_their_inputs_change() {
        let a = Config::from_yaml(EXAMPLE_CONFIG).unwrap();
        let b = Config::from_yaml(&EXAMPLE_CONFIG.replace("idle_grace: 10m", "idle_grace: 20m"))
            .unwrap();
        assert!(!observers_differ(&a, &b));
        let c = Config::from_yaml(&EXAMPLE_CONFIG.replace("resync: 60s", "resync: 30s")).unwrap();
        assert!(observers_differ(&a, &c));
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
}
