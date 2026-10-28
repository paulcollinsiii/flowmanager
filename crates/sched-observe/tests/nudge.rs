//! A nudge brings an `http_json` poll forward, is rate-limited, and survives the observer
//! being restarted by a config reload. Uses the example config with the paperless sources
//! pointed at a local server and a `resync` of minutes, far longer than the test.

use std::sync::Arc;
use std::time::{Duration, Instant};

use sched_model::Config;
use sched_model::test_support::EXAMPLE_CONFIG;
use sched_observe::testserver::{Reply, TestServer};
use sched_observe::{Nudges, Observers, Report, SpawnOptions};
use tokio::sync::mpsc;

const OCR: &str = "/api/documents/?tags__name__iexact=ocr-pending&page_size=1";
const MIN: Duration = Duration::from_millis(400);

fn config(server: &TestServer, timeout: &str, webhook: bool) -> Arc<Config> {
    let yaml = EXAMPLE_CONFIG.replace("http://paperless-ngx.docs.svc:8000", &server.url(""));
    // Of the first paperless source: a `resync` of 9m (under the staleness limit) so that
    // only a nudge can cause a second poll, no auth (it would need an environment
    // variable), and a `timeout` that the caller varies to make the spec differ.
    let from = "    resync: 60s\n    params:\n      timeout: 10s\n      auth: { header: Authorization, prefix: \"Token \", env: PAPERLESS_API_TOKEN }\n      requests:\n        # page_size=1";
    assert_eq!(yaml.matches(from).count(), 1, "the example changed");
    let to = format!(
        "    resync: 9m\n    params:\n      timeout: {timeout}\n      requests:\n        # page_size=1"
    );
    let mut yaml = yaml.replacen(from, &to, 1);
    if !webhook {
        yaml = yaml.replacen(
            "webhook: { path: /hooks/paperless }",
            "# no webhook here",
            1,
        );
    }
    Arc::new(Config::from_yaml(&yaml).expect("the variant is valid"))
}

fn polls(server: &TestServer) -> usize {
    server.requests().iter().filter(|(k, _)| k == OCR).count()
}

async fn until(what: &str, mut ok: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !ok() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn nudges_poll_early_coalesce_survive_a_restart_and_go_with_the_webhook_setting() {
    let server = TestServer::start().await;
    server.set(OCR, Reply::json(r#"{"count": 3}"#));
    let (tx, mut rx) = mpsc::channel::<Report>(1024);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let nudges = Nudges::default();
    let opts = SpawnOptions {
        nudges: nudges.clone(),
        nudge_min_interval: Some(MIN),
        ..SpawnOptions::default()
    };
    let id = "paperless-ocr-pending".into();
    let mut observers = Observers::default();

    observers.update(&config(&server, "10s", true), &tx, &opts);
    until("the first poll", || polls(&server) == 1).await;
    assert!(nudges.contains(&id));
    assert!(
        !nudges.contains(&"immich-queues".into()),
        "only the sources with a webhook setting take nudges"
    );
    let handle = nudges.handle(&id);

    // One nudge: a poll long before the 9m resync.
    tokio::time::sleep(MIN).await;
    assert!(nudges.nudge(&id));
    until("the nudged poll", || polls(&server) == 2).await;

    // A flood: one poll for all of it, and not before the minimum interval has passed.
    for _ in 0..500 {
        nudges.nudge(&id);
    }
    until("the flood's poll", || polls(&server) == 3).await;
    let times = server.request_times();
    let ocr_times: Vec<Instant> = server
        .requests()
        .iter()
        .zip(times)
        .filter(|((k, _), _)| k == OCR)
        .map(|(_, t)| t)
        .collect();
    assert!(
        ocr_times[2].duration_since(ocr_times[1]) >= MIN - Duration::from_millis(30),
        "polls {:?} apart",
        ocr_times[2].duration_since(ocr_times[1])
    );
    tokio::time::sleep(MIN * 3).await;
    assert_eq!(polls(&server), 3, "nothing is left over from the flood");

    // A reload that changes the source restarts its observer (which polls at once); the
    // nudge handle is the same one and still works.
    let restarted = observers.update(&config(&server, "11s", true), &tx, &opts);
    assert!(restarted.contains(&id), "{restarted:?}");
    until("the poll of the restarted observer", || polls(&server) == 4).await;
    assert!(Arc::ptr_eq(&handle, &nudges.handle(&id)));
    tokio::time::sleep(MIN).await;
    assert!(nudges.nudge(&id));
    until("a nudge after the restart", || polls(&server) == 5).await;

    // Taking the webhook setting away drops the handle: nothing to nudge any more.
    observers.update(&config(&server, "11s", false), &tx, &opts);
    until("the handle to go", || !nudges.contains(&id)).await;
    assert!(!nudges.nudge(&id));
    drain.abort();
}
