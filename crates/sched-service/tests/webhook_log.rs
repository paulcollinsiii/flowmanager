//! The `webhook` log line, as emitted: structured fields, an escaped caller-controlled
//! path, the left-out count of the rate limit, and no token. Like `toggle_log.rs` this is
//! the only test in its binary on purpose: it installs a thread-local subscriber.

use std::sync::{Arc, Mutex};

use hyper::Method;
use sched_model::Millis;
use sched_model::test_support::EXAMPLE_CONFIG;
use sched_observe::Nudges;
use sched_service::clock::ManualClock;
use sched_service::config_source::ConfigHolder;
use sched_service::listener::BodyError;
use sched_service::metrics::Metrics;
use sched_service::tick::Shared;
use sched_service::webhook::{Auth, Webhook};

const TOKEN: &str = "tok-0123456789";

#[derive(Clone, Default)]
struct Buf(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Buf {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn requests_log_structured_escaped_rate_limited_lines_without_the_token() {
    let buf = Buf::default();
    let sink = buf.clone();
    let sub = tracing_subscriber::fmt()
        .with_writer(move || sink.clone())
        .with_ansi(false)
        .finish();
    let _guard = tracing::subscriber::set_default(sub);

    let dir = std::env::temp_dir().join(format!("webhook-log-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let token_file = dir.join("token");
    std::fs::write(&token_file, format!("{TOKEN}\n")).unwrap();
    let shared = Arc::new(Shared::new(
        Arc::new(ConfigHolder::new(EXAMPLE_CONFIG).unwrap()),
        Arc::new(Metrics::new()),
        Arc::new(ManualClock::new(Millis(1_000))),
    ));
    let nudges = Nudges::default();
    nudges.handle(&"paperless-ocr-pending".into());
    nudges.handle(&"paperless-post-ocr".into());
    let hook = Webhook::new(shared, nudges, Auth::Token(token_file));
    let auth = format!("Bearer {TOKEN}");
    let ok = Some(auth.as_str());
    let body = || async { Ok::<(), BodyError>(()) };

    let calls: [(Method, &str, Option<&str>); 5] = [
        (Method::POST, "/hooks/paperless", ok),
        (
            Method::POST,
            "/hooks/paperless",
            Some("Bearer not-the-token"),
        ),
        (Method::POST, "/hooks/nowhere", ok),
        (Method::GET, "/hooks/paperless", ok),
        // Caller-controlled text with a newline in it, refused before anything else.
        (
            Method::POST,
            "/hooks/\n2026-01-01 INFO forged line",
            Some("Bearer not-the-token"),
        ),
    ];
    let mut statuses = vec![];
    for (m, p, a) in &calls {
        statuses.push(hook.handle(m, p, *a, body()).await.status);
    }
    assert_eq!(statuses, [202, 401, 404, 405, 401]);

    let text = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    let lines: Vec<&str> = text.lines().filter(|l| l.contains("webhook")).collect();
    assert_eq!(lines.len(), 5, "{text}");
    assert!(
        lines[0].contains("INFO")
            && lines[0].contains("event=\"webhook\"")
            && lines[0].contains("path=\"/hooks/paperless\"")
            && lines[0].contains("outcome=\"accepted\"")
            && lines[0].contains("status=202")
            && lines[0].contains("sources=2")
            && lines[0].contains("nudged=2"),
        "{}",
        lines[0]
    );
    assert!(
        lines[1].contains("WARN")
            && lines[1].contains("outcome=\"unauthorized\"")
            && lines[1].contains("status=401"),
        "{}",
        lines[1]
    );
    assert!(lines[2].contains("outcome=\"not_found\"") && lines[2].contains("status=404"));
    assert!(lines[3].contains("outcome=\"method_not_allowed\""));
    // The forged newline is escaped, so it stays inside one line.
    assert!(
        lines[4].contains(r#"path="/hooks/\n2026-01-01 INFO forged line""#),
        "{}",
        lines[4]
    );
    assert!(
        !text.contains(TOKEN) && !text.contains("not-the-token"),
        "{text}"
    );
    // `target` is tracing's own field; the line must not use it for something else.
    assert!(!text.contains("target="), "{text}");

    // A flood is cut off: at most 30 lines in a window, the rest counted.
    for _ in 0..100 {
        hook.handle(&Method::POST, "/hooks/paperless", ok, body())
            .await;
    }
    let text = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    let n = text
        .lines()
        .filter(|l| l.contains("event=\"webhook\""))
        .count();
    assert!(n <= 31, "{n} lines");
}
