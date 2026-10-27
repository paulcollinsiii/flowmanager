//! The `toggle` log line, as emitted: the structured fields of change requests and of
//! refused ones, escaped names, and no token. This is the only test in this binary on
//! purpose: it installs a thread-local subscriber, and tracing caches whether a call site is
//! enabled, which other tests running in parallel on other threads in the same process can
//! confuse (the unit tests of the library cannot assert on log output for that reason).

use std::sync::{Arc, Mutex};

use hyper::Method;
use sched_model::test_support::EXAMPLE_CONFIG;
use sched_model::{Lease, LeaseId, Millis, ModelId, ResourceId};
use sched_service::clock::ManualClock;
use sched_service::config_source::ConfigHolder;
use sched_service::leases::LeaseGrant;
use sched_service::metrics::Metrics;
use sched_service::object::BoxFut;
use sched_service::resume::ResumeAck;
use sched_service::tick::Shared;
use sched_service::toggle::{ApiError, Backend, Toggle};

const TOKEN: &str = "tok-0123456789";

/// A backend that holds nothing and refuses every change with a 409.
struct Stub;

fn refused<T: Send + 'static>() -> BoxFut<'static, Result<T, ApiError>> {
    Box::pin(async {
        Err(ApiError {
            status: 409,
            message: "refused by the stub".into(),
        })
    })
}

impl Backend for Stub {
    fn leases(&self) -> BoxFut<'_, Result<Vec<Lease>, ApiError>> {
        Box::pin(async { Ok(vec![]) })
    }
    fn acquire_model(&self, _: &ModelId) -> BoxFut<'_, Result<LeaseGrant, ApiError>> {
        refused()
    }
    fn release_lease(&self, _: &LeaseId) -> BoxFut<'_, Result<Lease, ApiError>> {
        refused()
    }
    fn acquire_host(&self, _: &ResourceId) -> BoxFut<'_, Result<LeaseGrant, ApiError>> {
        refused()
    }
    fn release_host(&self, _: &ResourceId) -> BoxFut<'_, Result<Lease, ApiError>> {
        refused()
    }
    fn resume(&self, _: &ResourceId) -> BoxFut<'_, Result<ResumeAck, ApiError>> {
        refused()
    }
    fn reclaim(&self, _: &ResourceId) -> BoxFut<'_, Result<LeaseGrant, ApiError>> {
        refused()
    }
    fn unreclaim(&self, _: &ResourceId) -> BoxFut<'_, Result<Lease, ApiError>> {
        refused()
    }
}

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
async fn change_requests_and_refusals_log_structured_escaped_lines_without_the_token() {
    let buf = Buf::default();
    let sink = buf.clone();
    let sub = tracing_subscriber::fmt()
        .with_writer(move || sink.clone())
        .with_ansi(false)
        .finish();
    let _guard = tracing::subscriber::set_default(sub);

    let dir = std::env::temp_dir().join(format!("toggle-log-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let token_file = dir.join("token");
    std::fs::write(&token_file, format!("{TOKEN}\n")).unwrap();
    let shared = Arc::new(Shared::new(
        Arc::new(ConfigHolder::new(EXAMPLE_CONFIG).unwrap()),
        Arc::new(Metrics::new()),
        Arc::new(ManualClock::new(Millis(1_000))),
    ));
    let toggle = Toggle::new(shared, Arc::new(Stub), token_file);
    let auth = format!("Bearer {TOKEN}");
    let ok = Some(auth.as_str());

    let calls: [(Method, &str, Option<&str>); 8] = [
        (Method::POST, "/v1/models/llm/on", ok),
        (Method::POST, "/v1/reclaims/cuda-central", ok),
        (Method::POST, "/v1/resources/basestar-nfs/resume", ok),
        (Method::GET, "/v1/models/llm", ok), // a read that succeeds: no line
        (Method::GET, "/v1/leases", None),
        (Method::DELETE, "/v1/models/llm", ok),
        (Method::GET, "/nowhere", ok),
        // Caller-controlled text with a newline in it, before any authentication.
        (
            Method::POST,
            "/v1/models/%0A2026-01-01%20INFO%20forged%20line/on",
            Some("Bearer not-the-token"),
        ),
    ];
    let mut statuses = vec![];
    for (m, p, a) in &calls {
        statuses.push(toggle.handle(m, p, *a, "ON").await.status);
    }
    assert_eq!(statuses, [409, 409, 409, 200, 401, 405, 404, 401]);

    let text = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    let lines: Vec<&str> = text
        .lines()
        .filter(|l| l.contains("event=\"toggle\""))
        .collect();
    assert_eq!(lines.len(), 7, "{text}");
    let has = |i: usize, parts: &[&str]| {
        for p in parts {
            assert!(lines[i].contains(p), "line {i} lacks {p}: {}", lines[i]);
        }
    };
    has(
        0,
        &[
            "WARN",
            "kind=\"model\"",
            "name=\"llm\"",
            "action=\"on\"",
            "outcome=\"conflict\"",
            "status=409",
        ],
    );
    has(
        1,
        &["kind=\"reclaim\"", "name=\"cuda-central\"", "action=\"on\""],
    );
    has(
        2,
        &["kind=\"host\"", "action=\"resume\"", "outcome=\"conflict\""],
    );
    has(
        3,
        &["kind=\"list\"", "outcome=\"unauthorized\"", "status=401"],
    );
    has(
        4,
        &[
            "kind=\"unmatched\"",
            "name=\"/v1/models/llm\"",
            "outcome=\"method_not_allowed\"",
            "status=405",
        ],
    );
    has(
        5,
        &["kind=\"unmatched\"", "name=\"/nowhere\"", "status=404"],
    );
    has(6, &["outcome=\"unauthorized\"", "action=\"on\""]);
    // The forged text is one escaped field of one line, never a line of its own.
    assert!(
        lines[6].contains("\\n2026-01-01 INFO forged line"),
        "{}",
        lines[6]
    );
    assert!(
        text.lines()
            .all(|l| !l.starts_with("2026-01-01 INFO forged")),
        "{text}"
    );
    assert!(
        !text.contains(TOKEN) && !text.contains("not-the-token"),
        "{text}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
