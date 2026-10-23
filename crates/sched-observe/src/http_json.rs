//! The generic HTTP/JSON adapter. Everything service-specific (endpoints, which field is
//! the count, which failures count as demand) comes from the source's `params`; nothing
//! here knows paperless, speakr or any other service. It only ever issues GET requests.

use std::time::Duration;

use sched_model::{
    AuthSpec, ClassId, CountsAs, ElementFilter, Fidelity, HttpJsonParams, ObservedValue,
    RequestSpec, SourceSpec,
};
use serde_json::Value;

use crate::{Report, ReportTx};

/// Looks up an environment variable; injected so tests need not touch the process environment.
pub type EnvFn = dyn Fn(&str) -> Option<String> + Sync;

/// Largest response body read.
const MAX_BODY: usize = 8 * 1024 * 1024;

/// Why a response could not be turned into a number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtractError {
    Missing(String),
    NotANumber(String),
    NotAnArray(String),
}

impl std::fmt::Display for ExtractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExtractError::Missing(p) => write!(f, "no value at {p:?} in the response"),
            ExtractError::NotANumber(p) => {
                write!(f, "value at {p:?} is not a non-negative whole number")
            }
            ExtractError::NotAnArray(p) => write!(f, "value at {p:?} is not an array"),
        }
    }
}

fn matches(filter: &ElementFilter, element: &Value) -> bool {
    let Some(text) = element.pointer(&filter.field).and_then(Value::as_str) else {
        return false;
    };
    match (&filter.contains, &filter.equals) {
        (Some(needle), _) => text.to_lowercase().contains(&needle.to_lowercase()),
        (None, Some(exact)) => text == exact,
        (None, None) => false,
    }
}

/// The number one request contributes: the value at `count`, or the number of elements of
/// the array at `length` that pass `where`.
pub fn extract(doc: &Value, req: &RequestSpec) -> Result<u64, ExtractError> {
    if let Some(ptr) = &req.count {
        let v = doc
            .pointer(ptr)
            .ok_or_else(|| ExtractError::Missing(ptr.clone()))?;
        return v
            .as_u64()
            .or_else(|| {
                v.as_f64()
                    .filter(|f| *f >= 0.0 && f.fract() == 0.0 && *f < u64::MAX as f64)
                    .map(|f| f as u64)
            })
            .ok_or_else(|| ExtractError::NotANumber(ptr.clone()));
    }
    let ptr = req.length.as_deref().unwrap_or("");
    let items = doc
        .pointer(ptr)
        .ok_or_else(|| ExtractError::Missing(ptr.to_string()))?
        .as_array()
        .ok_or_else(|| ExtractError::NotAnArray(ptr.to_string()))?;
    let n = match &req.filter {
        Some(f) => items.iter().filter(|e| matches(f, e)).count(),
        None => items.len(),
    };
    Ok(n as u64)
}

/// The sum rule: numbers counting as ready and as in flight are each added up.
pub fn totals(parts: &[(CountsAs, u64)]) -> (u64, u64) {
    parts.iter().fold((0, 0), |(r, i), (kind, n)| match kind {
        CountsAs::Ready => (r.saturating_add(*n), i),
        CountsAs::InFlight => (r, i.saturating_add(*n)),
    })
}

/// One demand observation per class that takes its demand from this source.
pub fn demand_values(classes: &[ClassId], ready: u64, in_flight: u64) -> Vec<ObservedValue> {
    classes
        .iter()
        .map(|class| ObservedValue::Demand {
            class: class.clone(),
            ready,
            in_flight,
            oldest_ready_at: None,
        })
        .collect()
}

/// Reads the secret an `auth` entry points at and builds the header. Errors never contain
/// the secret.
pub async fn resolve_auth(auth: &AuthSpec, env: &EnvFn) -> Result<(String, String), String> {
    let secret = match (&auth.env, &auth.file) {
        (Some(var), _) => {
            env(var).ok_or_else(|| format!("environment variable {var} is not set"))?
        }
        (None, Some(path)) => tokio::fs::read_to_string(path)
            .await
            .map_err(|e| format!("cannot read secret file {path}: {e}"))?,
        (None, None) => return Err("auth names neither env nor file".to_string()),
    };
    let secret = secret.trim();
    if secret.is_empty() {
        return Err("the secret is empty".to_string());
    }
    Ok((auth.header.clone(), format!("{}{secret}", auth.prefix)))
}

async fn fetch(
    client: &reqwest::Client,
    req: &RequestSpec,
    auth: Option<&(String, String)>,
) -> Result<Value, String> {
    let mut builder = client.get(&req.url);
    if let Some((name, value)) = auth {
        let mut v = reqwest::header::HeaderValue::from_str(value)
            .map_err(|_| "the secret is not a valid header value".to_string())?;
        v.set_sensitive(true);
        builder = builder.header(name, v);
    }
    let resp = builder
        .send()
        .await
        .map_err(|e| e.without_url().to_string())?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("HTTP {status}"));
    }
    let body = resp
        .bytes()
        .await
        .map_err(|e| e.without_url().to_string())?;
    if body.len() > MAX_BODY {
        return Err(format!("response is larger than {MAX_BODY} bytes"));
    }
    serde_json::from_slice(&body).map_err(|e| format!("response is not JSON: {e}"))
}

/// One poll of every request. Any failure fails the whole poll: a partial sum would read
/// as less demand than there is.
pub async fn poll(
    client: &reqwest::Client,
    spec: &SourceSpec,
    params: &HttpJsonParams,
    classes: &[ClassId],
    env: &EnvFn,
) -> Report {
    let fail = |error: String| Report::Failed {
        source: spec.id.clone(),
        error,
    };
    let auth = match &params.auth {
        Some(a) => match resolve_auth(a, env).await {
            Ok(h) => Some(h),
            Err(e) => return fail(e),
        },
        None => None,
    };
    let mut parts = Vec::with_capacity(params.requests.len());
    for (i, req) in params.requests.iter().enumerate() {
        let doc = match fetch(client, req, auth.as_ref()).await {
            Ok(d) => d,
            Err(e) => return fail(format!("request {i} ({}): {e}", req.url)),
        };
        match extract(&doc, req) {
            Ok(n) => parts.push((req.counts_as, n)),
            Err(e) => return fail(format!("request {i} ({}): {e}", req.url)),
        }
    }
    let (ready, in_flight) = totals(&parts);
    Report::Observed {
        source: spec.id.clone(),
        fidelity: Fidelity::Polled,
        values: demand_values(classes, ready, in_flight),
    }
}

/// Polls forever, every `resync`, until the receiver is gone.
pub async fn run(spec: SourceSpec, params: HttpJsonParams, classes: Vec<ClassId>, tx: ReportTx) {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_millis(params.timeout.as_u64()))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            let _ = tx
                .send(Report::Failed {
                    source: spec.id.clone(),
                    error: format!("cannot build the HTTP client: {e}"),
                })
                .await;
            return;
        }
    };
    let env = |name: &str| std::env::var(name).ok();
    let mut ticker = tokio::time::interval(Duration::from_millis(spec.resync.as_u64().max(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        let report = poll(&client, &spec, &params, &classes, &env).await;
        if tx.send(report).await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testserver::{Reply, TestServer};
    use sched_model::Millis;
    use sched_model::test_support::example_config;

    fn req(
        count: Option<&str>,
        length: Option<&str>,
        filter: Option<ElementFilter>,
    ) -> RequestSpec {
        RequestSpec {
            url: "http://x".into(),
            count: count.map(Into::into),
            length: length.map(Into::into),
            filter,
            counts_as: CountsAs::Ready,
        }
    }

    fn doc(text: &str) -> Value {
        serde_json::from_str(text).unwrap()
    }

    #[test]
    fn extracts_counts_and_lengths() {
        let d = doc(r#"{"count": 7, "results": [1,2,3], "f": 4.0}"#);
        assert_eq!(extract(&d, &req(Some("/count"), None, None)), Ok(7));
        assert_eq!(extract(&d, &req(Some("/f"), None, None)), Ok(4));
        assert_eq!(extract(&d, &req(None, Some("/results"), None)), Ok(3));
        assert_eq!(
            extract(&d, &req(Some("/nope"), None, None)),
            Err(ExtractError::Missing("/nope".into()))
        );
        assert_eq!(
            extract(&d, &req(Some("/results"), None, None)),
            Err(ExtractError::NotANumber("/results".into()))
        );
        assert_eq!(
            extract(&doc(r#"{"count": -1}"#), &req(Some("/count"), None, None)),
            Err(ExtractError::NotANumber("/count".into()))
        );
        assert_eq!(
            extract(&d, &req(None, Some("/count"), None)),
            Err(ExtractError::NotAnArray("/count".into()))
        );
        // An empty pointer addresses the whole document.
        assert_eq!(extract(&doc("[1,2]"), &req(None, Some(""), None)), Ok(2));
    }

    #[test]
    fn filters_elements_by_field() {
        let d = doc(
            r#"[{"e": "Connection refused"}, {"e": "bad audio"}, {"e": null}, {}, {"e": "connection reset"}]"#,
        );
        let contains = ElementFilter {
            field: "/e".into(),
            contains: Some("connection".into()),
            equals: None,
        };
        assert_eq!(extract(&d, &req(None, Some(""), Some(contains))), Ok(2));
        let equals = ElementFilter {
            field: "/e".into(),
            contains: None,
            equals: Some("bad audio".into()),
        };
        assert_eq!(extract(&d, &req(None, Some(""), Some(equals))), Ok(1));
    }

    #[test]
    fn totals_sum_by_role() {
        use CountsAs::*;
        assert_eq!(totals(&[(Ready, 2), (InFlight, 5), (Ready, 3)]), (5, 5));
        assert_eq!(totals(&[(Ready, u64::MAX), (Ready, 1)]).0, u64::MAX);
        assert_eq!(totals(&[]), (0, 0));
    }

    /// The example source `id`, with every request pointed at `server`.
    fn example_source(id: &str, server: &TestServer) -> (SourceSpec, HttpJsonParams, Vec<ClassId>) {
        let config = example_config();
        let spec = config.source(&id.into()).unwrap().clone();
        let mut params = spec.http_json_params().unwrap().unwrap();
        for r in &mut params.requests {
            let at = r.url.find("/api/").unwrap();
            r.url = server.url(&r.url[at..]);
        }
        let classes = config
            .classes()
            .iter()
            .filter(|c| c.demand.as_ref().is_some_and(|d| d.source.as_str() == id))
            .map(|c| c.id.clone())
            .collect();
        (spec, params, classes)
    }

    const SPEAKR_PENDING: &str = "/api/v1/recordings?status=pending&per_page=100";
    const SPEAKR_FAILED: &str = "/api/v1/recordings?status=failed&per_page=100";
    const SPEAKR_PROCESSING: &str = "/api/v1/recordings?status=processing&per_page=100";

    fn env(name: &str) -> Option<String> {
        (name == "SPEAKR_API_TOKEN").then(|| "s3cret\n".to_string())
    }

    fn no_env(_: &str) -> Option<String> {
        None
    }

    fn demand(r: &Report) -> (u64, u64) {
        match r {
            Report::Observed { values, .. } => match &values[0] {
                ObservedValue::Demand {
                    ready, in_flight, ..
                } => (*ready, *in_flight),
                v => panic!("{v:?}"),
            },
            Report::Failed { error, .. } => panic!("failed: {error}"),
        }
    }

    #[tokio::test]
    async fn speakr_counts_pending_plus_connection_error_failures() {
        let server = TestServer::start().await;
        server.set(
            SPEAKR_PENDING,
            Reply::json(r#"{"recordings": [{"id": 1}, {"id": 2}]}"#),
        );
        server.set(
            SPEAKR_FAILED,
            Reply::json(
                r#"{"recordings": [
                    {"id": 3, "error_message": "Connection error: whisperx unreachable"},
                    {"id": 4, "error_message": "unsupported codec"},
                    {"id": 5, "error_message": "APIConnectionError"}]}"#,
            ),
        );
        server.set(
            SPEAKR_PROCESSING,
            Reply::json(r#"{"recordings": [{"id": 6}]}"#),
        );
        let (spec, params, classes) = example_source("speakr-pending", &server);
        assert_eq!(classes.len(), 1);
        let client = reqwest::Client::new();
        let report = poll(&client, &spec, &params, &classes, &env).await;
        // 2 pending + 2 connection-error failures; the codec failure is not demand.
        assert_eq!(demand(&report), (4, 1));
        // The secret is read from the environment, trimmed, and sent with its prefix.
        assert!(
            server
                .requests()
                .iter()
                .all(|(_, auth)| auth.as_deref() == Some("Bearer s3cret"))
        );
    }

    #[tokio::test]
    async fn paperless_uses_the_document_list_count() {
        let server = TestServer::start().await;
        let (spec, params, classes) = example_source("paperless-ocr-pending", &server);
        server.set(
            "/api/documents/?tags__name__iexact=ocr-pending&page_size=1",
            Reply::json(r#"{"count": 12, "next": null, "results": [{"id": 9}]}"#),
        );
        let env = |n: &str| (n == "PAPERLESS_API_TOKEN").then(|| "tok".to_string());
        let report = poll(&reqwest::Client::new(), &spec, &params, &classes, &env).await;
        assert_eq!(demand(&report), (12, 0));
        assert_eq!(server.requests()[0].1.as_deref(), Some("Token tok"));
    }

    async fn failure(server: &TestServer, params: &HttpJsonParams, spec: &SourceSpec) -> String {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(params.timeout.as_u64()))
            .build()
            .unwrap();
        match poll(&client, spec, params, &[], &env).await {
            Report::Failed { source, error } => {
                assert_eq!(source, spec.id);
                let _ = server;
                error
            }
            other => panic!("expected a failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn every_kind_of_adapter_error_is_an_explicit_failure() {
        let server = TestServer::start().await;
        let (spec, mut params, _) = example_source("speakr-pending", &server);
        server.set(SPEAKR_PENDING, Reply::json(r#"{"recordings": []}"#));
        server.set(SPEAKR_FAILED, Reply::json(r#"{"recordings": []}"#));
        server.set(SPEAKR_PROCESSING, Reply::json(r#"{"recordings": []}"#));

        // A server error on one request fails the whole poll, not just that request.
        server.set(SPEAKR_FAILED, Reply::status(503));
        let e = failure(&server, &params, &spec).await;
        assert!(e.contains("HTTP 503") && e.contains("request 1"), "{e}");

        server.set(SPEAKR_FAILED, Reply::json("<html>"));
        assert!(failure(&server, &params, &spec).await.contains("not JSON"));

        server.set(SPEAKR_FAILED, Reply::json(r#"{"other": []}"#));
        assert!(
            failure(&server, &params, &spec)
                .await
                .contains("no value at")
        );

        // Slower than the configured timeout.
        server.set(
            SPEAKR_FAILED,
            Reply::json(r#"{"recordings": []}"#).delayed(std::time::Duration::from_millis(800)),
        );
        params.timeout = Millis(100);
        assert!(!failure(&server, &params, &spec).await.is_empty());

        // Nothing is listening.
        let dead = TestServer::start().await;
        let dead_url = dead.url("/api/v1/recordings?status=pending&per_page=100");
        drop(dead);
        params.timeout = Millis(1000);
        params.requests[0].url = dead_url;
        assert!(!failure(&server, &params, &spec).await.is_empty());
    }

    #[tokio::test]
    async fn a_missing_secret_is_a_failure_and_no_request_is_made() {
        let server = TestServer::start().await;
        let (spec, params, classes) = example_source("speakr-pending", &server);
        let report = poll(&reqwest::Client::new(), &spec, &params, &classes, &no_env).await;
        match report {
            Report::Failed { error, .. } => assert!(error.contains("SPEAKR_API_TOKEN"), "{error}"),
            other => panic!("{other:?}"),
        }
        assert!(server.requests().is_empty());
    }

    #[tokio::test]
    async fn secret_files_are_read_and_trimmed_and_empty_ones_refused() {
        let dir = std::env::temp_dir().join(format!("sched-observe-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("token");
        std::fs::write(&path, "  abc \n").unwrap();
        let auth = AuthSpec {
            header: "X-Api-Key".into(),
            prefix: String::new(),
            env: None,
            file: Some(path.display().to_string()),
        };
        assert_eq!(
            resolve_auth(&auth, &no_env).await,
            Ok(("X-Api-Key".to_string(), "abc".to_string()))
        );
        std::fs::write(&path, "\n").unwrap();
        assert!(resolve_auth(&auth, &no_env).await.is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn run_reports_failures_then_recovers() {
        let server = TestServer::start().await;
        let (mut spec, mut params, classes) = example_source("paperless-ocr-pending", &server);
        spec.resync = Millis(50);
        params.auth = None;
        let url = "/api/documents/?tags__name__iexact=ocr-pending&page_size=1";
        server.set(url, Reply::status(500));
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let task = tokio::spawn(run(spec, params, classes, tx));
        assert!(matches!(rx.recv().await.unwrap(), Report::Failed { .. }));
        server.set(url, Reply::json(r#"{"count": 3}"#));
        loop {
            match rx.recv().await.unwrap() {
                Report::Failed { .. } => continue,
                ok => {
                    assert_eq!(demand(&ok), (3, 0));
                    break;
                }
            }
        }
        task.abort();
    }
}
