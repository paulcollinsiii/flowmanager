//! The generic HTTP/JSON adapter. Everything service-specific (endpoints, which field is
//! the count, which failures count as demand) comes from the source's `params`; nothing
//! here knows paperless, speakr or any other service. It only ever issues GET requests.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sched_model::{
    AuthSpec, ClassId, CountsAs, ElementFilter, Fidelity, FilterValue, HttpJsonParams, Millis,
    ObservedValue, RequestSpec, SourceSpec,
};
use serde_json::Value;

use crate::{Report, ReportTx};

/// Looks up an environment variable; injected so tests need not touch the process environment.
pub type EnvFn = dyn Fn(&str) -> Option<String> + Sync;

/// The scheduler's clock, as the adapter sees it: read once per poll to date `since` windows.
pub type NowFn = Arc<dyn Fn() -> Millis + Send + Sync>;

/// The system clock. This crate is the I/O layer; the service passes its own clock instead.
pub fn system_now() -> NowFn {
    Arc::new(|| {
        let ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        Millis(u64::try_from(ms).unwrap_or(u64::MAX))
    })
}

/// `ms` since the unix epoch as `YYYY-MM-DDTHH:MM:SSZ` (UTC), with `:` percent-encoded so
/// the text can go straight into a query string.
pub fn iso8601_query(ms: Millis) -> String {
    let secs = ms.as_u64() / 1000;
    let (days, rem) = (secs / 86_400, secs % 86_400);
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}%3A{:02}%3A{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// The URL to request: `req.url`, plus the `since` window ending at `now` if set.
pub fn request_url(req: &RequestSpec, now: Millis) -> String {
    match &req.since {
        None => req.url.clone(),
        Some(s) => {
            let from = now.saturating_sub(s.ago);
            let sep = if req.url.contains('?') { '&' } else { '?' };
            format!("{}{sep}{}={}", req.url, s.param, iso8601_query(from))
        }
    }
}

/// Largest response body read.
pub(crate) const MAX_BODY: usize = 8 * 1024 * 1024;

/// Why a response could not be turned into a number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtractError {
    Missing(String),
    NotANumber(String),
    NotAnArray(String),
    /// The array is as long as the page can be, so items may be missing.
    AtLimit(u64),
    /// The `where` field is in none of the (non-empty) array's items, so the filter would
    /// silently match nothing.
    FilterFieldMissing(String),
    /// An item's `where` field holds a value of another JSON type than the filter compares
    /// (for example a number where a boolean is expected), so the filter would silently
    /// match nothing. `null` is not a mismatch: it is an item that does not match.
    FilterTypeMismatch {
        field: String,
        expected: &'static str,
    },
}

impl std::fmt::Display for ExtractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExtractError::Missing(p) => write!(f, "no value at {p:?} in the response"),
            ExtractError::NotANumber(p) => {
                write!(f, "value at {p:?} is not a non-negative whole number")
            }
            ExtractError::NotAnArray(p) => write!(f, "value at {p:?} is not an array"),
            ExtractError::FilterFieldMissing(field) => write!(
                f,
                "the where field {field:?} is missing from every item, so the filter would count nothing; check the field name against the API"
            ),
            ExtractError::FilterTypeMismatch { field, expected } => write!(
                f,
                "the where field {field:?} holds a value that is not a {expected}, so the filter cannot compare it; check the field against the API"
            ),
            ExtractError::AtLimit(n) => write!(
                f,
                "the response holds {n} items, a full page; the count may be too low"
            ),
        }
    }
}

/// Whether the filter compares a boolean (`equals: true|false`); otherwise it compares text.
fn compares_bool(filter: &ElementFilter) -> bool {
    filter.contains.is_none() && matches!(filter.equals, Some(FilterValue::Bool(_)))
}

/// The JSON type the filter compares, by name.
fn filter_type(filter: &ElementFilter) -> &'static str {
    if compares_bool(filter) {
        "boolean"
    } else {
        "string"
    }
}

fn matches(filter: &ElementFilter, element: &Value) -> bool {
    let Some(value) = element.pointer(&filter.field) else {
        return false;
    };
    match (&filter.contains, &filter.equals) {
        (Some(needle), _) => value
            .as_str()
            .is_some_and(|text| text.to_lowercase().contains(&needle.to_lowercase())),
        (None, Some(FilterValue::Text(exact))) => value.as_str() == Some(exact.as_str()),
        (None, Some(FilterValue::Bool(b))) => value.as_bool() == Some(*b),
        (None, None) => false,
    }
}

/// Whether `value` (present at the filter's field) has the type the filter compares.
fn comparable(filter: &ElementFilter, value: &Value) -> bool {
    if compares_bool(filter) {
        value.is_boolean()
    } else {
        value.is_string()
    }
}

/// The number one request contributes: the value at `count`, or the number of elements of
/// the array at `length` that pass `where`.
pub fn extract(doc: &Value, req: &RequestSpec) -> Result<u64, ExtractError> {
    if let Some(ptr) = &req.count {
        return extract_number(doc, ptr);
    }
    let ptr = req.length.as_deref().unwrap_or("");
    let items = doc
        .pointer(ptr)
        .ok_or_else(|| ExtractError::Missing(ptr.to_string()))?
        .as_array()
        .ok_or_else(|| ExtractError::NotAnArray(ptr.to_string()))?;
    if req.max_items.is_some_and(|max| items.len() as u64 >= max) {
        return Err(ExtractError::AtLimit(items.len() as u64));
    }
    if let Some(f) = &req.filter
        && !items.is_empty()
        && items.iter().all(|e| e.pointer(&f.field).is_none())
    {
        return Err(ExtractError::FilterFieldMissing(f.field.clone()));
    }
    if let Some(f) = &req.filter
        && items
            .iter()
            .filter_map(|e| e.pointer(&f.field))
            .any(|v| !v.is_null() && !comparable(f, v))
    {
        return Err(ExtractError::FilterTypeMismatch {
            field: f.field.clone(),
            expected: filter_type(f),
        });
    }
    let n = match &req.filter {
        Some(f) => items.iter().filter(|e| matches(f, e)).count(),
        None => items.len(),
    };
    Ok(n as u64)
}

/// The non-negative whole number at `ptr`.
pub fn extract_number(doc: &Value, ptr: &str) -> Result<u64, ExtractError> {
    let v = doc
        .pointer(ptr)
        .ok_or_else(|| ExtractError::Missing(ptr.to_string()))?;
    v.as_u64()
        .or_else(|| {
            v.as_f64()
                .filter(|f| *f >= 0.0 && f.fract() == 0.0 && *f < u64::MAX as f64)
                .map(|f| f as u64)
        })
        .ok_or_else(|| ExtractError::NotANumber(ptr.to_string()))
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

/// `url` without credentials or query values, safe for logs and error text: userinfo is
/// dropped and each `key=value` of the query becomes `key=<redacted>`.
pub fn redact_url(url: &str) -> String {
    let (base, query) = match url.split_once('?') {
        Some((b, q)) => (b, Some(q)),
        None => (url, None),
    };
    let base = match base.split_once("://") {
        Some((scheme, rest)) => {
            let (authority, path) = rest.split_once('/').map_or((rest, ""), |(a, p)| (a, p));
            let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
            if path.is_empty() && !rest.contains('/') {
                format!("{scheme}://{host}")
            } else {
                format!("{scheme}://{host}/{path}")
            }
        }
        None => base.to_string(),
    };
    match query {
        None => base,
        Some(q) => {
            let pairs: Vec<String> = q
                .split('&')
                .filter(|p| !p.is_empty())
                .map(|p| format!("{}=<redacted>", p.split('=').next().unwrap_or("")))
                .collect();
            format!("{base}?{}", pairs.join("&"))
        }
    }
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

/// A GET of `url`, with the auth header (marked sensitive) if any, sent.
pub(crate) async fn send_get(
    client: &reqwest::Client,
    url: &str,
    auth: Option<&(String, String)>,
) -> Result<reqwest::Response, String> {
    let mut builder = client.get(url);
    if let Some((name, value)) = auth {
        let mut v = reqwest::header::HeaderValue::from_str(value)
            .map_err(|_| "the secret is not a valid header value".to_string())?;
        v.set_sensitive(true);
        builder = builder.header(name, v);
    }
    builder
        .send()
        .await
        .map_err(|e| e.without_url().to_string())
}

/// The error text for a non-success response: the status, and for a 429 when the service
/// asked us to come back (polling cadence is unchanged).
pub(crate) fn status_error(resp: &reqwest::Response) -> String {
    let status = resp.status();
    let retry_after = resp
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.chars().take(64).collect::<String>());
    match (status.as_u16(), retry_after) {
        (429, Some(after)) => format!("HTTP {status} (rate limited; Retry-After: {after})"),
        (429, None) => format!("HTTP {status} (rate limited)"),
        _ => format!("HTTP {status}"),
    }
}

/// The body, read chunk by chunk and abandoned at the limit, so a huge or endless body is
/// never held.
pub(crate) async fn read_body(
    resp: &mut reqwest::Response,
    max_body: usize,
) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| e.without_url().to_string())?
    {
        if body.len() + chunk.len() > max_body {
            return Err(format!("response is larger than {max_body} bytes"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

async fn fetch(
    client: &reqwest::Client,
    req: &RequestSpec,
    auth: Option<&(String, String)>,
    max_body: usize,
    now: Millis,
) -> Result<Value, String> {
    let mut resp = send_get(client, &request_url(req, now), auth).await?;
    if !resp.status().is_success() {
        return Err(status_error(&resp));
    }
    let body = read_body(&mut resp, max_body).await?;
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
    poll_with_limit(client, spec, params, classes, env, MAX_BODY).await
}

/// As [`poll`], with an explicit cap on the size of each response body.
pub async fn poll_with_limit(
    client: &reqwest::Client,
    spec: &SourceSpec,
    params: &HttpJsonParams,
    classes: &[ClassId],
    env: &EnvFn,
    max_body: usize,
) -> Report {
    poll_at(client, spec, params, classes, env, max_body, system_now()()).await
}

/// As [`poll_with_limit`], at `now` (the clock reading `since` windows are dated from).
pub async fn poll_at(
    client: &reqwest::Client,
    spec: &SourceSpec,
    params: &HttpJsonParams,
    classes: &[ClassId],
    env: &EnvFn,
    max_body: usize,
    now: Millis,
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
        let doc = match fetch(client, req, auth.as_ref(), max_body, now).await {
            Ok(d) => d,
            Err(e) => return fail(format!("request {i} ({}): {e}", redact_url(&req.url))),
        };
        let numbers = std::iter::once(extract(&doc, req).map(|n| (req.counts_as, n))).chain(
            req.also
                .iter()
                .map(|e| extract_number(&doc, &e.count).map(|n| (e.counts_as, n))),
        );
        for n in numbers {
            match n {
                Ok(part) => parts.push(part),
                Err(e) => return fail(format!("request {i} ({}): {e}", redact_url(&req.url))),
            }
        }
    }
    let (ready, in_flight) = totals(&parts);
    Report::Observed {
        source: spec.id.clone(),
        fidelity: Fidelity::Polled,
        values: demand_values(classes, ready, in_flight),
    }
}

/// The HTTP client for a source. It follows no redirects: a custom auth header must never
/// be forwarded to another origin, and a redirect from an API endpoint is an error anyway.
pub fn build_client(params: &HttpJsonParams) -> Result<reqwest::Client, reqwest::Error> {
    client_with_timeout(params.timeout)
}

/// As [`build_client`], for any adapter's timeout.
pub(crate) fn client_with_timeout(timeout: Millis) -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .timeout(Duration::from_millis(timeout.as_u64()))
        .redirect(reqwest::redirect::Policy::none())
        .build()
}

/// Polls forever, every `resync`, until the receiver is gone.
pub async fn run(
    spec: SourceSpec,
    params: HttpJsonParams,
    classes: Vec<ClassId>,
    tx: ReportTx,
    now: NowFn,
) {
    let client = match build_client(&params) {
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
        let report = poll_at(&client, &spec, &params, &classes, &env, MAX_BODY, now()).await;
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
            max_items: None,
            also: Vec::new(),
            since: None,
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

    const SPEAKR_STATS: &str = "/api/v1/stats";

    fn stats(pending: u64, queued: u64, processing: u64) -> Reply {
        Reply::json(&format!(
            r#"{{"recordings": {{"total": 9, "pending": {pending}}}, "queue": {{"jobs_queued": {queued}, "jobs_processing": {processing}}}}}"#
        ))
    }
    /// The clock reading speakr's failed-recordings window is dated from in these tests.
    const NOW: Millis = Millis(1_700_000_000_000);
    /// The example's failed-recordings request at [`NOW`]: the window is the last 6h.
    const SPEAKR_FAILED: &str = "/api/v1/recordings?status=failed&per_page=100&sort_by=updated_at&sort_order=desc&updated_since=2023-11-14T16%3A13%3A20Z";

    /// A speakr v1 list item as `GET /api/v1/recordings` returns it (the fields the
    /// adapter could look at, plus a few it does not).
    fn speakr_item(id: u64, status: &str, has_summary: bool) -> String {
        let error = if status == "FAILED" {
            r#""error_message": "Transcription failed: Connection error","#
        } else {
            ""
        };
        format!(
            r#"{{"id": {id}, "title": "Meeting {id}", "status": "{status}", {error} "created_at": "2023-11-14T20:00:00", "updated_at": "2023-11-14T21:00:00", "has_transcription": true, "has_summary": {has_summary}, "transcription_duration_seconds": null}}"#
        )
    }

    fn speakr_list(items: &[String]) -> Reply {
        Reply::json(&format!(
            r#"{{"recordings": [{}], "pagination": {{"page": 1, "per_page": 100, "total": {}, "total_pages": 1, "has_next": false, "has_prev": false}}}}"#,
            items.join(", "),
            items.len()
        ))
    }

    /// One poll at [`NOW`].
    async fn poll_now(
        client: &reqwest::Client,
        spec: &SourceSpec,
        params: &HttpJsonParams,
        classes: &[ClassId],
        env: &EnvFn,
    ) -> Report {
        poll_at(client, spec, params, classes, env, MAX_BODY, NOW).await
    }

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
    async fn speakr_counts_pending_queued_and_failed_transcriptions() {
        let server = TestServer::start().await;
        // 2 recordings waiting to be enqueued, 3 jobs queued, 1 running.
        server.set(SPEAKR_STATS, stats(2, 3, 1));
        // Three FAILED recordings in the window: two failed transcriptions (no summary) and
        // one failed summary (the error is stored in `summary`, so has_summary is true).
        server.set(
            SPEAKR_FAILED,
            speakr_list(&[
                speakr_item(3, "FAILED", false),
                speakr_item(4, "FAILED", true),
                speakr_item(5, "FAILED", false),
            ]),
        );
        let (spec, params, classes) = example_source("speakr-pending", &server);
        assert_eq!(classes, [ClassId::from("speakr-transcription")]);
        let client = reqwest::Client::new();
        let report = poll_now(&client, &spec, &params, &classes, &env).await;
        // 2 pending + 3 queued jobs + 2 failed transcriptions; one job running.
        assert_eq!(demand(&report), (7, 1));
        // Two requests per poll: stats (one response, three numbers) and the failed list.
        assert_eq!(server.requests().len(), 2);
        assert_eq!(server.requests()[1].0, SPEAKR_FAILED);
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
        let client = build_client(params).unwrap();
        match poll_now(&client, spec, params, &[], &env).await {
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
        let empty = r#"{"recordings": []}"#;
        server.set(SPEAKR_STATS, stats(0, 0, 0));
        server.set(SPEAKR_FAILED, Reply::json(empty));

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
        let task = tokio::spawn(run(spec, params, classes, tx, system_now()));
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

    #[test]
    fn redaction_drops_credentials_and_query_values() {
        assert_eq!(
            redact_url("https://user:pw@host:8000/api/x?token=abc&status=failed"),
            "https://host:8000/api/x?token=<redacted>&status=<redacted>"
        );
        assert_eq!(redact_url("http://host/p"), "http://host/p");
        assert_eq!(redact_url("http://u@host"), "http://host");
    }

    #[tokio::test]
    async fn errors_do_not_leak_credentials_from_the_url() {
        let server = TestServer::start().await;
        let (spec, mut params, _) = example_source("paperless-ocr-pending", &server);
        params.auth = None;
        params.requests[0].url = format!(
            "{}?api_key=SECRETVALUE",
            server
                .url("/api/documents/")
                .replace("http://", "http://bob:hunter2@")
        );
        server.set("/api/documents/?api_key=SECRETVALUE", Reply::status(500));
        let e = failure(&server, &params, &spec).await;
        assert!(
            e.contains("HTTP 500") && e.contains("api_key=<redacted>"),
            "{e}"
        );
        assert!(!e.contains("SECRETVALUE") && !e.contains("hunter2"), "{e}");
    }

    #[tokio::test]
    async fn redirects_are_not_followed_and_credentials_do_not_cross_origins() {
        let other = TestServer::start().await;
        other.set(
            "/api/documents/?tags__name__iexact=ocr-pending&page_size=1",
            Reply::json(r#"{"count": 99}"#),
        );
        let server = TestServer::start().await;
        let (spec, params, classes) = example_source("paperless-ocr-pending", &server);
        server.set(
            "/api/documents/?tags__name__iexact=ocr-pending&page_size=1",
            Reply::redirect(
                &other.url("/api/documents/?tags__name__iexact=ocr-pending&page_size=1"),
            ),
        );
        let env = |_: &str| Some("tok".to_string());
        let client = build_client(&params).unwrap();
        match poll(&client, &spec, &params, &classes, &env).await {
            Report::Failed { error, .. } => assert!(error.contains("HTTP 302"), "{error}"),
            other => panic!("{other:?}"),
        }
        assert!(
            other.requests().is_empty(),
            "the redirect target was contacted"
        );
    }

    #[tokio::test]
    async fn a_full_page_fails_instead_of_under_counting() {
        let d = doc(r#"{"r": [1,2,3]}"#);
        let mut r = req(None, Some("/r"), None);
        r.max_items = Some(3);
        assert_eq!(extract(&d, &r), Err(ExtractError::AtLimit(3)));
        r.max_items = Some(4);
        assert_eq!(extract(&d, &r), Ok(3));

        // End to end with the example speakr source: a full page of failed recordings.
        let server = TestServer::start().await;
        let (spec, params, classes) = example_source("speakr-pending", &server);
        let full: Vec<String> = (0..100).map(|i| speakr_item(i, "FAILED", false)).collect();
        server.set(SPEAKR_STATS, stats(0, 0, 0));
        server.set(SPEAKR_FAILED, speakr_list(&full));
        match poll_now(
            &build_client(&params).unwrap(),
            &spec,
            &params,
            &classes,
            &env,
        )
        .await
        {
            Report::Failed { error, .. } => assert!(error.contains("full page"), "{error}"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn an_endless_body_is_cut_off_while_streaming() {
        let server = TestServer::start().await;
        let (spec, params, classes) = example_source("paperless-ocr-pending", &server);
        server.set(
            "/api/documents/?tags__name__iexact=ocr-pending&page_size=1",
            Reply::endless(),
        );
        let env = |_: &str| Some("tok".to_string());
        let client = build_client(&params).unwrap();
        let report = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            poll_with_limit(&client, &spec, &params, &classes, &env, 100_000),
        )
        .await
        .expect("an endless body must not hang the poll");
        match report {
            Report::Failed { error, .. } => {
                assert!(error.contains("larger than 100000"), "{error}")
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_filter_field_missing_from_every_item_fails_instead_of_counting_zero() {
        let filter = ElementFilter {
            field: "/error_message".into(),
            contains: Some("connection".into()),
            equals: None,
        };
        let r = req(None, Some("/recordings"), Some(filter));
        // The list items do not carry the field at all: do not report "0 failed".
        let d = doc(r#"{"recordings": [{"id": 1}, {"id": 2}]}"#);
        assert_eq!(
            extract(&d, &r),
            Err(ExtractError::FilterFieldMissing("/error_message".into()))
        );
        // A null value means the field exists and the item did not fail that way.
        let d = doc(r#"{"recordings": [{"id": 1, "error_message": null}, {"id": 2}]}"#);
        assert_eq!(extract(&d, &r), Ok(0));
        // An empty page is fine.
        assert_eq!(extract(&doc(r#"{"recordings": []}"#), &r), Ok(0));
        // `contains` ignores case.
        let d = doc(r#"{"recordings": [{"error_message": "CONNECTION Refused"}]}"#);
        assert_eq!(extract(&d, &r), Ok(1));
    }

    #[tokio::test]
    async fn the_example_speakr_source_fails_explicitly_when_list_items_lack_has_summary() {
        let server = TestServer::start().await;
        let (spec, params, classes) = example_source("speakr-pending", &server);
        server.set(SPEAKR_STATS, stats(3, 0, 0));
        server.set(
            SPEAKR_FAILED,
            Reply::json(r#"{"recordings": [{"id": 9, "status": "FAILED"}]}"#),
        );
        match poll_now(
            &build_client(&params).unwrap(),
            &spec,
            &params,
            &classes,
            &env,
        )
        .await
        {
            Report::Failed { error, .. } => {
                assert!(error.contains("missing from every item"), "{error}")
            }
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn the_example_summaries_source_counts_summarizing_recordings() {
        let server = TestServer::start().await;
        let (spec, params, classes) = example_source("speakr-summaries", &server);
        server.set(
            "/api/v1/recordings?status=processing&per_page=100",
            Reply::json(
                r#"{"recordings": [{"status": "PROCESSING"}, {"status": "SUMMARIZING"}, {"status": "SUMMARIZING"}]}"#,
            ),
        );
        let report = poll(
            &build_client(&params).unwrap(),
            &spec,
            &params,
            &classes,
            &env,
        )
        .await;
        assert_eq!(demand(&report), (2, 0));
    }

    #[test]
    fn dates_are_iso8601_utc_with_encoded_colons() {
        assert_eq!(iso8601_query(Millis(0)), "1970-01-01T00%3A00%3A00Z");
        assert_eq!(
            iso8601_query(Millis(1_700_000_000_000)),
            "2023-11-14T22%3A13%3A20Z"
        );
        // A leap day, and the last second of a year.
        assert_eq!(
            iso8601_query(Millis(951_782_400_000)),
            "2000-02-29T00%3A00%3A00Z"
        );
        assert_eq!(
            iso8601_query(Millis(1_704_067_199_999)),
            "2023-12-31T23%3A59%3A59Z"
        );
    }

    #[test]
    fn the_since_window_is_appended_to_the_query_from_the_given_clock() {
        let mut r = req(Some("/count"), None, None);
        r.since = Some(sched_model::SinceSpec {
            param: "updated_since".into(),
            ago: Millis(86_400_000),
        });
        r.url = "http://x/a?b=1".into();
        let now = Millis(1_700_000_000_000);
        assert_eq!(
            request_url(&r, now),
            "http://x/a?b=1&updated_since=2023-11-13T22%3A13%3A20Z"
        );
        r.url = "http://x/a".into();
        assert_eq!(
            request_url(&r, now),
            "http://x/a?updated_since=2023-11-13T22%3A13%3A20Z"
        );
        r.since = None;
        assert_eq!(request_url(&r, now), "http://x/a");
    }

    #[tokio::test]
    async fn the_example_failed_request_only_asks_for_the_last_6h_using_the_clock_it_is_given() {
        let server = TestServer::start().await;
        let (spec, params, classes) = example_source("speakr-pending", &server);
        server.set(SPEAKR_STATS, stats(0, 0, 0));
        server.set(
            SPEAKR_FAILED,
            speakr_list(&[speakr_item(1, "FAILED", false)]),
        );
        let client = build_client(&params).unwrap();
        let report = poll_now(&client, &spec, &params, &classes, &env).await;
        assert_eq!(demand(&report), (1, 0));
        assert_eq!(server.requests()[1].0, SPEAKR_FAILED);
        // An hour later the window has moved with the clock: the same failure would be
        // listed (or not) by a different query, here unanswered.
        let later = Millis(NOW.as_u64() + 3_600_000);
        let report = poll_at(&client, &spec, &params, &classes, &env, MAX_BODY, later).await;
        assert!(matches!(report, Report::Failed { .. }), "{report:?}");
        assert!(
            server.requests()[3]
                .0
                .ends_with("&updated_since=2023-11-14T17%3A13%3A20Z"),
            "{:?}",
            server.requests()
        );
    }

    #[tokio::test]
    async fn failed_summaries_and_an_empty_window_are_not_transcription_demand() {
        let server = TestServer::start().await;
        let (spec, params, classes) = example_source("speakr-pending", &server);
        let client = build_client(&params).unwrap();
        server.set(SPEAKR_STATS, stats(0, 0, 0));
        server.set(
            SPEAKR_FAILED,
            speakr_list(&[speakr_item(7, "FAILED", true)]),
        );
        assert_eq!(
            demand(&poll_now(&client, &spec, &params, &classes, &env).await),
            (0, 0)
        );
        server.set(SPEAKR_FAILED, speakr_list(&[]));
        assert_eq!(
            demand(&poll_now(&client, &spec, &params, &classes, &env).await),
            (0, 0)
        );
        // has_summary of another type than boolean (as if the API changed): a failure, not 0.
        server.set(
            SPEAKR_FAILED,
            Reply::json(r#"{"recordings": [{"id": 1, "status": "FAILED", "has_summary": 0}]}"#),
        );
        match poll_now(&client, &spec, &params, &classes, &env).await {
            Report::Failed { error, .. } => {
                assert!(error.contains("not a boolean"), "{error}")
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn where_equals_compares_booleans_and_text_by_json_type() {
        let filter = |equals: FilterValue| ElementFilter {
            field: "/f".into(),
            contains: None,
            equals: Some(equals),
        };
        let d = doc(r#"[{"f": false}, {"f": true}, {"f": null}, {}, {"f": false}]"#);
        let r = req(None, Some(""), Some(filter(false.into())));
        assert_eq!(extract(&d, &r), Ok(2));
        let r = req(None, Some(""), Some(filter(true.into())));
        assert_eq!(extract(&d, &r), Ok(1));
        // The text "false" is not the boolean false: a type mismatch, not a silent 0.
        let r = req(None, Some(""), Some(filter("false".into())));
        assert_eq!(
            extract(&d, &r),
            Err(ExtractError::FilterTypeMismatch {
                field: "/f".into(),
                expected: "string"
            })
        );
        let d = doc(r#"[{"f": "false"}, {"f": 0}]"#);
        let r = req(None, Some(""), Some(filter(false.into())));
        assert_eq!(
            extract(&d, &r),
            Err(ExtractError::FilterTypeMismatch {
                field: "/f".into(),
                expected: "boolean"
            })
        );
        // `contains` on a non-string value is a mismatch as well.
        let contains = ElementFilter {
            field: "/f".into(),
            contains: Some("x".into()),
            equals: None,
        };
        let d = doc(r#"[{"f": "x"}, {"f": 5}]"#);
        assert!(matches!(
            extract(&d, &req(None, Some(""), Some(contains))),
            Err(ExtractError::FilterTypeMismatch { .. })
        ));
    }

    /// An immich `GET /api/queues/{name}` body (immich v2.4.0 and later).
    fn immich_queue(name: &str, waiting: u64, delayed: u64, active: u64) -> Reply {
        Reply::json(&format!(
            r#"{{"name": "{name}", "isPaused": false, "statistics": {{"active": {active}, "completed": 1204, "failed": 3, "delayed": {delayed}, "waiting": {waiting}, "paused": 0}}}}"#
        ))
    }

    fn immich_env(name: &str) -> Option<String> {
        (name == "IMMICH_API_KEY").then(|| "imm-key\n".to_string())
    }

    #[tokio::test]
    async fn immich_sums_waiting_and_delayed_as_ready_and_active_in_flight_over_ml_queues() {
        let server = TestServer::start().await;
        let (spec, params, classes) = example_source("immich-queues", &server);
        assert_eq!(classes, [ClassId::from("immich-ml")]);
        server.set(
            "/api/queues/smartSearch",
            immich_queue("smartSearch", 5, 1, 2),
        );
        server.set(
            "/api/queues/faceDetection",
            immich_queue("faceDetection", 0, 0, 1),
        );
        server.set("/api/queues/ocr", immich_queue("ocr", 3, 2, 0));
        // Other queues are never read.
        server.set("/api/jobs", Reply::json(r#"{"waiting": 99}"#));
        let client = build_client(&params).unwrap();
        let report = poll(&client, &spec, &params, &classes, &immich_env).await;
        // (5+1) + (0+0) + (3+2) ready; 2 + 1 + 0 running.
        assert_eq!(demand(&report), (11, 3));
        let seen: Vec<(String, Option<String>)> = server.requests_with_header("x-api-key");
        assert_eq!(
            seen,
            [
                (
                    "/api/queues/smartSearch".to_string(),
                    Some("imm-key".to_string())
                ),
                (
                    "/api/queues/faceDetection".to_string(),
                    Some("imm-key".to_string())
                ),
                ("/api/queues/ocr".to_string(), Some("imm-key".to_string())),
            ]
        );
        // The key goes in its own header only, with no prefix and no Authorization header.
        assert!(server.requests().iter().all(|(_, auth)| auth.is_none()));
    }

    #[tokio::test]
    async fn immich_fails_on_a_missing_key_queue_or_statistic() {
        let server = TestServer::start().await;
        let (spec, params, classes) = example_source("immich-queues", &server);
        let client = build_client(&params).unwrap();
        server.set(
            "/api/queues/smartSearch",
            immich_queue("smartSearch", 1, 0, 0),
        );
        server.set(
            "/api/queues/faceDetection",
            immich_queue("faceDetection", 1, 0, 0),
        );
        server.set("/api/queues/ocr", immich_queue("ocr", 1, 0, 0));
        match poll(&client, &spec, &params, &classes, &no_env).await {
            Report::Failed { error, .. } => assert!(error.contains("IMMICH_API_KEY"), "{error}"),
            other => panic!("{other:?}"),
        }
        assert!(server.requests().is_empty());
        // An immich older than v2.4.0 has no such endpoint.
        server.set("/api/queues/ocr", Reply::status(404));
        match poll(&client, &spec, &params, &classes, &immich_env).await {
            Report::Failed { error, .. } => {
                assert!(
                    error.contains("request 2") && error.contains("404"),
                    "{error}"
                )
            }
            other => panic!("{other:?}"),
        }
        // A body without the nested statistic is a failure, not a zero.
        server.set(
            "/api/queues/ocr",
            Reply::json(
                r#"{"name": "ocr", "isPaused": false, "statistics": {"active": 0, "waiting": 4}}"#,
            ),
        );
        match poll(&client, &spec, &params, &classes, &immich_env).await {
            Report::Failed { error, .. } => {
                assert!(error.contains("/statistics/delayed"), "{error}")
            }
            other => panic!("{other:?}"),
        }
        // Wrong key: immich answers 401.
        server.set("/api/queues/smartSearch", Reply::status(401));
        match poll(&client, &spec, &params, &classes, &immich_env).await {
            Report::Failed { error, .. } => assert!(error.contains("HTTP 401"), "{error}"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn rate_limiting_reports_retry_after() {
        let server = TestServer::start().await;
        let (spec, params, classes) = example_source("paperless-ocr-pending", &server);
        let path = "/api/documents/?tags__name__iexact=ocr-pending&page_size=1";
        let env = |_: &str| Some("tok".to_string());
        let client = build_client(&params).unwrap();
        server.set(path, Reply::status(429).header("Retry-After", "37"));
        match poll(&client, &spec, &params, &classes, &env).await {
            Report::Failed { error, .. } => {
                assert!(
                    error.contains("429") && error.contains("Retry-After: 37"),
                    "{error}"
                )
            }
            other => panic!("{other:?}"),
        }
        server.set(path, Reply::status(429));
        match poll(&client, &spec, &params, &classes, &env).await {
            Report::Failed { error, .. } => assert!(error.contains("rate limited"), "{error}"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn one_response_can_feed_several_numbers_and_each_must_be_present() {
        let server = TestServer::start().await;
        let (spec, params, classes) = example_source("speakr-pending", &server);
        server.set(SPEAKR_FAILED, Reply::json(r#"{"recordings": []}"#));
        // The extra pointer is missing from the stats document: the poll fails, it does
        // not count the numbers it did find.
        server.set(
            SPEAKR_STATS,
            Reply::json(r#"{"recordings": {"pending": 4}, "queue": {"jobs_queued": 2}}"#),
        );
        let client = build_client(&params).unwrap();
        match poll_now(&client, &spec, &params, &classes, &env).await {
            Report::Failed { error, .. } => {
                assert!(error.contains("/queue/jobs_processing"), "{error}")
            }
            other => panic!("{other:?}"),
        }
        server.set(SPEAKR_STATS, stats(4, 2, 5));
        assert_eq!(
            demand(&poll_now(&client, &spec, &params, &classes, &env).await),
            (6, 5)
        );
    }
}
