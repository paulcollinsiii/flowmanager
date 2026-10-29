//! The two requests of the `requeue_failed` action: list the failed items, and ask the
//! service to queue one again. Never panics and never blocks past the configured timeout;
//! every failure is a value for the caller to count and log.

use sched_model::{HttpMethod, Millis, RequeueFailedParams};
use sched_observe::http_json::{
    EnvFn, MAX_BODY, client_with_timeout, field_equals, read_body, redact_url, resolve_auth,
    select_items, send_get, send_request_with_body, status_error, url_with_since,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Largest answer to a re-queue request that is read.
const MAX_POST_BODY: usize = 1024 * 1024;
/// Longest identifier accepted into a URL.
const MAX_ID_LEN: usize = 128;

/// The HTTP client of the action: bounded by `timeout`, follows no redirects (a credential
/// must never be forwarded to another origin).
pub fn client(params: &RequeueFailedParams) -> Result<reqwest::Client, reqwest::Error> {
    client_with_timeout(params.timeout)
}

/// The auth header the action's params name, read from the environment (or a file); `None`
/// without an `auth` entry. Errors never contain the secret.
pub async fn auth(
    params: &RequeueFailedParams,
    env: &EnvFn,
) -> Result<Option<(String, String)>, String> {
    match &params.auth {
        Some(a) => resolve_auth(a, env).await.map(Some),
        None => Ok(None),
    }
}

/// The items of a successful list.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Listed {
    /// Identifiers of the items that passed `where`, in list order.
    pub ids: Vec<String>,
    /// The response was as long as the page can be, so items may be missing; the visible
    /// ones are still returned and the rest follow on later runs.
    pub truncated: bool,
    /// Items without a usable identifier (missing, not a string or integer, or with
    /// characters that do not belong in a URL path), skipped.
    pub invalid: usize,
}

/// An identifier as URL path text, or `None` if it is not a safe one.
fn id_text(v: &Value) -> Option<String> {
    let text = match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.as_u64().map(|u| u.to_string())?,
        _ => return None,
    };
    let safe = !text.is_empty()
        && text.len() <= MAX_ID_LEN
        && text != "."
        && text != ".."
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
    safe.then_some(text)
}

/// Lists the failed items at `now` (the clock `since` windows are dated from).
pub async fn list(
    client: &reqwest::Client,
    params: &RequeueFailedParams,
    auth: Option<&(String, String)>,
    now: Millis,
) -> Result<Listed, String> {
    let l = &params.list;
    let url = url_with_since(&l.url, l.since.as_ref(), now);
    let err = |e: String| format!("list ({}): {e}", redact_url(&l.url));
    let mut resp = send_get(client, &url, auth).await.map_err(err)?;
    if !resp.status().is_success() {
        return Err(err(status_error(&resp)));
    }
    let body = read_body(&mut resp, MAX_BODY).await.map_err(err)?;
    let doc: Value =
        serde_json::from_slice(&body).map_err(|e| err(format!("response is not JSON: {e}")))?;
    let items = doc
        .pointer(&l.items)
        .ok_or_else(|| err(format!("no value at {:?} in the response", l.items)))?
        .as_array()
        .ok_or_else(|| err(format!("value at {:?} is not an array", l.items)))?;
    let truncated = l.max_items.is_some_and(|max| items.len() as u64 >= max);
    let selected = select_items(items, l.filter.as_ref()).map_err(|e| err(e.to_string()))?;
    let mut out = Listed {
        truncated,
        ..Listed::default()
    };
    for item in selected {
        match item.pointer(&l.id).and_then(id_text) {
            Some(id) => out.ids.push(id),
            None => out.invalid += 1,
        }
    }
    Ok(out)
}

/// How one re-queue request went.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PostOutcome {
    /// The service accepted it (2xx, and every `expect` condition held).
    Queued,
    /// It did not work: network error, timeout, a non-2xx answer, an unreadable answer or
    /// an answer that says it was not queued. The text is safe to log (no URL, no secret).
    Failed(String),
}

/// Asks the service to queue `id` again. The attempt must already be recorded.
pub async fn requeue(
    client: &reqwest::Client,
    params: &RequeueFailedParams,
    auth: Option<&(String, String)>,
    id: &str,
) -> PostOutcome {
    if id_text(&Value::String(id.to_string())).is_none() {
        return PostOutcome::Failed("identifier is not safe to put in a URL".to_string());
    }
    let r = &params.requeue;
    let method = match r.method {
        HttpMethod::Post => reqwest::Method::POST,
        HttpMethod::Put => reqwest::Method::PUT,
        HttpMethod::Patch => reqwest::Method::PATCH,
    };
    let url = r.url.replace("{id}", id);
    let mut resp = match send_request_with_body(client, method, &url, auth, r.body.as_ref()).await {
        Ok(r) => r,
        Err(e) => return PostOutcome::Failed(e),
    };
    if !resp.status().is_success() {
        return PostOutcome::Failed(status_error(&resp));
    }
    if r.expect.is_empty() {
        return PostOutcome::Queued;
    }
    let body = match read_body(&mut resp, MAX_POST_BODY).await {
        Ok(b) => b,
        Err(e) => return PostOutcome::Failed(e),
    };
    let doc: Value = match serde_json::from_slice(&body) {
        Ok(d) => d,
        Err(e) => return PostOutcome::Failed(format!("response is not JSON: {e}")),
    };
    for cond in &r.expect {
        match field_equals(&doc, cond) {
            Ok(true) => {}
            Ok(false) => {
                return PostOutcome::Failed(format!(
                    "the response does not say it was queued ({} is not the expected value)",
                    cond.field
                ));
            }
            Err(e) => return PostOutcome::Failed(e.to_string()),
        }
    }
    PostOutcome::Queued
}

#[cfg(test)]
mod tests {
    use super::*;
    use sched_model::test_support::recovery_spec;
    use sched_observe::testserver::{Reply, TestServer};

    const NOW: Millis = Millis(1_700_000_000_000);

    fn params(server: &TestServer, extra: &str) -> RequeueFailedParams {
        recovery_spec(&format!(
            "action: requeue_failed
params:
  list: {{ url: '{}', items: /recordings, id: /id, where: {{ field: /has_summary, equals: false }}, since: {{ param: updated_since, ago: 15m }}, max_items: 100 }}
  requeue:
    url: '{}'
    expect: [ {{ field: /success, equals: true }}, {{ field: /status, equals: QUEUED }} ]
  auth: {{ header: Authorization, prefix: 'Bearer ', env: TOKEN }}
{extra}",
            server.url("/api/v1/recordings?status=failed&per_page=100"),
            server.url("/api/v1/recordings/{id}/transcribe"),
        ))
        .requeue_failed_params()
        .unwrap()
    }

    fn item(id: u64, has_summary: bool) -> String {
        format!(
            r#"{{"id": {id}, "status": "FAILED", "has_transcription": true, "has_summary": {has_summary}, "updated_at": "2023-11-14T21:00:00", "error_message": "boom"}}"#
        )
    }

    fn list_reply(items: &[String]) -> Reply {
        Reply::json(&format!(r#"{{"recordings": [{}]}}"#, items.join(",")))
    }

    fn token(name: &str) -> Option<String> {
        (name == "TOKEN").then(|| "s3cret\n".to_string())
    }

    const LIST_PATH: &str =
        "/api/v1/recordings?status=failed&per_page=100&updated_since=2023-11-14T21%3A58%3A20Z";

    #[tokio::test]
    async fn lists_failed_items_with_the_window_the_filter_and_the_token() {
        let server = TestServer::start().await;
        server.set(
            LIST_PATH,
            list_reply(&[item(3, false), item(4, true), item(5, false)]),
        );
        let p = params(&server, "");
        let a = auth(&p, &token).await.unwrap();
        let got = list(&client(&p).unwrap(), &p, a.as_ref(), NOW)
            .await
            .unwrap();
        assert_eq!(got.ids, ["3", "5"]);
        assert!(!got.truncated && got.invalid == 0);
        assert_eq!(
            server.requests(),
            [(LIST_PATH.to_string(), Some("Bearer s3cret".to_string()))]
        );
        assert_eq!(server.methods(), ["GET"]);
    }

    #[tokio::test]
    async fn list_failures_are_errors_never_panics() {
        let server = TestServer::start().await;
        let p = params(&server, "");
        let c = client(&p).unwrap();
        // 404 (nothing set), 500, 403, not JSON, wrong shape, filter field missing.
        let e = list(&c, &p, None, NOW).await.unwrap_err();
        assert!(e.contains("HTTP 404"), "{e}");
        server.set(LIST_PATH, Reply::status(500));
        assert!(
            list(&c, &p, None, NOW)
                .await
                .unwrap_err()
                .contains("HTTP 500")
        );
        server.set(LIST_PATH, Reply::status(403));
        assert!(
            list(&c, &p, None, NOW)
                .await
                .unwrap_err()
                .contains("HTTP 403")
        );
        server.set(LIST_PATH, Reply::json("<html>"));
        assert!(
            list(&c, &p, None, NOW)
                .await
                .unwrap_err()
                .contains("not JSON")
        );
        server.set(LIST_PATH, Reply::json(r#"{"nope": []}"#));
        assert!(
            list(&c, &p, None, NOW)
                .await
                .unwrap_err()
                .contains("no value at")
        );
        server.set(LIST_PATH, Reply::json(r#"{"recordings": 3}"#));
        assert!(
            list(&c, &p, None, NOW)
                .await
                .unwrap_err()
                .contains("not an array")
        );
        server.set(LIST_PATH, Reply::json(r#"{"recordings": [{"id": 1}]}"#));
        assert!(
            list(&c, &p, None, NOW)
                .await
                .unwrap_err()
                .contains("missing from every item")
        );
        // The URL (and its query) never appear in an error.
        let e = list(&c, &p, None, NOW).await.unwrap_err();
        assert!(!e.contains("updated_since=2"), "{e}");
    }

    #[tokio::test]
    async fn unusable_identifiers_are_skipped_and_a_full_page_is_flagged() {
        let server = TestServer::start().await;
        let mut items = vec![
            r#"{"id": "../etc", "has_summary": false}"#.to_string(),
            r#"{"id": "a/b", "has_summary": false}"#.to_string(),
            r#"{"id": null, "has_summary": false}"#.to_string(),
            r#"{"has_summary": false}"#.to_string(),
            r#"{"id": 1.5, "has_summary": false}"#.to_string(),
            r#"{"id": "abc-1_2.x", "has_summary": false}"#.to_string(),
            r#"{"id": 7, "has_summary": false}"#.to_string(),
        ];
        server.set(LIST_PATH, list_reply(&items));
        let p = params(&server, "");
        let got = list(&client(&p).unwrap(), &p, None, NOW).await.unwrap();
        assert_eq!(got.ids, ["abc-1_2.x", "7"]);
        assert_eq!(got.invalid, 5);
        // A response as long as max_items is flagged, not refused.
        items.clear();
        items.extend((0..100).map(|i| item(1000 + i, false)));
        server.set(LIST_PATH, list_reply(&items));
        let got = list(&client(&p).unwrap(), &p, None, NOW).await.unwrap();
        assert!(got.truncated);
        assert_eq!(got.ids.len(), 100);
    }

    #[tokio::test]
    async fn a_requeue_counts_only_when_the_answer_says_it_was_queued() {
        let server = TestServer::start().await;
        let p = params(&server, "");
        let c = client(&p).unwrap();
        let at = |id: &str| format!("/api/v1/recordings/{id}/transcribe");
        server.set(
            &at("1"),
            Reply::json(r#"{"success": true, "job_id": 9, "status": "QUEUED"}"#),
        );
        server.set(
            &at("2"),
            Reply::json(r#"{"success": false, "status": "QUEUED"}"#),
        );
        server.set(
            &at("3"),
            Reply::json(r#"{"success": true, "status": "PROCESSING"}"#),
        );
        server.set(&at("4"), Reply::status(500));
        server.set(&at("5"), Reply::status(403));
        server.set(&at("6"), Reply::json("not json"));
        server.set(&at("7"), Reply::json(r#"{"status": "QUEUED"}"#));
        server.set(
            &at("8"),
            Reply::json(r#"{"success": "yes", "status": "QUEUED"}"#),
        );
        let a = auth(&p, &token).await.unwrap();
        let go = |id: &'static str| {
            let (c, p, a) = (c.clone(), p.clone(), a.clone());
            async move { requeue(&c, &p, a.as_ref(), id).await }
        };
        assert_eq!(go("1").await, PostOutcome::Queued);
        for id in ["2", "3", "4", "5", "6", "7", "8", "9"] {
            assert!(matches!(go(id).await, PostOutcome::Failed(_)), "{id}");
        }
        assert!(server.methods().iter().all(|m| m == "POST"));
        assert_eq!(server.requests()[0].1.as_deref(), Some("Bearer s3cret"));
        // Bad ids never reach the network.
        let n = server.requests().len();
        assert!(matches!(
            requeue(&c, &p, None, "../x").await,
            PostOutcome::Failed(_)
        ));
        assert_eq!(server.requests().len(), n);
    }

    #[tokio::test]
    async fn a_configured_body_is_sent_as_json_and_otherwise_none_is() {
        let server = TestServer::start().await;
        let mut p = params(&server, "");
        let c = client(&p).unwrap();
        let path = "/api/v1/recordings/1/transcribe";
        server.set(
            path,
            Reply::json(r#"{"success": true, "job_id": 9, "status": "QUEUED"}"#),
        );
        assert_eq!(requeue(&c, &p, None, "1").await, PostOutcome::Queued);
        p.requeue.body = Some(serde_json::json!({}));
        assert_eq!(requeue(&c, &p, None, "1").await, PostOutcome::Queued);
        let kind = server.requests_with_header("content-type");
        assert_eq!(kind[0].1, None);
        assert_eq!(kind[1].1.as_deref(), Some("application/json"));
        let len = server.requests_with_header("content-length");
        assert_ne!(len[0].1.as_deref(), Some("2"));
        assert_eq!(len[1].1.as_deref(), Some("2"), "the body is {{}}");
    }

    #[tokio::test]
    async fn a_hanging_service_is_cut_off_at_the_timeout() {
        let server = TestServer::start().await;
        server.set(
            "/api/v1/recordings/1/transcribe",
            Reply::json("{}").delayed(std::time::Duration::from_secs(5)),
        );
        let p = params(&server, "  timeout: 300\n");
        let started = std::time::Instant::now();
        let out = requeue(&client(&p).unwrap(), &p, None, "1").await;
        assert!(matches!(out, PostOutcome::Failed(_)), "{out:?}");
        assert!(started.elapsed() < std::time::Duration::from_secs(3));
    }

    #[tokio::test]
    async fn redirects_are_not_followed_and_a_missing_secret_is_a_clear_error() {
        let server = TestServer::start().await;
        server.set(
            "/api/v1/recordings/1/transcribe",
            Reply::redirect("http://127.0.0.1:1/x"),
        );
        let p = params(&server, "");
        assert!(matches!(
            requeue(&client(&p).unwrap(), &p, None, "1").await,
            PostOutcome::Failed(e) if e.contains("302")
        ));
        let e = auth(&p, &|_| None).await.unwrap_err();
        assert!(e.contains("TOKEN is not set"), "{e}");
    }
}
