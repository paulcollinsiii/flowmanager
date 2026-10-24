//! The Prometheus adapter: reachability of one resource from one instant query. Which
//! query, server and resource come from the source's `params`; nothing here knows any
//! particular host or exporter. It only ever issues GET requests.
//!
//! The query must return exactly one series whose value is `1` (up) or `0` (down).
//! Everything else (an HTTP or Prometheus error, a result that is not a vector, no series,
//! several series, any other value) is a failed read and is reported as one, never as
//! "down": a missing or ambiguous reading says nothing about the host.

use std::time::Duration;

use sched_model::{Fidelity, ObservedValue, PrometheusParams, SourceSpec};
use serde_json::Value;

use crate::http_json::{self, EnvFn, MAX_BODY};
use crate::kube_watch::Debouncer;
use crate::{Report, ReportTx};

/// `text` percent-encoded for a query string value: every byte outside RFC 3986's
/// unreserved set becomes `%XX`.
pub fn percent_encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for b in text.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// The instant-query URL for `params`.
pub fn query_url(params: &PrometheusParams) -> String {
    format!(
        "{}/api/v1/query?query={}",
        params.url.trim_end_matches('/'),
        percent_encode(&params.query)
    )
}

/// What an error response says, if it is Prometheus's JSON error shape.
fn api_error(doc: &Value) -> Option<String> {
    let kind = doc.get("errorType").and_then(Value::as_str).unwrap_or("");
    let error = doc.get("error").and_then(Value::as_str)?;
    let text = if kind.is_empty() {
        error.to_string()
    } else {
        format!("{kind}: {error}")
    };
    Some(text.chars().take(300).collect())
}

/// The reading in a query response: `Ok(true)` for up, `Ok(false)` for down, and an error
/// for anything that is not exactly one series with the value 0 or 1.
pub fn reading(doc: &Value) -> Result<bool, String> {
    match doc.get("status").and_then(Value::as_str) {
        Some("success") => {}
        Some("error") => {
            return Err(format!(
                "query failed: {}",
                api_error(doc).unwrap_or_else(|| "no error text".to_string())
            ));
        }
        other => return Err(format!("response status is {other:?}, not \"success\"")),
    }
    let data = doc.get("data").ok_or("response has no data")?;
    match data.get("resultType").and_then(Value::as_str) {
        Some("vector") => {}
        other => {
            return Err(format!(
                "result type is {other:?}, not \"vector\"; use an instant-vector query"
            ));
        }
    }
    let series = data
        .get("result")
        .and_then(Value::as_array)
        .ok_or("response has no result array")?;
    let one = match series.as_slice() {
        [one] => one,
        [] => {
            return Err(
                "the query returned no series (the probe may be missing); not treated as down"
                    .to_string(),
            );
        }
        many => {
            return Err(format!(
                "the query returned {} series; exactly one is required, so narrow the query",
                many.len()
            ));
        }
    };
    let text = one
        .get("value")
        .and_then(Value::as_array)
        .and_then(|v| v.get(1))
        .and_then(Value::as_str)
        .ok_or("the series has no [time, \"value\"] pair")?;
    match text.trim().parse::<f64>() {
        Ok(1.0) => Ok(true),
        Ok(0.0) => Ok(false),
        _ => Err(format!(
            "the series value {:?} is neither 0 nor 1",
            text.chars().take(64).collect::<String>()
        )),
    }
}

/// One read of the query: the raw (not debounced) up/down reading.
pub async fn read(
    client: &reqwest::Client,
    params: &PrometheusParams,
    env: &EnvFn,
    max_body: usize,
) -> Result<bool, String> {
    let auth = match &params.auth {
        Some(a) => Some(http_json::resolve_auth(a, env).await?),
        None => None,
    };
    let url = query_url(params);
    let at = http_json::redact_url(&url);
    let mut resp = http_json::send_get(client, &url, auth.as_ref())
        .await
        .map_err(|e| format!("{at}: {e}"))?;
    let failed = (!resp.status().is_success()).then(|| http_json::status_error(&resp));
    let body = http_json::read_body(&mut resp, max_body).await;
    if let Some(status) = failed {
        // Prometheus answers bad queries and overload with a JSON error body; say what it is.
        let detail = body
            .ok()
            .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
            .as_ref()
            .and_then(api_error);
        return Err(match detail {
            Some(d) => format!("{at}: {status}: {d}"),
            None => format!("{at}: {status}"),
        });
    }
    let body = body.map_err(|e| format!("{at}: {e}"))?;
    let doc: Value =
        serde_json::from_slice(&body).map_err(|e| format!("{at}: response is not JSON: {e}"))?;
    reading(&doc).map_err(|e| format!("{at}: {e}"))
}

/// One source's observer state: the debounce of its readings.
pub struct Probe {
    spec: SourceSpec,
    params: PrometheusParams,
    debouncer: Debouncer,
}

impl Probe {
    pub fn new(spec: SourceSpec, params: PrometheusParams) -> Self {
        let debouncer = Debouncer::new(params.reachability_confirmations);
        Probe {
            spec,
            params,
            debouncer,
        }
    }

    /// Reads once and turns the result into a report. A failed read is a failure and
    /// does not count as a reading for the debounce.
    pub async fn poll(&mut self, client: &reqwest::Client, env: &EnvFn) -> Report {
        self.poll_with_limit(client, env, MAX_BODY).await
    }

    /// As [`Probe::poll`], with an explicit cap on the size of the response body.
    pub async fn poll_with_limit(
        &mut self,
        client: &reqwest::Client,
        env: &EnvFn,
        max_body: usize,
    ) -> Report {
        match read(client, &self.params, env, max_body).await {
            Ok(up) => Report::Observed {
                source: self.spec.id.clone(),
                fidelity: Fidelity::Polled,
                values: vec![ObservedValue::ResourceUp {
                    resource: self.params.resource.clone(),
                    up: self.debouncer.check(up),
                }],
            },
            Err(error) => Report::Failed {
                source: self.spec.id.clone(),
                error,
            },
        }
    }
}

/// The HTTP client for a source; like `http_json`'s, it follows no redirects.
pub fn build_client(params: &PrometheusParams) -> Result<reqwest::Client, reqwest::Error> {
    http_json::client_with_timeout(params.timeout)
}

/// Polls forever, every `resync`, until the receiver is gone.
pub async fn run(spec: SourceSpec, params: PrometheusParams, tx: ReportTx) {
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
    let mut probe = Probe::new(spec, params);
    loop {
        ticker.tick().await;
        let report = probe.poll(&client, &env).await;
        if tx.send(report).await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testserver::{Reply, TestServer};
    use crate::{ObserverState, Observers, SpawnOptions, observer_state};
    use sched_model::test_support::{EXAMPLE_CONFIG, example_config};
    use sched_model::{Config, Millis, ResourceId};

    /// The example's instant query, as the adapter sends it.
    const QUERY: &str = "/api/v1/query?query=probe_success%7Binstance%3D%22basestar%22%7D";

    fn example_probe(server: &TestServer) -> (SourceSpec, PrometheusParams) {
        let spec = example_config()
            .source(&"basestar-probe".into())
            .unwrap()
            .clone();
        let mut params = spec.prometheus_params().unwrap().unwrap();
        params.url = server.url("");
        (spec, params)
    }

    /// A Prometheus instant-query success body with one series per value.
    fn vector(values: &[&str]) -> Reply {
        let series: Vec<String> = values
            .iter()
            .enumerate()
            .map(|(i, v)| {
                format!(
                    r#"{{"metric": {{"__name__": "probe_success", "instance": "basestar", "job": "blackbox-{i}"}}, "value": [1700000000.123, "{v}"]}}"#
                )
            })
            .collect();
        Reply::json(&format!(
            r#"{{"status": "success", "data": {{"resultType": "vector", "result": [{}]}}}}"#,
            series.join(", ")
        ))
    }

    /// `body` with HTTP `status`.
    fn error_reply(status: u16, body: &str) -> Reply {
        Reply {
            status,
            ..Reply::json(body)
        }
    }

    fn no_env(_: &str) -> Option<String> {
        None
    }

    fn up_of(r: &Report) -> bool {
        match r {
            Report::Observed {
                source,
                fidelity,
                values,
            } => {
                assert_eq!(source.as_str(), "basestar-probe");
                assert_eq!(*fidelity, Fidelity::Polled);
                match values.as_slice() {
                    [ObservedValue::ResourceUp { resource, up }] => {
                        assert_eq!(resource, &ResourceId::new("basestar-nfs"));
                        *up
                    }
                    v => panic!("{v:?}"),
                }
            }
            Report::Failed { error, .. } => panic!("failed: {error}"),
        }
    }

    fn error_of(r: Report) -> String {
        match r {
            Report::Failed { source, error } => {
                assert_eq!(source.as_str(), "basestar-probe");
                error
            }
            other => panic!("expected a failure, got {other:?}"),
        }
    }

    #[test]
    fn the_query_is_percent_encoded_onto_the_base_url() {
        assert_eq!(
            percent_encode(r#"probe_success{instance="basestar"} == 1"#),
            "probe_success%7Binstance%3D%22basestar%22%7D%20%3D%3D%201"
        );
        assert_eq!(percent_encode("a-b_c.d~e/f&g+h"), "a-b_c.d~e%2Ff%26g%2Bh");
        let mut params = example_config()
            .source(&"basestar-probe".into())
            .unwrap()
            .prometheus_params()
            .unwrap()
            .unwrap();
        assert_eq!(
            query_url(&params),
            format!("http://prometheus.monitoring.svc:9090{QUERY}")
        );
        params.url = "http://prom:9090/".into();
        assert_eq!(query_url(&params), format!("http://prom:9090{QUERY}"));
        params.url = "http://prom:9090/sub".into();
        assert_eq!(query_url(&params), format!("http://prom:9090/sub{QUERY}"));
    }

    #[test]
    fn readings_are_one_series_of_exactly_zero_or_one() {
        let doc = |text: &str| serde_json::from_str::<Value>(text).unwrap();
        let body = |v: &str| {
            doc(&format!(
                r#"{{"status":"success","data":{{"resultType":"vector","result":[{{"metric":{{}},"value":[1.5,{v}]}}]}}}}"#
            ))
        };
        assert_eq!(reading(&body(r#""1""#)), Ok(true));
        assert_eq!(reading(&body(r#""0""#)), Ok(false));
        assert_eq!(reading(&body(r#""1.0""#)), Ok(true));
        for bad in [
            r#""2""#,
            r#""0.5""#,
            r#""-1""#,
            r#""NaN""#,
            r#""+Inf""#,
            r#""up""#,
            r#""""#,
            "1",
            "null",
        ] {
            let e = reading(&body(bad)).unwrap_err();
            assert!(
                e.contains("neither 0 nor 1") || e.contains("no [time"),
                "{bad}: {e}"
            );
        }
        let e = reading(&doc(
            r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{}}]}}"#,
        ))
        .unwrap_err();
        assert!(e.contains("no [time"), "{e}");
        let e = reading(&doc(
            r#"{"status":"success","data":{"resultType":"scalar","result":[1.5,"1"]}}"#,
        ))
        .unwrap_err();
        assert!(e.contains("not \"vector\""), "{e}");
        let e = reading(&doc(
            r#"{"status":"success","data":{"resultType":"matrix","result":[]}}"#,
        ))
        .unwrap_err();
        assert!(e.contains("not \"vector\""), "{e}");
        let e = reading(&doc(r#"{"status":"success"}"#)).unwrap_err();
        assert!(e.contains("no data"), "{e}");
        let e = reading(&doc(r#"{"data":{}}"#)).unwrap_err();
        assert!(e.contains("not \"success\""), "{e}");
        let e = reading(&doc(r#"{"status":"error","errorType":"timeout","error":"query timed out in expression evaluation"}"#)).unwrap_err();
        assert_eq!(
            e,
            "query failed: timeout: query timed out in expression evaluation"
        );
    }

    #[tokio::test]
    async fn up_and_down_are_read_from_the_example_query() {
        let server = TestServer::start().await;
        let (spec, mut params) = example_probe(&server);
        params.reachability_confirmations = 1;
        let client = build_client(&params).unwrap();
        let mut probe = Probe::new(spec, params);
        server.set(QUERY, vector(&["1"]));
        assert!(up_of(&probe.poll(&client, &no_env).await));
        server.set(QUERY, vector(&["0"]));
        assert!(!up_of(&probe.poll(&client, &no_env).await));
        assert_eq!(server.requests()[0].0, QUERY);
        assert!(server.requests().iter().all(|(_, auth)| auth.is_none()));
    }

    #[tokio::test]
    async fn readings_are_debounced_and_failed_reads_are_not_readings() {
        let server = TestServer::start().await;
        let (spec, params) = example_probe(&server);
        assert_eq!(params.reachability_confirmations, 3);
        let client = build_client(&params).unwrap();
        let mut probe = Probe::new(spec, params);
        // The first reading is adopted as is.
        server.set(QUERY, vector(&["1"]));
        assert!(up_of(&probe.poll(&client, &no_env).await));
        server.set(QUERY, vector(&["0"]));
        assert!(up_of(&probe.poll(&client, &no_env).await));
        // A failed read in between neither confirms nor resets the pending "down".
        server.set(QUERY, vector(&[]));
        error_of(probe.poll(&client, &no_env).await);
        server.set(QUERY, vector(&["0"]));
        assert!(up_of(&probe.poll(&client, &no_env).await));
        assert!(!up_of(&probe.poll(&client, &no_env).await));
    }

    #[tokio::test]
    async fn every_unusable_answer_is_a_failed_read_never_down() {
        let server = TestServer::start().await;
        let (spec, mut params) = example_probe(&server);
        let client = build_client(&params).unwrap();
        let mut probe = Probe::new(spec.clone(), params.clone());
        let mut check = async |reply: Reply, needle: &str| {
            server.set(QUERY, reply);
            let e = error_of(probe.poll(&client, &no_env).await);
            assert!(e.contains(needle), "{needle}: {e}");
            // The error names where it came from, without query values.
            assert!(e.contains("/api/v1/query?query=<redacted>"), "{e}");
        };
        check(vector(&[]), "no series").await;
        check(vector(&["1", "1"]), "returned 2 series").await;
        check(vector(&["1", "0", "1"]), "returned 3 series").await;
        check(vector(&["2"]), "neither 0 nor 1").await;
        check(vector(&["0.5"]), "neither 0 nor 1").await;
        check(vector(&["NaN"]), "neither 0 nor 1").await;
        check(vector(&["yes"]), "neither 0 nor 1").await;
        check(
            Reply::json(r#"{"status":"success","data":{"resultType":"scalar","result":[1700000000.1,"1"]}}"#),
            "not \"vector\"",
        )
        .await;
        // Prometheus's error shape: HTTP 400 for a bad query, 422 for an execution error,
        // 503 when overloaded, each with a JSON body naming the error.
        check(
            error_reply(400, r#"{"status":"error","errorType":"bad_data","error":"invalid parameter \"query\": 1:15: parse error: unexpected \"}\""}"#),
            "HTTP 400 Bad Request: bad_data: invalid parameter",
        )
        .await;
        check(
            error_reply(422, r#"{"status":"error","errorType":"execution","error":"query processing would load too many samples into memory"}"#),
            "HTTP 422 Unprocessable Entity: execution",
        )
        .await;
        check(
            error_reply(503, r#"{"status":"error","errorType":"unavailable","error":"too many concurrent queries"}"#),
            "HTTP 503 Service Unavailable: unavailable",
        )
        .await;
        check(Reply::status(502), "HTTP 502").await;
        check(
            Reply::status(429).header("Retry-After", "5"),
            "Retry-After: 5",
        )
        .await;
        // An error body with HTTP 200 is still an error.
        check(
            Reply::json(r#"{"status":"error","errorType":"internal","error":"boom"}"#),
            "query failed: internal: boom",
        )
        .await;
        check(Reply::json("<html>proxy</html>"), "not JSON").await;
        check(Reply::redirect("http://elsewhere/"), "HTTP 302").await;
        // Slower than the timeout.
        server.set(
            QUERY,
            vector(&["1"]).delayed(std::time::Duration::from_millis(800)),
        );
        params.timeout = Millis(100);
        let slow = build_client(&params).unwrap();
        let mut probe = Probe::new(spec.clone(), params.clone());
        assert!(!error_of(probe.poll(&slow, &no_env).await).is_empty());
        // Nothing is listening.
        let dead = TestServer::start().await;
        params.url = dead.url("");
        drop(dead);
        let mut probe = Probe::new(spec, params);
        assert!(!error_of(probe.poll(&client, &no_env).await).is_empty());
    }

    #[tokio::test]
    async fn an_endless_body_is_cut_off() {
        let server = TestServer::start().await;
        let (spec, params) = example_probe(&server);
        let client = build_client(&params).unwrap();
        server.set(QUERY, Reply::endless());
        let mut probe = Probe::new(spec, params);
        let e = error_of(
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                probe.poll_with_limit(&client, &no_env, 100_000),
            )
            .await
            .expect("an endless body must not hang the poll"),
        );
        assert!(e.contains("larger than 100000"), "{e}");
    }

    #[tokio::test]
    async fn auth_is_sent_from_the_environment_and_a_missing_secret_makes_no_request() {
        let server = TestServer::start().await;
        let (spec, mut params) = example_probe(&server);
        params.auth = Some(sched_model::AuthSpec {
            header: "Authorization".into(),
            prefix: "Bearer ".into(),
            env: Some("PROM_TOKEN".into()),
            file: None,
        });
        let client = build_client(&params).unwrap();
        let mut probe = Probe::new(spec, params);
        server.set(QUERY, vector(&["1"]));
        let e = error_of(probe.poll(&client, &no_env).await);
        assert!(e.contains("PROM_TOKEN"), "{e}");
        assert!(server.requests().is_empty());
        let env = |n: &str| (n == "PROM_TOKEN").then(|| "p-tok\n".to_string());
        assert!(up_of(&probe.poll(&client, &env).await));
        assert_eq!(server.requests()[0].1.as_deref(), Some("Bearer p-tok"));
    }

    #[tokio::test]
    async fn the_example_probe_has_a_running_observer_that_reports_reachability() {
        let server = TestServer::start().await;
        server.set(QUERY, vector(&["1"]));
        let spec = example_config()
            .source(&"basestar-probe".into())
            .unwrap()
            .clone();
        assert_eq!(observer_state(&spec), ObserverState::Running);
        let mut no_params = spec.clone();
        no_params.params = Default::default();
        assert_eq!(observer_state(&no_params), ObserverState::NoParams);

        let text = EXAMPLE_CONFIG.replace(
            "url: http://prometheus.monitoring.svc:9090\n",
            &format!("url: {}\n", server.url("")),
        );
        assert_ne!(text, EXAMPLE_CONFIG);
        let config = Config::from_yaml(&text).unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(256);
        let mut observers = Observers::default();
        let started = observers.update(&config, &tx, &SpawnOptions::default());
        assert!(
            started.iter().any(|s| s.as_str() == "basestar-probe"),
            "{started:?}"
        );
        let report = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let r = rx.recv().await.unwrap();
                if r.source().as_str() == "basestar-probe" {
                    return r;
                }
            }
        })
        .await
        .expect("the probe reports");
        assert!(up_of(&report));
    }
}
