//! Typed `params` of the adapter kinds `sched-observe` implements. `SourceSpec::params` stays
//! opaque in the schema; these types are what the adapters read, and they are checked when
//! the configuration is loaded so a bad source entry is rejected rather than failing at
//! run time. A source with no `params` at all is accepted: it is not polled and can only be
//! fed by pushes.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_yaml_ng::Value;

use crate::config::{SourceKind, SourceSpec};
use crate::ids::ResourceId;
use crate::time::Millis;

/// Longest accepted `timeout` of an HTTP request.
pub const MAX_HTTP_TIMEOUT: Millis = Millis(300_000);

/// Default number of consecutive agreeing checks before a host's reachability flips.
pub const DEFAULT_REACHABILITY_CONFIRMATIONS: u32 = 3;

/// Largest accepted `reachability_confirmations`.
pub const MAX_REACHABILITY_CONFIRMATIONS: u32 = 100;

/// Params of a `kubernetes_watch` source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KubeWatchParams {
    /// Namespaces whose pods are watched.
    #[serde(default)]
    pub namespaces: Vec<String>,
    /// Pod label whose value names the workload the pod belongs to.
    #[serde(default = "default_workload_label")]
    pub workload_label: String,
    /// Maps a resource to the name of the Kubernetes node that is its host. Needed when the
    /// source declares `reachability`; only listed resources are reported.
    #[serde(default)]
    pub nodes: BTreeMap<ResourceId, String>,
    /// Consecutive agreeing checks (one per resync) before a reachability change is
    /// reported. A single flaky reading never flips a host.
    #[serde(default = "default_confirmations")]
    pub reachability_confirmations: u32,
}

fn default_workload_label() -> String {
    "app.kubernetes.io/name".to_string()
}

fn default_confirmations() -> u32 {
    DEFAULT_REACHABILITY_CONFIRMATIONS
}

/// Which side of a class's demand a request contributes to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CountsAs {
    /// Work waiting to run.
    #[default]
    Ready,
    /// Work already running.
    InFlight,
}

/// A value a `where` or `when` condition compares a field with exactly. An unquoted YAML
/// scalar keeps its type: `equals: false` is the JSON boolean, `equals: 3` the JSON number 3
/// (compared numerically, so `3` matches `3.0`), `equals: FAILED` text. Quote a scalar to
/// compare it as text: `equals: "false"`, `equals: "3"`. The field must have the same JSON
/// type as the value.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum FilterValue {
    Bool(bool),
    /// Always finite (`.nan` and `.inf` are refused when the config is read).
    Number(f64),
    Text(String),
}

// Sound because a `Number` is never NaN (see `Deserialize`), so equality is reflexive.
impl Eq for FilterValue {}

impl FilterValue {
    /// The JSON type this value compares with, by name.
    pub fn json_type(&self) -> &'static str {
        match self {
            FilterValue::Bool(_) => "boolean",
            FilterValue::Number(_) => "number",
            FilterValue::Text(_) => "string",
        }
    }
}

impl<'de> Deserialize<'de> for FilterValue {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl serde::de::Visitor<'_> for V {
            type Value = FilterValue;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a string, a boolean or a number to compare the field with")
            }

            fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<FilterValue, E> {
                Ok(FilterValue::Bool(v))
            }

            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<FilterValue, E> {
                Ok(FilterValue::Number(v as f64))
            }

            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<FilterValue, E> {
                Ok(FilterValue::Number(v as f64))
            }

            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<FilterValue, E> {
                if v.is_finite() {
                    Ok(FilterValue::Number(v))
                } else {
                    Err(E::custom(format!(
                        "{v} cannot be compared with; use a finite number"
                    )))
                }
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<FilterValue, E> {
                Ok(FilterValue::Text(v.to_string()))
            }
        }
        d.deserialize_any(V)
    }
}

impl From<&str> for FilterValue {
    fn from(s: &str) -> Self {
        FilterValue::Text(s.to_string())
    }
}

impl From<bool> for FilterValue {
    fn from(b: bool) -> Self {
        FilterValue::Bool(b)
    }
}

impl From<f64> for FilterValue {
    fn from(n: f64) -> Self {
        FilterValue::Number(n)
    }
}

/// Counts a response only when one of its fields has a given value: when it does not, the
/// request (its own number and every `also` number) contributes zero. A response without
/// the field, or with a value of another JSON type, fails the poll.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponseCondition {
    /// JSON pointer into the response.
    pub field: String,
    pub equals: FilterValue,
}

/// Keeps only the array elements whose field matches. An element whose field holds a
/// non-null value of another JSON type than the filter compares fails the poll (the
/// source's demand becomes unknown) rather than silently not matching; `null` or a missing
/// field is simply no match.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ElementFilter {
    /// JSON pointer, relative to one element.
    pub field: String,
    /// Case-insensitive substring match of a string field.
    #[serde(default)]
    pub contains: Option<String>,
    /// Exact match of a string, boolean or number field (see [`FilterValue`]).
    #[serde(default)]
    pub equals: Option<FilterValue>,
}

/// Bounds a request by age: adds `param=<now - ago>` (ISO 8601, UTC) to its query, so
/// only items changed since then come back. `now` is the scheduler's clock at the poll.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SinceSpec {
    /// Name of the query parameter, for example `updated_since`.
    pub param: String,
    /// How far back from now.
    pub ago: Millis,
}

/// Longest accepted `since.ago`.
pub const MAX_SINCE_AGO: Millis = Millis(365 * 86_400_000);

/// A further number read from the same response as the request's own, so a service that
/// reports several counts in one document needs one request, not several.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtraCount {
    /// JSON pointer to a number in the response.
    pub count: String,
    #[serde(default)]
    pub counts_as: CountsAs,
}

/// One request and how its response becomes a number. Exactly one of `count` and `length`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestSpec {
    pub url: String,
    /// JSON pointer to a number in the response.
    #[serde(default)]
    pub count: Option<String>,
    /// JSON pointer to an array in the response; its (filtered) length is the number.
    #[serde(default)]
    pub length: Option<String>,
    /// With `length`: only count elements matching this.
    #[serde(default, rename = "where")]
    pub filter: Option<ElementFilter>,
    #[serde(default)]
    pub counts_as: CountsAs,
    /// With `length`: the most elements one response can hold (the page size in the URL).
    /// A response that reaches it may be truncated, so the poll fails instead of
    /// under-counting.
    #[serde(default)]
    pub max_items: Option<u64>,
    /// More numbers from the same response, each added to ready or in-flight work.
    #[serde(default)]
    pub also: Vec<ExtraCount>,
    /// Only items changed within a window ending now (see [`SinceSpec`]).
    #[serde(default)]
    pub since: Option<SinceSpec>,
    /// Count this response only if a field of it has a given value (see
    /// [`ResponseCondition`]).
    #[serde(default)]
    pub when: Option<ResponseCondition>,
}

/// Where the value of an authentication header comes from. Never the config itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthSpec {
    pub header: String,
    /// Text put before the secret, for example `"Bearer "` or `"Token "`.
    #[serde(default)]
    pub prefix: String,
    /// Name of an environment variable holding the secret.
    #[serde(default)]
    pub env: Option<String>,
    /// Path of a file holding the secret (surrounding whitespace is trimmed).
    #[serde(default)]
    pub file: Option<String>,
}

/// Params of an `http_json` source. Every request is made on each poll; the numbers of
/// requests with the same `counts_as` are summed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpJsonParams {
    pub requests: Vec<RequestSpec>,
    #[serde(default)]
    pub auth: Option<AuthSpec>,
    #[serde(default = "default_timeout")]
    pub timeout: Millis,
    /// Reserved for the webhook receiver (a later milestone); ignored here.
    #[serde(default)]
    pub webhook: Option<Value>,
}

fn default_timeout() -> Millis {
    Millis(10_000)
}

/// Params of a `prometheus` source: one instant query (`GET <url>/api/v1/query`) whose
/// result says whether one resource's host is up. The query must return exactly one series
/// whose value is `1` (up) or `0` (down); anything else is a failed read, never "down".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrometheusParams {
    /// Base URL of the Prometheus server; the adapter adds `/api/v1/query`.
    pub url: String,
    /// PromQL instant query, for example a blackbox exporter's `probe_success{...}`.
    pub query: String,
    /// The resource whose reachability the query's value is.
    pub resource: ResourceId,
    #[serde(default = "default_timeout")]
    pub timeout: Millis,
    #[serde(default)]
    pub auth: Option<AuthSpec>,
    /// Consecutive agreeing readings (one per resync) before a reported up/down flips, as
    /// for `kubernetes_watch`. Failed reads are not readings.
    #[serde(default = "default_confirmations")]
    pub reachability_confirmations: u32,
}

impl SourceSpec {
    /// The typed params of a `kubernetes_watch` source; `None` when it has no params.
    pub fn kube_watch_params(&self) -> Result<Option<KubeWatchParams>, String> {
        typed(self, SourceKind::KubernetesWatch)
    }

    /// The typed params of an `http_json` source; `None` when it has no params.
    pub fn http_json_params(&self) -> Result<Option<HttpJsonParams>, String> {
        typed(self, SourceKind::HttpJson)
    }

    /// The typed params of a `prometheus` source; `None` when it has no params.
    pub fn prometheus_params(&self) -> Result<Option<PrometheusParams>, String> {
        typed(self, SourceKind::Prometheus)
    }
}

fn typed<T: for<'de> Deserialize<'de>>(
    spec: &SourceSpec,
    kind: SourceKind,
) -> Result<Option<T>, String> {
    if spec.kind != kind {
        return Err(format!("source {} is not of kind {kind:?}", spec.id));
    }
    if spec.params.is_null() {
        return Ok(None);
    }
    serde_yaml_ng::from_value(spec.params.clone())
        .map(Some)
        .map_err(|e| format!("invalid params: {e}"))
}

fn pointer_problem(p: &str) -> Option<String> {
    (!p.is_empty() && !p.starts_with('/'))
        .then(|| format!("JSON pointer {p:?} must be empty or start with '/'"))
}

fn http_problems(p: &HttpJsonParams) -> Vec<String> {
    let mut out = Vec::new();
    if p.requests.is_empty() {
        out.push("requests must not be empty".to_string());
    }
    if !p.requests.iter().any(|r| r.counts_as == CountsAs::Ready) {
        out.push("at least one request must count as ready".to_string());
    }
    out.extend(timeout_problem(p.timeout));
    for (i, r) in p.requests.iter().enumerate() {
        let at = format!("requests[{i}]");
        if !(r.url.starts_with("http://") || r.url.starts_with("https://")) {
            out.push(format!("{at}.url must start with http:// or https://"));
        }
        match (&r.count, &r.length) {
            (Some(_), None) | (None, Some(_)) => {}
            _ => out.push(format!("{at}: set exactly one of count and length")),
        }
        for ptr in [&r.count, &r.length].into_iter().flatten() {
            out.extend(pointer_problem(ptr).map(|m| format!("{at}: {m}")));
        }
        for (k, e) in r.also.iter().enumerate() {
            out.extend(pointer_problem(&e.count).map(|m| format!("{at}.also[{k}]: {m}")));
            // The same number counted twice would silently double it.
            let primary =
                r.count.as_deref() == Some(e.count.as_str()) && r.counts_as == e.counts_as;
            let earlier = r.also[..k]
                .iter()
                .any(|o| o.count == e.count && o.counts_as == e.counts_as);
            if primary || earlier {
                out.push(format!(
                    "{at}.also[{k}] repeats {} (same pointer and counts_as); it would be counted twice",
                    if primary { "the request's own count" } else { "an earlier also entry" }
                ));
            }
        }
        if let Some(sc) = &r.since {
            let plain = !sc.param.is_empty()
                && sc
                    .param
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'));
            if !plain {
                out.push(format!(
                    "{at}.since.param {:?} must be a plain query parameter name",
                    sc.param
                ));
            }
            let query = r.url.split_once('?').map_or("", |(_, q)| q);
            if query
                .split('&')
                .any(|kv| kv.split('=').next() == Some(sc.param.as_str()))
            {
                out.push(format!(
                    "{at}.url already has a {:?} parameter; since would add a second one",
                    sc.param
                ));
            }
            if sc.ago == Millis::ZERO || sc.ago > MAX_SINCE_AGO {
                out.push(format!(
                    "{at}.since.ago ({}) must be greater than zero and at most {}",
                    sc.ago, MAX_SINCE_AGO
                ));
            }
        }
        if r.max_items.is_some() && r.length.is_none() {
            out.push(format!("{at}: max_items is only valid with length"));
        }
        if r.max_items == Some(0) {
            out.push(format!("{at}: max_items must be greater than zero"));
        }
        if let Some(j) = p.requests[..i].iter().position(|earlier| earlier == r) {
            out.push(format!(
                "{at} is identical to requests[{j}]; it would be counted twice"
            ));
        }
        if let Some(w) = &r.when {
            out.extend(pointer_problem(&w.field).map(|m| format!("{at}.when: {m}")));
        }
        if let Some(f) = &r.filter {
            if r.length.is_none() {
                out.push(format!("{at}: where is only valid with length"));
            }
            out.extend(pointer_problem(&f.field).map(|m| format!("{at}.where: {m}")));
            if f.contains.is_some() == f.equals.is_some() {
                out.push(format!(
                    "{at}.where: set exactly one of contains and equals"
                ));
            }
        }
    }
    if let Some(a) = &p.auth {
        out.extend(auth_problems(a));
    }
    out
}

fn timeout_problem(timeout: Millis) -> Option<String> {
    (timeout == Millis::ZERO || timeout > MAX_HTTP_TIMEOUT).then(|| {
        format!("timeout ({timeout}) must be greater than zero and at most {MAX_HTTP_TIMEOUT}")
    })
}

fn confirmations_problem(n: u32) -> Option<String> {
    (n == 0 || n > MAX_REACHABILITY_CONFIRMATIONS).then(|| {
        format!(
            "reachability_confirmations ({n}) must be between 1 and {MAX_REACHABILITY_CONFIRMATIONS}"
        )
    })
}

fn auth_problems(a: &AuthSpec) -> Vec<String> {
    let mut out = Vec::new();
    let token = !a.header.is_empty()
        && a.header
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-');
    if !token {
        out.push(format!(
            "auth.header {:?} must be a plain header name",
            a.header
        ));
    }
    if a.env.is_some() == a.file.is_some() {
        out.push("auth: set exactly one of env and file (secrets never go in the config)".into());
    }
    if a.env.as_deref() == Some("") || a.file.as_deref() == Some("") {
        out.push("auth: env or file must not be empty".to_string());
    }
    out
}

fn prometheus_problems(p: &PrometheusParams, spec: &SourceSpec) -> Vec<String> {
    use crate::config::Capability;
    let mut out = Vec::new();
    if !(p.url.starts_with("http://") || p.url.starts_with("https://")) {
        out.push("url must start with http:// or https://".to_string());
    }
    if p.url.contains(['?', '#']) {
        out.push("url must be the server's base URL, without a query or fragment".to_string());
    }
    if p.url.trim_end_matches('/').ends_with("/api/v1/query") {
        out.push(
            "url must be the server's base URL; the adapter adds /api/v1/query itself".to_string(),
        );
    }
    if p.query.trim().is_empty() {
        out.push("query must not be empty".to_string());
    }
    out.extend(timeout_problem(p.timeout));
    out.extend(confirmations_problem(p.reachability_confirmations));
    if let Some(a) = &p.auth {
        out.extend(auth_problems(a));
    }
    // The observer reads one up/down value; it supplies nothing else.
    if !spec.observe.contains(&Capability::Reachability) {
        out.push(
            "a prometheus source with params reports reachability; observe must include reachability"
                .to_string(),
        );
    }
    for cap in spec
        .observe
        .iter()
        .filter(|c| **c != Capability::Reachability)
    {
        out.push(format!(
            "a prometheus source with params reports reachability only, not {cap}"
        ));
    }
    out
}

fn kube_problems(p: &KubeWatchParams, spec: &SourceSpec) -> Vec<String> {
    use crate::config::Capability;
    let mut out = Vec::new();
    if spec.observe.contains(&Capability::Pods) && p.namespaces.is_empty() {
        out.push("namespaces must not be empty when the source observes pods".to_string());
    }
    if p.namespaces.iter().any(|n| n.trim().is_empty()) {
        out.push("namespaces must not contain empty names".to_string());
    }
    if p.workload_label.trim().is_empty() {
        out.push("workload_label must not be empty".to_string());
    }
    if spec.observe.contains(&Capability::Reachability) && p.nodes.is_empty() {
        out.push(
            "nodes must map at least one resource to a node when the source observes reachability"
                .to_string(),
        );
    }
    if p.nodes.values().any(|n| n.trim().is_empty()) {
        out.push("nodes must not contain empty node names".to_string());
    }
    out.extend(confirmations_problem(p.reachability_confirmations));
    out
}

/// Problems with the params of `spec`, each as a message; empty when valid. Kinds without
/// typed params are not checked.
pub(crate) fn problems(spec: &SourceSpec) -> Vec<String> {
    match spec.kind {
        SourceKind::HttpJson => match spec.http_json_params() {
            Ok(Some(p)) => http_problems(&p),
            Ok(None) => Vec::new(),
            Err(e) => vec![e],
        },
        SourceKind::KubernetesWatch => match spec.kube_watch_params() {
            Ok(Some(p)) => kube_problems(&p, spec),
            Ok(None) => Vec::new(),
            Err(e) => vec![e],
        },
        SourceKind::Prometheus => match spec.prometheus_params() {
            Ok(Some(p)) => prometheus_problems(&p, spec),
            Ok(None) => Vec::new(),
            Err(e) => vec![e],
        },
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{Config, ConfigError};
    use crate::test_support::{EXAMPLE_CONFIG, example_config};

    fn problems_of(yaml: &str) -> Vec<String> {
        match Config::from_yaml(yaml) {
            Err(ConfigError::Invalid(ps)) => ps.iter().map(|p| p.to_string()).collect(),
            other => panic!("expected validation problems, got {other:?}"),
        }
    }

    fn has(ps: &[String], needle: &str) -> bool {
        ps.iter().any(|p| p.contains(needle))
    }

    /// The single request of the example's `paperless-post-ocr`: replacing it changes one
    /// request of one source.
    const ANCHOR: &str = "url: http://paperless-ngx.docs.svc:8000/api/documents/?tags__name__iexact=post-ocr&page_size=1\n          count: /count";

    #[test]
    fn example_sources_have_typed_params() {
        let c = example_config();
        let speakr = c.source(&"speakr-pending".into()).unwrap();
        let p = speakr.http_json_params().unwrap().unwrap();
        assert_eq!(p.requests.len(), 2);
        assert_eq!(p.requests[0].also.len(), 2);
        // Failed transcriptions: a windowed list of FAILED recordings without a summary.
        let failed = &p.requests[1];
        assert_eq!(failed.length.as_deref(), Some("/recordings"));
        assert_eq!(
            failed.filter,
            Some(super::ElementFilter {
                field: "/has_summary".into(),
                contains: None,
                equals: Some(super::FilterValue::Bool(false)),
            })
        );
        assert_eq!(failed.max_items, Some(100));
        assert!(failed.url.contains("per_page=100") && failed.url.contains("status=failed"));
        let since = failed.since.as_ref().unwrap();
        assert_eq!(
            (since.param.as_str(), since.ago),
            ("updated_since", crate::Millis(15 * 60_000))
        );
        let immich = c
            .source(&"immich-queues".into())
            .unwrap()
            .http_json_params()
            .unwrap()
            .unwrap();
        let auth = immich.auth.as_ref().unwrap();
        assert_eq!(
            (
                auth.header.as_str(),
                auth.prefix.as_str(),
                auth.env.as_deref()
            ),
            ("x-api-key", "", Some("IMMICH_API_KEY"))
        );
        let queues: Vec<&str> = immich
            .requests
            .iter()
            .map(|r| r.url.rsplit('/').next().unwrap())
            .collect();
        assert_eq!(queues, ["smartSearch", "faceDetection", "ocr"]);
        for r in &immich.requests {
            assert_eq!(
                r.when,
                Some(super::ResponseCondition {
                    field: "/isPaused".into(),
                    equals: super::FilterValue::Bool(false),
                })
            );
            assert_eq!(r.count.as_deref(), Some("/statistics/waiting"));
            assert_eq!(r.counts_as, super::CountsAs::Ready);
            let also: Vec<_> = r
                .also
                .iter()
                .map(|e| (e.count.as_str(), e.counts_as))
                .collect();
            assert_eq!(
                also,
                [
                    ("/statistics/delayed", super::CountsAs::Ready),
                    ("/statistics/active", super::CountsAs::InFlight)
                ]
            );
        }
        let probe = c
            .source(&"basestar-probe".into())
            .unwrap()
            .prometheus_params()
            .unwrap()
            .unwrap();
        assert_eq!(probe.resource.as_str(), "basestar-nfs");
        assert_eq!(probe.url, "http://prometheus.monitoring.svc:9090");
        assert_eq!(probe.query, "probe_success{instance=\"basestar\"}");
        assert_eq!(probe.timeout, crate::Millis(5_000));
        assert_eq!(probe.reachability_confirmations, 3);
        assert!(probe.auth.is_none());
        assert_eq!(
            p.auth.as_ref().unwrap().env.as_deref(),
            Some("SPEAKR_API_TOKEN")
        );
        let k = c
            .source(&"cluster-pods".into())
            .unwrap()
            .kube_watch_params()
            .unwrap()
            .unwrap();
        assert_eq!(k.reachability_confirmations, 3);
        assert_eq!(k.nodes.len(), 1);
    }

    #[test]
    fn sources_without_params_are_accepted() {
        let start = EXAMPLE_CONFIG.find("  - id: immich-queues\n").unwrap();
        let params = start + EXAMPLE_CONFIG[start..].find("    params:\n").unwrap();
        let end = params + EXAMPLE_CONFIG[params..].find("\n\n").unwrap() + 1;
        let y = format!("{}{}", &EXAMPLE_CONFIG[..params], &EXAMPLE_CONFIG[end..]);
        assert!(y.contains("  - id: immich-queues\n") && !y.contains("url: http://immich-server"));
        assert!(Config::from_yaml(&y).is_ok());
    }

    #[test]
    fn rejects_bad_http_params() {
        let url = ANCHOR;
        for (replacement, needle) in [
            (
                "url: ftp://x\n          count: /waiting",
                "must start with http",
            ),
            (
                "url: http://x\n          count: count",
                "must be empty or start with '/'",
            ),
            ("url: http://x", "exactly one of count and length"),
            (
                "url: http://x\n          count: /a\n          length: /b",
                "exactly one of count and length",
            ),
            (
                "url: http://x\n          count: /a\n          where: { field: /f, contains: x }",
                "where is only valid with length",
            ),
            (
                "url: http://x\n          length: /a\n          where: { field: /f }",
                "exactly one of contains and equals",
            ),
            (
                "url: http://x\n          count: /a\n          counts_as: in_flight",
                "at least one request must count as ready",
            ),
        ] {
            let ps = problems_of(&EXAMPLE_CONFIG.replace(url, replacement));
            assert!(has(&ps, needle), "{needle}: {ps:?}");
        }
    }

    #[test]
    fn rejects_duplicate_identical_requests_and_bad_max_items() {
        let url = ANCHOR;
        let dup = format!("{url}\n        - {url}");
        let ps = problems_of(&EXAMPLE_CONFIG.replace(url, &dup));
        assert!(
            has(&ps, "requests[1] is identical to requests[0]"),
            "{ps:?}"
        );
        let ps = problems_of(&EXAMPLE_CONFIG.replace(
            url,
            "url: http://x\n          count: /a\n          max_items: 5",
        ));
        assert!(has(&ps, "max_items is only valid with length"), "{ps:?}");
        let ps = problems_of(&EXAMPLE_CONFIG.replace(
            url,
            "url: http://x\n          length: /a\n          max_items: 0",
        ));
        assert!(has(&ps, "max_items must be greater than zero"), "{ps:?}");
    }

    #[test]
    fn extra_counts_validate_and_duplicates_compare_the_whole_rule() {
        let url = ANCHOR;
        let with = |extra: &str| EXAMPLE_CONFIG.replace(url, &format!("{url}\n{extra}"));
        let ok = with("          also: [{ count: /active, counts_as: in_flight }]");
        assert!(Config::from_yaml(&ok).is_ok());
        let ps = problems_of(&with("          also: [{ count: active }]"));
        assert!(has(&ps, "also[0]"), "{ps:?}");
        // The same URL twice with different rules is two different requests...
        let two = EXAMPLE_CONFIG.replace(
            url,
            &format!("{url}\n        - {url}\n          counts_as: in_flight"),
        );
        assert!(Config::from_yaml(&two).is_ok());
        // ...and a request differing only in its extra counts is not a duplicate either.
        let three = EXAMPLE_CONFIG.replace(
            url,
            &format!("{url}\n        - {url}\n          also: [{{ count: /active }}]"),
        );
        assert!(Config::from_yaml(&three).is_ok());
    }

    #[test]
    fn rejects_an_also_entry_that_repeats_a_number_and_a_since_param_already_in_the_url() {
        let url = ANCHOR;
        let with = |extra: &str| EXAMPLE_CONFIG.replace(url, &format!("{url}\n{extra}"));
        let ps = problems_of(&with("          also: [{ count: /count }]"));
        assert!(
            has(&ps, "also[0] repeats the request's own count"),
            "{ps:?}"
        );
        let ps = problems_of(&with("          also: [{ count: /a }, { count: /a }]"));
        assert!(has(&ps, "also[1] repeats an earlier also entry"), "{ps:?}");
        // Same pointer but counted on the other side is a different number being read.
        let ok = with("          also: [{ count: /count, counts_as: in_flight }]");
        assert!(Config::from_yaml(&ok).is_ok());
        let dup_since = EXAMPLE_CONFIG.replace(
            ANCHOR,
            "url: http://paperless-ngx.docs.svc:8000/api/documents/?a=1&updated_since=x\n          count: /count\n          since: { param: updated_since, ago: 1h }",
        );
        let ps = problems_of(&dup_since);
        assert!(
            has(&ps, "already has a \"updated_since\" parameter"),
            "{ps:?}"
        );
        // A parameter that merely shares a prefix is fine.
        let ok = dup_since.replace("updated_since=x", "updated_since_x=x");
        assert!(Config::from_yaml(&ok).is_ok());
    }

    #[test]
    fn rejects_bad_since_options() {
        let url = ANCHOR;
        let with =
            |since: &str| EXAMPLE_CONFIG.replace(url, &format!("{url}\n          since: {since}"));
        assert!(Config::from_yaml(&with("{ param: updated_since, ago: 24h }")).is_ok());
        let ps = problems_of(&with("{ param: updated_since, ago: 0 }"));
        assert!(
            has(&ps, "since.ago (0ms) must be greater than zero"),
            "{ps:?}"
        );
        let ps = problems_of(&with("{ param: updated_since, ago: 400d }"));
        assert!(has(&ps, "since.ago"), "{ps:?}");
        let ps = problems_of(&with("{ param: \"a=b&c\", ago: 1h }"));
        assert!(has(&ps, "plain query parameter name"), "{ps:?}");
        let ps = problems_of(&with("{ param: \"\", ago: 1h }"));
        assert!(has(&ps, "plain query parameter name"), "{ps:?}");
    }

    #[test]
    fn rejects_secrets_in_config_shape_and_unknown_fields() {
        let auth = r#"auth: { header: Authorization, prefix: "Bearer ", env: SPEAKR_API_TOKEN }"#;
        let ps = problems_of(&EXAMPLE_CONFIG.replace(auth, "auth: { header: Authorization }"));
        assert!(has(&ps, "exactly one of env and file"), "{ps:?}");
        let ps = problems_of(
            &EXAMPLE_CONFIG.replace(auth, "auth: { header: Authorization, token: abc, env: X }"),
        );
        assert!(has(&ps, "unknown field `token`"), "{ps:?}");
        let ps = problems_of(&EXAMPLE_CONFIG.replace("timeout: 10s\n      auth: { header: Authorization, prefix: \"Bearer \", env: SPEAKR_API_TOKEN }\n      requests:\n        - url: http://speakr.media.svc:8899/api/v1/stats", "timeout: 0\n      requests:\n        - url: http://speakr.media.svc:8899/api/v1/stats"));
        assert!(has(&ps, "timeout"), "{ps:?}");
    }

    #[test]
    fn rejects_bad_kube_params() {
        let ps = problems_of(&EXAMPLE_CONFIG.replace(
            "reachability_confirmations: 3",
            "reachability_confirmations: 0",
        ));
        assert!(has(&ps, "reachability_confirmations (0)"), "{ps:?}");
        let ps = problems_of(&EXAMPLE_CONFIG.replace(
            "nodes: { cuda-central: cuda-central }",
            "nodes: { ghost: n1 }",
        ));
        assert!(has(&ps, "unknown resource \"ghost\""), "{ps:?}");
        let ps = problems_of(
            &EXAMPLE_CONFIG.replace("nodes: { cuda-central: cuda-central }", "nodes: {}"),
        );
        assert!(has(&ps, "nodes must map"), "{ps:?}");
        let ps =
            problems_of(&EXAMPLE_CONFIG.replace("namespaces: [ai, media, docs]", "namespaces: []"));
        assert!(has(&ps, "namespaces must not be empty"), "{ps:?}");
    }

    #[test]
    fn where_equals_takes_text_or_a_boolean_and_quoted_booleans_stay_text() {
        let failed = "where: { field: /has_summary, equals: false }";
        assert!(EXAMPLE_CONFIG.contains(failed));
        let source = |y: &str| {
            Config::from_yaml(y)
                .unwrap()
                .source(&"speakr-pending".into())
                .unwrap()
                .http_json_params()
                .unwrap()
                .unwrap()
                .requests[1]
                .filter
                .clone()
                .unwrap()
                .equals
        };
        assert_eq!(
            source(EXAMPLE_CONFIG),
            Some(super::FilterValue::Bool(false))
        );
        let quoted =
            EXAMPLE_CONFIG.replace(failed, "where: { field: /has_summary, equals: \"false\" }");
        assert_eq!(source(&quoted), Some("false".into()));
        let text = EXAMPLE_CONFIG.replace(failed, "where: { field: /status, equals: FAILED }");
        assert_eq!(source(&text), Some("FAILED".into()));
        // `contains` is a substring match of text only.
        assert!(matches!(
            Config::from_yaml(
                &EXAMPLE_CONFIG.replace(failed, "where: { field: /has_summary, contains: true }")
            ),
            Err(ConfigError::Parse(_) | ConfigError::Invalid(_))
        ));
        let ps = problems_of(&EXAMPLE_CONFIG.replace(
            failed,
            "where: { field: /has_summary, equals: false, contains: x }",
        ));
        assert!(has(&ps, "exactly one of contains and equals"), "{ps:?}");
    }

    /// The example's basestar-probe params, as one line each.
    const PROBE: &str = "      url: http://prometheus.monitoring.svc:9090\n      query: probe_success{instance=\"basestar\"}\n      resource: basestar-nfs\n      timeout: 5s\n";

    fn probe_with(from: &str, to: &str) -> String {
        assert!(EXAMPLE_CONFIG.contains(PROBE));
        EXAMPLE_CONFIG.replace(PROBE, &PROBE.replace(from, to))
    }

    #[test]
    fn rejects_bad_prometheus_params() {
        for (from, to, needle) in [
            (
                "url: http://prometheus",
                "url: ftp://prometheus",
                "url must start with http",
            ),
            (
                ":9090\n",
                ":9090/api/v1/query\n",
                "the adapter adds /api/v1/query",
            ),
            (":9090\n", ":9090/?x=1\n", "without a query or fragment"),
            (
                "probe_success{instance=\"basestar\"}",
                "\"  \"",
                "query must not be empty",
            ),
            ("timeout: 5s", "timeout: 0", "timeout (0ms)"),
            ("timeout: 5s", "timeout: 10m", "at most"),
            (
                "resource: basestar-nfs",
                "resource: ghost",
                "params.resource: unknown resource \"ghost\"",
            ),
            (
                "timeout: 5s",
                "timeout: 5s\n      reachability_confirmations: 0",
                "reachability_confirmations (0)",
            ),
            (
                "timeout: 5s",
                "timeout: 5s\n      auth: { header: Authorization }",
                "exactly one of env and file",
            ),
            (
                "timeout: 5s",
                "timeout: 5s\n      auth: { header: \"X Bad\", env: T }",
                "plain header name",
            ),
            (
                "timeout: 5s",
                "timeout: 5s\n      step: 1m",
                "unknown field `step`",
            ),
        ] {
            // reachability_confirmations already appears once in the example's params.
            let y = probe_with(from, to).replace(
                "      reachability_confirmations: 0\n      # Consecutive agreeing readings before up/down flips (as for cluster-pods).\n      reachability_confirmations: 3\n",
                "      reachability_confirmations: 0\n",
            );
            let ps = problems_of(&y);
            assert!(has(&ps, needle), "{needle}: {ps:?}");
        }
        // The resource is required.
        let ps = problems_of(&probe_with("      resource: basestar-nfs\n", ""));
        assert!(has(&ps, "missing field `resource`"), "{ps:?}");
    }

    #[test]
    fn prometheus_params_accept_auth_and_defaults_and_report_only_reachability() {
        let y = probe_with(
            "timeout: 5s\n",
            "auth: { header: Authorization, prefix: \"Bearer \", env: PROM_TOKEN }\n",
        );
        let c = Config::from_yaml(&y).unwrap();
        let p = c
            .source(&"basestar-probe".into())
            .unwrap()
            .prometheus_params()
            .unwrap()
            .unwrap();
        assert_eq!(p.timeout, crate::Millis(10_000));
        assert_eq!(p.auth.unwrap().env.as_deref(), Some("PROM_TOKEN"));
        // With params, the source must declare reachability and nothing else.
        let probe_head =
            "  - id: basestar-probe\n    kind: prometheus\n    observe: [reachability]\n";
        assert!(EXAMPLE_CONFIG.contains(probe_head));
        let ps = problems_of(&EXAMPLE_CONFIG.replace(
            probe_head,
            "  - id: basestar-probe\n    kind: prometheus\n    observe: [reachability, demand]\n",
        ));
        assert!(has(&ps, "reachability only, not demand"), "{ps:?}");
        let ps = problems_of(&EXAMPLE_CONFIG.replace(
            probe_head,
            "  - id: basestar-probe\n    kind: prometheus\n    observe: [demand]\n",
        ));
        assert!(has(&ps, "observe must include reachability"), "{ps:?}");
    }

    #[test]
    fn where_equals_numbers_are_numbers_and_unusable_values_say_what_is_expected() {
        let failed = "where: { field: /has_summary, equals: false }";
        let equals = |v: &str| {
            Config::from_yaml(
                &EXAMPLE_CONFIG.replace(failed, &format!("where: {{ field: /n, equals: {v} }}")),
            )
            .map(|c| {
                c.source(&"speakr-pending".into())
                    .unwrap()
                    .http_json_params()
                    .unwrap()
                    .unwrap()
                    .requests[1]
                    .filter
                    .clone()
                    .unwrap()
                    .equals
                    .unwrap()
            })
        };
        assert_eq!(equals("123").unwrap(), super::FilterValue::Number(123.0));
        assert_eq!(equals("-4").unwrap(), super::FilterValue::Number(-4.0));
        assert_eq!(equals("1.5").unwrap(), super::FilterValue::Number(1.5));
        assert_eq!(equals("\"123\"").unwrap(), "123".into());
        assert_eq!(equals("\"true\"").unwrap(), "true".into());
        assert_eq!(equals("true").unwrap(), true.into());
        // `~` (null) is no value at all.
        let e = equals("~").unwrap_err().to_string();
        assert!(e.contains("exactly one of contains and equals"), "{e}");
        for bad in ["[1, 2]", "{ a: 1 }", ".nan", ".inf"] {
            let e = equals(bad).unwrap_err().to_string();
            assert!(
                e.contains("a string, a boolean or a number") || e.contains("finite number"),
                "{bad}: {e}"
            );
            assert!(!e.contains("untagged"), "{bad}: {e}");
        }
    }

    #[test]
    fn when_conditions_are_validated() {
        let when = "when: { field: /isPaused, equals: false }";
        assert_eq!(EXAMPLE_CONFIG.matches(when).count(), 3);
        let ps = problems_of(&EXAMPLE_CONFIG.replacen(
            when,
            "when: { field: isPaused, equals: false }",
            1,
        ));
        assert!(has(&ps, ".when: JSON pointer \"isPaused\""), "{ps:?}");
        let ps = problems_of(&EXAMPLE_CONFIG.replacen(when, "when: { field: /isPaused }", 1));
        assert!(has(&ps, "missing field `equals`"), "{ps:?}");
        let ps = problems_of(&EXAMPLE_CONFIG.replacen(
            when,
            "when: { field: /isPaused, contains: x, equals: false }",
            1,
        ));
        assert!(has(&ps, "unknown field `contains`"), "{ps:?}");
        let text = EXAMPLE_CONFIG.replacen(when, "when: { field: /state, equals: active }", 1);
        assert!(Config::from_yaml(&text).is_ok());
    }
}
