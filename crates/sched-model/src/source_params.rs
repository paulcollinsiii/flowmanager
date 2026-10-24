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

/// Keeps only the array elements whose string field matches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ElementFilter {
    /// JSON pointer, relative to one element.
    pub field: String,
    /// Case-insensitive substring match.
    #[serde(default)]
    pub contains: Option<String>,
    /// Exact match.
    #[serde(default)]
    pub equals: Option<String>,
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
    /// Only items changed within a window ending now (see [`SinceSpec`]).
    #[serde(default)]
    pub since: Option<SinceSpec>,
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

impl SourceSpec {
    /// The typed params of a `kubernetes_watch` source; `None` when it has no params.
    pub fn kube_watch_params(&self) -> Result<Option<KubeWatchParams>, String> {
        typed(self, SourceKind::KubernetesWatch)
    }

    /// The typed params of an `http_json` source; `None` when it has no params.
    pub fn http_json_params(&self) -> Result<Option<HttpJsonParams>, String> {
        typed(self, SourceKind::HttpJson)
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
    if p.timeout == Millis::ZERO || p.timeout > MAX_HTTP_TIMEOUT {
        out.push(format!(
            "timeout ({}) must be greater than zero and at most {}",
            p.timeout, MAX_HTTP_TIMEOUT
        ));
    }
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
            out.push(
                "auth: set exactly one of env and file (secrets never go in the config)".into(),
            );
        }
        if a.env.as_deref() == Some("") || a.file.as_deref() == Some("") {
            out.push("auth: env or file must not be empty".to_string());
        }
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
    if p.reachability_confirmations == 0
        || p.reachability_confirmations > MAX_REACHABILITY_CONFIRMATIONS
    {
        out.push(format!(
            "reachability_confirmations ({}) must be between 1 and {MAX_REACHABILITY_CONFIRMATIONS}",
            p.reachability_confirmations
        ));
    }
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

    #[test]
    fn example_sources_have_typed_params() {
        let c = example_config();
        let speakr = c.source(&"speakr-pending".into()).unwrap();
        let p = speakr.http_json_params().unwrap().unwrap();
        assert_eq!(p.requests.len(), 3);
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
        let y = EXAMPLE_CONFIG.replace(
            "    params:\n      requests:\n        - url: http://immich-server.media.svc:2283/api/jobs\n          count: /waiting\n",
            "",
        );
        assert!(Config::from_yaml(&y).is_ok());
    }

    #[test]
    fn rejects_bad_http_params() {
        let url = "url: http://immich-server.media.svc:2283/api/jobs\n          count: /waiting";
        for (replacement, needle) in [
            (
                "url: ftp://x\n          count: /waiting",
                "must start with http",
            ),
            (
                "url: http://x\n          count: waiting",
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
        let url = "url: http://immich-server.media.svc:2283/api/jobs\n          count: /waiting";
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
    fn rejects_bad_since_options() {
        let url = "url: http://immich-server.media.svc:2283/api/jobs\n          count: /waiting";
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
        let ps = problems_of(&EXAMPLE_CONFIG.replace("timeout: 10s\n      auth: { header: Authorization, prefix: \"Bearer \", env: SPEAKR_API_TOKEN }\n      requests:\n        - url: http://speakr.media.svc:8899/api/v1/recordings?status=pending&per_page=1", "timeout: 0\n      requests:\n        - url: http://speakr.media.svc:8899/api/v1/recordings?status=pending&per_page=1"));
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
}
