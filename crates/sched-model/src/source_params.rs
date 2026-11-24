//! Typed `params` of the adapter kinds `sched-observe` implements. `SourceSpec::params` stays
//! opaque in the schema; these types are what the adapters read, and they are checked when
//! the configuration is loaded so a bad source entry is rejected rather than failing at
//! run time. A source with no `params` at all is accepted: it is not polled and can only be
//! fed by pushes.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::config::{SourceKind, SourceSpec};
use crate::ids::ResourceId;
use crate::observation::SwitchState;
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
    /// For the `mounts` capability: per resource, the names or addresses of its NFS server
    /// as they appear in pod volumes and in PersistentVolumes (`spec.nfs.server`, or the
    /// `server` attribute of an NFS CSI volume). A pod mounts the resource if one of its
    /// volumes, directly or through a PersistentVolumeClaim, is an NFS volume of one of
    /// these servers (compared ignoring case). List both the name and the address if
    /// volumes use both.
    #[serde(default)]
    pub nfs_servers: BTreeMap<ResourceId, Vec<String>>,
    /// For the `jobs` capability: the namespaces whose Jobs are listed (those flowmanager
    /// created for its scheduled jobs, found by label). Each scheduled job's CronJob must be
    /// in one of them.
    #[serde(default)]
    pub job_namespaces: Vec<String>,
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
    /// Work that failed and waits to be re-queued (by the class's recovery action). It is
    /// not ready work: it never ages into starvation priority.
    Failed,
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
/// field is simply no match. Exactly one of `contains`, `equals` and `is_null` is set.
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
    /// `true`: matches an element whose field is present and JSON `null`. A field of any
    /// other type is no match (never a type error). `false` is refused.
    #[serde(default)]
    pub is_null: Option<bool>,
}

/// The `where` of a request or recovery list: one filter, or a list of filters that must
/// all match. Written as either `where: {field: ..}` or `where: [{field: ..}, ..]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElementFilters(pub Vec<ElementFilter>);

impl From<ElementFilter> for ElementFilters {
    fn from(f: ElementFilter) -> Self {
        ElementFilters(vec![f])
    }
}

impl Serialize for ElementFilters {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self.0.as_slice() {
            [one] => one.serialize(s),
            many => many.serialize(s),
        }
    }
}

impl<'de> Deserialize<'de> for ElementFilters {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // A visitor rather than an untagged enum, so a bad filter keeps its own message.
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = ElementFilters;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a filter ({ field, contains | equals | is_null }) or a list of them")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                m: A,
            ) -> Result<Self::Value, A::Error> {
                let f =
                    ElementFilter::deserialize(serde::de::value::MapAccessDeserializer::new(m))?;
                Ok(ElementFilters(vec![f]))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut s: A,
            ) -> Result<Self::Value, A::Error> {
                let mut v = Vec::new();
                while let Some(f) = s.next_element::<ElementFilter>()? {
                    v.push(f);
                }
                Ok(ElementFilters(v))
            }
        }
        d.deserialize_any(V)
    }
}

/// Load-time problems of a `where`, each prefixed with `at` (for example `list.where`). A
/// list entry is indexed (`at[1]`).
pub(crate) fn where_problems(at: &str, w: &ElementFilters) -> Vec<String> {
    let mut out = Vec::new();
    if w.0.is_empty() {
        out.push(format!("{at}: the list is empty; give at least one filter"));
    }
    for (i, f) in w.0.iter().enumerate() {
        let at = if w.0.len() == 1 {
            at.to_string()
        } else {
            format!("{at}[{i}]")
        };
        out.extend(pointer_problem(&f.field).map(|m| format!("{at}: {m}")));
        let set = usize::from(f.contains.is_some())
            + usize::from(f.equals.is_some())
            + usize::from(f.is_null.is_some());
        if set != 1 {
            out.push(format!(
                "{at}: set exactly one of contains, equals and is_null"
            ));
        }
        if f.is_null == Some(false) {
            out.push(format!(
                "{at}: is_null must be true (false has no meaning; use equals or contains)"
            ));
        }
    }
    out
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

/// What a `map` entry turns a response string into: a switch state (a switch source) or a
/// number (a demand count). Anything else is refused when the config is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum MapValue {
    Switch(SwitchState),
    Number(u64),
}

impl<'de> Deserialize<'de> for MapValue {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl serde::de::Visitor<'_> for V {
            type Value = MapValue;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a switch state (on, off or off_now) or a non-negative whole number")
            }

            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<MapValue, E> {
                Ok(MapValue::Number(v))
            }

            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<MapValue, E> {
                u64::try_from(v)
                    .map(MapValue::Number)
                    .map_err(|_| E::custom(format!("{v} is negative; use a non-negative number")))
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<MapValue, E> {
                match v {
                    "on" => Ok(MapValue::Switch(SwitchState::On)),
                    "off" => Ok(MapValue::Switch(SwitchState::Off)),
                    "off_now" => Ok(MapValue::Switch(SwitchState::OffNow)),
                    other => Err(E::custom(format!(
                        "{other:?} is not a switch state; use on, off or off_now"
                    ))),
                }
            }
        }
        d.deserialize_any(V)
    }
}

/// A map from the text of a response value (a JSON string's content, or `true` / `false`
/// for a boolean) to what it means. A value not in it fails the poll.
pub type ValueMap = BTreeMap<String, MapValue>;

/// A further number read from the same response as the request's own, so a service that
/// reports several counts in one document needs one request, not several.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtraCount {
    /// JSON pointer to a number in the response (or, with `map`, to a string or boolean).
    pub count: String,
    /// Turns the value at `count` from text into a number (values must be numbers).
    #[serde(default)]
    pub map: Option<ValueMap>,
    #[serde(default)]
    pub counts_as: CountsAs,
}

/// One request and how its response becomes a number. Exactly one of `count` and `length`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestSpec {
    pub url: String,
    /// JSON pointer to a number in the response (or, with `map`, to a string or boolean).
    #[serde(default)]
    pub count: Option<String>,
    /// JSON pointer to a string or boolean in the response that a switch source reads (a
    /// request of a switch source has `value` and `map` instead of `count` or `length`).
    #[serde(default)]
    pub value: Option<String>,
    /// Turns the text at `value` into a switch state (the values must be `on`, `off` or
    /// `off_now`), or the text at `count` into a number (the values must be numbers). A
    /// value not in it, or of another JSON type than string or boolean, fails the poll.
    #[serde(default)]
    pub map: Option<ValueMap>,
    /// JSON pointer to an array in the response; its (filtered) length is the number.
    #[serde(default)]
    pub length: Option<String>,
    /// With `length`: only count elements matching this.
    #[serde(default, rename = "where")]
    pub filter: Option<ElementFilters>,
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
    /// The path of the webhook listener that means "poll this source now" (see
    /// `WebhookParams`). Without it the source is polled only at its `resync`.
    #[serde(default)]
    pub webhook: Option<WebhookParams>,
}

/// The `webhook:` setting of an `http_json` source: an inbound request on `path` of the
/// webhook listener asks for this source to be polled now. The request's body is ignored.
/// Several sources may share a path; one request then nudges all of them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebhookParams {
    pub path: String,
}

/// Why `path` cannot be a webhook path, if it cannot. The listener matches the request path
/// exactly, so the accepted alphabet needs no decoding: letters, digits and `-._~/`.
fn webhook_path_problem(path: &str) -> Option<String> {
    if !path.starts_with('/') || path.len() < 2 {
        return Some("webhook.path must start with / and name something after it".to_string());
    }
    if path.ends_with('/') || path.contains("//") {
        return Some("webhook.path must not end with / or contain //".to_string());
    }
    if let Some(c) = path
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || "-._~/".contains(*c)))
    {
        return Some(format!(
            "webhook.path may use only letters, digits and -._~/ (found {c:?})"
        ));
    }
    None
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

/// Default `timeout` of a TCP connect probe.
pub const DEFAULT_TCP_PROBE_TIMEOUT: Millis = Millis(2_000);

/// Shortest accepted `timeout` of a TCP connect probe. Durations without a unit are
/// milliseconds, so this also refuses `timeout: 2` written to mean two seconds.
pub const MIN_TCP_PROBE_TIMEOUT: Millis = Millis(100);

/// Longest accepted `timeout` of a TCP connect probe.
pub const MAX_TCP_PROBE_TIMEOUT: Millis = Millis(60_000);

/// Params of a `tcp_probe` source: one TCP connect to `host:port` per resync says whether
/// one resource's host is up. A connection is up; a refused connection, no answer within
/// `timeout`, or "no route to host" (the host does not answer) is down. Anything else (the
/// name does not resolve, "network unreachable", an unusable address, any other error) is a
/// failed read, never "down".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TcpProbeParams {
    /// Host name or IP address (an IPv6 address without brackets).
    pub host: String,
    pub port: u16,
    /// The resource whose reachability the connect result is.
    pub resource: ResourceId,
    /// How long one probe may take, name lookup and connect together.
    #[serde(default = "default_tcp_probe_timeout")]
    pub timeout: Millis,
    /// Consecutive agreeing readings (one per resync) before a reported up/down flips, as
    /// for `kubernetes_watch`. Failed reads are not readings.
    #[serde(default = "default_confirmations")]
    pub reachability_confirmations: u32,
}

fn default_tcp_probe_timeout() -> Millis {
    DEFAULT_TCP_PROBE_TIMEOUT
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

    /// The typed params of a `tcp_probe` source; `None` when it has no params.
    pub fn tcp_probe_params(&self) -> Result<Option<TcpProbeParams>, String> {
        typed(self, SourceKind::TcpProbe)
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

pub(crate) fn pointer_problem(p: &str) -> Option<String> {
    (!p.is_empty() && !p.starts_with('/'))
        .then(|| format!("JSON pointer {p:?} must be empty or start with '/'"))
}

/// Problems with a `map`: every value must be a switch state (`switch`) or a number.
fn map_problems(map: &ValueMap, switch: bool, at: &str) -> Vec<String> {
    let mut out = Vec::new();
    if map.is_empty() {
        out.push(format!("{at}.map must not be empty"));
    }
    for (k, v) in map {
        match (switch, v) {
            (true, MapValue::Switch(_)) | (false, MapValue::Number(_)) => {}
            (true, MapValue::Number(_)) => out.push(format!(
                "{at}.map[{k:?}] must be a switch state (on, off or off_now), not a number"
            )),
            (false, MapValue::Switch(_)) => out.push(format!(
                "{at}.map[{k:?}] must be a non-negative number, not a switch state"
            )),
        }
    }
    out
}

/// The rules for a source that observes `switch`: it reads one value and nothing else.
fn switch_source_problems(p: &HttpJsonParams, spec: &SourceSpec) -> Vec<String> {
    use crate::config::Capability;
    let mut out = Vec::new();
    if let Some(other) = spec.observe.iter().find(|c| **c != Capability::Switch) {
        out.push(format!(
            "a source that observes switch must observe nothing else (it also declares {other})"
        ));
    }
    if p.requests.len() != 1 {
        out.push(format!(
            "a source that observes switch must have exactly one request (it has {})",
            p.requests.len()
        ));
    }
    for (i, r) in p.requests.iter().enumerate() {
        if r.value.is_none() || r.map.is_none() {
            out.push(format!(
                "requests[{i}]: a switch source's request needs value and map"
            ));
        }
    }
    out
}

fn http_problems(p: &HttpJsonParams, spec: &SourceSpec) -> Vec<String> {
    use crate::config::Capability;
    let switch_source = spec.observe.contains(&Capability::Switch);
    let mut out = Vec::new();
    if p.requests.is_empty() {
        out.push("requests must not be empty".to_string());
    }
    if switch_source {
        out.extend(switch_source_problems(p, spec));
    } else if !p.requests.iter().any(|r| r.counts_as == CountsAs::Ready) {
        out.push("at least one request must count as ready".to_string());
    }
    out.extend(timeout_problem(p.timeout));
    out.extend(
        p.webhook
            .iter()
            .filter_map(|w| webhook_path_problem(&w.path)),
    );
    for (i, r) in p.requests.iter().enumerate() {
        let at = format!("requests[{i}]");
        if !(r.url.starts_with("http://") || r.url.starts_with("https://")) {
            out.push(format!("{at}.url must start with http:// or https://"));
        }
        if let Some(v) = &r.value {
            if !switch_source {
                out.push(format!(
                    "{at}: value is only valid on a source that observes switch"
                ));
            }
            out.extend(pointer_problem(v).map(|m| format!("{at}: {m}")));
            if r.count.is_some()
                || r.length.is_some()
                || r.filter.is_some()
                || !r.also.is_empty()
                || r.max_items.is_some()
                || r.when.is_some()
            {
                out.push(format!(
                    "{at}: a request with value takes only url, value and map (not count, length, where, max_items, also or when)"
                ));
            }
            match &r.map {
                None => out.push(format!("{at}: value needs a map")),
                Some(m) => out.extend(map_problems(m, true, &at)),
            }
        } else {
            match (&r.count, &r.length) {
                (Some(_), None) | (None, Some(_)) => {}
                _ => out.push(format!("{at}: set exactly one of count and length")),
            }
            match (&r.map, &r.count) {
                (Some(m), Some(_)) => out.extend(map_problems(m, false, &at)),
                (Some(_), None) => out.push(format!("{at}: map is only valid with count or value")),
                (None, _) => {}
            }
        }
        for ptr in [&r.count, &r.length].into_iter().flatten() {
            out.extend(pointer_problem(ptr).map(|m| format!("{at}: {m}")));
        }
        for (k, e) in r.also.iter().enumerate() {
            out.extend(pointer_problem(&e.count).map(|m| format!("{at}.also[{k}]: {m}")));
            if let Some(m) = &e.map {
                out.extend(map_problems(m, false, &format!("{at}.also[{k}]")));
            }
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
            out.extend(where_problems(&format!("{at}.where"), f));
        }
    }
    if let Some(a) = &p.auth {
        out.extend(auth_problems(a));
    }
    out
}

pub(crate) fn timeout_problem(timeout: Millis) -> Option<String> {
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

pub(crate) fn auth_problems(a: &AuthSpec) -> Vec<String> {
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
    out.extend(reachability_only_problems("prometheus", spec));
    out
}

/// A source whose observer reads one up/down value supplies nothing else: with params, it
/// must declare reachability and only that.
fn reachability_only_problems(kind: &str, spec: &SourceSpec) -> Vec<String> {
    use crate::config::Capability;
    let mut out = Vec::new();
    if !spec.observe.contains(&Capability::Reachability) {
        out.push(format!(
            "a {kind} source with params reports reachability; observe must include reachability"
        ));
    }
    for cap in spec
        .observe
        .iter()
        .filter(|c| **c != Capability::Reachability)
    {
        out.push(format!(
            "a {kind} source with params reports reachability only, not {cap}"
        ));
    }
    out
}

/// Problems with a `tcp_probe` host: it must be a bare host name or IP address, since the
/// port is its own field and nothing else is looked up.
fn tcp_host_problem(host: &str) -> Option<String> {
    if host.is_empty() {
        return Some("host must not be empty".to_string());
    }
    if host
        .chars()
        .any(|c| c.is_whitespace() || c.is_control() || matches!(c, '/' | '@' | '[' | ']'))
    {
        return Some(format!(
            "host {host:?} must be a bare host name or IP address (no scheme, path, brackets or spaces)"
        ));
    }
    if host.contains(':') && host.parse::<std::net::Ipv6Addr>().is_err() {
        return Some(format!(
            "host {host:?} contains ':' but is not an IPv6 address; put the port in port"
        ));
    }
    None
}

fn tcp_probe_problems(p: &TcpProbeParams, spec: &SourceSpec) -> Vec<String> {
    let mut out = Vec::new();
    out.extend(tcp_host_problem(&p.host));
    if p.port == 0 {
        out.push("port must be between 1 and 65535".to_string());
    }
    if p.timeout < MIN_TCP_PROBE_TIMEOUT || p.timeout > MAX_TCP_PROBE_TIMEOUT {
        out.push(format!(
            "timeout ({}) must be between {MIN_TCP_PROBE_TIMEOUT} and {MAX_TCP_PROBE_TIMEOUT}",
            p.timeout
        ));
    }
    // One probe per resync: a connect that may outlast the interval would overlap the next.
    if p.timeout > spec.resync {
        out.push(format!(
            "timeout ({}) must not exceed the source's resync ({})",
            p.timeout, spec.resync
        ));
    }
    out.extend(confirmations_problem(p.reachability_confirmations));
    out.extend(reachability_only_problems("tcp_probe", spec));
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
    if spec.observe.contains(&Capability::Jobs) && p.job_namespaces.is_empty() {
        out.push(
            "job_namespaces must name at least one namespace when the source observes jobs"
                .to_string(),
        );
    }
    if p.job_namespaces.iter().any(|n| n.trim().is_empty()) {
        out.push("job_namespaces must not contain empty names".to_string());
    }
    if spec.observe.contains(&Capability::Mounts) && p.nfs_servers.values().all(Vec::is_empty) {
        out.push(
            "nfs_servers must name at least one NFS server of a resource when the source observes mounts"
                .to_string(),
        );
    }
    for (resource, servers) in &p.nfs_servers {
        if servers.is_empty() {
            out.push(format!("nfs_servers[{resource}] must not be empty"));
        }
        if servers
            .iter()
            .any(|s| s.trim().is_empty() || s.contains(char::is_whitespace))
        {
            out.push(format!(
                "nfs_servers[{resource}] must contain host names or addresses without whitespace"
            ));
        }
    }
    out
}

/// Problems with the params of `spec`, each as a message; empty when valid. Kinds without
/// typed params are not checked.
pub(crate) fn problems(spec: &SourceSpec) -> Vec<String> {
    match spec.kind {
        SourceKind::HttpJson => match spec.http_json_params() {
            Ok(Some(p)) => http_problems(&p, spec),
            Ok(None) if spec.observe.contains(&crate::config::Capability::Switch) => {
                vec!["a source that observes switch needs params with exactly one request".into()]
            }
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
        SourceKind::TcpProbe => match spec.tcp_probe_params() {
            Ok(Some(p)) => tcp_probe_problems(&p, spec),
            Ok(None) => Vec::new(),
            Err(e) => vec![e],
        },
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{Config, ConfigError};
    use crate::test_support::{EXAMPLE_CONFIG, example_config, example_with_prometheus_probe};

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
            Some(super::ElementFilters(vec![super::ElementFilter {
                field: "/has_summary".into(),
                contains: None,
                equals: Some(super::FilterValue::Bool(false)),
                is_null: None,
            }]))
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
            .tcp_probe_params()
            .unwrap()
            .unwrap();
        assert_eq!(probe.resource.as_str(), "basestar-nfs");
        assert_eq!((probe.host.as_str(), probe.port), ("basestar", 2049));
        assert_eq!(probe.timeout, crate::Millis(2_000));
        assert_eq!(probe.reachability_confirmations, 3);
        let prom = Config::from_yaml(&example_with_prometheus_probe())
            .unwrap()
            .source(&"basestar-probe".into())
            .unwrap()
            .prometheus_params()
            .unwrap()
            .unwrap();
        assert_eq!(prom.resource.as_str(), "basestar-nfs");
        assert_eq!(prom.url, "http://prometheus.monitoring.svc:9090");
        assert_eq!(prom.query, "probe_success{instance=\"basestar\"}");
        assert_eq!(prom.timeout, crate::Millis(5_000));
        assert_eq!(prom.reachability_confirmations, 3);
        assert!(prom.auth.is_none());
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
                "exactly one of contains, equals and is_null",
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
    fn webhook_paths_are_typed_checked_and_looked_up() {
        let c = example_config();
        let ids = |p: &str| -> Vec<String> {
            c.webhook_sources(p)
                .iter()
                .map(|s| s.id.to_string())
                .collect()
        };
        assert_eq!(
            ids("/hooks/paperless"),
            ["paperless-ocr-pending", "paperless-post-ocr"]
        );
        assert_eq!(ids("/hooks/speakr"), ["speakr-pending", "speakr-summaries"]);
        assert!(ids("/hooks/nothing").is_empty());
        assert!(ids("/hooks/paperless/").is_empty());
        assert_eq!(c.webhook_paths(), ["/hooks/paperless", "/hooks/speakr"]);

        let with = |path: &str| {
            EXAMPLE_CONFIG.replacen(
                "webhook: { path: /hooks/paperless }",
                &format!("webhook: {{ path: \"{path}\" }}"),
                1,
            )
        };
        assert!(Config::from_yaml(&with("/hooks/a-b_c.d~e/f")).is_ok());
        for bad in [
            "", "/", "hooks/x", "/hooks/", "/a//b", "/a?b=1", "/a b", "/a%2Fb", "/a#x",
        ] {
            let ps = problems_of(&with(bad));
            assert!(has(&ps, "webhook.path"), "{bad:?}: {ps:?}");
        }
        // Unknown keys under webhook are refused rather than silently ignored.
        let ps = problems_of(&EXAMPLE_CONFIG.replacen(
            "webhook: { path: /hooks/paperless }",
            "webhook: { path: /hooks/paperless, token: x }",
            1,
        ));
        assert!(has(&ps, "token"), "{ps:?}");
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
                .0[0]
                .equals
                .clone()
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
        assert!(
            has(&ps, "exactly one of contains, equals and is_null"),
            "{ps:?}"
        );
    }

    #[test]
    fn where_takes_one_filter_or_a_list_and_is_null_is_a_third_match_kind() {
        let failed = "where: { field: /has_summary, equals: false }";
        let requests = |w: &str| {
            Config::from_yaml(&EXAMPLE_CONFIG.replace(failed, w))
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
        };
        // The single form still parses, as one filter.
        assert_eq!(requests(failed).0.len(), 1);
        // The list form parses, in order; is_null stands alone.
        let list = requests(
            "where: [ { field: /has_summary, equals: true }, { field: /dur, is_null: true } ]",
        );
        assert_eq!(list.0.len(), 2);
        assert_eq!(list.0[0].equals, Some(true.into()));
        assert_eq!(list.0[1].is_null, Some(true));
        assert!(list.0[1].contains.is_none() && list.0[1].equals.is_none());
        // Serialized, a one-filter list is the single form, and both round-trip.
        for w in [&list, &requests(failed)] {
            let json = serde_json::to_string(w).unwrap();
            assert_eq!(
                &serde_json::from_str::<super::ElementFilters>(&json).unwrap(),
                w
            );
        }
        assert!(
            serde_json::to_string(&requests(failed))
                .unwrap()
                .starts_with('{')
        );
    }

    #[test]
    fn an_unknown_key_in_a_filter_is_refused_in_both_forms() {
        for json in [
            r#"{"field": "/f", "equals": true, "nope": 1}"#,
            r#"[{"field": "/a", "equals": true}, {"field": "/f", "is_null": true, "nope": 1}]"#,
        ] {
            let e = serde_json::from_str::<super::ElementFilters>(json)
                .unwrap_err()
                .to_string();
            assert!(e.contains("unknown field") && e.contains("nope"), "{e}");
        }
        let failed = "where: { field: /has_summary, equals: false }";
        for w in [
            "where: { field: /has_summary, equals: false, nope: 1 }",
            "where: [ { field: /has_summary, equals: false, nope: 1 } ]",
        ] {
            assert!(
                matches!(
                    Config::from_yaml(&EXAMPLE_CONFIG.replace(failed, w)),
                    Err(ConfigError::Parse(_) | ConfigError::Invalid(_))
                ),
                "{w}"
            );
        }
    }

    #[test]
    fn where_problems_name_the_filter_and_refuse_empty_lists_and_is_null_false() {
        let failed = "where: { field: /has_summary, equals: false }";
        let ps = problems_of(&EXAMPLE_CONFIG.replace(failed, "where: []"));
        assert!(has(&ps, ".where: the list is empty"), "{ps:?}");
        let ps = problems_of(
            &EXAMPLE_CONFIG.replace(failed, "where: { field: /has_summary, is_null: false }"),
        );
        assert!(has(&ps, ".where: is_null must be true"), "{ps:?}");
        for both in [
            "{ field: /f, is_null: true, equals: x }",
            "{ field: /f, is_null: true, contains: x }",
            "{ field: /f }",
        ] {
            let ps = problems_of(&EXAMPLE_CONFIG.replace(failed, &format!("where: {both}")));
            assert!(
                has(
                    &ps,
                    ".where: set exactly one of contains, equals and is_null"
                ),
                "{both}: {ps:?}"
            );
        }
        // List entries are indexed in the message.
        let ps = problems_of(&EXAMPLE_CONFIG.replace(
            failed,
            "where: [ { field: /a, equals: true }, { field: nope, is_null: true }, { field: /b, is_null: false } ]",
        ));
        assert!(has(&ps, ".where[1]: "), "{ps:?}");
        assert!(has(&ps, ".where[2]: is_null must be true"), "{ps:?}");
        assert!(!has(&ps, ".where[0]"), "{ps:?}");
    }

    /// The example's basestar-probe params, as one line each.
    const PROBE: &str = "      url: http://prometheus.monitoring.svc:9090\n      query: probe_success{instance=\"basestar\"}\n      resource: basestar-nfs\n      timeout: 5s\n";

    fn probe_with(from: &str, to: &str) -> String {
        let example = example_with_prometheus_probe();
        assert!(example.contains(PROBE));
        example.replace(PROBE, &PROBE.replace(from, to))
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
                "      reachability_confirmations: 0\n      reachability_confirmations: 3\n",
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
        let example = example_with_prometheus_probe();
        let probe_head =
            "  - id: basestar-probe\n    kind: prometheus\n    observe: [reachability]\n";
        assert!(example.contains(probe_head));
        let ps = problems_of(&example.replace(
            probe_head,
            "  - id: basestar-probe\n    kind: prometheus\n    observe: [reachability, demand]\n",
        ));
        assert!(has(&ps, "reachability only, not demand"), "{ps:?}");
        let ps = problems_of(&example.replace(
            probe_head,
            "  - id: basestar-probe\n    kind: prometheus\n    observe: [demand]\n",
        ));
        assert!(has(&ps, "observe must include reachability"), "{ps:?}");
    }

    /// The example's basestar-probe params (a TCP probe), as one line each.
    const TCP: &str =
        "      host: basestar\n      port: 2049\n      resource: basestar-nfs\n      timeout: 2s\n";

    fn tcp_with(from: &str, to: &str) -> String {
        assert!(EXAMPLE_CONFIG.contains(TCP));
        let replaced = TCP.replace(from, to);
        assert_ne!(replaced, TCP, "{from:?} is not in the probe params");
        EXAMPLE_CONFIG.replace(TCP, &replaced)
    }

    fn tcp_params(yaml: &str) -> super::TcpProbeParams {
        Config::from_yaml(yaml)
            .unwrap()
            .source(&"basestar-probe".into())
            .unwrap()
            .tcp_probe_params()
            .unwrap()
            .unwrap()
    }

    #[test]
    fn rejects_bad_tcp_probe_params() {
        for (from, to, needle) in [
            ("host: basestar", "host: \"\"", "host must not be empty"),
            (
                "host: basestar",
                "host: basestar:2049",
                "is not an IPv6 address; put the port in port",
            ),
            (
                "host: basestar",
                "host: nfs://basestar",
                "must be a bare host name or IP address",
            ),
            (
                "host: basestar",
                "host: \"base star\"",
                "must be a bare host name or IP address",
            ),
            (
                "host: basestar",
                "host: \"[::1]\"",
                "must be a bare host name or IP address",
            ),
            ("port: 2049", "port: 0", "port must be between 1 and 65535"),
            ("port: 2049", "port: 70000", "invalid value"),
            ("port: 2049", "port: -1", "invalid value"),
            ("port: 2049", "port: nfs", "invalid type"),
            ("      port: 2049\n", "", "missing field `port`"),
            ("      host: basestar\n", "", "missing field `host`"),
            (
                "      resource: basestar-nfs\n",
                "",
                "missing field `resource`",
            ),
            (
                "resource: basestar-nfs",
                "resource: ghost",
                "params.resource: unknown resource \"ghost\"",
            ),
            ("timeout: 2s", "timeout: 0", "timeout (0ms) must be between"),
            // A plain number is milliseconds: `2` is 2ms, not 2s.
            ("timeout: 2s", "timeout: 2", "timeout (2ms) must be between"),
            ("timeout: 2s", "timeout: 99ms", "must be between 100ms and"),
            ("timeout: 2s", "timeout: 2m", "and 60000ms"),
            (
                "timeout: 2s",
                "timeout: 11s",
                "timeout (11000ms) must not exceed the source's resync (10000ms)",
            ),
            (
                "timeout: 2s",
                "timeout: 2s\n      url: http://basestar",
                "unknown field `url`",
            ),
        ] {
            let ps = problems_of(&tcp_with(from, to));
            assert!(has(&ps, needle), "{needle}: {ps:?}");
        }
        for (n, ok) in [("0", false), ("1", true), ("100", true), ("101", false)] {
            let y = EXAMPLE_CONFIG.replace(
                &format!("{TCP}      reachability_confirmations: 3\n"),
                &format!("{TCP}      reachability_confirmations: {n}\n"),
            );
            assert_ne!(y, EXAMPLE_CONFIG);
            match Config::from_yaml(&y) {
                Ok(_) => assert!(ok, "{n} accepted"),
                Err(_) => {
                    assert!(!ok, "{n} refused");
                    let ps = problems_of(&y);
                    assert!(
                        has(&ps, &format!("reachability_confirmations ({n})")),
                        "{ps:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn tcp_probe_params_take_ip_addresses_and_defaults() {
        for host in ["192.168.1.20", "fd00::20", "::1", "basestar.lan", "nas-01"] {
            let p = tcp_params(&tcp_with("host: basestar", &format!("host: \"{host}\"")));
            assert_eq!(p.host, host);
        }
        let p = tcp_params(&tcp_with("      timeout: 2s\n", ""));
        assert_eq!(p.timeout, super::DEFAULT_TCP_PROBE_TIMEOUT);
        assert_eq!(p.timeout, crate::Millis(2_000));
        let p = tcp_params(&tcp_with("timeout: 2s", "timeout: 100ms"));
        assert_eq!(p.timeout, crate::Millis(100));
        // The longest timeout is accepted once resync allows it.
        let y = tcp_with("timeout: 2s", "timeout: 60s").replace(
            "    kind: tcp_probe\n    observe: [reachability]\n    resync: 10s\n",
            "    kind: tcp_probe\n    observe: [reachability]\n    resync: 60s\n",
        );
        // resync 60s exceeds the example's host_reading_stale_after (30s), so raise that as well.
        let y = y.replace(
            "host_reading_stale_after: 30s",
            "host_reading_stale_after: 60s",
        );
        assert_eq!(tcp_params(&y).timeout, crate::Millis(60_000));
        let p = tcp_params(&EXAMPLE_CONFIG.replace(
            "      timeout: 2s\n      reachability_confirmations: 3\n",
            "",
        ));
        assert_eq!(p.reachability_confirmations, 3);
        assert_eq!(p.timeout, crate::Millis(2_000));
    }

    #[test]
    fn a_tcp_probe_reports_only_reachability() {
        let head = "  - id: basestar-probe\n    kind: tcp_probe\n    observe: [reachability]\n";
        assert!(EXAMPLE_CONFIG.contains(head));
        let with = |observe: &str| {
            EXAMPLE_CONFIG.replace(
                head,
                &format!("  - id: basestar-probe\n    kind: tcp_probe\n    observe: {observe}\n"),
            )
        };
        let ps = problems_of(&with("[reachability, demand]"));
        assert!(has(&ps, "reachability only, not demand"), "{ps:?}");
        assert!(
            has(&ps, "kind TcpProbe cannot supply the demand capability"),
            "{ps:?}"
        );
        let ps = problems_of(&with("[reachability, pods]"));
        assert!(
            has(&ps, "kind TcpProbe cannot supply the pods capability"),
            "{ps:?}"
        );
        let ps = problems_of(&with("[]"));
        assert!(
            has(
                &ps,
                "a tcp_probe source with params reports reachability; observe must include reachability"
            ),
            "{ps:?}"
        );
        // The params belong to this kind only.
        let ps = problems_of(&EXAMPLE_CONFIG.replace(
            head,
            "  - id: basestar-probe\n    kind: prometheus\n    observe: [reachability]\n",
        ));
        assert!(has(&ps, "unknown field `host`"), "{ps:?}");
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
                    .0[0]
                    .equals
                    .clone()
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
        assert!(
            e.contains("exactly one of contains, equals and is_null"),
            "{e}"
        );
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
