//! Typed `params` of the recovery actions `sched-actuate` implements, checked when the
//! configuration is loaded so a bad recovery entry is rejected rather than failing at run
//! time. The action names live in code; everything service-specific is in the params.

use serde::{Deserialize, Serialize};

use crate::config::RecoverySpec;
use crate::source_params::{
    AuthSpec, ElementFilters, MAX_SINCE_AGO, ResponseCondition, SinceSpec, auth_problems,
    pointer_problem, timeout_problem, where_problems,
};
use crate::time::Millis;

/// The action that lists failed items and asks their service to queue them again.
pub const REQUEUE_FAILED: &str = "requeue_failed";

/// Every recovery action this build can run.
pub const RECOVERY_ACTIONS: [&str; 1] = [REQUEUE_FAILED];

/// Most re-queue attempts one item may be given.
pub const MAX_ATTEMPTS_LIMIT: u32 = 10;
/// Most re-queue requests one run may send.
pub const MAX_PER_RUN_LIMIT: u32 = 100;
/// Longest accepted `min_interval`.
pub const MAX_MIN_INTERVAL: Millis = Millis(3_600_000);

/// The request that lists the failed items.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListSpec {
    pub url: String,
    /// JSON pointer to the array of items in the response.
    pub items: String,
    /// JSON pointer, relative to one item, to its identifier (a string or an integer).
    pub id: String,
    /// Only items matching this are re-queued (see `ElementFilters`).
    #[serde(default, rename = "where")]
    pub filter: Option<ElementFilters>,
    /// Only items changed within a window ending now (as for an `http_json` request).
    #[serde(default)]
    pub since: Option<SinceSpec>,
    /// The most items one response can hold (the page size in the URL). A response that
    /// reaches it may be truncated: the visible items are re-queued, the rest on a later run.
    #[serde(default)]
    pub max_items: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    #[default]
    Post,
    Put,
    Patch,
}

/// The request that queues one item again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequeueSpec {
    #[serde(default)]
    pub method: HttpMethod,
    /// URL with `{id}` where the item's identifier goes (percent-encoding is not applied:
    /// an identifier that is not made of letters, digits and `-_.` is skipped).
    pub url: String,
    /// A JSON body to send (for example `{}`), with `Content-Type: application/json`.
    /// Absent: the request has no body and no content type.
    #[serde(default)]
    pub body: Option<serde_json::Value>,
    /// Conditions on the JSON response that must all hold for the request to count as
    /// queued (for example `{field: /success, equals: true}`); a non-2xx answer never does.
    #[serde(default)]
    pub expect: Vec<ResponseCondition>,
}

/// Params of the `requeue_failed` action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequeueFailedParams {
    pub list: ListSpec,
    pub requeue: RequeueSpec,
    #[serde(default)]
    pub auth: Option<AuthSpec>,
    /// Re-queue requests one item may be given within `window` (default 2).
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// The list's `since` window, for reference (default 15m): how long a failure stays
    /// listed. `retention` must not be shorter.
    #[serde(default = "default_window")]
    pub window: Millis,
    /// How long an item's attempts are remembered after the last one, listed or not
    /// (default 24h). Independent of `window`: a recording that used its attempts leaves the
    /// list after the window and returns when it is edited, and must not get fresh attempts
    /// then. After `retention` the record is forgotten and an edited recording may get a
    /// fresh set (the deliberate limit); it also bounds the stored book.
    #[serde(default = "default_retention")]
    pub retention: Millis,
    /// Least time between two runs that list and re-queue (default 60s).
    #[serde(default = "default_min_interval")]
    pub min_interval: Millis,
    /// Most re-queue requests one run sends (default 5).
    #[serde(default = "default_max_per_run")]
    pub max_per_run: u32,
    /// Bound on each HTTP request, the list and every re-queue (default 10s).
    #[serde(default = "default_timeout")]
    pub timeout: Millis,
}

fn default_max_attempts() -> u32 {
    2
}
fn default_window() -> Millis {
    Millis(15 * 60_000)
}
fn default_retention() -> Millis {
    Millis(24 * 3_600_000)
}
fn default_min_interval() -> Millis {
    Millis(60_000)
}
fn default_max_per_run() -> u32 {
    5
}
fn default_timeout() -> Millis {
    Millis(10_000)
}

impl RecoverySpec {
    /// Whether this action is one this build can run.
    pub fn is_known_action(&self) -> bool {
        RECOVERY_ACTIONS.contains(&self.action.as_str())
    }

    /// Whether the action only makes sense while the class's model is allowed (it needs the
    /// workload up). Such an action is not run after in-flight work was cut off, when the
    /// model has just been withdrawn; it runs while the model serves and items are waiting.
    pub fn needs_serving_model(&self) -> bool {
        self.action == REQUEUE_FAILED
    }

    /// The typed params of a `requeue_failed` action.
    pub fn requeue_failed_params(&self) -> Result<RequeueFailedParams, String> {
        if self.action != REQUEUE_FAILED {
            return Err(format!("action {:?} is not {REQUEUE_FAILED}", self.action));
        }
        serde_yaml_ng::from_value(self.params.clone()).map_err(|e| format!("invalid params: {e}"))
    }

    /// Everything wrong with the action and its params.
    pub fn problems(&self) -> Vec<String> {
        if self.action.trim().is_empty() {
            return vec!["action must not be empty".to_string()];
        }
        if !self.is_known_action() {
            return vec![format!(
                "unknown action {:?}; this build runs: {}",
                self.action,
                RECOVERY_ACTIONS.join(", ")
            )];
        }
        match self.requeue_failed_params() {
            Ok(p) => requeue_problems(&p),
            Err(e) => vec![e],
        }
    }
}

fn http_url_problem(at: &str, url: &str) -> Option<String> {
    (!(url.starts_with("http://") || url.starts_with("https://")))
        .then(|| format!("{at} must start with http:// or https://"))
}

fn requeue_problems(p: &RequeueFailedParams) -> Vec<String> {
    let mut out = Vec::new();
    out.extend(http_url_problem("list.url", &p.list.url));
    out.extend(http_url_problem("requeue.url", &p.requeue.url));
    for (at, ptr) in [("list.items", &p.list.items), ("list.id", &p.list.id)] {
        out.extend(pointer_problem(ptr).map(|m| format!("{at}: {m}")));
    }
    if p.list.id.is_empty() {
        out.push("list.id must point at the identifier inside one item, not be empty".into());
    }
    for (at, url) in [("list.url", &p.list.url), ("requeue.url", &p.requeue.url)] {
        let authority = url
            .split_once("://")
            .map_or("", |(_, r)| r.split(['/', '?']).next().unwrap_or(""));
        if authority.contains('@') {
            out.push(format!(
                "{at} must not contain credentials (userinfo); use auth"
            ));
        }
    }
    if p.requeue.url.matches("{id}").count() != 1 {
        out.push("requeue.url must contain {id} exactly once".to_string());
    }
    if p.requeue.url.replace("{id}", "").contains(['{', '}']) {
        out.push("requeue.url has braces other than {id}".to_string());
    }
    for (i, c) in p.requeue.expect.iter().enumerate() {
        out.extend(pointer_problem(&c.field).map(|m| format!("requeue.expect[{i}]: {m}")));
    }
    if let Some(f) = &p.list.filter {
        out.extend(where_problems("list.where", f));
    }
    if let Some(sc) = &p.list.since {
        let plain = !sc.param.is_empty()
            && sc
                .param
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'));
        if !plain {
            out.push(format!(
                "list.since.param {:?} must be a plain query parameter name",
                sc.param
            ));
        }
        let query = p.list.url.split_once('?').map_or("", |(_, q)| q);
        if query
            .split('&')
            .any(|kv| kv.split('=').next() == Some(sc.param.as_str()))
        {
            out.push(format!(
                "list.url already has a {:?} parameter; since would add a second one",
                sc.param
            ));
        }
        if sc.ago == Millis::ZERO || sc.ago > MAX_SINCE_AGO {
            out.push(format!(
                "list.since.ago ({}) must be greater than zero and at most {MAX_SINCE_AGO}",
                sc.ago
            ));
        }
    }
    if p.list.max_items == Some(0) {
        out.push("list.max_items must be greater than zero".to_string());
    }
    if let Some(a) = &p.auth {
        out.extend(auth_problems(a));
    }
    if p.max_attempts == 0 || p.max_attempts > MAX_ATTEMPTS_LIMIT {
        out.push(format!(
            "max_attempts ({}) must be between 1 and {MAX_ATTEMPTS_LIMIT}",
            p.max_attempts
        ));
    }
    if p.window == Millis::ZERO || p.window > MAX_SINCE_AGO {
        out.push(format!(
            "window ({}) must be greater than zero and at most {MAX_SINCE_AGO}",
            p.window
        ));
    }
    if p.retention < p.window || p.retention > MAX_SINCE_AGO {
        out.push(format!(
            "retention ({}) must be at least window ({}) and at most {MAX_SINCE_AGO}",
            p.retention, p.window
        ));
    }
    if p.min_interval > MAX_MIN_INTERVAL {
        out.push(format!(
            "min_interval ({}) must be at most {MAX_MIN_INTERVAL}",
            p.min_interval
        ));
    }
    if p.max_per_run == 0 || p.max_per_run > MAX_PER_RUN_LIMIT {
        out.push(format!(
            "max_per_run ({}) must be between 1 and {MAX_PER_RUN_LIMIT}",
            p.max_per_run
        ));
    }
    out.extend(timeout_problem(p.timeout));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(params: &str) -> RecoverySpec {
        serde_yaml_ng::from_str(&format!(
            "action: requeue_failed\nparams:\n{}",
            params
                .lines()
                .map(|l| format!("  {l}"))
                .collect::<Vec<_>>()
                .join("\n")
        ))
        .unwrap()
    }

    const OK: &str =
        "list: { url: 'http://x/api/failed?per_page=100', items: /recordings, id: /id }
requeue: { url: 'http://x/api/{id}/again' }";

    #[test]
    fn minimal_params_get_the_documented_defaults() {
        let s = spec(OK);
        assert_eq!(s.problems(), Vec::<String>::new());
        let p = s.requeue_failed_params().unwrap();
        assert_eq!(p.max_attempts, 2);
        assert_eq!(p.window, Millis(15 * 60_000));
        assert_eq!(p.retention, Millis(24 * 3_600_000));
        assert_eq!(p.min_interval, Millis(60_000));
        assert_eq!(p.max_per_run, 5);
        assert_eq!(p.timeout, Millis(10_000));
        assert_eq!(p.requeue.method, HttpMethod::Post);
    }

    #[test]
    fn list_where_takes_one_filter_or_a_list() {
        let params = |w: &str| {
            spec(&format!(
                "list: {{ url: 'http://x', items: /r, id: /id, where: {w} }}\nrequeue: {{ url: 'http://x/{{id}}' }}"
            ))
        };
        let one = params("{ field: /f, equals: true }");
        assert_eq!(one.problems(), Vec::<String>::new());
        assert_eq!(
            one.requeue_failed_params()
                .unwrap()
                .list
                .filter
                .unwrap()
                .0
                .len(),
            1
        );
        let many = params("[ { field: /f, equals: true }, { field: /g, is_null: true } ]");
        assert_eq!(many.problems(), Vec::<String>::new());
        let f = many.requeue_failed_params().unwrap().list.filter.unwrap();
        assert_eq!(f.0.len(), 2);
        assert_eq!(f.0[1].is_null, Some(true));
    }

    #[test]
    fn bad_params_are_reported_clearly() {
        let cases: &[(&str, &str)] = &[
            (
                "list: { url: 'http://x', items: /r, id: /id, where: [] }\nrequeue: { url: 'http://x/{id}' }",
                "list.where: the list is empty",
            ),
            (
                "list: { url: 'http://x', items: /r, id: /id, where: { field: /f, is_null: false } }\nrequeue: { url: 'http://x/{id}' }",
                "list.where: is_null must be true",
            ),
            (
                "list: { url: 'http://x', items: /r, id: /id, where: { field: /f, is_null: true, equals: 1 } }\nrequeue: { url: 'http://x/{id}' }",
                "list.where: set exactly one of contains, equals and is_null",
            ),
            (
                "list: { url: 'http://x', items: /r, id: /id, where: [ { field: /a, equals: true }, { field: /b } ] }\nrequeue: { url: 'http://x/{id}' }",
                "list.where[1]: set exactly one of contains, equals and is_null",
            ),
            (
                "list: { url: 'ftp://x', items: /r, id: /id }\nrequeue: { url: 'http://x/{id}' }",
                "list.url must start with http",
            ),
            (
                "list: { url: 'http://x', items: r, id: /id }\nrequeue: { url: 'http://x/{id}' }",
                "list.items: JSON pointer",
            ),
            (
                "list: { url: 'http://x', items: /r, id: '' }\nrequeue: { url: 'http://x/{id}' }",
                "list.id must point",
            ),
            (
                "list: { url: 'http://x', items: /r, id: /id }\nrequeue: { url: 'http://x/again' }",
                "{id} exactly once",
            ),
            (
                "list: { url: 'http://x', items: /r, id: /id }\nrequeue: { url: 'http://x/{id}/{id}' }",
                "{id} exactly once",
            ),
            (
                "list: { url: 'http://x', items: /r, id: /id }\nrequeue: { url: 'http://x/{id}/{other}' }",
                "braces other than",
            ),
            (
                "list: { url: 'http://x?updated_since=1', items: /r, id: /id, since: { param: updated_since, ago: 15m } }\nrequeue: { url: 'http://x/{id}' }",
                "already has",
            ),
            (
                "list: { url: 'http://x', items: /r, id: /id }\nrequeue: { url: 'http://x/{id}' }\nmax_attempts: 0",
                "max_attempts (0)",
            ),
            (
                "list: { url: 'http://x', items: /r, id: /id }\nrequeue: { url: 'http://x/{id}' }\nmax_attempts: 11",
                "max_attempts (11)",
            ),
            (
                "list: { url: 'http://x', items: /r, id: /id }\nrequeue: { url: 'http://x/{id}' }\nmax_per_run: 0",
                "max_per_run (0)",
            ),
            (
                "list: { url: 'http://x', items: /r, id: /id }\nrequeue: { url: 'http://x/{id}' }\nwindow: 0",
                "window (0ms)",
            ),
            (
                "list: { url: 'http://x', items: /r, id: /id }\nrequeue: { url: 'http://x/{id}' }\ntimeout: 0",
                "timeout",
            ),
            (
                "list: { url: 'http://x', items: /r, id: /id }\nrequeue: { url: 'http://x/{id}' }\nretention: 1m",
                "retention (60000ms)",
            ),
            (
                "list: { url: 'http://x', items: /r, id: /id }\nrequeue: { url: 'http://user:pw@x/{id}' }",
                "must not contain credentials",
            ),
            (
                "list: { url: 'http://x', items: /r, id: /id }\nrequeue: { url: 'http://x/{id}' }\nauth: { header: Authorization, env: A, file: /f }",
                "exactly one of env and file",
            ),
            (
                "list: { url: 'http://x', items: /r, id: /id }\nrequeue: { url: 'http://x/{id}' }\nsurprise: 1",
                "unknown field",
            ),
        ];
        for (params, want) in cases {
            let ps = spec(params).problems();
            assert!(
                ps.iter().any(|p| p.contains(want)),
                "{params}: wanted {want:?}, got {ps:?}"
            );
        }
    }

    #[test]
    fn unknown_and_empty_actions_are_refused() {
        let s: RecoverySpec = serde_yaml_ng::from_str("action: requeue").unwrap();
        assert!(s.problems()[0].contains("unknown action"));
        let s: RecoverySpec = serde_yaml_ng::from_str("action: ''").unwrap();
        assert!(s.problems()[0].contains("must not be empty"));
    }
}
