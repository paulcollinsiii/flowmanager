//! A small authenticated HTTP listener for switching manual leases from outside the
//! cluster's own tooling (Home Assistant's REST switch): one switch per configured model
//! (a manual model lease), one per configured resource with a power section (a manual host
//! lease), a `resume` for a resource's stopped power phase, and a listing of all of them.
//!
//! Nothing is decided here. A request is turned into the Restate handler a person would
//! call (`acquire_model_lease`, `release_lease`, `acquire_host_lease`, `release_host_lease`,
//! `resume`, `leases`) and sent through the Restate ingress, so the object's lock, its
//! journal and its persisted state are used exactly as for any other caller; this module
//! keeps no lease state of its own. A switch's richer status is read from what the scheduler
//! already published (the last tick's report, in this process).
//!
//! Endpoints (every one needs `Authorization: Bearer <token>`; names are config ids):
//!
//! - `GET  /v1/leases`: every model and resource with its state.
//! - `GET  /v1/models/{model}`: one model's state.
//! - `POST /v1/models/{model}/on` and `/off`, or `POST /v1/models/{model}` with a body
//!   of `on` or `off` (also `true`/`false`, or JSON `{"state": "on"}` / `{"held": true}`),
//!   which is the shape Home Assistant's REST switch sends.
//! - `GET  /v1/resources/{resource}`, `POST /v1/resources/{resource}/on|off` (or a body),
//!   and `POST /v1/resources/{resource}/resume`: the manual host lease of a host with a power
//!   section. A host found running reads on (flowmanager holds it for the user, see `adopt`).
//!   The vocabulary is the same on every host, shared or exclusive: `on` powers it and keeps
//!   it up, `off` releases the lease with a **graceful** requested shutdown (queued work no
//!   longer keeps the host up; in-flight work gets `drain_timeout`; also with no lease held,
//!   through `drain_host`), and `POST /v1/resources/{resource}/off-now` with an
//!   **immediate** one (the consumers are withdrawn at once). Neither overrides a job's hold
//!   or a lease taken afterwards.
//!   `POST /v1/resources/{resource}/release` is a plain release of the lease (an adopted one
//!   too) with **no drain**: the host goes down only when nothing holds it, as before; `404`
//!   if no lease is held, and the reply reads `on` while the host is up.
//! - `GET  /v1/lockouts/{resource}` and `POST /v1/lockouts/{resource}[/on|/off]`: the
//!   lockout of a host with a power section, shared or exclusive ("flowmanager, keep away
//!   from this host until I clear it"): `lockout` and `clear_lockout`.
//!
//! Every POST logs one `toggle` line (kind, name, action, outcome, status; never the
//! token) and is counted in `sched_toggle_requests_total`.
//!
//! Plain HTTP: the intended client cannot trust a private CA, so the bearer token is the
//! control, together with the network (NetworkPolicy, mesh) around the port.

use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use hyper::Method;
use sched_model::{
    Config, DrainMode, Lease, LeaseId, LeaseKind, Millis, ModePhase, ModelId, PowerPhase,
    ResourceId, ResourceKind,
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::net::TcpListener;

use crate::leases::LeaseGrant;
pub use crate::listener::{Limits, Reply, constant_time_eq};
use crate::listener::{LogLimiter, bearer, read_body, read_token};
use crate::object::{BoxFut, DrainAck};
use crate::resume::ResumeAck;
use crate::runtime::OBJECT;
use crate::scheduler::KEY;
use crate::tick::{Shared, TickReport};

/// The largest request body read.
const MAX_BODY: usize = 1024;

/// A refusal with the HTTP status to answer with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    pub status: u16,
    pub message: String,
}

impl ApiError {
    fn new(status: u16, message: impl Into<String>) -> ApiError {
        ApiError {
            status,
            message: message.into(),
        }
    }
}

/// What the listener asks of the scheduler: the handlers of the `Scheduler` object that a
/// person would call. Implemented over the Restate ingress ([`RestateIngress`]).
pub trait Backend: Send + Sync {
    fn leases(&self) -> BoxFut<'_, Result<Vec<Lease>, ApiError>>;
    fn acquire_model(&self, model: &ModelId) -> BoxFut<'_, Result<LeaseGrant, ApiError>>;
    fn release_lease(&self, id: &LeaseId) -> BoxFut<'_, Result<Lease, ApiError>>;
    fn acquire_host(&self, resource: &ResourceId) -> BoxFut<'_, Result<LeaseGrant, ApiError>>;
    /// Releases the manual host lease with no drain (`release_host_lease`).
    fn release_host(&self, resource: &ResourceId) -> BoxFut<'_, Result<Lease, ApiError>>;
    /// Takes the host down: releases its manual lease if one is held and records a drain.
    fn drain_host(
        &self,
        resource: &ResourceId,
        mode: DrainMode,
    ) -> BoxFut<'_, Result<DrainAck, ApiError>>;
    fn resume(&self, resource: &ResourceId) -> BoxFut<'_, Result<ResumeAck, ApiError>>;
    fn lockout(&self, resource: &ResourceId) -> BoxFut<'_, Result<LeaseGrant, ApiError>>;
    fn clear_lockout(&self, resource: &ResourceId) -> BoxFut<'_, Result<Lease, ApiError>>;
}

/// [`Backend`] over `POST {ingress}/Scheduler/main/{handler}`.
pub struct RestateIngress {
    client: reqwest::Client,
    base: String,
}

impl RestateIngress {
    pub fn new(ingress: &str) -> RestateIngress {
        Self::with_timeout(ingress, Duration::from_secs(15))
    }

    /// As [`RestateIngress::new`], with the time one call may take (tests use a short one).
    pub fn with_timeout(ingress: &str, timeout: Duration) -> RestateIngress {
        RestateIngress {
            client: reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .expect("client builds"),
            base: ingress.trim_end_matches('/').to_string(),
        }
    }

    async fn call<T: DeserializeOwned>(
        &self,
        handler: &str,
        body: Option<Value>,
    ) -> Result<T, ApiError> {
        let url = format!("{}/{OBJECT}/{KEY}/{handler}", self.base);
        let mut req = self.client.post(&url);
        if let Some(b) = &body {
            req = req.json(b);
        }
        let resp = req.send().await.map_err(|e| {
            if e.is_timeout() {
                ApiError::new(504, "the scheduler did not answer in time")
            } else {
                ApiError::new(
                    503,
                    format!("the scheduler could not be reached: {}", e.without_url()),
                )
            }
        })?;
        let status = resp.status();
        if status.is_success() {
            return resp.json::<T>().await.map_err(|e| {
                ApiError::new(
                    502,
                    format!("unreadable answer from the scheduler: {}", e.without_url()),
                )
            });
        }
        let text = resp.text().await.unwrap_or_default();
        // Restate carries a handler's refusal as `{"message": "..."}` with its status.
        let message = serde_json::from_str::<Value>(&text)
            .ok()
            .and_then(|v| v.get("message").and_then(Value::as_str).map(str::to_string))
            .unwrap_or(text);
        // Only a handler's 400 and 409 are relayed. A 404 is not: the handlers answer it only
        // for a release with nothing to release (the callers list first and never rely on
        // it), while Restate answers it for a service or handler it does not know, which
        // must not read as "unknown name" or as "nothing held".
        Err(match status.as_u16() {
            s @ (400 | 409) => ApiError::new(s, message),
            s => ApiError::new(502, format!("the scheduler answered HTTP {s}: {message}")),
        })
    }
}

impl Backend for RestateIngress {
    fn leases(&self) -> BoxFut<'_, Result<Vec<Lease>, ApiError>> {
        Box::pin(self.call("leases", None))
    }

    fn acquire_model(&self, model: &ModelId) -> BoxFut<'_, Result<LeaseGrant, ApiError>> {
        let body = json!({ "model": model });
        Box::pin(self.call("acquire_model_lease", Some(body)))
    }

    fn release_lease(&self, id: &LeaseId) -> BoxFut<'_, Result<Lease, ApiError>> {
        let body = json!({ "id": id });
        Box::pin(self.call("release_lease", Some(body)))
    }

    fn acquire_host(&self, resource: &ResourceId) -> BoxFut<'_, Result<LeaseGrant, ApiError>> {
        let body = json!({ "resource": resource });
        Box::pin(self.call("acquire_host_lease", Some(body)))
    }

    fn release_host(&self, resource: &ResourceId) -> BoxFut<'_, Result<Lease, ApiError>> {
        let body = json!({ "resource": resource });
        Box::pin(self.call("release_host_lease", Some(body)))
    }

    fn drain_host(
        &self,
        resource: &ResourceId,
        mode: DrainMode,
    ) -> BoxFut<'_, Result<DrainAck, ApiError>> {
        let body = json!({ "resource": resource, "drain": mode });
        Box::pin(self.call("drain_host", Some(body)))
    }

    fn resume(&self, resource: &ResourceId) -> BoxFut<'_, Result<ResumeAck, ApiError>> {
        let body = json!({ "resource": resource });
        Box::pin(self.call("resume", Some(body)))
    }

    fn lockout(&self, resource: &ResourceId) -> BoxFut<'_, Result<LeaseGrant, ApiError>> {
        let body = json!({ "resource": resource });
        Box::pin(self.call("lockout", Some(body)))
    }

    fn clear_lockout(&self, resource: &ResourceId) -> BoxFut<'_, Result<Lease, ApiError>> {
        let body = json!({ "resource": resource });
        Box::pin(self.call("clear_lockout", Some(body)))
    }
}

/// What a request asks for, from its method and path alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    List,
    Switch(Kind, String, Act),
    NotFound,
    /// Carries the methods the path does allow, for the `Allow` header.
    MethodNotAllowed(&'static str),
}

/// What a switch is on: a model lease, a manual host lease or a lockout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Model,
    Host,
    Lockout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Act {
    Get,
    /// `Some(true)` is on, `Some(false)` off; `None` takes it from the body.
    Set(Option<bool>),
    Resume,
    /// A host switched off with an immediate drain (`POST /v1/resources/{resource}/off-now`).
    OffNow,
    /// A plain release of the manual host lease, no drain (`POST /v1/resources/{resource}/release`).
    Release,
}

/// Decodes `%XX` escapes; `None` for a malformed escape or a result that is not UTF-8.
fn percent_decode(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let hex = s.get(i + 1..i + 3)?;
            // Exactly two hex digits: `from_str_radix` would also take a leading `+`.
            if !hex.bytes().all(|c| c.is_ascii_hexdigit()) {
                return None;
            }
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Routes a request.
pub fn route(method: &Method, path: &str) -> Route {
    let parts: Vec<&str> = path.split('/').collect();
    let (kind, name, tail) = match parts.as_slice() {
        ["", "v1", "leases"] => {
            return if method == Method::GET {
                Route::List
            } else {
                Route::MethodNotAllowed("GET")
            };
        }
        ["", "v1", k @ ("models" | "resources" | "lockouts"), n] => (*k, *n, None),
        [
            "",
            "v1",
            k @ ("models" | "resources" | "lockouts"),
            n,
            t @ ("on" | "off" | "resume" | "off-now" | "release"),
        ] => (*k, *n, Some(*t)),
        _ => return Route::NotFound,
    };
    let Some(name) = percent_decode(name).filter(|n| !n.is_empty()) else {
        return Route::NotFound;
    };
    let kind = match kind {
        "models" => Kind::Model,
        "resources" => Kind::Host,
        _ => Kind::Lockout,
    };
    let act = match (tail, method) {
        (None, m) if m == Method::GET => Act::Get,
        (None, m) if m == Method::POST => Act::Set(None),
        (Some("on"), m) if m == Method::POST => Act::Set(Some(true)),
        (Some("off"), m) if m == Method::POST => Act::Set(Some(false)),
        (Some("resume"), m) if m == Method::POST && kind == Kind::Host => Act::Resume,
        (Some("off-now"), m) if m == Method::POST && kind == Kind::Host => Act::OffNow,
        (Some("release"), m) if m == Method::POST && kind == Kind::Host => Act::Release,
        (Some("release"), _) if kind != Kind::Host => return Route::NotFound,
        (Some("resume"), _) if kind != Kind::Host => return Route::NotFound,
        (Some("off-now"), _) if kind != Kind::Host => return Route::NotFound,
        _ => {
            return Route::MethodNotAllowed(if tail.is_none() { "GET, POST" } else { "POST" });
        }
    };
    Route::Switch(kind, name, act)
}

/// The metrics label of an endpoint; `want` is the resolved on/off wish of a set.
fn endpoint_label(route: &Route, want: Option<Option<bool>>) -> &'static str {
    let Route::Switch(kind, _, act) = route else {
        return match route {
            Route::List => "list",
            _ => "unmatched",
        };
    };
    let labels: [&'static str; 7] = match kind {
        Kind::Model => [
            "model_get",
            "model_on",
            "model_off",
            "model_set",
            "unmatched",
            "unmatched",
            "unmatched",
        ],
        Kind::Host => [
            "resource_get",
            "resource_on",
            "resource_off",
            "resource_set",
            "resource_resume",
            "resource_off_now",
            "resource_release",
        ],
        Kind::Lockout => [
            "lockout_get",
            "lockout_on",
            "lockout_off",
            "lockout_set",
            "unmatched",
            "unmatched",
            "unmatched",
        ],
    };
    match act {
        Act::Get => labels[0],
        Act::Resume => labels[4],
        Act::OffNow => labels[5],
        Act::Release => labels[6],
        Act::Set(w) => match w.or(want.flatten()) {
            Some(true) => labels[1],
            Some(false) => labels[2],
            None => labels[3],
        },
    }
}

/// The fields of the `toggle` log line: one per change request (an on/off or a resume) and
/// one per refused request of any kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToggleLog {
    pub kind: &'static str,
    /// What the caller named, or for an unknown path the path: caller-controlled text,
    /// shortened here and only ever logged with its `Debug` form (escaped).
    pub name: String,
    pub action: &'static str,
    pub outcome: &'static str,
    pub status: u16,
}

/// The longest name or path kept in a log line.
const LOG_NAME_MAX: usize = 64;

impl ToggleLog {
    /// `None` for a request that succeeded and changed nothing (a read, a listing).
    /// `want` is the resolved wish of a set (`None` before the caller was authorized).
    pub fn of(
        route: &Route,
        path: &str,
        want: Option<Option<bool>>,
        outcome: &'static str,
        status: u16,
    ) -> Option<ToggleLog> {
        let short = |s: &str| s.chars().take(LOG_NAME_MAX).collect::<String>();
        let (kind, name, action, change) = match route {
            Route::Switch(kind, name, act) => {
                let wish = match act {
                    Act::Set(Some(w)) => Some(*w),
                    _ => want.flatten(),
                };
                let action = match (act, wish) {
                    (Act::Get, _) => "get",
                    (Act::Resume, _) => "resume",
                    (Act::OffNow, _) => "off_now",
                    (Act::Release, _) => "release",
                    (_, Some(true)) => "on",
                    (_, Some(false)) => "off",
                    _ => "unknown",
                };
                let kind = match kind {
                    Kind::Model => "model",
                    Kind::Host => "host",
                    Kind::Lockout => "lockout",
                };
                (kind, short(name), action, *act != Act::Get)
            }
            Route::List => ("list", String::new(), "get", false),
            Route::NotFound | Route::MethodNotAllowed(_) => {
                ("unmatched", short(path), "unknown", false)
            }
        };
        if !change && status == 200 {
            return None;
        }
        Some(ToggleLog {
            kind,
            name,
            action,
            outcome,
            status,
        })
    }

    fn emit(&self, suppressed: u32) {
        if self.status == 200 {
            tracing::info!(event = "toggle", kind = self.kind, name = ?self.name, action = self.action, outcome = self.outcome, status = self.status, suppressed, "toggle request");
        } else {
            tracing::warn!(event = "toggle", kind = self.kind, name = ?self.name, action = self.action, outcome = self.outcome, status = self.status, suppressed, "toggle request refused");
        }
    }
}

fn outcome_label(status: u16) -> &'static str {
    match status {
        200 => "ok",
        400 => "bad_request",
        401 => "unauthorized",
        404 => "not_found",
        405 => "method_not_allowed",
        409 => "conflict",
        _ => "error",
    }
}

/// Reads the on/off wish from a POST body: `on`, `off`, `true`, `false` (any case), or a
/// JSON object with `state` (`"on"`/`"off"`) or `held` (a boolean).
pub fn parse_wish(body: &str) -> Option<bool> {
    let text = body.trim();
    match text.to_ascii_lowercase().as_str() {
        "on" | "true" => return Some(true),
        "off" | "false" => return Some(false),
        _ => {}
    }
    let v: Value = serde_json::from_str(text).ok()?;
    if let Some(b) = v.get("held").and_then(Value::as_bool) {
        return Some(b);
    }
    match v
        .get("state")
        .and_then(Value::as_str)?
        .to_ascii_lowercase()
        .as_str()
    {
        "on" => Some(true),
        "off" => Some(false),
        _ => None,
    }
}

/// The toggle listener's logic: authentication, routing, and the calls behind them.
pub struct Toggle {
    shared: Arc<Shared>,
    backend: Arc<dyn Backend>,
    token_file: PathBuf,
    limiter: LogLimiter,
}

impl Toggle {
    pub fn new(shared: Arc<Shared>, backend: Arc<dyn Backend>, token_file: PathBuf) -> Toggle {
        Toggle {
            shared,
            backend,
            token_file,
            limiter: LogLimiter::new(std::time::Instant::now()),
        }
    }

    /// The token, read from its file on every request. A missing, unreadable or empty file
    /// is an error, never a match.
    async fn token(&self) -> Result<String, ApiError> {
        read_token(&self.token_file, "toggle")
            .await
            .ok_or_else(|| ApiError::new(503, "the toggle token is not available"))
    }

    /// Answers one request whose body is already in hand.
    pub async fn handle(
        &self,
        method: &Method,
        path: &str,
        authorization: Option<&str>,
        body: &str,
    ) -> Reply {
        self.handle_with(method, path, authorization, async {
            Some(body.to_string())
        })
        .await
    }

    /// Answers one request. `authorization` is the header value. `read_body` is not polled
    /// until the caller is authenticated, and only for a request that takes its wish from
    /// the body; it yields `None` for a body that is too large or unreadable.
    pub async fn handle_with(
        &self,
        method: &Method,
        path: &str,
        authorization: Option<&str>,
        read_body: impl Future<Output = Option<String>>,
    ) -> Reply {
        let route = route(method, path);
        let mut want = None;
        let reply = match self.authorize(authorization).await {
            Err(reply) => reply,
            Ok(()) => match &route {
                Route::Switch(_, _, Act::Set(None)) => match read_body.await {
                    Some(body) => {
                        want = Some(parse_wish(&body));
                        self.dispatch(&route, want).await
                    }
                    None => {
                        want = Some(None);
                        error_reply(ApiError::new(
                            400,
                            format!(
                                "the request body is larger than {MAX_BODY} bytes or unreadable"
                            ),
                        ))
                    }
                },
                Route::Switch(_, _, Act::Set(Some(w))) => {
                    want = Some(Some(*w));
                    self.dispatch(&route, want).await
                }
                _ => self.dispatch(&route, None).await,
            },
        };
        let outcome = outcome_label(reply.status);
        self.shared
            .metrics
            .toggle_request(endpoint_label(&route, want), outcome);
        if let Some(l) = ToggleLog::of(&route, path, want, outcome, reply.status)
            && let Some(left_out) = self
                .limiter
                .admit(std::time::Instant::now(), reply.status != 200)
        {
            l.emit(left_out);
        }
        reply
    }

    async fn authorize(&self, header: Option<&str>) -> Result<(), Reply> {
        let token = self.token().await.map_err(error_reply)?;
        match bearer(header) {
            Some(given) if constant_time_eq(given.as_bytes(), token.as_bytes()) => Ok(()),
            _ => Err(Reply::unauthorized()),
        }
    }

    async fn dispatch(&self, route: &Route, want: Option<Option<bool>>) -> Reply {
        let result = match route {
            Route::NotFound => Err(ApiError::new(404, "not found")),
            Route::MethodNotAllowed(allow) => {
                let mut r = error_reply(ApiError::new(405, "method not allowed"));
                r.allow = Some(allow);
                return r;
            }
            Route::List => self.list().await,
            Route::Switch(kind, name, Act::Get) => self.switch(*kind, name, None).await,
            Route::Switch(kind, name, Act::Set(_)) => match want.flatten() {
                Some(on) => self.switch(*kind, name, Some(on)).await,
                None => Err(bad_wish()),
            },
            Route::Switch(_, name, Act::Resume) => self.resume(name).await,
            Route::Switch(_, name, Act::OffNow) => {
                self.resource(name, Some(false), DrainMode::Immediate).await
            }
            Route::Switch(_, name, Act::Release) => self.release(name).await,
        };
        match result {
            Ok(v) => Reply::json(200, v),
            Err(e) => error_reply(e),
        }
    }

    async fn switch(&self, kind: Kind, name: &str, set: Option<bool>) -> Result<Value, ApiError> {
        match kind {
            Kind::Model => self.model(name, set).await,
            Kind::Host => self.resource(name, set, DrainMode::Graceful).await,
            Kind::Lockout => self.lockout(name, set).await,
        }
    }

    async fn list(&self) -> Result<Value, ApiError> {
        let leases = self.backend.leases().await?;
        let config = self.shared.config.current();
        let (report, now) = (self.shared.last_report(), self.shared.clock.now());
        let models: Vec<Value> = config
            .models()
            .iter()
            .map(|m| {
                let locked_out = Some(find_lockout(&leases, &m.resource).is_some());
                let held = find_model(&leases, &m.id);
                model_state(&m.id, &m.resource, held, locked_out, &report, now)
            })
            .collect();
        let resources: Vec<Value> = config
            .resources()
            .iter()
            .filter(|r| r.power.is_some())
            .map(|r| {
                resource_state(
                    &config,
                    &r.id,
                    find_host(&leases, &r.id),
                    is_locked_out(&config, &leases, &r.id),
                    None,
                    &report,
                    now,
                )
            })
            .collect();
        let lockouts: Vec<Value> = config
            .resources()
            .iter()
            .filter(|r| config.supports_lockout(&r.id))
            .map(|r| lockout_state(&config, &r.id, find_lockout(&leases, &r.id), &report, now))
            .collect();
        let attention = resources
            .iter()
            .chain(&lockouts)
            .any(|r| r["needs_attention"] == true);
        Ok(json!({
            "needs_attention": attention,
            "models": models,
            "resources": resources,
            "lockouts": lockouts,
        }))
    }

    /// A release that failed counts as done only if the lease book, read again, no longer
    /// holds the lease; otherwise the failure is the answer. (Never "nothing to release"
    /// from a status code alone: Restate answers 404 for a service it does not know.)
    async fn gone_after(&self, e: ApiError, id: &LeaseId) -> Result<Vec<Lease>, ApiError> {
        match self.backend.leases().await {
            Ok(l) if !l.iter().any(|x| &x.id == id) => Ok(l),
            _ => Err(e),
        }
    }

    /// One model's state, after setting it first if `set` says so.
    async fn model(&self, name: &str, set: Option<bool>) -> Result<Value, ApiError> {
        let config = self.shared.config.current();
        let id = ModelId::from(name);
        let spec = config.model(&id).ok_or_else(|| {
            let known: Vec<&str> = config.models().iter().map(|m| m.id.as_str()).collect();
            ApiError::new(
                404,
                format!("unknown model {name:?} (configured: {})", known.join(", ")),
            )
        })?;
        let (held, leases) = match set {
            None => {
                let l = self.backend.leases().await?;
                (find_model(&l, &id).cloned(), Some(l))
            }
            Some(true) => {
                let lease = self.backend.acquire_model(&id).await?.lease;
                (Some(lease), self.backend.leases().await.ok())
            }
            Some(false) => {
                let l = self.backend.leases().await?;
                let l = match find_model(&l, &id).cloned() {
                    None => l,
                    Some(lease) => match self.backend.release_lease(&lease.id).await {
                        Ok(_) => l.into_iter().filter(|x| x.id != lease.id).collect(),
                        Err(e) => self.gone_after(e, &lease.id).await?,
                    },
                };
                (None, Some(l))
            }
        };
        let locked_out = leases
            .as_ref()
            .map(|l| find_lockout(l, &spec.resource).is_some());
        Ok(model_state(
            &id,
            &spec.resource,
            held.as_ref(),
            locked_out,
            &self.shared.last_report(),
            self.shared.clock.now(),
        ))
    }

    /// A host switch. Off takes the host down with a drain (`drain`: the host's switch is
    /// `graceful`, `off-now` is `immediate`), whether or not a lease is held: it releases the
    /// manual lease if there is one and records the drain, so queued and in-flight work no
    /// longer keeps the host up. A drain already under way is changed in place (immediate
    /// escalates, graceful leaves an immediate one). The answer is not "off" while the host is
    /// up and draining: `state` is `draining` and `drain` says how far it is. `held` is false.
    async fn resource(
        &self,
        name: &str,
        set: Option<bool>,
        drain: DrainMode,
    ) -> Result<Value, ApiError> {
        let config = self.shared.config.current();
        let id = known_resource(&config, name)?;
        let (held, ack) = match set {
            None => (find_host(&self.backend.leases().await?, &id).cloned(), None),
            // A new lease cancels any drain at the next tick; the reply shows none.
            Some(true) => (
                Some(self.backend.acquire_host(&id).await?.lease),
                Some(None),
            ),
            // Off is a requested shutdown on every host, whatever its kind; the listener
            // translates and the sequencer decides.
            Some(false) => (None, Some(self.backend.drain_host(&id, drain).await?.drain)),
        };
        let locked = is_locked_out(&config, &self.backend.leases().await?, &id);
        Ok(resource_state(
            &config,
            &id,
            held.as_ref(),
            locked,
            ack,
            &self.shared.last_report(),
            self.shared.clock.now(),
        ))
    }

    /// A plain release of the manual host lease (an adopted one included), with no drain: the
    /// host is shut down only when nothing holds it, as for `release_host_lease` without
    /// `drain`. No lease held is a `404` (the listener's own: Restate's 404 is never relayed),
    /// a release that fails counts only if the lease is gone afterwards. The reply is the
    /// host's state, which reads `on` while the last report shows the host up (`held` is false).
    async fn release(&self, name: &str) -> Result<Value, ApiError> {
        let config = self.shared.config.current();
        let id = known_resource(&config, name)?;
        let leases = self.backend.leases().await?;
        let Some(lease) = find_host(&leases, &id).cloned() else {
            return Err(ApiError::new(
                404,
                format!("resource {name:?} holds no manual host lease"),
            ));
        };
        self.release_held(&config, &id, &lease).await
    }

    /// Releases `lease`, the manual host lease of `id`, with no drain, and answers with the
    /// host's state (see `release`).
    async fn release_held(
        &self,
        config: &Config,
        id: &ResourceId,
        lease: &Lease,
    ) -> Result<Value, ApiError> {
        if let Err(e) = self.backend.release_host(id).await {
            self.gone_after(e, &lease.id).await?;
        }
        let report = self.shared.last_report();
        let locked = is_locked_out(config, &self.backend.leases().await?, id);
        let mut v = resource_state(
            config,
            id,
            None,
            locked,
            Some(None),
            &report,
            self.shared.clock.now(),
        );
        let up = report
            .as_ref()
            .and_then(|r| r.state.resource(id))
            .is_some_and(|st| st.is_on());
        if up {
            v["state"] = json!("on");
        }
        Ok(v)
    }

    async fn lockout(&self, name: &str, set: Option<bool>) -> Result<Value, ApiError> {
        let config = self.shared.config.current();
        let id = known_resource(&config, name)?;
        let held = match set {
            None => find_lockout(&self.backend.leases().await?, &id).cloned(),
            Some(true) => Some(self.backend.lockout(&id).await?.lease),
            Some(false) => {
                let l = self.backend.leases().await?;
                if let Some(lease) = find_lockout(&l, &id).cloned()
                    && let Err(e) = self.backend.clear_lockout(&id).await
                {
                    self.gone_after(e, &lease.id).await?;
                }
                None
            }
        };
        Ok(lockout_state(
            &config,
            &id,
            held.as_ref(),
            &self.shared.last_report(),
            self.shared.clock.now(),
        ))
    }

    async fn resume(&self, name: &str) -> Result<Value, ApiError> {
        let id = known_resource(&self.shared.config.current(), name)?;
        let ack = self.backend.resume(&id).await?;
        Ok(json!({ "resource": ack.resource, "cleared": ack.cleared }))
    }
}

fn bad_wish() -> ApiError {
    ApiError::new(
        400,
        "the body must be on or off (or {\"state\": \"on\"} / {\"held\": true})",
    )
}

/// The resource `name` in the config, or a 404 listing what is configured.
fn known_resource(config: &Config, name: &str) -> Result<ResourceId, ApiError> {
    let id = ResourceId::from(name);
    if config.resource(&id).is_some() {
        return Ok(id);
    }
    let known: Vec<&str> = config.resources().iter().map(|r| r.id.as_str()).collect();
    Err(ApiError::new(
        404,
        format!(
            "unknown resource {name:?} (configured: {})",
            known.join(", ")
        ),
    ))
}

fn find_model<'a>(leases: &'a [Lease], model: &ModelId) -> Option<&'a Lease> {
    leases
        .iter()
        .find(|l| matches!(&l.kind, LeaseKind::ManualModel { model: m } if m == model))
}

fn find_host<'a>(leases: &'a [Lease], resource: &ResourceId) -> Option<&'a Lease> {
    leases
        .iter()
        .find(|l| &l.resource == resource && l.kind == LeaseKind::ManualHost)
}

/// The fields every switch carries.
fn switch(
    kind: &str,
    id: &str,
    held: Option<&Lease>,
    now: Millis,
) -> serde_json::Map<String, Value> {
    let mut m = serde_json::Map::new();
    m.insert("kind".into(), json!(kind));
    m.insert("id".into(), json!(id));
    m.insert("held".into(), json!(held.is_some()));
    m.insert(
        "state".into(),
        json!(if held.is_some() { "on" } else { "off" }),
    );
    if let Some(l) = held {
        // Who made the lease: a host lease flowmanager minted for a host it found running is
        // `adopted` (it ends when the host is seen down); every other is `requested`.
        m.insert("origin".into(), json!(l.origin.as_str()));
        m.insert("held_since_ms".into(), json!(l.granted_at.as_u64()));
        m.insert(
            "held_for_seconds".into(),
            json!(now.as_u64().saturating_sub(l.granted_at.as_u64()) / 1000),
        );
    }
    m
}

/// A model switch. `status`, when present, is derived from the last tick's report of this
/// process: `active` (the model is in the allowed set), `waiting_for_host` (its host is not
/// up), `host_failed` (its host's power transition failed), `draining` (another model of
/// the resource is being wound down, its in-flight work awaited), or `pending` (the
/// scheduler has not yet acted on the lease, or is serving another model for a reason the
/// report does not give). Absent while the process has not ticked. While a lockout is held
/// on the model's resource the status is `locked out` whatever the report says: the planner
/// gives a locked out resource no model, so the lease cannot become active until the lockout
/// is released. `locked out` is whether the lease book holds one; `None` (the book could not
/// be read just now) leaves the status out.
pub fn model_state(
    model: &ModelId,
    resource: &ResourceId,
    held: Option<&Lease>,
    locked_out: Option<bool>,
    report: &Option<TickReport>,
    now: Millis,
) -> Value {
    let mut m = switch("model", model.as_str(), held, now);
    m.insert("resource".into(), json!(resource));
    if let (Some(lease), Some(locked_out)) = (held, locked_out) {
        let status = if locked_out {
            let power = report
                .as_ref()
                .and_then(|r| r.state.resource(resource))
                .map(|st| format!(" (power: {})", power_name(&st.power)))
                .unwrap_or_default();
            Some((
                "locked_out",
                format!(
                    "{resource} is locked out{power}: no model is allowed on it until the lockout is released"
                ),
            ))
        } else {
            report
                .as_ref()
                .map(|r| model_status(r, lease, model, resource))
        };
        if let Some((status, detail)) = status {
            m.insert("status".into(), json!(status));
            m.insert("detail".into(), json!(detail));
        }
    }
    Value::Object(m)
}

fn model_status(
    r: &TickReport,
    lease: &Lease,
    model: &ModelId,
    resource: &ResourceId,
) -> (&'static str, String) {
    if !r.leases.iter().any(|l| l.id == lease.id) {
        return (
            "pending",
            "the last tick was decided before this lease was taken; the next tick acts on it"
                .into(),
        );
    }
    if r.allowed_models.contains(model) {
        return ("active", format!("{model} is allowed on {resource}"));
    }
    let Some(st) = r.state.resource(resource) else {
        return ("pending", format!("no state for {resource} yet"));
    };
    match &st.power {
        PowerPhase::On { .. } => {}
        PowerPhase::Failed { .. } => {
            return (
                "host_failed",
                format!(
                    "a power transition of {resource} failed; resume it once the host is fixed"
                ),
            );
        }
        other => {
            return (
                "waiting_for_host",
                format!("{resource} is {}", power_name(other)),
            );
        }
    }
    match &st.mode {
        ModePhase::Serving {
            model: other,
            wind_down: Some(_),
            ..
        } => (
            "draining",
            format!("{other} is winding down on {resource}; its in-flight work is awaited"),
        ),
        ModePhase::Serving { model: other, .. } => (
            "pending",
            format!("{other} is still allowed on {resource}; the switch has not been decided"),
        ),
        ModePhase::Idle => (
            "pending",
            format!("{resource} is up and idle; {model} is not allowed yet"),
        ),
    }
}

fn power_name(p: &PowerPhase) -> &'static str {
    match p {
        PowerPhase::Off { .. } => "off",
        PowerPhase::PoweringOn { .. } => "powering_on",
        PowerPhase::On { .. } => "on",
        PowerPhase::ShuttingDown { .. } => "shutting_down",
        PowerPhase::Failed { .. } => "failed",
    }
}

/// Whether a lockout is held on `resource` (and means something there: it has a power section).
fn is_locked_out(config: &Config, leases: &[Lease], resource: &ResourceId) -> bool {
    config.supports_lockout(resource) && find_lockout(leases, resource).is_some()
}

fn find_lockout<'a>(leases: &'a [Lease], resource: &ResourceId) -> Option<&'a Lease> {
    leases
        .iter()
        .find(|l| &l.resource == resource && l.kind == LeaseKind::Lockout)
}

/// A lockout switch ("keep away from this host until I clear it") on a resource with a power
/// section, shared or exclusive.
/// `supported` is false for a resource without a power section, which cannot be locked out. While held and
/// seen by the last tick, `status` is `host_failed` (a power transition failed),
/// `powered_off` (the host is down and is not woken), `left_for_person` (up, with `control:
/// manual`: flowmanager never shuts it down), `left_alone` (up again after the lockout shut it
/// down: flowmanager leaves it alone until the lockout is cleared), `shutting_down`, `draining` (its work
/// is awaited for at most the lockout timeout, then cut off) or `pending` (the tick has
/// not acted on it yet, or the report does not say more); `tick_reason` is that tick's
/// reason as the sequencer gave it. `needs_attention` is as for a host.
pub fn lockout_state(
    config: &Config,
    resource: &ResourceId,
    held: Option<&Lease>,
    report: &Option<TickReport>,
    now: Millis,
) -> Value {
    let mut m = switch("lockout", resource.as_str(), held, now);
    m.insert("supported".into(), json!(config.supports_lockout(resource)));
    if let Some(r) = report {
        if let Some(st) = r.state.resource(resource) {
            m.insert(
                "power".into(),
                serde_json::to_value(st.power).unwrap_or(Value::Null),
            );
            m.insert(
                "needs_attention".into(),
                json!(attention(r, resource, &st.power)),
            );
        }
        if let Some(lease) = held {
            let managed = config.power_managed(resource);
            let (status, detail) = lockout_status(r, lease, resource, managed);
            m.insert("status".into(), json!(status));
            m.insert("detail".into(), json!(detail));
            m.insert("tick_reason".into(), json!(r.reason));
        }
    }
    Value::Object(m)
}

/// Whether a person has to act on the resource: its power transition failed, or its shutdown
/// was stopped by the veto.
fn attention(r: &TickReport, resource: &ResourceId, power: &PowerPhase) -> bool {
    matches!(power, PowerPhase::Failed { .. })
        || r.veto_held
            .iter()
            .any(|h| &h.resource == resource && h.stopped)
}

fn lockout_status(
    r: &TickReport,
    lease: &Lease,
    resource: &ResourceId,
    managed: bool,
) -> (&'static str, String) {
    if !r.leases.iter().any(|l| l.id == lease.id) {
        return (
            "pending",
            "the last tick was decided before this lockout was taken; the next tick acts on it"
                .into(),
        );
    }
    let Some(st) = r.state.resource(resource) else {
        return ("pending", format!("no state for {resource} yet"));
    };
    match &st.power {
        PowerPhase::Failed { .. } => (
            "host_failed",
            format!("a power transition of {resource} failed; resume it once the host is fixed"),
        ),
        PowerPhase::Off { .. } => (
            "powered_off",
            format!("{resource} is off and is not woken while locked out"),
        ),
        PowerPhase::ShuttingDown { .. } => {
            ("shutting_down", format!("{resource} is shutting down"))
        }
        PowerPhase::PoweringOn { .. } => (
            "pending",
            format!("{resource} is powering on; it is shut down once the tick decides it"),
        ),
        PowerPhase::On { .. } => match &st.mode {
            ModePhase::Serving {
                model,
                wind_down: Some(_),
                ..
            } => (
                "draining",
                format!(
                    "{model} is winding down on {resource}; its work is cut off after the lockout timeout"
                ),
            ),
            _ if st.shutdown.is_some_and(|s| {
                s.is_lockout() && s.withdrawal() == sched_model::Withdrawal::Pending
            }) =>
            {
                (
                    "draining",
                    format!(
                        "the consumers of {resource} are given the lockout timeout to finish, then withdrawn and the host shut down"
                    ),
                )
            }
            _ if st.locked_down => (
                "left_alone",
                format!(
                    "{resource} was shut down by the lockout and is up again (another OS, say); flowmanager leaves it alone until the lockout is cleared"
                ),
            ),
            _ if !managed => (
                "left_for_person",
                format!(
                    "{resource} is up; its power is control: manual, so flowmanager does not shut it down: power it off yourself"
                ),
            ),
            _ => (
                "pending",
                format!("{resource} is up; the lockout has not yet shut it down"),
            ),
        },
    }
}

/// A resource (host) switch. `power`, `wait` and `needs_attention` come from the last tick's
/// report of this process and are absent before the first tick; `wait` names why the
/// scheduler holds something back (`min_off`, `shutdown_held_by_veto`). `drain` is present while
/// a drain is under way (a switch-off asked for the host to go down): its mode and whether
/// the consumers are already scaled down. `requested` is the drain a request has just recorded
/// (the last tick has not seen it yet); while one is under way the switch is not off: `held` is
/// false and `state` is `draining`. `locked_out` (from the lease book) says a lockout is held
/// on the host: the reply then has `locked_out: true` and a `wait` entry `locked_out`, whatever
/// was just asked (a request is still recorded or refused as before: the lockout decides what
/// happens to the host), and `state` reads `locked_out` instead of `off` while the last report
/// does not show the host off.
pub fn resource_state(
    config: &Config,
    resource: &ResourceId,
    held: Option<&Lease>,
    locked_out: bool,
    requested: Option<Option<crate::drain::RecordedDrain>>,
    report: &Option<TickReport>,
    now: Millis,
) -> Value {
    let mut m = switch("resource", resource.as_str(), held, now);
    let kind = config.resource(resource).map(|r| r.kind);
    m.insert(
        "resource_kind".into(),
        json!(match kind {
            Some(ResourceKind::Shared) => "shared",
            _ => "exclusive",
        }),
    );
    m.insert(
        "lease_supported".into(),
        json!(config.resource(resource).is_some_and(|r| r.power.is_some())),
    );
    let draining = match requested {
        Some(d) => d.map(|d| {
            (
                d.mode.as_str().to_string(),
                d.since,
                d.scaled_down,
                "requested".to_string(),
            )
        }),
        None => report.as_ref().and_then(|r| {
            r.state.resource(resource).filter(|st| st.is_on())?;
            let d = r.drains.iter().find(|d| &d.resource == resource)?;
            Some((d.mode.clone(), d.since, d.scaled_down, d.origin.clone()))
        }),
    };
    if let Some((mode, since, scaled_down, origin)) = draining {
        m.insert(
            "drain".into(),
            json!({ "mode": mode, "origin": origin, "since_ms": since.as_u64(), "scaled_down": scaled_down }),
        );
        if held.is_none() {
            m.insert("state".into(), json!("draining"));
        }
    }
    if let Some(r) = report
        && let Some(st) = r.state.resource(resource)
    {
        m.insert(
            "power".into(),
            serde_json::to_value(st.power).unwrap_or(Value::Null),
        );
        let veto = r.veto_held.iter().find(|h| &h.resource == resource);
        let mut waits = Vec::new();
        if let Some(h) = veto {
            waits.push(json!({
                "reason": "shutdown_held_by_veto",
                "readings": h.readings,
                "blocking": h.blocking,
                "stopped": h.stopped,
            }));
        }
        if let Some(w) = r.min_off_waits.iter().find(|w| &w.resource == resource) {
            waits.push(json!({
                "reason": "min_off",
                "remaining_seconds": w.remaining.as_u64() / 1000,
            }));
        }
        m.insert("wait".into(), Value::Array(waits));
        m.insert(
            "needs_attention".into(),
            json!(attention(r, resource, &st.power)),
        );
    }
    m.insert("locked_out".into(), json!(locked_out));
    if locked_out {
        let mut waits = match m.remove("wait") {
            Some(Value::Array(w)) => w,
            _ => Vec::new(),
        };
        waits.push(json!({ "reason": "locked_out" }));
        m.insert("wait".into(), Value::Array(waits));
        // Not `off` while the host is not seen off: the lockout is shutting it down, or
        // leaving it alone after another boot.
        let off = report
            .as_ref()
            .and_then(|r| r.state.resource(resource))
            .is_some_and(|st| matches!(st.power, PowerPhase::Off { .. }));
        if held.is_none() && !off && m.get("state") != Some(&json!("draining")) {
            m.insert("state".into(), json!("locked_out"));
        }
    }
    Value::Object(m)
}

fn error_reply(e: ApiError) -> Reply {
    Reply::failure(e.status, &e.message)
}

/// Serves until the task is dropped or aborted.
pub async fn serve(listener: TcpListener, toggle: Arc<Toggle>, limits: Limits) {
    crate::listener::serve(
        listener,
        limits,
        "toggle",
        move |method, path, auth, body| {
            let toggle = Arc::clone(&toggle);
            async move {
                // The body is read only once the caller has been authenticated.
                let body = read_body(body, MAX_BODY);
                toggle
                    .handle_with(&method, &path, auth.as_deref(), body)
                    .await
            }
        },
    )
    .await
}

/// Reads the token once (so a missing or empty file fails startup), binds `addr` (so a taken
/// port fails startup) and serves on its own task.
pub async fn spawn(toggle: Toggle, addr: SocketAddr) -> Result<SocketAddr, String> {
    toggle
        .token()
        .await
        .map_err(|e| format!("{} ({})", e.message, toggle.token_file.display()))?;
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| format!("cannot listen on {addr}: {e}"))?;
    let local = listener.local_addr().map_err(|e| e.to_string())?;
    tokio::spawn(serve(listener, Arc::new(toggle), Limits::default()));
    Ok(local)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ManualClock;
    use crate::config_source::ConfigHolder;
    use crate::listener::{LOG_BURST, LOG_WINDOW};
    use crate::metrics::Metrics;
    use crate::object::{self, AcquireModelLease, ReleaseLease, ResourceRequest, fake::FakeCtx};
    use crate::persist::LEASES_KEY;
    use crate::resume::Resume;
    use restate_sdk::errors::HandlerError;
    use sched_model::test_support::{
        EXAMPLE_CONFIG, example_yaml_managed, example_yaml_managed_nfs_only,
    };
    use sched_model::{Fidelity, ObservedValue};
    use sched_observe::Report;
    use sched_observe::testserver::{Reply as TestReply, TestServer};

    const T0: u64 = 10_000_000;
    const TOKEN: &str = "s3cret-token";

    /// A backend that runs the same `object` functions the Restate handlers run, over a fake
    /// context (one committed invocation per call).
    struct Fake {
        shared: Arc<Shared>,
        ctx: FakeCtx,
    }

    fn api(e: HandlerError) -> ApiError {
        let text = format!("{e:?}");
        let status = [400u16, 404, 409]
            .into_iter()
            .find(|c| text.contains(&format!("code: {c}")))
            .unwrap_or(500);
        ApiError::new(status, text)
    }

    impl Backend for Fake {
        fn leases(&self) -> BoxFut<'_, Result<Vec<Lease>, ApiError>> {
            Box::pin(async move {
                let raw = self.ctx.stored(LEASES_KEY);
                object::list_leases(&self.shared, raw.as_deref()).map_err(api)
            })
        }

        fn acquire_model(&self, model: &ModelId) -> BoxFut<'_, Result<LeaseGrant, ApiError>> {
            let req = AcquireModelLease {
                model: model.clone(),
            };
            Box::pin(async move {
                let r = object::acquire_model_lease(&self.shared, &self.ctx, req).await;
                self.ctx.commit();
                r.map_err(api)
            })
        }

        fn release_lease(&self, id: &LeaseId) -> BoxFut<'_, Result<Lease, ApiError>> {
            let req = ReleaseLease { id: id.clone() };
            Box::pin(async move {
                let r = object::release_lease(&self.shared, &self.ctx, req).await;
                self.ctx.commit();
                r.map_err(api)
            })
        }

        fn acquire_host(&self, resource: &ResourceId) -> BoxFut<'_, Result<LeaseGrant, ApiError>> {
            let req = ResourceRequest {
                resource: resource.clone(),
            };
            Box::pin(async move {
                let r = object::acquire_host_lease(&self.shared, &self.ctx, req).await;
                self.ctx.commit();
                r.map_err(api)
            })
        }

        fn release_host(&self, resource: &ResourceId) -> BoxFut<'_, Result<Lease, ApiError>> {
            let req = crate::object::ReleaseHostLease {
                resource: resource.clone(),
                drain: None,
            };
            Box::pin(async move {
                let r = object::release_host_lease(&self.shared, &self.ctx, req).await;
                self.ctx.commit();
                r.map_err(api)
            })
        }

        fn drain_host(
            &self,
            resource: &ResourceId,
            mode: DrainMode,
        ) -> BoxFut<'_, Result<DrainAck, ApiError>> {
            let req = crate::object::DrainHost {
                resource: resource.clone(),
                drain: mode,
            };
            Box::pin(async move {
                let r = object::drain_host(&self.shared, &self.ctx, req).await;
                self.ctx.commit();
                r.map_err(api)
            })
        }

        fn resume(&self, resource: &ResourceId) -> BoxFut<'_, Result<ResumeAck, ApiError>> {
            let req = Resume {
                resource: resource.clone(),
            };
            Box::pin(async move {
                let r = object::resume(&self.shared, &self.ctx, req).await;
                self.ctx.commit();
                r.map_err(api)
            })
        }

        fn lockout(&self, resource: &ResourceId) -> BoxFut<'_, Result<LeaseGrant, ApiError>> {
            let req = ResourceRequest {
                resource: resource.clone(),
            };
            Box::pin(async move {
                let r = object::lockout(&self.shared, &self.ctx, req).await;
                self.ctx.commit();
                r.map_err(api)
            })
        }

        fn clear_lockout(&self, resource: &ResourceId) -> BoxFut<'_, Result<Lease, ApiError>> {
            let req = ResourceRequest {
                resource: resource.clone(),
            };
            Box::pin(async move {
                let r = object::clear_lockout(&self.shared, &self.ctx, req).await;
                self.ctx.commit();
                r.map_err(api)
            })
        }
    }

    struct Rig {
        toggle: Toggle,
        shared: Arc<Shared>,
        backend: Arc<Fake>,
        token_file: PathBuf,
        _dir: std::path::PathBuf,
    }

    fn rig(yaml: &str) -> Rig {
        let clock = Arc::new(ManualClock::new(Millis(T0)));
        let holder = Arc::new(ConfigHolder::new(yaml).unwrap());
        let shared = Arc::new(Shared::new(holder, Arc::new(Metrics::new()), clock));
        let backend = Arc::new(Fake {
            shared: Arc::clone(&shared),
            ctx: FakeCtx::default(),
        });
        let dir = std::env::temp_dir().join(format!(
            "toggle-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let token_file = dir.join(format!("token-{}", rand_suffix()));
        std::fs::write(&token_file, format!("{TOKEN}\n")).unwrap();
        let toggle = Toggle::new(
            Arc::clone(&shared),
            Arc::clone(&backend) as Arc<dyn Backend>,
            token_file.clone(),
        );
        Rig {
            toggle,
            shared,
            backend,
            token_file,
            _dir: dir,
        }
    }

    fn rand_suffix() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

    impl Rig {
        async fn call(&self, method: Method, path: &str, body: &str) -> (u16, Value) {
            let auth = format!("Bearer {TOKEN}");
            let r = self.toggle.handle(&method, path, Some(&auth), body).await;
            (r.status, serde_json::from_str(&r.body).unwrap())
        }

        async fn get(&self, path: &str) -> (u16, Value) {
            self.call(Method::GET, path, "").await
        }

        async fn post(&self, path: &str, body: &str) -> (u16, Value) {
            self.call(Method::POST, path, body).await
        }

        fn lease_count(&self) -> usize {
            object::list_leases(&self.shared, self.backend.ctx.stored(LEASES_KEY).as_deref())
                .unwrap()
                .len()
        }

        fn push(&self, source: &str, values: Vec<ObservedValue>) {
            self.shared.ingest_now(Report::Observed {
                source: source.into(),
                fidelity: Fidelity::Polled,
                values,
            });
        }

        fn hosts_up(&self, gpu: bool) {
            let mut v: Vec<ObservedValue> = ["mineru", "llm", "whisperx", "immich-ml"]
                .iter()
                .map(|m| ObservedValue::ModelPods {
                    model: (*m).into(),
                    pods: 0,
                    ready_pods: 0,
                })
                .collect();
            v.push(ObservedValue::ResourceUp {
                resource: "cuda-central".into(),
                up: gpu,
            });
            self.push("cluster-pods", v);
            self.push(
                "basestar-probe",
                vec![ObservedValue::ResourceUp {
                    resource: "basestar-nfs".into(),
                    up: true,
                }],
            );
        }

        async fn tick(&self) {
            object::tick(&self.shared, &self.backend.ctx).await.unwrap();
            self.backend.ctx.commit();
        }
    }

    #[test]
    fn routes_are_by_name_and_only_the_listed_ones_exist() {
        let g = Method::GET;
        let p = Method::POST;
        assert_eq!(route(&g, "/v1/leases"), Route::List);
        assert_eq!(route(&p, "/v1/leases"), Route::MethodNotAllowed("GET"));
        assert_eq!(
            route(&g, "/v1/models/llm"),
            Route::Switch(Kind::Model, "llm".into(), Act::Get)
        );
        assert_eq!(
            route(&p, "/v1/models/llm"),
            Route::Switch(Kind::Model, "llm".into(), Act::Set(None))
        );
        assert_eq!(
            route(&p, "/v1/models/llm/on"),
            Route::Switch(Kind::Model, "llm".into(), Act::Set(Some(true)))
        );
        assert_eq!(
            route(&p, "/v1/models/a%3Ab/off"),
            Route::Switch(Kind::Model, "a:b".into(), Act::Set(Some(false)))
        );
        assert_eq!(
            route(&p, "/v1/resources/nfs/resume"),
            Route::Switch(Kind::Host, "nfs".into(), Act::Resume)
        );
        assert_eq!(
            route(&p, "/v1/resources/nfs/off-now"),
            Route::Switch(Kind::Host, "nfs".into(), Act::OffNow)
        );
        assert_eq!(
            route(&p, "/v1/resources/nfs/release"),
            Route::Switch(Kind::Host, "nfs".into(), Act::Release)
        );
        assert_eq!(
            route(&g, "/v1/resources/nfs/release"),
            Route::MethodNotAllowed("POST")
        );
        assert_eq!(route(&p, "/v1/models/llm/release"), Route::NotFound);
        assert_eq!(route(&p, "/v1/lockouts/gpu/release"), Route::NotFound);
        assert_eq!(route(&p, "/v1/models/llm/off-now"), Route::NotFound);
        assert_eq!(route(&p, "/v1/lockouts/gpu/off-now"), Route::NotFound);
        assert_eq!(
            route(&g, "/v1/resources/nfs/off-now"),
            Route::MethodNotAllowed("POST")
        );
        assert_eq!(route(&p, "/v1/models/llm/resume"), Route::NotFound);
        assert_eq!(
            route(&p, "/v1/lockouts/gpu/on"),
            Route::Switch(Kind::Lockout, "gpu".into(), Act::Set(Some(true)))
        );
        assert_eq!(
            route(&g, "/v1/lockouts/gpu"),
            Route::Switch(Kind::Lockout, "gpu".into(), Act::Get)
        );
        assert_eq!(route(&p, "/v1/lockouts/gpu/resume"), Route::NotFound);
        // The route this one replaced no longer exists.
        assert_eq!(route(&p, "/v1/reclaims/gpu/on"), Route::NotFound);
        assert_eq!(route(&g, "/v1/reclaims/gpu"), Route::NotFound);
        assert_eq!(
            route(&g, "/v1/resources/nfs/on"),
            Route::MethodNotAllowed("POST")
        );
        assert_eq!(
            route(&Method::DELETE, "/v1/models/llm"),
            Route::MethodNotAllowed("GET, POST")
        );
        for gone in [
            "/v1/resources/nfs/lockout",
            "/v1/resources/nfs/clear_lockout",
            "/lockout",
            "/v1/models/",
            "/v1/models/%zz",
            "/v1/leases/extra",
            "/",
        ] {
            assert_eq!(route(&p, gone), Route::NotFound, "{gone}");
        }
    }

    #[test]
    fn wishes_tokens_and_comparison() {
        for (b, w) in [
            ("ON", Some(true)),
            (" off\n", Some(false)),
            ("true", Some(true)),
            ("false", Some(false)),
            (r#"{"state":"On"}"#, Some(true)),
            (r#"{"state":"off"}"#, Some(false)),
            (r#"{"held":true}"#, Some(true)),
            ("", None),
            ("maybe", None),
            (r#"{"state":"x"}"#, None),
        ] {
            assert_eq!(parse_wish(b), w, "{b:?}");
        }
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(!constant_time_eq(b"", b"abc"));
        // Only the length differs (the shorter one reads as zero bytes past its end).
        assert!(!constant_time_eq(b"abc", b"abc\0"));
        assert!(!constant_time_eq(b"abc\0\0", b"abc"));
        assert_eq!(bearer(Some("Bearer  x ")), Some("x"));
        assert_eq!(bearer(Some("bearer x")), Some("x"));
        assert_eq!(bearer(Some("Basic x")), None);
        assert_eq!(bearer(Some("Bearer")), None);
        assert_eq!(bearer(None), None);
    }

    #[tokio::test]
    async fn every_request_needs_the_token_and_it_is_read_per_request() {
        let r = rig(EXAMPLE_CONFIG);
        let h = |t: Option<&str>| t.map(str::to_string);
        for auth in [
            h(None),
            h(Some("Bearer wrong")),
            h(Some("Bearer s3cret-toke")),
            h(Some("Bearer s3cret-token-and-more")),
            h(Some(TOKEN)),
            h(Some("Basic s3cret-token")),
        ] {
            for (m, p) in [
                (Method::GET, "/v1/leases"),
                (Method::GET, "/v1/models/llm"),
                (Method::POST, "/v1/models/llm/on"),
                (Method::GET, "/nothing"),
            ] {
                let reply = r.toggle.handle(&m, p, auth.as_deref(), "").await;
                assert_eq!(reply.status, 401, "{auth:?} {m} {p}");
                assert!(reply.challenge);
                assert!(!reply.body.contains(TOKEN));
            }
        }
        assert_eq!(r.lease_count(), 0, "nothing was done without the token");
        let ok = r
            .toggle
            .handle(&Method::GET, "/v1/leases", Some("Bearer s3cret-token"), "")
            .await;
        assert_eq!(ok.status, 200);

        // Rotating the Secret takes effect without a restart.
        std::fs::write(&r.token_file, "rotated\n").unwrap();
        let old = r
            .toggle
            .handle(&Method::GET, "/v1/leases", Some("Bearer s3cret-token"), "")
            .await;
        assert_eq!(old.status, 401);
        let new = r
            .toggle
            .handle(&Method::GET, "/v1/leases", Some("Bearer rotated"), "")
            .await;
        assert_eq!(new.status, 200);

        // An empty or missing file never lets anyone in, not even with an empty token.
        std::fs::write(&r.token_file, "\n").unwrap();
        for auth in [Some("Bearer "), Some("Bearer x"), None] {
            let reply = r.toggle.handle(&Method::GET, "/v1/leases", auth, "").await;
            assert_eq!(reply.status, 503, "{auth:?}");
            assert!(!reply.challenge);
        }
        std::fs::remove_file(&r.token_file).unwrap();
        let reply = r
            .toggle
            .handle(&Method::GET, "/v1/leases", Some("Bearer rotated"), "")
            .await;
        assert_eq!(reply.status, 503);
        let text = r.shared.metrics.render();
        assert!(
            text.contains(
                "sched_toggle_requests_total{endpoint=\"model_on\",outcome=\"unauthorized\"}"
            ),
            "{text}"
        );
        assert!(
            text.contains(
                "sched_toggle_requests_total{endpoint=\"unmatched\",outcome=\"unauthorized\"}"
            ),
            "{text}"
        );
    }

    #[tokio::test]
    async fn a_model_switch_is_idempotent_both_ways() {
        let r = rig(EXAMPLE_CONFIG);
        let (s, v) = r.get("/v1/models/llm").await;
        assert_eq!(
            (s, &v["held"], &v["state"]),
            (200, &json!(false), &json!("off"))
        );
        assert!(v.get("status").is_none() && v.get("held_since_ms").is_none());

        let (s, first) = r.post("/v1/models/llm/on", "").await;
        assert_eq!(s, 200);
        assert_eq!(
            (&first["held"], &first["state"]),
            (&json!(true), &json!("on"))
        );
        assert_eq!(first["resource"], "cuda-central");
        assert_eq!(first["held_since_ms"], T0);
        assert!(first.get("id").is_some() && first.get("lease").is_none());
        let (s, again) = r.post("/v1/models/llm/on", "").await;
        assert_eq!(s, 200);
        assert_eq!(again["held_since_ms"], first["held_since_ms"]);
        assert_eq!(
            r.lease_count(),
            1,
            "asking again does not take a second lease"
        );
        // The switch in Home Assistant's shape: one URL, the body says which way.
        let (s, v) = r.post("/v1/models/llm", "ON").await;
        assert_eq!((s, &v["held"]), (200, &json!(true)));
        assert_eq!(r.lease_count(), 1);
        let (_, v) = r.get("/v1/models/llm").await;
        assert_eq!(v["held"], true);

        let (s, v) = r.post("/v1/models/llm/off", "").await;
        assert_eq!(
            (s, &v["held"], &v["state"]),
            (200, &json!(false), &json!("off"))
        );
        assert_eq!(r.lease_count(), 0);
        let (s, v) = r.post("/v1/models/llm", "OFF").await;
        assert_eq!((s, &v["held"]), (200, &json!(false)));
        let (s, v) = r.post("/v1/models/llm/off", "").await;
        assert_eq!((s, &v["held"]), (200, &json!(false)));
        // A later lease is a new one, not an error.
        assert_eq!(r.post("/v1/models/llm/on", "").await.0, 200);
        assert_eq!(r.lease_count(), 1);
    }

    #[tokio::test]
    async fn errors_from_the_handlers_keep_their_message_and_a_4xx_status() {
        let r = rig(EXAMPLE_CONFIG);
        assert_eq!(r.post("/v1/models/llm/on", "").await.0, 200);
        // Another model of the same resource: the handler's 409.
        let (s, v) = r.post("/v1/models/mineru/on", "").await;
        assert_eq!(s, 409);
        assert!(v["error"].as_str().unwrap().contains("already has"), "{v}");
        // Turning off a model that is not the one leased leaves the lease alone.
        let (s, v) = r.post("/v1/models/mineru/off", "").await;
        assert_eq!((s, &v["held"]), (200, &json!(false)));
        assert_eq!(r.lease_count(), 1);
        assert_eq!(r.get("/v1/models/llm").await.1["held"], true);
        // Unknown names are 404 and list what is configured.
        let (s, v) = r.post("/v1/models/nope/on", "").await;
        assert_eq!(s, 404);
        assert!(v["error"].as_str().unwrap().contains("immich-ml"), "{v}");
        assert_eq!(r.post("/v1/models/nope/off", "").await.0, 404);
        assert_eq!(r.get("/v1/models/nope").await.0, 404);
        assert_eq!(r.get("/v1/resources/nope").await.0, 404);
        assert_eq!(r.post("/v1/resources/nope/resume", "").await.0, 404);
        // A body that says neither way is refused before anything is done.
        assert_eq!(r.post("/v1/models/mineru", "toggle").await.0, 400);
        assert_eq!(r.post("/v1/models/mineru", "").await.0, 400);
        assert_eq!(r.get("/v1/nothing").await.0, 404);
        assert_eq!(r.call(Method::DELETE, "/v1/models/llm", "").await.0, 405);
        assert_eq!(r.lease_count(), 1);
    }

    #[tokio::test]
    async fn a_host_switch_leases_a_shared_host_and_an_exclusive_one_alike() {
        let r = rig(&example_yaml_managed());
        let (s, v) = r.get("/v1/resources/basestar-nfs").await;
        assert_eq!(
            (s, &v["held"], &v["lease_supported"]),
            (200, &json!(false), &json!(true))
        );
        let (s, v) = r.post("/v1/resources/basestar-nfs/on", "").await;
        assert_eq!(
            (s, &v["held"], &v["resource_kind"]),
            (200, &json!(true), &json!("shared"))
        );
        // A person's lease reads as requested, never adopted.
        assert_eq!(v["origin"], json!("requested"), "{v}");
        assert_eq!(r.post("/v1/resources/basestar-nfs", "on").await.0, 200);
        assert_eq!(r.lease_count(), 1);
        assert_eq!(
            r.post("/v1/resources/basestar-nfs/off", "").await.1["held"],
            false
        );
        // Nothing held: still a success.
        let (s, v) = r.post("/v1/resources/basestar-nfs/off", "").await;
        assert_eq!((s, &v["held"]), (200, &json!(false)));
        assert_eq!(r.lease_count(), 0);
        // The GPU host is exclusive: the same switch, with a plain release as its off.
        let (s, v) = r.get("/v1/resources/cuda-central").await;
        assert_eq!(
            (s, &v["held"], &v["lease_supported"], &v["resource_kind"]),
            (200, &json!(false), &json!(true), &json!("exclusive"))
        );
        let (s, v) = r.post("/v1/resources/cuda-central/on", "").await;
        assert_eq!((s, &v["held"]), (200, &json!(true)), "{v}");
        assert_eq!(r.lease_count(), 1);
        let (s, v) = r.get("/v1/leases").await;
        let gpu = v["resources"]
            .as_array()
            .unwrap()
            .iter()
            .find(|x| x["id"] == "cuda-central")
            .unwrap_or_else(|| panic!("the listing has the GPU host: {v}"));
        assert_eq!((s, &gpu["held"]), (200, &json!(true)));
        // off-now is accepted for an exclusive host as for a shared one: it releases the
        // lease (no drain is shown: the host has not been seen up by a tick yet).
        let (s, v) = r.post("/v1/resources/cuda-central/off-now", "").await;
        assert_eq!((s, &v["held"]), (200, &json!(false)), "{v}");
        assert_eq!(r.lease_count(), 0);
        r.post("/v1/resources/cuda-central/on", "").await;
        assert_eq!(r.lease_count(), 1);
        // off likewise, and again with nothing held.
        let (s, v) = r.post("/v1/resources/cuda-central/off", "").await;
        assert_eq!((s, &v["held"]), (200, &json!(false)), "{v}");
        assert_eq!(r.lease_count(), 0);
        assert_eq!(r.post("/v1/resources/cuda-central/off", "").await.0, 200);
        // release answers 404 when nothing is held, as for a shared host.
        assert_eq!(
            r.post("/v1/resources/cuda-central/release", "").await.0,
            404
        );
        r.post("/v1/resources/cuda-central/on", "").await;
        assert_eq!(
            r.post("/v1/resources/cuda-central/release", "").await.0,
            200
        );
        assert_eq!(r.lease_count(), 0);
        // Resume of a host that is not stopped: the handler's 409.
        let (s, v) = r.post("/v1/resources/basestar-nfs/resume", "").await;
        assert_eq!(s, 409, "{v}");
        assert!(v["error"].is_string());
    }
    #[tokio::test]

    async fn an_adopted_host_reads_on_and_off_and_off_now_start_a_drain_with_or_without_a_lease() {
        let r = rig(&example_yaml_managed_nfs_only());
        r.hosts_up(true);
        r.tick().await;
        // Found running: the adopted lease is a manual host lease, so the switch reads on.
        let (s, v) = r.get("/v1/resources/basestar-nfs").await;
        assert_eq!(
            (s, &v["held"], &v["state"]),
            (200, &json!(true), &json!("on")),
            "{v}"
        );
        assert_eq!(v["origin"], json!("adopted"), "{v}");
        let drain = |r: &Rig| {
            let core = crate::persist::decode_state(
                r.backend.ctx.stored(crate::persist::STATE_KEY).as_deref(),
            )
            .unwrap();
            core.state
                .resource(&"basestar-nfs".into())
                .and_then(crate::drain::RecordedDrain::of)
        };
        // Off: the lease is released and a graceful drain starts. The host is up, so the
        // answer says draining, not off.
        let (s, v) = r.post("/v1/resources/basestar-nfs/off", "").await;
        assert_eq!(
            (s, &v["held"], &v["state"], &v["drain"]["mode"]),
            (200, &json!(false), &json!("draining"), &json!("graceful")),
            "{v}"
        );
        assert_eq!(r.lease_count(), 0);
        assert_eq!(
            drain(&r).map(|d| d.mode),
            Some(sched_model::DrainMode::Graceful)
        );
        r.tick().await;
        let (_, v) = r.get("/v1/resources/basestar-nfs").await;
        assert_eq!(
            (&v["state"], &v["drain"]["mode"]),
            (&json!("draining"), &json!("graceful")),
            "{v}"
        );
        // Off-now with no lease held: the drain under way is made immediate in place, keeping
        // its start (nothing is taken or released, and no report is consulted).
        let since = drain(&r).map(|d| d.since);
        let (s, v) = r.post("/v1/resources/basestar-nfs/off-now", "").await;
        assert_eq!((s, &v["drain"]["mode"]), (200, &json!("immediate")), "{v}");
        let d = drain(&r).unwrap();
        assert_eq!(d.mode, sched_model::DrainMode::Immediate);
        assert_eq!(Some(d.since), since);
        assert_eq!(r.lease_count(), 0);
        // Graceful asked of an immediate drain leaves it immediate.
        r.post("/v1/resources/basestar-nfs/off", "").await;
        assert_eq!(
            drain(&r).map(|d| d.mode),
            Some(sched_model::DrainMode::Immediate)
        );
        // A new lease cancels it (the tick sees the lease); off-now from a held lease drains.
        r.post("/v1/resources/basestar-nfs/on", "").await;
        r.tick().await;
        assert_eq!(drain(&r), None, "the new lease cancelled it");
        let (s, v) = r.post("/v1/resources/basestar-nfs/off-now", "").await;
        assert_eq!((s, &v["state"]), (200, &json!("draining")), "{v}");
        assert_eq!(
            drain(&r).map(|d| d.mode),
            Some(sched_model::DrainMode::Immediate)
        );
        assert_eq!(r.lease_count(), 0);
    }

    #[tokio::test]
    async fn a_reply_reflects_the_handlers_answer_and_the_current_phase_not_a_stale_report() {
        let r = rig(&example_yaml_managed_nfs_only());
        r.hosts_up(true);
        r.tick().await;
        r.post("/v1/resources/basestar-nfs/off", "").await;
        r.tick().await;
        // The report now holds the drain. /on answers on, with no drain object.
        let (s, v) = r.post("/v1/resources/basestar-nfs/on", "").await;
        assert_eq!(
            (s, &v["state"], &v["held"]),
            (200, &json!("on"), &json!(true)),
            "{v}"
        );
        assert!(v.get("drain").is_none(), "{v}");
        // The host goes down and the tick sees it: the drain is over; a repeated off is plain off.
        r.tick().await;
        r.push(
            "basestar-probe",
            vec![ObservedValue::ResourceUp {
                resource: "basestar-nfs".into(),
                up: false,
            }],
        );
        r.tick().await;
        let (s, v) = r.post("/v1/resources/basestar-nfs/off", "").await;
        assert_eq!((s, &v["state"]), (200, &json!("off")), "{v}");
        assert!(v.get("drain").is_none(), "{v}");

        // The view itself: a report that holds a drain for a host no longer up shows none,
        // and a handler answer without a drain wins over the report's.
        let config = r.shared.config.current();
        let nfs: ResourceId = "basestar-nfs".into();
        let mut rep = r.shared.last_report().unwrap();
        rep.drains = vec![crate::tick::DrainReport {
            resource: nfs.clone(),
            mode: "graceful".into(),
            since: Millis(1),
            scaled_down: false,
            origin: "requested".into(),
        }];
        let mut st = sched_model::ResourceState::new(PowerPhase::ShuttingDown { since: Millis(2) });
        rep.state.resources.insert(nfs.clone(), st.clone());
        let v = resource_state(
            &config,
            &nfs,
            None,
            false,
            None,
            &Some(rep.clone()),
            Millis(9),
        );
        assert_eq!(v["state"], "off", "{v}");
        assert!(v.get("drain").is_none(), "{v}");
        st.power = PowerPhase::On {
            since: Millis(2),
            ours: false,
        };
        rep.state.resources.insert(nfs.clone(), st);
        let v = resource_state(
            &config,
            &nfs,
            None,
            false,
            None,
            &Some(rep.clone()),
            Millis(9),
        );
        assert_eq!(
            (&v["state"], &v["drain"]["mode"]),
            (&json!("draining"), &json!("graceful")),
            "{v}"
        );
        let v = resource_state(
            &config,
            &nfs,
            None,
            false,
            Some(None),
            &Some(rep),
            Millis(9),
        );
        assert_eq!(v["state"], "off", "{v}");
        assert!(v.get("drain").is_none(), "{v}");
    }

    #[tokio::test]
    async fn release_is_a_plain_release_with_no_drain_that_reports_the_real_state() {
        let r = rig(&example_yaml_managed_nfs_only());
        r.hosts_up(true);
        r.tick().await;
        // The adopted lease is released; the host is up, so the reply still reads on.
        assert_eq!(r.lease_count(), 1);
        let (s, v) = r.post("/v1/resources/basestar-nfs/release", "").await;
        assert_eq!(
            (s, &v["held"], &v["state"]),
            (200, &json!(false), &json!("on")),
            "{v}"
        );
        assert!(v.get("drain").is_none(), "{v}");
        assert_eq!(r.lease_count(), 0);
        let core = crate::persist::decode_state(
            r.backend.ctx.stored(crate::persist::STATE_KEY).as_deref(),
        )
        .unwrap();
        assert_eq!(
            core.state
                .resource(&"basestar-nfs".into())
                .unwrap()
                .shutdown,
            None
        );
        // Nothing held: the listener's 404.
        let (s, v) = r.post("/v1/resources/basestar-nfs/release", "").await;
        assert_eq!(s, 404, "{v}");
        assert!(
            v["error"]
                .as_str()
                .unwrap()
                .contains("no manual host lease"),
            "{v}"
        );
        // A lease taken through the API is released the same way.
        r.post("/v1/resources/basestar-nfs/on", "").await;
        assert_eq!(
            r.post("/v1/resources/basestar-nfs/release", "").await.0,
            200
        );
        assert_eq!(r.lease_count(), 0);
        // An unknown host is 404, the exclusive host 404 (nothing to release), a GET 405.
        assert_eq!(r.post("/v1/resources/nope/release", "").await.0, 404);
        assert_eq!(
            r.post("/v1/resources/cuda-central/release", "").await.0,
            404
        );
        assert_eq!(r.get("/v1/resources/basestar-nfs/release").await.0, 405);
        assert_eq!(r.post("/v1/models/llm/release", "").await.0, 404);
    }

    #[tokio::test]
    async fn release_relays_upstream_errors_and_counts_a_failed_release_only_if_the_lease_is_gone()
    {
        let r = rig(&example_yaml_managed_nfs_only());
        let server = TestServer::start().await;
        let listed = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let flag = Arc::clone(&listed);
        server.set_dynamic("/Scheduler/main/leases", move |_| {
            if flag.load(std::sync::atomic::Ordering::SeqCst) {
                TestReply::json(
                    r#"[{"id":"manual-host-1","resource":"basestar-nfs","kind":{"type":"manual_host"},"granted_at":5}]"#,
                )
            } else {
                TestReply::json("[]")
            }
        });
        let t = over(&r, Arc::new(RestateIngress::new(&server.url(""))));
        // The release fails and the lease is still listed: the error is the answer.
        server.set_dynamic("/Scheduler/main/release_host_lease", |_| {
            ingress_reply(500, "boom")
        });
        let (s, v) = ask(&t, Method::POST, "/v1/resources/basestar-nfs/release").await;
        assert_eq!(s, 502, "{v}");
        assert!(v["error"].as_str().unwrap().contains("boom"));
        // The handler's own 409 is relayed.
        server.set_dynamic("/Scheduler/main/release_host_lease", |_| {
            ingress_reply(409, r#"{"code":409,"message":"busy"}"#)
        });
        let (s, v) = ask(&t, Method::POST, "/v1/resources/basestar-nfs/release").await;
        assert_eq!((s, v["error"].as_str()), (409, Some("busy")), "{v}");
        // It fails (404) only because someone released the lease just before: a success.
        let flag = Arc::clone(&listed);
        server.set_dynamic("/Scheduler/main/release_host_lease", move |_| {
            flag.store(false, std::sync::atomic::Ordering::SeqCst);
            ingress_reply(
                404,
                r#"{"code":404,"message":"holds no manual host lease"}"#,
            )
        });
        let (s, v) = ask(&t, Method::POST, "/v1/resources/basestar-nfs/release").await;
        assert_eq!((s, &v["held"]), (200, &json!(false)), "{v}");
    }

    #[tokio::test]
    async fn off_on_a_host_that_is_up_with_no_lease_held_starts_the_drain() {
        let r = rig(&example_yaml_managed_nfs_only());
        r.hosts_up(true);
        r.tick().await;
        // The adopted lease goes (released by id, as a person with the API could); the host
        // is up and held only by whatever work is queued.
        let lease = r.backend.leases().await.unwrap().remove(0);
        r.backend.release_lease(&lease.id).await.unwrap();
        r.tick().await;
        assert_eq!(r.lease_count(), 0);
        let (s, v) = r.post("/v1/resources/basestar-nfs/off", "").await;
        assert_eq!(
            (s, &v["state"], &v["drain"]["mode"]),
            (200, &json!("draining"), &json!("graceful")),
            "{v}"
        );
        // A host that is not up has nothing to drain: a plain off.
        let r = rig(&example_yaml_managed_nfs_only());
        r.hosts_up(true);
        r.push(
            "basestar-probe",
            vec![ObservedValue::ResourceUp {
                resource: "basestar-nfs".into(),
                up: false,
            }],
        );
        r.tick().await;
        let (s, v) = r.post("/v1/resources/basestar-nfs/off-now", "").await;
        assert_eq!((s, &v["state"]), (200, &json!("off")), "{v}");
        assert!(v.get("drain").is_none(), "{v}");
        // The GPU host stays under `control: manual` in this rig: flowmanager never shuts it
        // down, so a request records nothing (and is not refused for being exclusive).
        let (s, v) = r.post("/v1/resources/cuda-central/off-now", "").await;
        assert_eq!((s, v.get("drain").is_none()), (200, true), "{v}");
        let (s, v) = r.post("/v1/resources/cuda-central/off", "").await;
        assert_eq!((s, v.get("drain").is_none()), (200, true), "{v}");
    }

    #[tokio::test]
    async fn the_state_of_a_locked_out_host_reads_locked_out_only_when_it_is_not_seen_off_and_not_held()
     {
        let r = rig(&example_yaml_managed());
        r.hosts_up(true);
        r.tick().await;
        let config = r.shared.config.current();
        let nfs: ResourceId = "basestar-nfs".into();
        let mut rep = r.shared.last_report().unwrap();
        let lease = Lease {
            id: LeaseId::new("manual-host-1"),
            resource: nfs.clone(),
            kind: LeaseKind::ManualHost,
            granted_at: Millis(0),
            origin: sched_model::LeaseOrigin::Requested,
        };
        let state_of = |rep: &TickReport, held: Option<&Lease>, locked: bool| {
            let v = resource_state(
                &config,
                &nfs,
                held,
                locked,
                None,
                &Some(rep.clone()),
                Millis(5_000),
            );
            v["state"].clone()
        };
        let mut set = |power: PowerPhase| {
            rep.state
                .resources
                .insert(nfs.clone(), sched_model::ResourceState::new(power));
            rep.clone()
        };
        // On and locked out, no lease: not `off`.
        let on = set(PowerPhase::On {
            since: Millis(1),
            ours: false,
        });
        assert_eq!(state_of(&on, None, true), json!("locked_out"));
        // The same host seen off: `off` (the truth), locked out or not.
        let off = set(PowerPhase::Off { ours: false });
        assert_eq!(state_of(&off, None, true), json!("off"));
        // A held lease keeps its own meaning, `on`, even while the host is up and locked out.
        assert_eq!(state_of(&on, Some(&lease), true), json!("on"));
        // Not locked out: unchanged.
        assert_eq!(state_of(&on, None, false), json!("off"));
    }

    #[tokio::test]
    async fn the_replies_for_a_locked_out_host_say_so_and_never_read_off_while_it_is_on() {
        for host in ["basestar-nfs", "cuda-central"] {
            let r = rig(&example_yaml_managed());
            r.hosts_up(true);
            r.tick().await;
            let path = format!("/v1/resources/{host}");
            let (_, v) = r.get(&path).await;
            assert_eq!(v["locked_out"], false, "{host}: {v}");
            assert!(!v["wait"].to_string().contains("locked_out"), "{host}: {v}");
            assert_eq!(r.post(&format!("/v1/lockouts/{host}/on"), "").await.0, 200);
            r.tick().await;
            // GET says it, and the host is on.
            let (s, v) = r.get(&path).await;
            assert_eq!((s, &v["locked_out"]), (200, &json!(true)), "{host}: {v}");
            assert!(v["wait"].to_string().contains("locked_out"), "{host}: {v}");
            // off and off-now: 200 as before, but the answer says the lockout and does not
            // read `off` while the host is on (the lockout, not the request, decides).
            for act in ["off", "off-now"] {
                let (s, v) = r.post(&format!("{path}/{act}"), "").await;
                assert_eq!(s, 200, "{host} {act}: {v}");
                assert_eq!(v["locked_out"], true, "{host} {act}: {v}");
                assert!(
                    v["wait"].to_string().contains("locked_out"),
                    "{host} {act}: {v}"
                );
                assert_ne!(v["state"], "off", "{host} {act}: {v}");
                assert!(v.get("drain").is_none() || v["state"] == "draining", "{v}");
            }
            // The listing carries it too, for this host only.
            let (_, list) = r.get("/v1/leases").await;
            for x in list["resources"].as_array().unwrap() {
                assert_eq!(x["locked_out"], x["id"] == host, "{host}: {x}");
            }
            // `on` takes the lease (it does not cancel the lockout) and says so.
            let (s, v) = r.post(&format!("{path}/on"), "").await;
            assert_eq!(
                (s, &v["held"], &v["locked_out"]),
                (200, &json!(true), &json!(true))
            );
            // Cleared: the replies are the ordinary ones again.
            assert_eq!(r.post(&format!("/v1/lockouts/{host}/off"), "").await.0, 200);
            let (_, v) = r.get(&path).await;
            assert_eq!(v["locked_out"], false, "{host}: {v}");
            assert!(!v["wait"].to_string().contains("locked_out"), "{host}: {v}");
        }
    }

    #[tokio::test]
    async fn off_and_off_now_start_a_drain_on_a_managed_exclusive_host_as_on_a_shared_one() {
        let r = rig(&example_yaml_managed());
        r.hosts_up(true);
        r.tick().await;
        // The adopted lease reads on for both kinds; `off` releases it and records a graceful
        // requested shutdown.
        let (s, v) = r.get("/v1/resources/cuda-central").await;
        assert_eq!(
            (s, &v["held"], &v["resource_kind"]),
            (200, &json!(true), &json!("exclusive")),
            "{v}"
        );
        let (s, v) = r.post("/v1/resources/cuda-central/off", "").await;
        assert_eq!(
            (s, &v["held"], &v["state"], &v["drain"]["mode"]),
            (200, &json!(false), &json!("draining"), &json!("graceful")),
            "{v}"
        );
        // `off-now` escalates it in place.
        let (s, v) = r.post("/v1/resources/cuda-central/off-now", "").await;
        assert_eq!(
            (s, &v["state"], &v["drain"]["mode"]),
            (200, &json!("draining"), &json!("immediate")),
            "{v}"
        );
        // The other host is unaffected.
        assert_eq!(r.get("/v1/resources/basestar-nfs").await.1["held"], true);
        // A new manual lease cancels it at the next tick, as on a shared host.
        let (s, v) = r.post("/v1/resources/cuda-central/on", "").await;
        assert_eq!(
            (s, &v["held"], v.get("drain").is_none()),
            (200, &json!(true), true),
            "{v}"
        );
    }

    #[tokio::test]
    async fn the_listing_has_every_model_and_every_powered_resource() {
        let r = rig(EXAMPLE_CONFIG);
        r.post("/v1/models/whisperx/on", "").await;
        r.post("/v1/resources/basestar-nfs/on", "").await;
        let (s, v) = r.get("/v1/leases").await;
        assert_eq!(s, 200);
        let models = v["models"].as_array().unwrap();
        let ids: Vec<&str> = models.iter().map(|m| m["id"].as_str().unwrap()).collect();
        assert_eq!(ids, ["mineru", "llm", "whisperx", "immich-ml"]);
        let held: Vec<bool> = models
            .iter()
            .map(|m| m["held"].as_bool().unwrap())
            .collect();
        assert_eq!(held, [false, false, true, false]);
        let resources = v["resources"].as_array().unwrap();
        let by_id = |id: &str| resources.iter().find(|x| x["id"] == id).unwrap();
        assert_eq!(by_id("basestar-nfs")["held"], true);
        assert_eq!(by_id("cuda-central")["held"], false);
        // No lease ids anywhere.
        assert!(!v.to_string().contains("manual-"), "{v}");
    }

    #[tokio::test]
    async fn the_status_follows_the_last_tick_and_is_left_out_when_it_cannot_be_said() {
        let r = rig(EXAMPLE_CONFIG);
        r.hosts_up(true);
        let (_, v) = r.post("/v1/models/llm/on", "").await;
        // No tick in this process yet: nothing honest to say beyond "held".
        assert!(v.get("status").is_none(), "{v}");
        r.tick().await;
        let (_, v) = r.get("/v1/models/llm").await;
        assert_eq!(v["status"], "active", "{v}");
        assert!(v["detail"].is_string());
        // A lease taken after the last tick is not yet known to it.
        let (_, v) = r.post("/v1/resources/basestar-nfs/on", "").await;
        assert_eq!(v["power"]["phase"], "on", "{v}");
        assert_eq!(v["needs_attention"], false);
        assert_eq!(v["wait"], json!([]));
        // The GPU host goes down: the held model waits for it.
        r.hosts_up(false);
        r.tick().await;
        let (_, v) = r.get("/v1/models/llm").await;
        assert_eq!(v["status"], "waiting_for_host", "{v}");
        assert!(v["detail"].as_str().unwrap().contains("cuda-central"));
        // Released: no status for a switch that is off.
        let (_, v) = r.post("/v1/models/llm/off", "").await;
        assert!(v.get("status").is_none(), "{v}");

        // A lease newer than the report is "pending", not guessed at.
        let (_, v) = r.post("/v1/models/mineru/on", "").await;
        assert_eq!(v["status"], "pending", "{v}");
    }

    #[tokio::test]
    async fn the_status_names_a_wind_down_and_a_failed_host() {
        use sched_model::{ResourceState, WindDown};
        let r = rig(EXAMPLE_CONFIG);
        r.hosts_up(true);
        r.tick().await;
        let lease = r.backend.acquire_model(&"llm".into()).await.unwrap().lease;
        let mut rep = r.shared.last_report().unwrap();
        rep.leases.push(lease.clone());
        rep.allowed_models.clear();
        let status =
            |rep: &TickReport| model_status(rep, &lease, &"llm".into(), &"cuda-central".into());
        let gpu: ResourceId = "cuda-central".into();

        let mut st = ResourceState::new(PowerPhase::On {
            since: Millis(1),
            ours: false,
        });
        st.mode = ModePhase::Serving {
            model: "mineru".into(),
            since: Millis(1),
            idle_since: None,
            wind_down: Some(WindDown {
                since: Millis(2),
                timeout: Some(Millis(600_000)),
                lockout: false,
                killed: Default::default(),
            }),
        };
        rep.state.resources.insert(gpu.clone(), st);
        let (s, detail) = status(&rep);
        assert_eq!(s, "draining");
        assert!(detail.contains("mineru"), "{detail}");

        // Serving another model with no wind-down: only the fact is given.
        if let Some(ModePhase::Serving { wind_down, .. }) =
            rep.state.resources.get_mut(&gpu).map(|s| &mut s.mode)
        {
            *wind_down = None;
        }
        assert_eq!(status(&rep).0, "pending");

        let failed = PowerPhase::Failed {
            since: Millis(1),
            during: sched_model::PowerTransition::PoweringOn,
        };
        rep.state
            .resources
            .insert(gpu.clone(), ResourceState::new(failed));
        assert_eq!(status(&rep).0, "host_failed");
        rep.state.resources.insert(
            gpu,
            ResourceState::new(PowerPhase::PoweringOn { since: Millis(1) }),
        );
        let (s, detail) = status(&rep);
        assert_eq!(s, "waiting_for_host");
        assert!(detail.contains("powering_on"), "{detail}");
    }

    #[tokio::test]
    async fn served_over_http_with_a_bounded_body() {
        let r = rig(EXAMPLE_CONFIG);
        let toggle = Arc::new(Toggle::new(
            Arc::clone(&r.shared),
            Arc::clone(&r.backend) as Arc<dyn Backend>,
            r.token_file.clone(),
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(serve(listener, toggle, Limits::default()));
        let url = |p: &str| format!("http://{addr}{p}");
        let client = reqwest::Client::new();

        let denied = client.get(url("/v1/leases")).send().await.unwrap();
        assert_eq!(denied.status(), 401);
        assert_eq!(denied.headers()["www-authenticate"], "Bearer");

        let on = client
            .post(url("/v1/models/llm"))
            .bearer_auth(TOKEN)
            .body("ON")
            .send()
            .await
            .unwrap();
        assert_eq!(on.status(), 200);
        assert_eq!(on.headers()["content-type"], "application/json");
        let v: Value = on.json().await.unwrap();
        assert_eq!(v["held"], true);
        let v: Value = client
            .get(url("/v1/leases"))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(v["models"][1]["held"], true);

        let big = client
            .post(url("/v1/models/llm"))
            .bearer_auth(TOKEN)
            .body("x".repeat(MAX_BODY + 1))
            .send()
            .await
            .unwrap();
        assert_eq!(big.status(), 400);
        assert_eq!(r.lease_count(), 1);
        task.abort();
    }

    fn ingress_reply(status: u16, body: &str) -> TestReply {
        let mut reply = TestReply::status(status);
        reply.body = body.to_string();
        reply
    }

    #[tokio::test]
    async fn the_ingress_backend_calls_the_handlers_and_maps_their_refusals() {
        let server = TestServer::start().await;
        let ingress = RestateIngress::new(&server.url(""));
        let lease = r#"{"id":"manual-model-1","resource":"cuda-central","kind":{"type":"manual_model","model":"llm"},"granted_at":5}"#;
        server.set(
            "/Scheduler/main/acquire_model_lease",
            TestReply::json(&format!(r#"{{"lease":{lease},"created":true}}"#)),
        );
        server.set(
            "/Scheduler/main/leases",
            TestReply::json(&format!("[{lease}]")),
        );
        server.set(
            "/Scheduler/main/release_lease",
            ingress_reply(404, r#"{"code":404,"message":"no lease with id"}"#),
        );
        server.set(
            "/Scheduler/main/acquire_host_lease",
            ingress_reply(409, r#"{"code":409,"message":"held by another"}"#),
        );
        server.set("/Scheduler/main/drain_host", ingress_reply(500, "boom"));
        server.set(
            "/Scheduler/main/resume",
            ingress_reply(400, "not json, still carried"),
        );

        let grant = ingress.acquire_model(&"llm".into()).await.unwrap();
        assert!(grant.created && grant.lease.id.as_str() == "manual-model-1");
        assert_eq!(ingress.leases().await.unwrap().len(), 1);
        let e = ingress.release_lease(&"x".into()).await.unwrap_err();
        // A 404 is never relayed as a handler answer (Restate gives it for unknown services).
        assert_eq!(e.status, 502);
        assert!(
            e.message.contains("404") && e.message.contains("no lease with id"),
            "{e:?}"
        );
        let e = ingress.acquire_host(&"nfs".into()).await.unwrap_err();
        assert_eq!((e.status, e.message.as_str()), (409, "held by another"));
        let e = ingress
            .drain_host(&"nfs".into(), DrainMode::Graceful)
            .await
            .unwrap_err();
        assert_eq!(e.status, 502, "{e:?}");
        assert!(e.message.contains("500") && e.message.contains("boom"));
        let e = ingress.resume(&"nfs".into()).await.unwrap_err();
        assert_eq!(
            (e.status, e.message.as_str()),
            (400, "not json, still carried")
        );

        // Every call is a POST to the Scheduler object's key, nothing else.
        assert!(server.methods().iter().all(|m| m == "POST"));
        assert!(
            server
                .requests()
                .iter()
                .all(|(p, _)| p.starts_with("/Scheduler/main/"))
        );

        let free = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", free.local_addr().unwrap());
        drop(free);
        let e = RestateIngress::new(&url).leases().await.unwrap_err();
        assert_eq!(e.status, 503, "{e:?}");
        assert!(
            !e.message.contains("127.0.0.1"),
            "no address in the message: {e:?}"
        );
    }

    #[tokio::test]
    async fn a_lockout_switch_is_idempotent_on_either_kind_of_host() {
        let r = rig(&example_yaml_managed_nfs_only());
        let (s, v) = r.get("/v1/lockouts/cuda-central").await;
        assert_eq!(
            (s, &v["held"], &v["supported"]),
            (200, &json!(false), &json!(true))
        );
        let (s, v) = r.post("/v1/lockouts/cuda-central/on", "").await;
        assert_eq!(
            (s, &v["held"], &v["kind"]),
            (200, &json!(true), &json!("lockout"))
        );
        assert_eq!(r.post("/v1/lockouts/cuda-central", "ON").await.0, 200);
        assert_eq!(r.lease_count(), 1);
        // A manual host lease and a lockout are different switches.
        assert_eq!(r.get("/v1/resources/cuda-central").await.1["held"], false);
        let (s, v) = r.post("/v1/lockouts/cuda-central/off", "").await;
        assert_eq!((s, &v["held"]), (200, &json!(false)));
        let (s, v) = r.post("/v1/lockouts/cuda-central/off", "").await;
        assert_eq!((s, &v["held"]), (200, &json!(false)));
        assert_eq!(r.lease_count(), 0);
        // A shared host can be locked out too, and its lockout is its own switch.
        let (s, v) = r.get("/v1/lockouts/basestar-nfs").await;
        assert_eq!(
            (s, &v["held"], &v["supported"]),
            (200, &json!(false), &json!(true))
        );
        let (s, v) = r.post("/v1/lockouts/basestar-nfs/on", "").await;
        assert_eq!(
            (s, &v["held"], &v["kind"]),
            (200, &json!(true), &json!("lockout"))
        );
        assert_eq!(r.lease_count(), 1);
        assert_eq!(r.get("/v1/resources/basestar-nfs").await.1["held"], false);
        let (s, v) = r.post("/v1/lockouts/basestar-nfs/off", "").await;
        assert_eq!((s, &v["held"]), (200, &json!(false)));
        assert_eq!(r.lease_count(), 0);
        assert_eq!(r.post("/v1/lockouts/nowhere/on", "").await.0, 404);
        assert_eq!(r.post("/v1/lockouts/nowhere/off", "").await.0, 404);

        // Listed apart from the host leases, one entry per host with a power section.
        r.post("/v1/lockouts/cuda-central/on", "").await;
        let (_, v) = r.get("/v1/leases").await;
        let lockouts = v["lockouts"].as_array().unwrap();
        assert_eq!(lockouts.len(), 2);
        let by_id = |id: &str| lockouts.iter().find(|x| x["id"] == id).unwrap();
        assert_eq!(by_id("cuda-central")["held"], true);
        assert_eq!(by_id("basestar-nfs")["held"], false);
        assert_eq!(v["needs_attention"], false);
    }

    #[tokio::test]
    async fn a_lockout_status_and_needs_attention_come_from_the_report() {
        use sched_model::ResourceState;
        let lease = Lease {
            id: "lockout-1".into(),
            resource: "cuda-central".into(),
            kind: LeaseKind::Lockout,
            granted_at: Millis(1),
            origin: sched_model::LeaseOrigin::Requested,
        };
        let gpu: ResourceId = "cuda-central".into();
        let r = rig(&example_yaml_managed());
        let config = r.shared.config.current();
        r.hosts_up(true);
        r.tick().await;
        let mut rep = r.shared.last_report().unwrap();
        rep.leases.clear();
        rep.veto_held.clear();
        let at = |rep: &TickReport| {
            lockout_state(
                &config,
                &gpu,
                Some(&lease),
                &Some(rep.clone()),
                Millis(5_000),
            )
        };
        // Not in the report's leases yet.
        assert_eq!(at(&rep)["status"], "pending");
        rep.leases.push(lease.clone());
        rep.reason = "locked_out".into();
        for (power, want) in [
            (PowerPhase::Off { ours: false }, "powered_off"),
            (
                PowerPhase::ShuttingDown { since: Millis(1) },
                "shutting_down",
            ),
            (
                PowerPhase::On {
                    since: Millis(1),
                    ours: false,
                },
                "pending",
            ),
        ] {
            rep.state
                .resources
                .insert(gpu.clone(), ResourceState::new(power));
            let v = at(&rep);
            assert_eq!(v["status"], want, "{v}");
            assert_eq!(v["needs_attention"], false);
            assert_eq!(v["tick_reason"], "locked_out");
            assert_eq!(v["held_for_seconds"], 4);
        }
        rep.state.resources.insert(
            gpu.clone(),
            ResourceState::new(PowerPhase::Failed {
                since: Millis(1),
                during: sched_model::PowerTransition::ShuttingDown,
            }),
        );
        let v = at(&rep);
        assert_eq!(
            (&v["status"], &v["needs_attention"]),
            (&json!("host_failed"), &json!(true))
        );

        // A shutdown stopped by the veto needs a person too (a host resource).
        let nfs: ResourceId = "basestar-nfs".into();
        r.hosts_up(true);
        r.tick().await;
        let mut rep = r.shared.last_report().unwrap();
        rep.leases.clear();
        rep.veto_held.clear();
        rep.state.resources.insert(
            nfs.clone(),
            ResourceState::new(PowerPhase::On {
                since: Millis(1),
                ours: false,
            }),
        );
        let v = resource_state(
            &config,
            &nfs,
            None,
            false,
            None,
            &Some(rep.clone()),
            Millis(5_000),
        );
        assert_eq!(v["needs_attention"], false);
        rep.veto_held.push(crate::tick::VetoHeld {
            readings: vec!["mounts".to_string()],
            blocking: vec![],
            resource: nfs.clone(),
            since: Millis(1),
            stopped: true,
        });
        let v = resource_state(&config, &nfs, None, false, None, &Some(rep), Millis(5_000));
        assert_eq!(v["needs_attention"], true);
        assert_eq!(v["wait"][0]["reason"], "shutdown_held_by_veto");
    }

    #[test]
    fn every_change_request_and_every_refusal_has_one_log_record_and_ok_reads_none() {
        let l = |m: &str, p: &str, want, outcome, status| {
            ToggleLog::of(&route(&m.parse().unwrap(), p), p, want, outcome, status)
        };
        let got = l("POST", "/v1/models/llm/on", None, "ok", 200).unwrap();
        assert_eq!(
            (
                got.kind,
                got.name.as_str(),
                got.action,
                got.outcome,
                got.status
            ),
            ("model", "llm", "on", "ok", 200)
        );
        let got = l(
            "POST",
            "/v1/lockouts/cuda-central",
            Some(Some(false)),
            "ok",
            200,
        )
        .unwrap();
        assert_eq!((got.kind, got.action), ("lockout", "off"));
        let got = l("POST", "/v1/resources/nfs", Some(None), "bad_request", 400).unwrap();
        assert_eq!((got.kind, got.action), ("host", "unknown"));
        let got = l("POST", "/v1/resources/nfs/resume", None, "conflict", 409).unwrap();
        assert_eq!((got.kind, got.action, got.status), ("host", "resume", 409));
        // Refused before authorization: the wish is not read, the path's own is kept.
        let got = l("POST", "/v1/models/llm/off", None, "unauthorized", 401).unwrap();
        assert_eq!((got.action, got.outcome), ("off", "unauthorized"));
        // Refused reads, listings and unknown paths are logged too; the path is shortened.
        let got = l("GET", "/v1/models/llm", None, "unauthorized", 401).unwrap();
        assert_eq!((got.kind, got.action), ("model", "get"));
        let got = l("GET", "/v1/leases", None, "unauthorized", 401).unwrap();
        assert_eq!(got.kind, "list");
        let long = format!("/{}", "x".repeat(500));
        let got = l("GET", &long, None, "not_found", 404).unwrap();
        assert_eq!((got.kind, got.name.chars().count()), ("unmatched", 64));
        let got = l("PUT", "/v1/leases", None, "method_not_allowed", 405).unwrap();
        assert_eq!(got.status, 405);
        // Only a read or listing that went well is silent.
        for (m, p) in [("GET", "/v1/models/llm"), ("GET", "/v1/leases")] {
            assert!(l(m, p, None, "ok", 200).is_none(), "{m} {p}");
        }
    }

    #[test]
    fn a_burst_of_refusals_is_limited_and_the_next_line_counts_what_was_left_out() {
        let t0 = std::time::Instant::now();
        let l = LogLimiter::new(t0);
        for i in 0..LOG_BURST {
            assert_eq!(l.admit(t0, true), Some(0), "{i}");
        }
        assert_eq!(l.admit(t0, true), None);
        assert_eq!(l.admit(t0, true), None);
        // A change that went well is always logged and does not disturb the count.
        assert_eq!(l.admit(t0, false), Some(0));
        assert_eq!(l.admit(t0 + LOG_WINDOW, true), Some(2));
        assert_eq!(l.admit(t0 + LOG_WINDOW, true), Some(0));
    }

    #[test]
    fn a_percent_escape_is_exactly_two_hex_digits() {
        assert_eq!(percent_decode("a%41b").as_deref(), Some("aAb"));
        for bad in ["%+1", "%-1", "%1", "%", "%zz", "% 1", "%ff"] {
            assert_eq!(percent_decode(bad), None, "{bad}");
        }
        assert_eq!(route(&Method::POST, "/v1/models/%+1/on"), Route::NotFound);
    }

    fn over(r: &Rig, backend: Arc<dyn Backend>) -> Toggle {
        Toggle::new(Arc::clone(&r.shared), backend, r.token_file.clone())
    }

    async fn ask(t: &Toggle, method: Method, path: &str) -> (u16, Value) {
        let auth = format!("Bearer {TOKEN}");
        let r = t.handle(&method, path, Some(&auth), "ON").await;
        (r.status, serde_json::from_str(&r.body).unwrap())
    }

    #[tokio::test]
    async fn a_scheduler_that_is_not_registered_is_an_error_never_an_answer() {
        let r = rig(&example_yaml_managed_nfs_only());
        let server = TestServer::start().await;
        for h in [
            "leases",
            "acquire_model_lease",
            "release_lease",
            "acquire_host_lease",
            "drain_host",
            "lockout",
            "clear_lockout",
            "resume",
        ] {
            server.set(
                &format!("/Scheduler/main/{h}"),
                ingress_reply(
                    404,
                    r#"{"code":404,"message":"Service 'Scheduler' not found"}"#,
                ),
            );
        }
        let t = over(&r, Arc::new(RestateIngress::new(&server.url(""))));
        for (m, p) in [
            (Method::GET, "/v1/models/llm"),
            (Method::GET, "/v1/leases"),
            (Method::GET, "/v1/resources/basestar-nfs"),
            (Method::GET, "/v1/lockouts/cuda-central"),
            (Method::POST, "/v1/models/llm/on"),
            (Method::POST, "/v1/models/llm/off"),
            (Method::POST, "/v1/resources/basestar-nfs/on"),
            (Method::POST, "/v1/resources/basestar-nfs/off"),
            (Method::POST, "/v1/resources/basestar-nfs/resume"),
            (Method::POST, "/v1/lockouts/cuda-central/on"),
            (Method::POST, "/v1/lockouts/cuda-central/off"),
        ] {
            let (s, v) = ask(&t, m.clone(), p).await;
            assert_eq!(s, 502, "{m} {p}: {v}");
            assert!(v["error"].as_str().unwrap().contains("404"), "{v}");
            assert!(v.get("held").is_none(), "{m} {p}: {v}");
        }
    }

    #[tokio::test]
    async fn a_failed_drain_request_is_the_answer_and_a_refusal_keeps_its_status() {
        let r = rig(&example_yaml_managed_nfs_only());
        let server = TestServer::start().await;
        server.set_dynamic("/Scheduler/main/drain_host", |_| ingress_reply(500, "boom"));
        let t = over(&r, Arc::new(RestateIngress::new(&server.url(""))));
        let (s, v) = ask(&t, Method::POST, "/v1/resources/basestar-nfs/off").await;
        assert_eq!(s, 502, "{v}");
        assert!(v["error"].as_str().unwrap().contains("boom"));
        // The handler's own 400 is relayed.
        server.set_dynamic("/Scheduler/main/drain_host", |_| {
            ingress_reply(400, r#"{"code":400,"message":"not a shared resource"}"#)
        });
        let (s, v) = ask(&t, Method::POST, "/v1/resources/basestar-nfs/off-now").await;
        assert_eq!(s, 400, "{v}");
        assert!(v["error"].as_str().unwrap().contains("shared"));
    }

    #[tokio::test]
    async fn the_body_is_read_only_after_authentication_and_only_when_it_is_needed() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let r = rig(EXAMPLE_CONFIG);
        let polled = Arc::new(AtomicBool::new(false));
        let reader = |p: &Arc<AtomicBool>, out: Option<String>| {
            let p = Arc::clone(p);
            async move {
                p.store(true, Ordering::SeqCst);
                out
            }
        };
        let auth = format!("Bearer {TOKEN}");
        // Not authenticated: refused, and the (oversize) body is never touched.
        let reply = r
            .toggle
            .handle_with(&Method::POST, "/v1/models/llm", None, reader(&polled, None))
            .await;
        assert_eq!(reply.status, 401);
        assert!(!polled.load(Ordering::SeqCst));
        // Authenticated, body too large or unreadable: 400, counted, nothing done.
        let reply = r
            .toggle
            .handle_with(
                &Method::POST,
                "/v1/models/llm",
                Some(&auth),
                reader(&polled, None),
            )
            .await;
        assert_eq!(reply.status, 400);
        assert!(
            reply.body.contains("larger than 1024 bytes"),
            "{}",
            reply.body
        );
        assert!(polled.load(Ordering::SeqCst));
        assert_eq!(r.lease_count(), 0);
        let text = r.shared.metrics.render();
        assert!(
            text.contains(
                "sched_toggle_requests_total{endpoint=\"model_set\",outcome=\"bad_request\"} 1"
            ),
            "{text}"
        );
        assert!(
            text.contains(
                "sched_toggle_requests_total{endpoint=\"model_set\",outcome=\"unauthorized\"} 1"
            ),
            "{text}"
        );
        // A path that carries its wish never reads the body.
        polled.store(false, Ordering::SeqCst);
        let reply = r
            .toggle
            .handle_with(
                &Method::POST,
                "/v1/models/llm/on",
                Some(&auth),
                reader(&polled, None),
            )
            .await;
        assert_eq!(reply.status, 200);
        assert!(!polled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn the_body_cap_is_exact_and_a_405_names_the_methods_it_allows() {
        let r = rig(EXAMPLE_CONFIG);
        let toggle = Arc::new(over(&r, Arc::clone(&r.backend) as Arc<dyn Backend>));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(serve(listener, toggle, Limits::default()));
        let url = format!("http://{addr}/v1/models/llm");
        let client = reqwest::Client::new();
        let post = |body: Vec<u8>, token: bool| {
            let mut req = client.post(&url).body(body);
            if token {
                req = req.bearer_auth(TOKEN);
            }
            async move { req.send().await.unwrap().status().as_u16() }
        };
        let padded = |n: usize| {
            let mut b = b"ON".to_vec();
            b.resize(n, b' ');
            b
        };
        assert_eq!(post(padded(MAX_BODY), true).await, 200);
        assert_eq!(post(padded(MAX_BODY + 1), true).await, 400);
        assert_eq!(post(padded(MAX_BODY + 1), false).await, 401);
        assert_eq!(post(vec![0xff, 0xfe], true).await, 400);
        assert_eq!(post(vec![0xff, 0xfe], false).await, 401);
        assert_eq!(r.lease_count(), 1);

        for (method, path, allow) in [
            (Method::DELETE, "/v1/models/llm", "GET, POST"),
            (Method::GET, "/v1/models/llm/on", "POST"),
            (Method::POST, "/v1/leases", "GET"),
        ] {
            let resp = client
                .request(method, format!("http://{addr}{path}"))
                .bearer_auth(TOKEN)
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 405, "{path}");
            assert_eq!(resp.headers()["allow"], allow, "{path}");
        }
        task.abort();
    }

    #[tokio::test]
    async fn idle_connections_are_dropped_by_the_header_timeout_and_do_not_starve_the_listener() {
        use tokio::io::AsyncReadExt;
        let r = rig(EXAMPLE_CONFIG);
        let toggle = Arc::new(over(&r, Arc::clone(&r.backend) as Arc<dyn Backend>));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let limits = Limits {
            max_connections: 2,
            connection_timeout: Duration::from_secs(30),
            header_read_timeout: Duration::from_millis(300),
        };
        let task = tokio::spawn(serve(listener, toggle, limits));
        let mut idle = vec![];
        for _ in 0..2 {
            idle.push(tokio::net::TcpStream::connect(addr).await.unwrap());
        }
        let started = std::time::Instant::now();
        let served = reqwest::Client::new()
            .get(format!("http://{addr}/v1/leases"))
            .bearer_auth(TOKEN)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .expect("served once the idle connections are cut off")
            .status();
        assert_eq!(served, 200);
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "{:?}",
            started.elapsed()
        );
        for mut s in idle {
            let mut buf = [0u8; 64];
            let n = tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf))
                .await
                .expect("closed by the server");
            assert!(matches!(n, Ok(_) | Err(_)));
        }
        task.abort();
    }

    #[tokio::test]
    async fn a_connection_is_closed_at_the_connection_timeout_even_if_it_keeps_talking() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let r = rig(EXAMPLE_CONFIG);
        let toggle = Arc::new(over(&r, Arc::clone(&r.backend) as Arc<dyn Backend>));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let limits = Limits {
            max_connections: 4,
            connection_timeout: Duration::from_millis(400),
            header_read_timeout: Duration::from_secs(5),
        };
        let task = tokio::spawn(serve(listener, toggle, limits));
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req =
            format!("GET /v1/leases HTTP/1.1\r\nhost: x\r\nauthorization: Bearer {TOKEN}\r\n\r\n");
        s.write_all(req.as_bytes()).await.unwrap();
        let mut buf = vec![0u8; 8192];
        let n = s.read(&mut buf).await.unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).starts_with("HTTP/1.1 200"));
        // Keep-alive: nothing more is sent; the server ends the connection at its limit.
        let started = std::time::Instant::now();
        let end = tokio::time::timeout(Duration::from_secs(3), s.read(&mut buf))
            .await
            .expect("closed at the connection timeout");
        assert!(matches!(end, Ok(0) | Err(_)));
        assert!(
            started.elapsed() >= Duration::from_millis(150),
            "{:?}",
            started.elapsed()
        );
        task.abort();
    }

    #[tokio::test]
    async fn an_ingress_call_gives_up_at_its_timeout() {
        let server = TestServer::start().await;
        server.set(
            "/Scheduler/main/leases",
            TestReply::json("[]").delayed(Duration::from_millis(800)),
        );
        let slow = RestateIngress::with_timeout(&server.url(""), Duration::from_millis(100));
        let started = std::time::Instant::now();
        let e = slow.leases().await.unwrap_err();
        assert_eq!(e.status, 504, "{e:?}");
        assert!(started.elapsed() < Duration::from_millis(700));
        let patient = RestateIngress::with_timeout(&server.url(""), Duration::from_secs(5));
        assert!(patient.leases().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_model_lease_on_a_locked_out_resource_says_it_is_locked_out() {
        let r = rig(&example_yaml_managed_nfs_only());
        r.hosts_up(true);
        assert_eq!(r.post("/v1/models/llm/on", "").await.0, 200);
        assert_eq!(r.post("/v1/lockouts/cuda-central/on", "").await.0, 200);
        let check = |v: &Value| {
            assert_eq!(v["status"], "locked_out", "{v}");
            assert!(v["detail"].as_str().unwrap().contains("locked out"));
        };
        check(&r.get("/v1/models/llm").await.1);
        check(&r.post("/v1/models/llm/on", "").await.1);
        r.tick().await;
        let (_, v) = r.get("/v1/models/llm").await;
        check(&v);
        assert!(v["detail"].as_str().unwrap().contains("power:"), "{v}");
        let (_, list) = r.get("/v1/leases").await;
        check(&list["models"][1]);
        // Other models on it hold no lease, so no status.
        assert!(list["models"][0].get("status").is_none());
        // Released, the lockout no longer answers for the model.
        assert_eq!(r.post("/v1/lockouts/cuda-central/off", "").await.0, 200);
        let (_, v) = r.get("/v1/models/llm").await;
        assert_ne!(v["status"], "locked_out", "{v}");
    }

    #[tokio::test]
    async fn a_lockout_on_a_manual_host_is_left_for_a_person_to_power_off() {
        let r = rig(EXAMPLE_CONFIG);
        r.hosts_up(true);
        assert_eq!(r.post("/v1/lockouts/cuda-central/on", "").await.0, 200);
        r.tick().await;
        let (_, v) = r.get("/v1/lockouts/cuda-central").await;
        assert_eq!(v["status"], "left_for_person", "{v}");
        assert!(
            v["detail"]
                .as_str()
                .unwrap()
                .contains("power it off yourself")
        );

        // Under managed control the same state is not that: flowmanager does the shutdown.
        let m = rig(&example_yaml_managed());
        m.hosts_up(true);
        assert_eq!(m.post("/v1/lockouts/cuda-central/on", "").await.0, 200);
        m.tick().await;
        let (_, v) = m.get("/v1/lockouts/cuda-central").await;
        assert_ne!(v["status"], "left_for_person", "{v}");
        assert!(v.get("status").is_some());
    }

    #[tokio::test]
    async fn a_tick_sets_the_lease_age_gauge_and_a_release_removes_the_series() {
        let r = rig(EXAMPLE_CONFIG);
        r.hosts_up(true);
        r.post("/v1/models/llm/on", "").await;
        r.post("/v1/resources/basestar-nfs/on", "").await;
        r.post("/v1/lockouts/cuda-central/on", "").await;
        assert!(
            !r.shared
                .metrics
                .render()
                .contains("sched_manual_lease_age_seconds{")
        );
        r.tick().await;
        let text = r.shared.metrics.render();
        for series in [
            "kind=\"model\",target=\"llm\"",
            "kind=\"host\",target=\"basestar-nfs\"",
            "kind=\"lockout\",target=\"cuda-central\"",
        ] {
            assert!(
                text.contains(&format!("sched_manual_lease_age_seconds{{{series}}}")),
                "{series}: {text}"
            );
        }
        r.post("/v1/models/llm/off", "").await;
        r.tick().await;
        let text = r.shared.metrics.render();
        assert!(!text.contains("target=\"llm\""), "{text}");
        assert!(text.contains("target=\"basestar-nfs\""));
    }

    #[tokio::test]
    async fn the_resume_and_conflict_labels_are_counted() {
        let r = rig(&example_yaml_managed_nfs_only());
        assert_eq!(r.post("/v1/resources/basestar-nfs/resume", "").await.0, 409);
        r.post("/v1/models/llm/on", "").await;
        assert_eq!(r.post("/v1/models/mineru/on", "").await.0, 409);
        assert_eq!(r.get("/v1/models/nope").await.0, 404);
        let text = r.shared.metrics.render();
        for series in [
            "endpoint=\"resource_resume\",outcome=\"conflict\"",
            "endpoint=\"model_on\",outcome=\"conflict\"",
            "endpoint=\"model_on\",outcome=\"ok\"",
            "endpoint=\"model_get\",outcome=\"not_found\"",
        ] {
            assert!(
                text.contains(&format!("sched_toggle_requests_total{{{series}}} 1")),
                "{series}: {text}"
            );
        }
    }
}
