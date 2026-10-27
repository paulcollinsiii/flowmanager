//! The plumbing shared by the small HTTP listeners (the toggle listener and the webhook
//! listener): a bounded accept loop, per-connection time limits, a capped body read that is
//! only polled once the caller has been authenticated, a rate limit for refusal log lines,
//! and the bearer-token helpers.
//!
//! Each listener supplies a handler that turns a request (method, path, `Authorization`
//! header and a lazily read body) into a [`Reply`]; nothing here knows what a request means.

use std::convert::Infallible;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use hyper::body::{Body, Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::{TokioIo, TokioTimer};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

use crate::metrics_server::OneShot;

/// Keeps a burst of refused requests (a scan, a wrong token tried in a loop) from flooding
/// the log: at most `LOG_BURST` refusals are logged per `LOG_WINDOW`, and the next line
/// that is logged says how many were left out. Changes that succeed are always logged.
pub struct LogLimiter(std::sync::Mutex<(std::time::Instant, u32, u32)>);

pub const LOG_BURST: u32 = 30;
pub const LOG_WINDOW: Duration = Duration::from_secs(10);

impl LogLimiter {
    pub fn new(now: std::time::Instant) -> LogLimiter {
        LogLimiter(std::sync::Mutex::new((now, 0, 0)))
    }

    /// `Some(left_out)` if the line is to be logged.
    pub fn admit(&self, now: std::time::Instant, refused: bool) -> Option<u32> {
        let mut s = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if !refused {
            return Some(0);
        }
        if now.duration_since(s.0) >= LOG_WINDOW {
            s.0 = now;
            s.1 = 0;
        }
        if s.1 < LOG_BURST {
            s.1 += 1;
            Some(std::mem::take(&mut s.2))
        } else {
            s.2 += 1;
            None
        }
    }
}

/// Equality that takes the same time wherever two inputs differ (the length is not hidden).
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        diff |= usize::from(a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0));
    }
    std::hint::black_box(diff) == 0
}

/// The token in an `Authorization: Bearer <token>` header value.
pub fn bearer(header: Option<&str>) -> Option<&str> {
    let h = header?.trim();
    let (scheme, token) = h.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then_some(token.trim())
}

/// A finished answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    pub status: u16,
    pub body: String,
    /// Sent with a 401 so a client knows what is wanted.
    pub challenge: bool,
    /// The methods a 405 allows (its `Allow` header).
    pub allow: Option<&'static str>,
}

impl Reply {
    pub fn json(status: u16, v: Value) -> Reply {
        Reply {
            status,
            body: v.to_string(),
            challenge: false,
            allow: None,
        }
    }

    /// `{"error": message}` with `status`.
    pub fn failure(status: u16, message: &str) -> Reply {
        Reply::json(status, json!({ "error": message }))
    }

    /// The 401 that asks for a bearer token.
    pub fn unauthorized() -> Reply {
        Reply {
            challenge: true,
            ..Reply::failure(401, "a valid bearer token is required")
        }
    }
}

/// The token in `file`, trimmed; `None` for a missing, unreadable or empty file (the caller
/// answers 503), never a match. `what` names the token in the log line.
pub async fn read_token(file: &std::path::Path, what: &str) -> Option<String> {
    match tokio::fs::read_to_string(file).await {
        Ok(t) if !t.trim().is_empty() => Some(t.trim().to_string()),
        Ok(_) => {
            tracing::error!(path = %file.display(), "the {what} token file is empty");
            None
        }
        Err(e) => {
            tracing::error!(path = %file.display(), "the {what} token file cannot be read: {e}");
            None
        }
    }
}

/// Limits on a listener (the metrics listener's, see `metrics_server`).
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_connections: usize,
    pub connection_timeout: Duration,
    /// How long a client may take to send the headers of a request. Short, and apart from
    /// `connection_timeout`, so that connections that say nothing cannot hold the slots.
    pub header_read_timeout: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        // A request may wait on a tick holding the object's lock, so it gets longer than
        // a metrics scrape.
        Limits {
            max_connections: 64,
            connection_timeout: Duration::from_secs(30),
            header_read_timeout: Duration::from_secs(5),
        }
    }
}

/// Why a body could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyError {
    /// More than the cap arrived (reading stopped there).
    TooLarge,
    /// The connection failed mid-body.
    Unreadable,
}

/// Reads at most `max` bytes.
pub async fn read_capped(mut body: Incoming, max: usize) -> Result<Vec<u8>, BodyError> {
    let mut buf = Vec::new();
    while let Some(frame) = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
        let frame = frame.map_err(|_| BodyError::Unreadable)?;
        if let Some(data) = frame.data_ref() {
            if buf.len() + data.len() > max {
                return Err(BodyError::TooLarge);
            }
            buf.extend_from_slice(data);
        }
    }
    Ok(buf)
}

/// Reads at most `max` bytes; `None` if the body is larger, fails, or is not UTF-8.
pub async fn read_body(body: Incoming, max: usize) -> Option<String> {
    String::from_utf8(read_capped(body, max).await.ok()?).ok()
}

fn response(reply: Reply) -> Response<OneShot> {
    let mut b = Response::builder()
        .status(reply.status)
        .header("content-type", "application/json")
        .header("cache-control", "no-store");
    if let Some(allow) = reply.allow {
        b = b.header("allow", allow);
    }
    if reply.challenge {
        b = b.header("www-authenticate", "Bearer");
    }
    b.body(OneShot::new(Bytes::from(reply.body)))
        .expect("static response parts are valid")
}

/// Serves until the task is dropped or aborted. `name` is for the log. `handler` gets the
/// method, the path, the `Authorization` header and the request body, which it must read
/// (with [`read_body`]) only once it has authenticated the caller.
pub async fn serve<H, Fut>(listener: TcpListener, limits: Limits, name: &'static str, handler: H)
where
    H: Fn(Method, String, Option<String>, Incoming) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Reply> + Send + 'static,
{
    let slots = Arc::new(Semaphore::new(limits.max_connections.max(1)));
    let mut backoff = Duration::from_millis(50);
    loop {
        let Ok(permit) = Arc::clone(&slots).acquire_owned().await else {
            return;
        };
        let stream = match listener.accept().await {
            Ok((stream, _)) => {
                backoff = Duration::from_millis(50);
                stream
            }
            Err(e) => {
                tracing::warn!("{name} listener accept failed: {e}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(2));
                continue;
            }
        };
        let handler = handler.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let svc = service_fn(move |req: Request<Incoming>| {
                let handler = handler.clone();
                async move {
                    let method = req.method().clone();
                    let path = req.uri().path().to_string();
                    let auth = req
                        .headers()
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string);
                    let reply = handler(method, path, auth, req.into_body()).await;
                    Ok::<_, Infallible>(response(reply))
                }
            });
            // A client that does not finish its request headers promptly is dropped long
            // before the whole-connection limit, so idle connections cannot hold the slots.
            let conn = hyper::server::conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(limits.header_read_timeout)
                .serve_connection(TokioIo::new(stream), svc);
            let _ = tokio::time::timeout(limits.connection_timeout, conn).await;
        });
    }
}
