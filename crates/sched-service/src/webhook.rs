//! An inbound "go poll now" listener. A request whose path some `http_json` source
//! declared (`webhook: { path: ... }` in its params) asks those sources to poll as soon as
//! they may. That is all a webhook means here: the body is ignored (read up to a cap and
//! thrown away), the poll's result enters the pipeline like any scheduled one, and nothing
//! about a decision depends on who called. If a webhook is lost, the source's `resync`
//! still polls it.
//!
//! Nothing is rate-limited here: each observer merges the nudges it gets and starts polls
//! no closer than the minimum interval (`sched_observe::poll_loop`), so a flood costs one
//! poll per source per interval, however many requests arrive.
//!
//! Endpoint: `POST <path>` for a configured path. Answers: `202` (`{"sources": n,
//! "nudged": m}`), `404` for a path no source declared (the same whatever the method),
//! `405` for another method on a known path, `413` for a body over [`MAX_BODY`], `503`
//! when the sources of the path have no running observer yet or (token mode) the token is
//! unavailable. Every request is counted in `sched_webhook_requests_total` and logged as
//! one `webhook` line (rate-limited, with a count of the lines left out).
//!
//! Authentication is chosen at startup and has no default:
//!
//! - [`Auth::Mesh`]: this process authenticates nobody. The service mesh must: STRICT
//!   mTLS on the port and an `AuthorizationPolicy` naming the callers (see
//!   `docs/kubernetes_setup.md`). Anything that can reach the port can nudge a poll, which
//!   costs at most one poll per source per interval.
//! - [`Auth::Token`]: `Authorization: Bearer <token>`, the token read from a file on every
//!   request. It is checked first, before the path is looked up and before any of the
//!   body is read, so an unauthenticated caller learns nothing about which paths exist.

use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use hyper::Method;
use sched_observe::Nudges;
use serde_json::json;
use tokio::net::TcpListener;

use crate::listener::{
    BodyError, Limits, LogLimiter, Reply, bearer, constant_time_eq, read_capped, read_token,
};
use crate::tick::Shared;

/// The most body bytes read (and ignored). A webhook sender's payload is a small JSON
/// document; anything larger is refused rather than buffered.
pub const MAX_BODY: usize = 64 * 1024;

/// The longest path kept in a log line.
const LOG_PATH_MAX: usize = 64;

/// Who is allowed to send webhooks, and how that is decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Auth {
    /// No check here; the mesh mediates.
    Mesh,
    /// A bearer token, read from this file on every request.
    Token(PathBuf),
}

/// What became of one request.
struct Answer {
    reply: Reply,
    outcome: &'static str,
    /// Sources the path maps to, and how many of them took the nudge.
    sources: usize,
    nudged: usize,
}

impl Answer {
    fn refused(status: u16, outcome: &'static str, message: &str) -> Answer {
        Answer {
            reply: Reply::failure(status, message),
            outcome,
            sources: 0,
            nudged: 0,
        }
    }
}

/// The webhook listener's logic: authentication, routing and the nudges.
pub struct Webhook {
    shared: Arc<Shared>,
    nudges: Nudges,
    auth: Auth,
    limiter: LogLimiter,
}

impl Webhook {
    pub fn new(shared: Arc<Shared>, nudges: Nudges, auth: Auth) -> Webhook {
        Webhook {
            shared,
            nudges,
            auth,
            limiter: LogLimiter::new(std::time::Instant::now()),
        }
    }

    /// Answers one request. `authorization` is the header value. `drain` reads (and
    /// discards) the body; it is not polled until the caller has been authenticated, nor
    /// for a request that is refused for its path or method.
    pub async fn handle(
        &self,
        method: &Method,
        path: &str,
        authorization: Option<&str>,
        drain: impl Future<Output = Result<(), BodyError>>,
    ) -> Reply {
        let a = self.answer(method, path, authorization, drain).await;
        self.shared.metrics.webhook_request(a.outcome);
        self.shared.metrics.webhook_nudged(a.nudged as u64);
        // Every line is rate-limited (a flood of accepted requests would flood the log as
        // much as a flood of refused ones).
        if let Some(left_out) = self.limiter.admit(std::time::Instant::now(), true) {
            let shown: String = path.chars().take(LOG_PATH_MAX).collect();
            if a.reply.status == 202 {
                tracing::info!(event = "webhook", path = ?shown, outcome = a.outcome, status = a.reply.status, sources = a.sources, nudged = a.nudged, suppressed = left_out, "webhook request");
            } else {
                tracing::warn!(event = "webhook", path = ?shown, outcome = a.outcome, status = a.reply.status, sources = a.sources, nudged = a.nudged, suppressed = left_out, "webhook request refused");
            }
        }
        a.reply
    }

    async fn answer(
        &self,
        method: &Method,
        path: &str,
        authorization: Option<&str>,
        drain: impl Future<Output = Result<(), BodyError>>,
    ) -> Answer {
        if let Err(refusal) = self.authorize(authorization).await {
            return refusal;
        }
        let config = self.shared.config.current();
        let sources = config.webhook_sources(path);
        if sources.is_empty() {
            return Answer::refused(404, "not_found", "not found");
        }
        if method != Method::POST {
            let mut a = Answer::refused(405, "method_not_allowed", "method not allowed");
            a.reply.allow = Some("POST");
            return a;
        }
        match drain.await {
            Ok(()) => {}
            Err(BodyError::TooLarge) => {
                return Answer::refused(
                    413,
                    "payload_too_large",
                    &format!("the request body is larger than {MAX_BODY} bytes"),
                );
            }
            Err(BodyError::Unreadable) => {
                return Answer::refused(400, "bad_request", "the request body is unreadable");
            }
        }
        let total = sources.len();
        let nudged = sources.iter().filter(|s| self.nudges.nudge(&s.id)).count();
        if nudged == 0 {
            return Answer {
                reply: Reply::failure(503, "no observer is running for this path yet"),
                outcome: "unavailable",
                sources: total,
                nudged,
            };
        }
        Answer {
            reply: Reply::json(202, json!({ "sources": total, "nudged": nudged })),
            outcome: "accepted",
            sources: total,
            nudged,
        }
    }

    /// Nothing to check in mesh mode. In token mode the token must be available and
    /// match; this is the first thing done for a request.
    async fn authorize(&self, header: Option<&str>) -> Result<(), Answer> {
        let Auth::Token(file) = &self.auth else {
            return Ok(());
        };
        let Some(token) = read_token(file, "webhook").await else {
            return Err(Answer::refused(
                503,
                "unavailable",
                "the webhook token is not available",
            ));
        };
        match bearer(header) {
            Some(given) if constant_time_eq(given.as_bytes(), token.as_bytes()) => Ok(()),
            _ => Err(Answer {
                reply: Reply::unauthorized(),
                outcome: "unauthorized",
                sources: 0,
                nudged: 0,
            }),
        }
    }
}

/// Serves until the task is dropped or aborted.
pub async fn serve(listener: TcpListener, webhook: Arc<Webhook>, limits: Limits) {
    crate::listener::serve(
        listener,
        limits,
        "webhook",
        move |method, path, auth, body| {
            let webhook = Arc::clone(&webhook);
            async move {
                let drain = async { read_capped(body, MAX_BODY).await.map(drop) };
                webhook.handle(&method, &path, auth.as_deref(), drain).await
            }
        },
    )
    .await
}

/// In token mode reads the token once (so a missing or empty file fails startup), binds
/// `addr` (so a taken port fails startup) and serves on its own task.
pub async fn spawn(webhook: Webhook, addr: SocketAddr) -> Result<SocketAddr, String> {
    if let Auth::Token(file) = &webhook.auth
        && read_token(file, "webhook").await.is_none()
    {
        return Err(format!(
            "the webhook token is not available ({})",
            file.display()
        ));
    }
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| format!("cannot listen on {addr}: {e}"))?;
    let local = listener.local_addr().map_err(|e| e.to_string())?;
    tokio::spawn(serve(listener, Arc::new(webhook), Limits::default()));
    Ok(local)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use sched_model::Millis;
    use sched_model::test_support::EXAMPLE_CONFIG;
    use tokio::sync::Notify;

    use super::*;
    use crate::clock::ManualClock;
    use crate::config_source::ConfigHolder;
    use crate::metrics::Metrics;

    const TOKEN: &str = "tok-0123456789";
    const PAPERLESS: &str = "/hooks/paperless";

    fn shared() -> Arc<Shared> {
        Arc::new(Shared::new(
            Arc::new(ConfigHolder::new(EXAMPLE_CONFIG).unwrap()),
            Arc::new(Metrics::new()),
            Arc::new(ManualClock::new(Millis(1_000))),
        ))
    }

    fn token_file(name: &str, contents: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("webhook-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("token");
        std::fs::write(&f, contents).unwrap();
        f
    }

    /// A webhook with handles for the two paperless and the two speakr sources.
    fn webhook(auth: Auth) -> (Webhook, Nudges, Arc<Shared>) {
        let s = shared();
        let nudges = Nudges::default();
        for id in [
            "paperless-ocr-pending",
            "paperless-post-ocr",
            "speakr-pending",
            "speakr-summaries",
        ] {
            nudges.handle(&id.into());
        }
        (
            Webhook::new(Arc::clone(&s), nudges.clone(), auth),
            nudges,
            s,
        )
    }

    async fn nudged(nudges: &Nudges, id: &str) -> bool {
        let n: Arc<Notify> = nudges.handle(&id.into());
        tokio::time::timeout(Duration::ZERO, n.notified())
            .await
            .is_ok()
    }

    async fn ok() -> Result<(), BodyError> {
        Ok(())
    }

    async fn post(w: &Webhook, path: &str, auth: Option<&str>) -> Reply {
        w.handle(&Method::POST, path, auth, ok()).await
    }

    fn count(s: &Shared, outcome: &str) -> bool {
        s.metrics.render().contains(&format!(
            "sched_webhook_requests_total{{outcome=\"{outcome}\"}} 1"
        ))
    }

    #[tokio::test]
    async fn a_known_path_nudges_exactly_the_sources_that_declared_it() {
        let (w, nudges, s) = webhook(Auth::Mesh);
        let r = post(&w, PAPERLESS, None).await;
        assert_eq!(r.status, 202, "{}", r.body);
        assert_eq!(r.body, r#"{"nudged":2,"sources":2}"#);
        assert!(nudged(&nudges, "paperless-ocr-pending").await);
        assert!(nudged(&nudges, "paperless-post-ocr").await);
        assert!(!nudged(&nudges, "speakr-pending").await);
        assert!(!nudged(&nudges, "speakr-summaries").await);
        let text = s.metrics.render();
        assert!(count(&s, "accepted"), "{text}");
        assert!(text.contains("sched_webhook_nudges_total 2"), "{text}");
    }

    #[tokio::test]
    async fn an_unknown_path_is_404_and_nudges_nothing() {
        let (w, nudges, s) = webhook(Auth::Mesh);
        for p in [
            "/",
            "/hooks",
            "/hooks/nothing",
            "/hooks/paperless/",
            PAPERLESS_UPPER,
        ] {
            let r = post(&w, p, None).await;
            assert_eq!(r.status, 404, "{p}");
        }
        for id in ["paperless-ocr-pending", "speakr-pending"] {
            assert!(!nudged(&nudges, id).await);
        }
        assert!(s.metrics.render().contains("outcome=\"not_found\"} 5"));
    }

    const PAPERLESS_UPPER: &str = "/hooks/PAPERLESS";

    #[tokio::test]
    async fn another_method_on_a_known_path_is_405_with_allow() {
        let (w, nudges, s) = webhook(Auth::Mesh);
        for m in [Method::GET, Method::PUT, Method::DELETE] {
            let r = w.handle(&m, PAPERLESS, None, ok()).await;
            assert_eq!((r.status, r.allow), (405, Some("POST")), "{m}");
        }
        assert!(!nudged(&nudges, "paperless-ocr-pending").await);
        assert!(
            s.metrics
                .render()
                .contains("outcome=\"method_not_allowed\"} 3")
        );
        // An unknown path is 404 whatever the method.
        let r = w.handle(&Method::GET, "/nope", None, ok()).await;
        assert_eq!(r.status, 404);
    }

    #[tokio::test]
    async fn a_body_over_the_cap_or_unreadable_is_refused_without_a_nudge() {
        let (w, nudges, s) = webhook(Auth::Mesh);
        let r = w
            .handle(&Method::POST, PAPERLESS, None, async {
                Err(BodyError::TooLarge)
            })
            .await;
        assert_eq!(r.status, 413);
        let r = w
            .handle(&Method::POST, PAPERLESS, None, async {
                Err(BodyError::Unreadable)
            })
            .await;
        assert_eq!(r.status, 400);
        assert!(!nudged(&nudges, "paperless-ocr-pending").await);
        let text = s.metrics.render();
        assert!(text.contains("outcome=\"payload_too_large\"} 1"), "{text}");
        assert!(text.contains("outcome=\"bad_request\"} 1"), "{text}");
    }

    #[tokio::test]
    async fn a_path_whose_sources_have_no_observer_is_503_not_a_silent_accept() {
        let s = shared();
        let w = Webhook::new(Arc::clone(&s), Nudges::default(), Auth::Mesh);
        let r = post(&w, PAPERLESS, None).await;
        assert_eq!(r.status, 503);
        assert!(count(&s, "unavailable"));
    }

    #[tokio::test]
    async fn mesh_mode_checks_no_header_and_ignores_one() {
        let (w, _, _) = webhook(Auth::Mesh);
        assert_eq!(post(&w, PAPERLESS, None).await.status, 202);
        assert_eq!(post(&w, PAPERLESS, Some("Bearer wrong")).await.status, 202);
    }

    #[tokio::test]
    async fn token_mode_checks_the_token_first_and_never_reads_the_body_unauthenticated() {
        let file = token_file("auth", &format!("{TOKEN}\n"));
        let (w, nudges, s) = webhook(Auth::Token(file.clone()));
        let good = format!("Bearer {TOKEN}");
        let polled = std::sync::atomic::AtomicBool::new(false);
        let probe = || async {
            polled.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok::<(), BodyError>(())
        };
        for header in [
            None,
            Some("Bearer wrong"),
            Some("Bearer"),
            Some("Basic dG9r"),
            Some(TOKEN),
        ] {
            // A known path, an unknown path and a wrong method all answer 401 alike.
            for (m, p) in [
                (Method::POST, PAPERLESS),
                (Method::POST, "/hooks/nothing"),
                (Method::GET, PAPERLESS),
            ] {
                let r = w.handle(&m, p, header, probe()).await;
                assert_eq!(r.status, 401, "{header:?} {m} {p}");
                assert!(r.challenge);
            }
        }
        assert!(
            !polled.load(std::sync::atomic::Ordering::SeqCst),
            "the body was read before authentication"
        );
        assert!(!nudged(&nudges, "paperless-ocr-pending").await);
        assert!(s.metrics.render().contains("outcome=\"unauthorized\"} 15"));

        let r = w
            .handle(&Method::POST, PAPERLESS, Some(&good), probe())
            .await;
        assert_eq!(r.status, 202);
        assert!(polled.load(std::sync::atomic::Ordering::SeqCst));
        assert!(nudged(&nudges, "paperless-ocr-pending").await);
        // Authenticated, the path rules apply.
        assert_eq!(post(&w, "/hooks/nothing", Some(&good)).await.status, 404);
    }

    #[tokio::test]
    async fn the_token_is_read_on_every_request_and_a_missing_or_empty_file_is_never_a_match() {
        let file = token_file("rotate", "first\n");
        let (w, _, _) = webhook(Auth::Token(file.clone()));
        assert_eq!(post(&w, PAPERLESS, Some("Bearer first")).await.status, 202);
        std::fs::write(&file, "second\n").unwrap();
        assert_eq!(post(&w, PAPERLESS, Some("Bearer first")).await.status, 401);
        assert_eq!(post(&w, PAPERLESS, Some("Bearer second")).await.status, 202);
        // An empty file is unavailable, not "any token" and not "no token".
        std::fs::write(&file, "\n").unwrap();
        for h in [None, Some("Bearer "), Some("Bearer second")] {
            assert_eq!(post(&w, PAPERLESS, h).await.status, 503, "{h:?}");
        }
        std::fs::remove_file(&file).unwrap();
        assert_eq!(post(&w, PAPERLESS, Some("Bearer second")).await.status, 503);
    }

    #[tokio::test]
    async fn spawn_refuses_to_start_without_a_usable_token_or_a_free_port() {
        let (w, _, _) = webhook(Auth::Token("/nonexistent/webhook-token".into()));
        let e = spawn(w, "127.0.0.1:0".parse().unwrap()).await.unwrap_err();
        assert!(e.contains("not available"), "{e}");
        let empty = token_file("empty", "  \n");
        let (w, _, _) = webhook(Auth::Token(empty));
        assert!(spawn(w, "127.0.0.1:0".parse().unwrap()).await.is_err());

        let taken = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (w, _, _) = webhook(Auth::Mesh);
        let e = spawn(w, taken.local_addr().unwrap()).await.unwrap_err();
        assert!(e.contains("cannot listen"), "{e}");
        let (w, _, _) = webhook(Auth::Mesh);
        assert!(spawn(w, "127.0.0.1:0".parse().unwrap()).await.is_ok());
    }

    /// Sends one raw HTTP/1.1 request and returns the status line's code.
    async fn raw(addr: SocketAddr, head: &str, body: &[u8]) -> u16 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
        c.write_all(head.as_bytes()).await.unwrap();
        c.write_all(body).await.unwrap();
        let mut out = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), async {
            let mut buf = [0u8; 512];
            loop {
                let n = c.read(&mut buf).await.unwrap_or(0);
                if n == 0 {
                    break;
                }
                out.extend_from_slice(&buf[..n]);
                if out.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
        })
        .await;
        let line = String::from_utf8_lossy(&out);
        line.split(' ')
            .nth(1)
            .and_then(|c| c.parse().ok())
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn over_a_socket_the_body_is_capped_and_auth_comes_before_the_body() {
        let file = token_file("socket", &format!("{TOKEN}\n"));
        let (w, nudges, _) = webhook(Auth::Token(file));
        let addr = spawn(w, "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let big = vec![b'x'; MAX_BODY + 1];
        let head = |auth: &str, len: usize| {
            format!(
                "POST {PAPERLESS} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n{auth}Content-Length: {len}\r\n\r\n"
            )
        };
        let bearer = format!("Authorization: Bearer {TOKEN}\r\n");
        // Wrong token and a huge body: refused as unauthenticated, not as too large.
        assert_eq!(raw(addr, &head("", big.len()), &big).await, 401);
        // Right token: a body within the cap is ignored; one over it is refused.
        assert_eq!(raw(addr, &head(&bearer, 5), b"{\"a\":").await, 202);
        assert!(nudged(&nudges, "paperless-ocr-pending").await);
        assert_eq!(raw(addr, &head(&bearer, big.len()), &big).await, 413);
        assert!(!nudged(&nudges, "paperless-ocr-pending").await);
        // A body that is not text is as good as any other: it is ignored.
        assert_eq!(raw(addr, &head(&bearer, 3), &[0xff, 0xfe, 0x00]).await, 202);
    }
}
