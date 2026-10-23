//! A local HTTP server for tests: canned replies by path and query, recorded requests.
//! Built from tokio and hyper so no service is ever contacted.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use hyper::body::{Body, Bytes, Frame, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

/// A response body made of one chunk.
pub struct OneShot(Option<Bytes>);

impl OneShot {
    pub fn new(text: impl Into<String>) -> Self {
        OneShot(Some(Bytes::from(text.into())))
    }
}

impl Body for OneShot {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(self.0.take().map(|b| Ok(Frame::data(b))))
    }
}

#[derive(Clone)]
pub struct Reply {
    pub status: u16,
    pub body: String,
    /// How long to wait before answering, to exercise timeouts.
    pub delay: Duration,
}

impl Reply {
    pub fn json(body: &str) -> Self {
        Reply {
            status: 200,
            body: body.to_string(),
            delay: Duration::ZERO,
        }
    }

    pub fn status(status: u16) -> Self {
        Reply {
            status,
            body: String::new(),
            delay: Duration::ZERO,
        }
    }

    pub fn delayed(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }
}

#[derive(Default)]
struct Inner {
    replies: HashMap<String, Reply>,
    requests: Vec<(String, Option<String>)>,
}

pub struct TestServer {
    addr: SocketAddr,
    inner: Arc<Mutex<Inner>>,
}

impl TestServer {
    pub async fn start() -> TestServer {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let inner = Arc::new(Mutex::new(Inner::default()));
        let shared = Arc::clone(&inner);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let shared = Arc::clone(&shared);
                tokio::spawn(async move {
                    let svc = service_fn(move |req: Request<Incoming>| {
                        let shared = Arc::clone(&shared);
                        async move {
                            let key = req
                                .uri()
                                .path_and_query()
                                .map(|p| p.to_string())
                                .unwrap_or_default();
                            let auth = req
                                .headers()
                                .get("authorization")
                                .and_then(|v| v.to_str().ok())
                                .map(str::to_string);
                            let reply = {
                                let mut g = shared.lock().unwrap();
                                g.requests.push((key.clone(), auth));
                                g.replies.get(&key).cloned()
                            };
                            let reply = reply.unwrap_or_else(|| Reply::status(404));
                            if !reply.delay.is_zero() {
                                tokio::time::sleep(reply.delay).await;
                            }
                            Ok::<_, Infallible>(
                                Response::builder()
                                    .status(reply.status)
                                    .header("content-type", "application/json")
                                    .body(OneShot::new(reply.body))
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                });
            }
        });
        TestServer { addr, inner }
    }

    /// The URL for `path_and_query` on this server.
    pub fn url(&self, path_and_query: &str) -> String {
        format!("http://{}{path_and_query}", self.addr)
    }

    /// Answers requests for exactly `path_and_query` with `reply` from now on.
    pub fn set(&self, path_and_query: &str, reply: Reply) {
        self.inner
            .lock()
            .unwrap()
            .replies
            .insert(path_and_query.to_string(), reply);
    }

    /// Every request seen so far: path and query, and the Authorization header if any.
    pub fn requests(&self) -> Vec<(String, Option<String>)> {
        self.inner.lock().unwrap().requests.clone()
    }
}
