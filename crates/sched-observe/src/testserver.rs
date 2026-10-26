//! A local HTTP server for tests: canned replies by path and query, recorded requests.
//! Built from tokio and hyper so no service is ever contacted.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use hyper::body::{Body, Bytes, Frame, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

/// How the body of a reply ends.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// The body, then the end of the response.
    Normal,
    /// The body, then nothing more and no end: a watch that stays open.
    Hang,
    /// The body, then data without end.
    Endless,
}

/// A response body: one chunk, then it ends, hangs or never stops, by [`Mode`].
pub struct Streaming {
    first: Option<Bytes>,
    mode: Mode,
    _tracked: Option<OpenGuard>,
}

impl Body for Streaming {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        if let Some(b) = self.first.take() {
            return Poll::Ready(Some(Ok(Frame::data(b))));
        }
        match self.mode {
            Mode::Normal => Poll::Ready(None),
            Mode::Hang => Poll::Pending,
            Mode::Endless => {
                cx.waker().wake_by_ref();
                Poll::Ready(Some(Ok(Frame::data(Bytes::from(vec![b'x'; 16 * 1024])))))
            }
        }
    }
}

#[derive(Clone)]
pub struct Reply {
    pub status: u16,
    pub body: String,
    /// How long to wait before answering, to exercise timeouts.
    pub delay: Duration,
    pub mode: Mode,
    pub location: Option<String>,
    /// Further response headers.
    pub headers: Vec<(String, String)>,
    /// Counts this response while its body is alive (until it ends or the client goes).
    pub track: Option<Arc<AtomicUsize>>,
}

impl Reply {
    pub fn json(body: &str) -> Self {
        Reply {
            status: 200,
            body: body.to_string(),
            delay: Duration::ZERO,
            mode: Mode::Normal,
            location: None,
            headers: Vec::new(),
            track: None,
        }
    }

    /// Adds a response header.
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    /// Counts this response in `counter` while it is open.
    pub fn tracked(mut self, counter: &Arc<AtomicUsize>) -> Self {
        self.track = Some(Arc::clone(counter));
        self
    }

    /// A 302 to `to`.
    pub fn redirect(to: &str) -> Self {
        Reply {
            location: Some(to.to_string()),
            ..Reply::status(302)
        }
    }

    /// Sends `body`, then keeps the response open for ever.
    pub fn hanging(body: &str) -> Self {
        Reply {
            mode: Mode::Hang,
            ..Reply::json(body)
        }
    }

    /// Sends data without end.
    pub fn endless() -> Self {
        Reply {
            mode: Mode::Endless,
            ..Reply::json("")
        }
    }

    pub fn status(status: u16) -> Self {
        Reply {
            status,
            body: String::new(),
            delay: Duration::ZERO,
            mode: Mode::Normal,
            location: None,
            headers: Vec::new(),
            track: None,
        }
    }

    pub fn delayed(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }
}

type Dynamic = Arc<dyn Fn(&str) -> Reply + Send + Sync>;

#[derive(Default)]
struct Inner {
    replies: HashMap<String, Reply>,
    /// By path alone; the function gets the query string.
    dynamic: HashMap<String, Dynamic>,
    requests: Vec<(String, Option<String>)>,
    /// Every header of each request in `requests`, names lower-cased.
    headers: Vec<Vec<(String, String)>>,
    times: Vec<std::time::Instant>,
    /// The HTTP method of each request in `requests`.
    methods: Vec<String>,
}

pub struct TestServer {
    addr: SocketAddr,
    inner: Arc<Mutex<Inner>>,
    open: Arc<AtomicUsize>,
}

struct OpenGuard(Arc<AtomicUsize>);

impl Drop for OpenGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl TestServer {
    pub async fn start() -> TestServer {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let inner = Arc::new(Mutex::new(Inner::default()));
        let shared = Arc::clone(&inner);
        let open = Arc::new(AtomicUsize::new(0));
        let open_c = Arc::clone(&open);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let shared = Arc::clone(&shared);
                open_c.fetch_add(1, Ordering::SeqCst);
                let guard = OpenGuard(Arc::clone(&open_c));
                tokio::spawn(async move {
                    let _guard = guard;
                    let svc = service_fn(move |req: Request<Incoming>| {
                        let shared = Arc::clone(&shared);
                        async move {
                            let method = req.method().as_str().to_string();
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
                            let headers = req
                                .headers()
                                .iter()
                                .map(|(n, v)| {
                                    (
                                        n.as_str().to_string(),
                                        String::from_utf8_lossy(v.as_bytes()).into_owned(),
                                    )
                                })
                                .collect();
                            let reply = {
                                let mut g = shared.lock().unwrap();
                                g.requests.push((key.clone(), auth));
                                g.headers.push(headers);
                                g.times.push(std::time::Instant::now());
                                g.methods.push(method);
                                let (path, query) = key.split_once('?').unwrap_or((&key, ""));
                                match g.dynamic.get(path) {
                                    Some(f) => Some(f(query)),
                                    None => g.replies.get(&key).cloned(),
                                }
                            };
                            let reply = reply.unwrap_or_else(|| Reply::status(404));
                            if !reply.delay.is_zero() {
                                tokio::time::sleep(reply.delay).await;
                            }
                            let mut b = Response::builder()
                                .status(reply.status)
                                .header("content-type", "application/json");
                            for (n, v) in &reply.headers {
                                b = b.header(n, v);
                            }
                            if let Some(l) = &reply.location {
                                b = b.header("location", l);
                            }
                            let tracked = reply.track.map(|c| {
                                c.fetch_add(1, Ordering::SeqCst);
                                OpenGuard(c)
                            });
                            Ok::<_, Infallible>(
                                b.body(Streaming {
                                    first: Some(Bytes::from(reply.body)),
                                    mode: reply.mode,
                                    _tracked: tracked,
                                })
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
        TestServer { addr, inner, open }
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

    /// Answers requests for `path` (any query) with the function's reply, given the query.
    pub fn set_dynamic(&self, path: &str, f: impl Fn(&str) -> Reply + Send + Sync + 'static) {
        self.inner
            .lock()
            .unwrap()
            .dynamic
            .insert(path.to_string(), Arc::new(f));
    }

    /// Connections currently open to the server.
    pub fn open_connections(&self) -> usize {
        self.open.load(Ordering::SeqCst)
    }

    /// Number of requests seen for `path` (ignoring the query).
    pub fn count(&self, path: &str) -> usize {
        self.requests()
            .iter()
            .filter(|(k, _)| k.split('?').next() == Some(path))
            .count()
    }

    /// When each request in `requests()` arrived.
    pub fn request_times(&self) -> Vec<std::time::Instant> {
        self.inner.lock().unwrap().times.clone()
    }

    /// Every request seen so far: path and query, and the value of header `name` (any
    /// case) if it was sent.
    pub fn requests_with_header(&self, name: &str) -> Vec<(String, Option<String>)> {
        let g = self.inner.lock().unwrap();
        let name = name.to_ascii_lowercase();
        g.requests
            .iter()
            .zip(&g.headers)
            .map(|((key, _), headers)| {
                let value = headers
                    .iter()
                    .find(|(n, _)| *n == name)
                    .map(|(_, v)| v.clone());
                (key.clone(), value)
            })
            .collect()
    }

    /// The HTTP method of each request in `requests()`, in order.
    pub fn methods(&self) -> Vec<String> {
        self.inner.lock().unwrap().methods.clone()
    }

    /// Every request seen so far: path and query, and the Authorization header if any.
    pub fn requests(&self) -> Vec<(String, Option<String>)> {
        self.inner.lock().unwrap().requests.clone()
    }
}
