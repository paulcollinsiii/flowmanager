//! A small HTTP listener for Prometheus: `/metrics` in the text format, `/healthz` for
//! liveness. Read-only; everything else is a 404.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use hyper::body::{Body, Bytes, Frame, Incoming};
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

use crate::metrics::Metrics;

/// A response body made of one chunk.
pub struct OneShot(Option<Bytes>);

impl OneShot {
    pub fn new(body: Bytes) -> OneShot {
        OneShot(Some(body))
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

/// Status, content type and body for a request.
pub fn route(method: &Method, path: &str, metrics: &Metrics) -> (u16, &'static str, String) {
    match (method, path) {
        (&Method::GET, "/metrics") => (200, "text/plain; version=0.0.4", metrics.render()),
        (&Method::GET, "/healthz") => (200, "text/plain", "ok\n".to_string()),
        _ => (404, "text/plain", "not found\n".to_string()),
    }
}

/// Limits on the metrics listener.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Connections served at once; further ones wait in the accept queue.
    pub max_connections: usize,
    /// How long one connection may live, so an idle or slow client cannot hold a slot.
    pub connection_timeout: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_connections: 32,
            connection_timeout: Duration::from_secs(10),
        }
    }
}

/// Serves until the task is dropped or aborted.
pub async fn serve(listener: TcpListener, metrics: Arc<Metrics>) {
    serve_with(listener, metrics, Limits::default()).await
}

pub async fn serve_with(listener: TcpListener, metrics: Arc<Metrics>, limits: Limits) {
    let slots = Arc::new(Semaphore::new(limits.max_connections.max(1)));
    let mut backoff = Duration::from_millis(50);
    loop {
        // Take a slot before accepting, so at most `max_connections` are ever open.
        let Ok(permit) = Arc::clone(&slots).acquire_owned().await else {
            return;
        };
        let stream = match listener.accept().await {
            Ok((stream, _)) => {
                backoff = Duration::from_millis(50);
                stream
            }
            Err(e) => {
                tracing::warn!("metrics listener accept failed: {e}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(2));
                continue;
            }
        };
        let metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            let _permit = permit;
            let svc = service_fn(move |req: Request<Incoming>| {
                let (status, ctype, body) = route(req.method(), req.uri().path(), &metrics);
                async move {
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(status)
                            .header("content-type", ctype)
                            .body(OneShot(Some(Bytes::from(body))))
                            .expect("static response parts are valid"),
                    )
                }
            });
            let conn = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), svc);
            let _ = tokio::time::timeout(limits.connection_timeout, conn).await;
        });
    }
}

pub async fn bind(addr: SocketAddr) -> std::io::Result<TcpListener> {
    TcpListener::bind(addr).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_metrics_health_and_nothing_else() {
        let m = Metrics::new();
        m.decision("wait");
        let (s, _, body) = route(&Method::GET, "/metrics", &m);
        assert_eq!(s, 200);
        assert!(body.contains("sched_tick_decisions_total"));
        assert_eq!(route(&Method::GET, "/healthz", &m).0, 200);
        assert_eq!(route(&Method::GET, "/other", &m).0, 404);
        assert_eq!(route(&Method::POST, "/metrics", &m).0, 404);
    }

    #[tokio::test]
    async fn serves_over_http() {
        let metrics = Arc::new(Metrics::new());
        metrics.config_reload("applied");
        let listener = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(serve(listener, Arc::clone(&metrics)));
        let body = reqwest::get(format!("http://{addr}/metrics"))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            body.contains("sched_config_reloads_total{outcome=\"applied\"} 1"),
            "{body}"
        );
        task.abort();
    }

    #[tokio::test]
    async fn idle_connections_time_out_and_the_connection_cap_is_enforced() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let metrics = Arc::new(Metrics::new());
        let listener = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let limits = Limits {
            max_connections: 1,
            connection_timeout: Duration::from_millis(400),
        };
        let task = tokio::spawn(serve_with(listener, metrics, limits));

        // A client that connects and says nothing holds the only slot...
        let mut idle = tokio::net::TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut second = tokio::net::TcpStream::connect(addr).await.unwrap();
        second
            .write_all(b"GET /healthz HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut buf = vec![0u8; 256];
        // ...so the second is not served yet...
        assert!(
            tokio::time::timeout(Duration::from_millis(150), second.read(&mut buf))
                .await
                .is_err()
        );
        // ...until the idle one is cut off by the timeout, which frees the slot.
        let n = tokio::time::timeout(Duration::from_secs(3), second.read(&mut buf))
            .await
            .expect("served after the idle connection timed out")
            .unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).starts_with("HTTP/1.1 200"));
        let mut one = [0u8; 1];
        let closed = tokio::time::timeout(Duration::from_secs(3), idle.read(&mut one)).await;
        assert!(matches!(closed, Ok(Ok(0)) | Ok(Err(_))));
        task.abort();
    }
}
