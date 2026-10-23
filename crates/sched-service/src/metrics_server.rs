//! A small HTTP listener for Prometheus: `/metrics` in the text format, `/healthz` for
//! liveness. Read-only; everything else is a 404.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use hyper::body::{Body, Bytes, Frame, Incoming};
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use crate::metrics::Metrics;

/// A response body made of one chunk.
pub struct OneShot(Option<Bytes>);

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

/// Serves until the task is dropped or aborted.
pub async fn serve(listener: TcpListener, metrics: Arc<Metrics>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
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
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), svc)
                .await;
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
        assert!(body.contains("sched_decisions_total"));
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
}
