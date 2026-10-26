//! KEDA external scaler server exposing which models and workload classes are currently
//! allowed.
//!
//! I/O crate: no logic beyond a lookup of the scheduler's decision. The decision arrives on
//! a `watch` channel of `Option<PublishedAllowed>` (`None` until the scheduler process has
//! published its first tick); the active configuration arrives on another, and is used only
//! to refuse ids it does not know.
//!
//! A ScaledObject names what it follows in its trigger metadata, exactly one of
//! `model: <id>` (active while that model is in the allowed set) or `class: <id>` (active
//! while that class is). Answers:
//!
//! - `IsActive`: whether it is allowed. `StreamIsActive` (the `external-push` trigger): the
//!   same answer at once, then again each time it changes.
//! - `GetMetricSpec`: one metric, [`Subject::metric_name`], with target 1. `GetMetrics`: 1
//!   while allowed, else 0, so with `maxReplicaCount: 1` the replica count is 0 or 1.
//! - Invalid metadata (no key, both, an unknown key, an empty value, an id the active config
//!   does not have) is `InvalidArgument`.
//! - Before the first decision, or once the decision can no longer change because the
//!   scheduler dropped its sender (the process is stopping), every call is `Unavailable`.
//!   KEDA leaves the replica count as it is when a scaler errors and the ScaledObject has
//!   no `fallback`, so a not-ready scheduler never scales anything up or down.
//!
//! KEDA passes every key of the trigger's `metadata` to the scaler, its own included, so
//! `scalerAddress`, `enableTLS` and `unsafeSsl` are accepted and ignored.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sched_model::{AllowedSet, ClassId, Config, ModelId, PublishedAllowed};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status};

/// The KEDA external scaler protocol, generated from `proto/externalscaler.proto` (vendored
/// from KEDA v2.21.0; see `proto/NOTICE`).
pub mod proto {
    tonic::include_proto!("externalscaler");
}

use proto::external_scaler_server::{ExternalScaler, ExternalScalerServer};
use proto::{
    GetMetricSpecResponse, GetMetricsRequest, GetMetricsResponse, IsActiveResponse, MetricSpec,
    MetricValue, ScaledObjectRef,
};

/// Metadata key naming a model.
pub const MODEL_KEY: &str = "model";
/// Metadata key naming a workload class.
pub const CLASS_KEY: &str = "class";
/// Keys of KEDA's own `external` / `external-push` trigger, which it also passes on.
pub const KEDA_KEYS: [&str; 3] = ["scalerAddress", "enableTLS", "unsafeSsl"];

/// Text of the error before the first decision.
pub const NOT_READY: &str = "flowmanager has not published a decision yet (no tick has completed since the scheduler process started); retry";
/// Text of the error once the decision can no longer change.
pub const STOPPED: &str =
    "flowmanager is no longer publishing decisions (the scheduler is stopping); retry";

/// What one ScaledObject follows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Subject {
    Model(ModelId),
    Class(ClassId),
}

impl Subject {
    /// Reads the subject from a ScaledObject's metadata and checks it against `config`.
    pub fn parse(metadata: &HashMap<String, String>, config: &Config) -> Result<Subject, String> {
        let mut unknown: Vec<&str> = metadata
            .keys()
            .map(String::as_str)
            .filter(|k| *k != MODEL_KEY && *k != CLASS_KEY && !KEDA_KEYS.contains(k))
            .collect();
        if !unknown.is_empty() {
            unknown.sort_unstable();
            return Err(format!(
                "unknown metadata key(s) {unknown:?}; set exactly one of `{MODEL_KEY}` or `{CLASS_KEY}`"
            ));
        }
        let value = |key: &str| -> Result<Option<&str>, String> {
            match metadata.get(key).map(String::as_str) {
                Some("") => Err(format!("metadata `{key}` is empty")),
                v => Ok(v),
            }
        };
        let unknown_id = |kind: &str, id: &str| {
            format!(
                "{kind} {id:?} is not in the active config (generation {})",
                config.generation()
            )
        };
        match (value(MODEL_KEY)?, value(CLASS_KEY)?) {
            (Some(_), Some(_)) => Err(format!(
                "metadata sets both `{MODEL_KEY}` and `{CLASS_KEY}`; set exactly one"
            )),
            (None, None) => Err(format!(
                "metadata sets neither `{MODEL_KEY}` nor `{CLASS_KEY}`; set exactly one"
            )),
            (Some(m), None) => {
                let id = ModelId::from(m);
                match config.model(&id) {
                    Some(_) => Ok(Subject::Model(id)),
                    None => Err(unknown_id("model", m)),
                }
            }
            (None, Some(c)) => {
                let id = ClassId::from(c);
                match config.class(&id) {
                    Some(_) => Ok(Subject::Class(id)),
                    None => Err(unknown_id("class", c)),
                }
            }
        }
    }

    /// The metric this subject is reported under; stable for as long as the metadata is.
    pub fn metric_name(&self) -> String {
        match self {
            Subject::Model(id) => format!("flowmanager-model-{id}"),
            Subject::Class(id) => format!("flowmanager-class-{id}"),
        }
    }

    pub fn is_allowed(&self, allowed: &AllowedSet) -> bool {
        match self {
            Subject::Model(id) => allowed.models.contains(id),
            Subject::Class(id) => allowed.classes.contains(id),
        }
    }
}

/// What the scaler can answer from.
enum Decision<'a> {
    /// No tick has been published by this process yet.
    NotReady,
    /// The publisher is gone, so the last decision can no longer follow the scheduler.
    Stopped,
    Ready(&'a PublishedAllowed),
}

/// How many ScaledObjects' last invalid-metadata error are remembered; see [`InvalidLog`].
const MAX_REMEMBERED: usize = 1024;

/// Decides how loudly an invalid-metadata error is logged. KEDA polls every ScaledObject
/// every `pollingInterval` (seconds), so one broken or removed-id ScaledObject would
/// otherwise write a warning per poll for as long as it stays broken. The error is logged at
/// warn when it is new for that ScaledObject (never seen, or different from the last one),
/// and at debug when it repeats; a ScaledObject that answers validly is forgotten, so
/// breaking again warns again. Memory is bounded: past [`MAX_REMEMBERED`] ScaledObjects the
/// memory is emptied, which at worst repeats a warning.
#[derive(Default)]
struct InvalidLog {
    last: Mutex<HashMap<(String, String), String>>,
}

impl InvalidLog {
    /// Records that `r` was refused with `error`; true if that deserves a warning.
    fn warn_for(&self, r: &ScaledObjectRef, error: &str) -> bool {
        let mut last = self.last.lock().unwrap_or_else(|e| e.into_inner());
        let key = (r.namespace.clone(), r.name.clone());
        if last.get(&key).is_some_and(|seen| seen == error) {
            return false;
        }
        if last.len() >= MAX_REMEMBERED && !last.contains_key(&key) {
            last.clear();
        }
        last.insert(key, error.to_string());
        true
    }

    /// Records that `r` was answered validly.
    fn valid(&self, r: &ScaledObjectRef) {
        let mut last = self.last.lock().unwrap_or_else(|e| e.into_inner());
        if !last.is_empty() {
            last.remove(&(r.namespace.clone(), r.name.clone()));
        }
    }
}

/// One answer: the subject, and whether it is allowed. Metadata is checked first, so a
/// broken ScaledObject reads as broken whether or not a decision exists.
fn evaluate(
    r: &ScaledObjectRef,
    config: &Config,
    decision: Decision<'_>,
    invalid: &InvalidLog,
) -> Result<(Subject, bool), Status> {
    let subject = match Subject::parse(&r.scaler_metadata, config) {
        Ok(subject) => {
            invalid.valid(r);
            subject
        }
        Err(e) => {
            if invalid.warn_for(r, &e) {
                tracing::warn!(namespace = %r.namespace, name = %r.name, "invalid ScaledObject metadata: {e}");
            } else {
                tracing::debug!(namespace = %r.namespace, name = %r.name, "invalid ScaledObject metadata (unchanged): {e}");
            }
            return Err(Status::invalid_argument(format!(
                "ScaledObject {}/{}: {e}",
                r.namespace, r.name
            )));
        }
    };
    match decision {
        Decision::NotReady => Err(Status::unavailable(NOT_READY)),
        Decision::Stopped => Err(Status::unavailable(STOPPED)),
        Decision::Ready(p) => {
            let allowed = subject.is_allowed(&p.allowed);
            Ok((subject, allowed))
        }
    }
}

/// Evaluates `r` against what `allowed` and `config` hold now, marking both as seen.
fn evaluate_now(
    r: &ScaledObjectRef,
    allowed: &mut watch::Receiver<Option<PublishedAllowed>>,
    config: &mut watch::Receiver<Arc<Config>>,
    invalid: &InvalidLog,
) -> Result<(Subject, bool), Status> {
    let cfg = Arc::clone(&config.borrow_and_update());
    let stopped = allowed.has_changed().is_err();
    let published = allowed.borrow_and_update().clone();
    let decision = match (&published, stopped) {
        (_, true) => Decision::Stopped,
        (None, false) => Decision::NotReady,
        (Some(p), false) => Decision::Ready(p),
    };
    evaluate(r, &cfg, decision, invalid)
}

/// The scaler: a read-only view of the published allowed set and the active config.
#[derive(Clone)]
pub struct Scaler {
    allowed: watch::Receiver<Option<PublishedAllowed>>,
    config: watch::Receiver<Arc<Config>>,
    /// Shared by every clone, so a repeat is a repeat whichever connection it arrives on.
    invalid: Arc<InvalidLog>,
}

impl Scaler {
    pub fn new(
        allowed: watch::Receiver<Option<PublishedAllowed>>,
        config: watch::Receiver<Arc<Config>>,
    ) -> Scaler {
        Scaler {
            allowed,
            config,
            invalid: Arc::default(),
        }
    }

    pub fn into_service(self) -> ExternalScalerServer<Scaler> {
        ExternalScalerServer::new(self)
    }

    /// Fresh receivers, for one call; a call never waits on another.
    fn receivers(
        &self,
    ) -> (
        watch::Receiver<Option<PublishedAllowed>>,
        watch::Receiver<Arc<Config>>,
    ) {
        (self.allowed.clone(), self.config.clone())
    }

    fn answer(&self, rpc: &str, r: &ScaledObjectRef) -> Result<(Subject, bool), Status> {
        let (mut allowed, mut config) = self.receivers();
        let out = evaluate_now(r, &mut allowed, &mut config, &self.invalid);
        log_answer(rpc, r, &out);
        out
    }
}

fn log_answer(rpc: &str, r: &ScaledObjectRef, out: &Result<(Subject, bool), Status>) {
    match out {
        Ok((_, allowed)) => {
            tracing::debug!(rpc, namespace = %r.namespace, name = %r.name, allowed, "scaler answer")
        }
        Err(s) => tracing::debug!(
            rpc,
            namespace = %r.namespace,
            name = %r.name,
            code = ?s.code(),
            "scaler error: {}",
            s.message()
        ),
    }
}

/// Watches one ScaledObject's answer for a `StreamIsActive` call: sends `first` at once,
/// then each changed answer; ends with the error if the answer turns into one (the id left
/// the config, or the publisher stopped); ends quietly when the client goes away.
async fn stream_answers(
    r: ScaledObjectRef,
    first: bool,
    mut allowed: watch::Receiver<Option<PublishedAllowed>>,
    mut config: watch::Receiver<Arc<Config>>,
    invalid: Arc<InvalidLog>,
    tx: mpsc::Sender<Result<IsActiveResponse, Status>>,
) {
    if tx
        .send(Ok(IsActiveResponse { result: first }))
        .await
        .is_err()
    {
        return;
    }
    let mut last = first;
    loop {
        let closed = tokio::select! {
            _ = tx.closed() => return,
            changed = allowed.changed() => changed.is_err(),
            changed = config.changed() => changed.is_err(),
        };
        let out = if closed {
            Err(Status::unavailable(STOPPED))
        } else {
            evaluate_now(&r, &mut allowed, &mut config, &invalid).map(|(_, a)| a)
        };
        match out {
            Ok(a) if a == last => {}
            Ok(a) => {
                tracing::debug!(namespace = %r.namespace, name = %r.name, allowed = a, "scaler stream: answer changed");
                last = a;
                if tx.send(Ok(IsActiveResponse { result: a })).await.is_err() {
                    return;
                }
            }
            Err(s) => {
                tracing::debug!(
                    namespace = %r.namespace,
                    name = %r.name,
                    code = ?s.code(),
                    "scaler stream ends: {}",
                    s.message()
                );
                // The client may already be gone; there is nothing left to tell it then.
                drop(tx.send(Err(s)).await);
                return;
            }
        }
    }
}

#[tonic::async_trait]
impl ExternalScaler for Scaler {
    async fn is_active(
        &self,
        request: Request<ScaledObjectRef>,
    ) -> Result<Response<IsActiveResponse>, Status> {
        let (_, allowed) = self.answer("IsActive", request.get_ref())?;
        Ok(Response::new(IsActiveResponse { result: allowed }))
    }

    type StreamIsActiveStream = ReceiverStream<Result<IsActiveResponse, Status>>;

    async fn stream_is_active(
        &self,
        request: Request<ScaledObjectRef>,
    ) -> Result<Response<Self::StreamIsActiveStream>, Status> {
        let r = request.into_inner();
        // The first answer marks what it read as seen on these receivers, so the stream
        // reacts to every later change and to none it has already answered.
        let (mut allowed, mut config) = self.receivers();
        let out = evaluate_now(&r, &mut allowed, &mut config, &self.invalid);
        log_answer("StreamIsActive", &r, &out);
        let (_, first) = out?;
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(stream_answers(
            r,
            first,
            allowed,
            config,
            Arc::clone(&self.invalid),
            tx,
        ));
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_metric_spec(
        &self,
        request: Request<ScaledObjectRef>,
    ) -> Result<Response<GetMetricSpecResponse>, Status> {
        let (subject, _) = self.answer("GetMetricSpec", request.get_ref())?;
        Ok(Response::new(GetMetricSpecResponse {
            metric_specs: vec![MetricSpec {
                metric_name: subject.metric_name(),
                target_size: 1,
                target_size_float: 1.0,
            }],
        }))
    }

    async fn get_metrics(
        &self,
        request: Request<GetMetricsRequest>,
    ) -> Result<Response<GetMetricsResponse>, Status> {
        let req = request.into_inner();
        let r = req
            .scaled_object_ref
            .ok_or_else(|| Status::invalid_argument("GetMetrics without a scaledObjectRef"))?;
        let (subject, allowed) = self.answer("GetMetrics", &r)?;
        let name = subject.metric_name();
        if req.metric_name != name {
            return Err(Status::invalid_argument(format!(
                "ScaledObject {}/{}: unknown metric {:?}; this ScaledObject's metric is {name:?}",
                r.namespace, r.name, req.metric_name
            )));
        }
        let value = i64::from(allowed);
        Ok(Response::new(GetMetricsResponse {
            metric_values: vec![MetricValue {
                metric_name: name,
                metric_value: value,
                metric_value_float: if allowed { 1.0 } else { 0.0 },
            }],
        }))
    }

    type StreamMetricSpecStream = ReceiverStream<Result<GetMetricSpecResponse, Status>>;

    /// Not offered: the spec never changes, and KEDA falls back to `GetMetricSpec` on
    /// `Unimplemented`.
    async fn stream_metric_spec(
        &self,
        _request: Request<ScaledObjectRef>,
    ) -> Result<Response<Self::StreamMetricSpecStream>, Status> {
        Err(Status::unimplemented(
            "StreamMetricSpec is not offered; the metric spec never changes",
        ))
    }
}

/// Keepalive pings on idle connections, so a long `StreamIsActive` survives proxies that
/// drop quiet connections.
const HTTP2_KEEPALIVE: Duration = Duration::from_secs(30);

pub async fn bind(addr: SocketAddr) -> std::io::Result<TcpListener> {
    TcpListener::bind(addr).await
}

/// Serves the scaler (plaintext gRPC) on `listener` for as long as the future is polled.
pub async fn serve(listener: TcpListener, scaler: Scaler) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(HTTP2_KEEPALIVE))
        .add_service(scaler.into_service())
        .serve_with_incoming(TcpIncoming::from(listener))
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use sched_model::test_support::example_config;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tracing::{Level, Metadata, Subscriber, span};

    /// Counts events by level; nothing else of the subscriber API is needed.
    #[derive(Default)]
    struct Count {
        warn: AtomicU64,
        debug: AtomicU64,
    }

    struct Counting(Arc<Count>);

    impl Subscriber for Counting {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
            span::Id::from_u64(1)
        }
        fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
        fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            let level = *event.metadata().level();
            if level == Level::WARN {
                self.0.warn.fetch_add(1, Ordering::SeqCst);
            } else if level == Level::DEBUG {
                self.0.debug.fetch_add(1, Ordering::SeqCst);
            }
        }
        fn enter(&self, _: &span::Id) {}
        fn exit(&self, _: &span::Id) {}
    }

    fn object(name: &str, metadata: &[(&str, &str)]) -> ScaledObjectRef {
        ScaledObjectRef {
            name: name.into(),
            namespace: "ns".into(),
            scaler_metadata: metadata
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    /// Runs `body` with a subscriber that counts events; returns (warn, debug).
    fn counted(body: impl FnOnce()) -> (u64, u64) {
        let count = Arc::new(Count::default());
        tracing::subscriber::with_default(Counting(Arc::clone(&count)), body);
        (
            count.warn.load(Ordering::SeqCst),
            count.debug.load(Ordering::SeqCst),
        )
    }

    fn answer(r: &ScaledObjectRef, config: &Config, invalid: &InvalidLog) -> Result<(), Status> {
        evaluate(r, config, Decision::NotReady, invalid).map(|_| ())
    }

    #[test]
    fn a_repeated_invalid_metadata_error_warns_once_and_then_logs_at_debug() {
        let config = example_config();
        let invalid = InvalidLog::default();
        let gone = object("gone", &[("model", "no-such-model")]);
        let (warn, debug) = counted(|| {
            for _ in 0..50 {
                // The error the caller gets is the same every time.
                let e = answer(&gone, &config, &invalid).unwrap_err();
                assert_eq!(e.code(), tonic::Code::InvalidArgument);
                assert!(
                    e.message().starts_with(
                        "ScaledObject ns/gone: model \"no-such-model\" is not in the active config"
                    ),
                    "{}",
                    e.message()
                );
            }
        });
        assert_eq!((warn, debug), (1, 49));

        // Once fixed and broken again, the next error warns again.
        let fixed = object("gone", &[("model", "mineru")]);
        let (warn, debug) = counted(|| {
            assert_eq!(
                answer(&fixed, &config, &invalid).unwrap_err().code(),
                tonic::Code::Unavailable
            );
            answer(&gone, &config, &invalid).unwrap_err();
            answer(&gone, &config, &invalid).unwrap_err();
        });
        assert_eq!((warn, debug), (1, 1));

        // A different error for the same ScaledObject warns again, once.
        let other = object("gone", &[("model", "mineru"), ("class", "paperless-gpt")]);
        let (warn, debug) = counted(|| {
            for _ in 0..5 {
                answer(&other, &config, &invalid).unwrap_err();
            }
        });
        assert_eq!((warn, debug), (1, 4));

        // Another ScaledObject with the same error warns for itself.
        let (warn, _) = counted(|| {
            answer(
                &object("second", &[("model", "no-such-model")]),
                &config,
                &invalid,
            )
            .unwrap_err();
        });
        assert_eq!(warn, 1);
    }

    #[test]
    fn the_scaler_rpcs_share_one_memory_of_what_they_refused() {
        let config = Arc::new(example_config());
        let (_allowed_tx, allowed) = watch::channel(None::<PublishedAllowed>);
        let (_config_tx, config_rx) = watch::channel(config);
        let scaler = Scaler::new(allowed, config_rx);
        let gone = object("gone", &[("class", "no-such-class")]);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let (warn, _) = counted(|| {
            rt.block_on(async {
                for _ in 0..4 {
                    // The unary RPCs, a clone of the scaler (another connection) and the
                    // stream's first answer all count as one ScaledObject's repeats.
                    let c = scaler.clone();
                    let e = c.is_active(Request::new(gone.clone())).await.unwrap_err();
                    assert_eq!(e.code(), tonic::Code::InvalidArgument);
                    scaler
                        .get_metric_spec(Request::new(gone.clone()))
                        .await
                        .unwrap_err();
                    scaler
                        .stream_is_active(Request::new(gone.clone()))
                        .await
                        .err()
                        .unwrap();
                }
            });
        });
        assert_eq!(warn, 1);
    }

    #[test]
    fn the_memory_of_invalid_scaled_objects_is_bounded() {
        let config = example_config();
        let invalid = InvalidLog::default();
        let (warn, _) = counted(|| {
            for i in 0..(3 * MAX_REMEMBERED) {
                answer(
                    &object(&format!("so-{i}"), &[("model", "no-such-model")]),
                    &config,
                    &invalid,
                )
                .unwrap_err();
            }
        });
        // Each distinct ScaledObject warned (none was a repeat) and the memory stayed bounded.
        assert_eq!(warn, (3 * MAX_REMEMBERED) as u64);
        let held = invalid.last.lock().unwrap().len();
        assert!(held <= MAX_REMEMBERED, "{held}");
    }
}
