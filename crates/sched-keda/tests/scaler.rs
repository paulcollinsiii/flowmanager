//! The scaler served over a real socket, called through the generated tonic client.

use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use sched_keda::proto::external_scaler_client::ExternalScalerClient;
use sched_keda::proto::{GetMetricsRequest, IsActiveResponse, ScaledObjectRef};
use sched_keda::{NOT_READY, STOPPED, Scaler};
use sched_model::test_support::EXAMPLE_CONFIG;
use sched_model::{AllowedSet, ClassId, Config, Millis, ModelId, PublishedAllowed};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use tonic::{Code, Status, Streaming};

/// How long to wait for a stream message that must not come.
const QUIET: Duration = Duration::from_millis(300);
/// How long to wait for one that must.
const PROMPT: Duration = Duration::from_secs(5);

struct Harness {
    allowed: watch::Sender<Option<PublishedAllowed>>,
    config: watch::Sender<Arc<Config>>,
    client: ExternalScalerClient<Channel>,
    _server: JoinHandle<()>,
}

fn example(generation: u64) -> Arc<Config> {
    Arc::new(Config::from_yaml_with_generation(EXAMPLE_CONFIG, generation).expect("valid"))
}

fn replaced(text: &str, from: &str, to: &str) -> String {
    assert!(text.contains(from), "{from:?} is not in the text");
    text.replacen(from, to, 1)
}

/// The example config with one more model (`extra`) and one more gate class
/// (`host-gate`), as a config reload would add them.
fn example_with_additions(generation: u64) -> Arc<Config> {
    let text = replaced(
        EXAMPLE_CONFIG,
        "models:\n",
        "models:\n  - { id: extra, resource: cuda-central, load_time: 30s, workloads: [extra] }\n",
    );
    let text = replaced(
        &text,
        "classes:\n",
        "classes:\n  - id: host-gate\n    requires:\n      - { resource: basestar-nfs }\n    priority: 0\n    preemptibility: none\n\n",
    );
    Arc::new(Config::from_yaml_with_generation(&text, generation).expect("valid"))
}

/// The example config without the `paperless-ngx` class.
fn example_without_paperless_ngx(generation: u64) -> Arc<Config> {
    let text = replaced(
        EXAMPLE_CONFIG,
        "  - id: paperless-ngx\n    requires:\n      - { resource: basestar-nfs }\n    priority: 0\n    preemptibility: none\n",
        "",
    );
    let config = Config::from_yaml_with_generation(&text, generation).expect("valid");
    assert!(config.class(&ClassId::from("paperless-ngx")).is_none());
    Arc::new(config)
}

fn published(models: &[&str], classes: &[&str]) -> Option<PublishedAllowed> {
    Some(PublishedAllowed {
        config_generation: 1,
        at: Millis(1_000),
        allowed: AllowedSet {
            models: models
                .iter()
                .map(|m| ModelId::from(*m))
                .collect::<BTreeSet<_>>(),
            classes: classes
                .iter()
                .map(|c| ClassId::from(*c))
                .collect::<BTreeSet<_>>(),
        },
    })
}

async fn start(config: Arc<Config>) -> Harness {
    let (allowed, allowed_rx) = watch::channel(None);
    let (config, config_rx) = watch::channel(config);
    let listener = sched_keda::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let scaler = Scaler::new(allowed_rx, config_rx);
    let server = tokio::spawn(async move {
        sched_keda::serve(listener, scaler).await.expect("serve");
    });
    let client = ExternalScalerClient::connect(format!("http://{addr}"))
        .await
        .expect("connect");
    Harness {
        allowed,
        config,
        client,
        _server: server,
    }
}

fn object(name: &str, metadata: &[(&str, &str)]) -> ScaledObjectRef {
    ScaledObjectRef {
        name: name.to_string(),
        namespace: "ns".to_string(),
        scaler_metadata: metadata
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<_, _>>(),
    }
}

fn model(id: &str) -> ScaledObjectRef {
    object(&format!("so-{id}"), &[("model", id)])
}

fn class(id: &str) -> ScaledObjectRef {
    object(&format!("so-{id}"), &[("class", id)])
}

/// The outcome of each unary RPC for `r`, as (is_active, metric spec, metric value).
#[derive(Debug)]
struct Unary {
    is_active: Result<bool, Status>,
    spec: Result<(String, i64, f64), Status>,
    value: Result<(String, i64, f64), Status>,
}

async fn unary(client: &mut ExternalScalerClient<Channel>, r: &ScaledObjectRef) -> Unary {
    let is_active = client
        .is_active(r.clone())
        .await
        .map(|a| a.into_inner().result);
    let spec = client.get_metric_spec(r.clone()).await.map(|s| {
        let specs = s.into_inner().metric_specs;
        assert_eq!(specs.len(), 1, "{specs:?}");
        (
            specs[0].metric_name.clone(),
            specs[0].target_size,
            specs[0].target_size_float,
        )
    });
    // Ask for the metric the spec names (as KEDA does); before a spec exists, the one it
    // would name.
    let metric_name = match &spec {
        Ok((name, _, _)) => name.clone(),
        Err(_) => String::from("flowmanager-model-x"),
    };
    let value = client
        .get_metrics(GetMetricsRequest {
            scaled_object_ref: Some(r.clone()),
            metric_name,
        })
        .await
        .map(|m| {
            let values = m.into_inner().metric_values;
            assert_eq!(values.len(), 1, "{values:?}");
            (
                values[0].metric_name.clone(),
                values[0].metric_value,
                values[0].metric_value_float,
            )
        });
    Unary {
        is_active,
        spec,
        value,
    }
}

/// Asserts that every RPC, the stream included, fails with `code`, and that each message
/// contains `text`.
async fn assert_all_fail(h: &mut Harness, r: &ScaledObjectRef, code: Code, text: &str) {
    let u = unary(&mut h.client, r).await;
    for (rpc, status) in [
        ("IsActive", u.is_active.err()),
        ("GetMetricSpec", u.spec.err()),
        ("GetMetrics", u.value.err()),
        (
            "StreamIsActive",
            h.client.stream_is_active(r.clone()).await.err(),
        ),
    ] {
        let status = status.unwrap_or_else(|| panic!("{rpc} for {r:?} succeeded"));
        assert_eq!(status.code(), code, "{rpc} for {r:?}: {status:?}");
        assert!(
            status.message().contains(text),
            "{rpc} for {r:?}: {:?} does not contain {text:?}",
            status.message()
        );
    }
}

async fn next(stream: &mut Streaming<IsActiveResponse>) -> Result<Option<bool>, Status> {
    tokio::time::timeout(PROMPT, stream.message())
        .await
        .expect("a stream message in time")
        .map(|m| m.map(|r| r.result))
}

async fn assert_quiet(stream: &mut Streaming<IsActiveResponse>) {
    if let Ok(m) = tokio::time::timeout(QUIET, stream.message()).await {
        panic!("unexpected stream message: {m:?}");
    }
}

#[tokio::test]
async fn every_rpc_is_unavailable_before_the_first_decision() {
    let mut h = start(example(1)).await;
    for r in [model("mineru"), class("paperless-ocr")] {
        assert_all_fail(&mut h, &r, Code::Unavailable, NOT_READY).await;
    }
}

#[tokio::test]
async fn answers_follow_the_published_allowed_set() {
    let mut h = start(example(1)).await;
    h.allowed
        .send_replace(published(&["mineru"], &["paperless-ocr"]));

    let u = unary(&mut h.client, &model("mineru")).await;
    assert!(u.is_active.unwrap());
    assert_eq!(
        u.spec.unwrap(),
        ("flowmanager-model-mineru".to_string(), 1, 1.0)
    );
    assert_eq!(
        u.value.unwrap(),
        ("flowmanager-model-mineru".to_string(), 1, 1.0)
    );

    let u = unary(&mut h.client, &model("llm")).await;
    assert!(!u.is_active.unwrap());
    assert_eq!(
        u.spec.unwrap(),
        ("flowmanager-model-llm".to_string(), 1, 1.0)
    );
    assert_eq!(
        u.value.unwrap(),
        ("flowmanager-model-llm".to_string(), 0, 0.0)
    );

    let u = unary(&mut h.client, &class("paperless-ocr")).await;
    assert!(u.is_active.unwrap());
    assert_eq!(
        u.value.unwrap(),
        ("flowmanager-class-paperless-ocr".to_string(), 1, 1.0)
    );

    // A model and a class of the same name are different subjects.
    let u = unary(&mut h.client, &class("immich-ml")).await;
    assert!(!u.is_active.unwrap());
    assert_eq!(
        u.spec.unwrap(),
        ("flowmanager-class-immich-ml".to_string(), 1, 1.0)
    );
    assert_eq!(
        u.value.unwrap(),
        ("flowmanager-class-immich-ml".to_string(), 0, 0.0)
    );

    // The next tick moves the answers.
    h.allowed.send_replace(published(&["llm"], &[]));
    let u = unary(&mut h.client, &model("mineru")).await;
    assert!(!u.is_active.unwrap());
    assert_eq!(u.value.unwrap().1, 0);
    assert!(unary(&mut h.client, &model("llm")).await.is_active.unwrap());
    assert!(
        !unary(&mut h.client, &class("paperless-ocr"))
            .await
            .is_active
            .unwrap()
    );
}

#[tokio::test]
async fn keda_trigger_keys_are_accepted_alongside_the_subject() {
    let mut h = start(example(1)).await;
    h.allowed.send_replace(published(&["whisperx"], &[]));
    let r = object(
        "so",
        &[
            ("model", "whisperx"),
            ("scalerAddress", "flowmanager.flowmanager.svc:9100"),
            ("enableTLS", "false"),
            ("unsafeSsl", "false"),
        ],
    );
    let u = unary(&mut h.client, &r).await;
    assert!(u.is_active.unwrap());
    assert_eq!(u.value.unwrap().1, 1);
}

#[tokio::test]
async fn invalid_metadata_is_invalid_argument_on_every_rpc_ready_or_not() {
    let cases: Vec<(ScaledObjectRef, &str)> = vec![
        (object("none", &[]), "neither `model` nor `class`"),
        (
            object("only-keda", &[("scalerAddress", "x:1")]),
            "neither `model` nor `class`",
        ),
        (
            object("both", &[("model", "mineru"), ("class", "paperless-ocr")]),
            "both `model` and `class`",
        ),
        (
            object("extra-key", &[("model", "mineru"), ("workload", "mineru")]),
            "unknown metadata key(s) [\"workload\"]",
        ),
        (
            object("typo", &[("modle", "mineru")]),
            "unknown metadata key(s) [\"modle\"]",
        ),
        (
            object("empty", &[("model", "")]),
            "metadata `model` is empty",
        ),
        (
            object("unknown-model", &[("model", "gpt-9")]),
            "model \"gpt-9\" is not in the active config (generation 1)",
        ),
        (
            object("unknown-class", &[("class", "nope")]),
            "class \"nope\" is not in the active config (generation 1)",
        ),
        // Ids are not shared between the two kinds.
        (
            object("class-as-model", &[("model", "paperless-ocr")]),
            "model \"paperless-ocr\" is not in the active config",
        ),
        (
            object("model-as-class", &[("class", "mineru")]),
            "class \"mineru\" is not in the active config",
        ),
    ];
    let mut h = start(example(1)).await;
    for (r, text) in &cases {
        assert_all_fail(&mut h, r, Code::InvalidArgument, text).await;
    }
    h.allowed
        .send_replace(published(&["mineru", "llm"], &["paperless-ocr"]));
    for (r, text) in &cases {
        assert_all_fail(&mut h, r, Code::InvalidArgument, text).await;
        // The message names the ScaledObject.
        let e = h.client.is_active(r.clone()).await.unwrap_err();
        assert!(
            e.message()
                .starts_with(&format!("ScaledObject ns/{}: ", r.name)),
            "{e:?}"
        );
    }
}

#[tokio::test]
async fn get_metrics_refuses_another_metric_name_or_a_missing_ref() {
    let mut h = start(example(1)).await;
    h.allowed.send_replace(published(&["mineru"], &[]));
    let e = h
        .client
        .get_metrics(GetMetricsRequest {
            scaled_object_ref: Some(model("mineru")),
            metric_name: "flowmanager-model-llm".to_string(),
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument, "{e:?}");
    assert!(e.message().contains("unknown metric"), "{e:?}");
    let e = h
        .client
        .get_metrics(GetMetricsRequest {
            scaled_object_ref: None,
            metric_name: "flowmanager-model-mineru".to_string(),
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument, "{e:?}");
}

#[tokio::test]
async fn stream_metric_spec_is_unimplemented_so_keda_falls_back_to_pull() {
    let mut h = start(example(1)).await;
    h.allowed.send_replace(published(&["mineru"], &[]));
    let e = h
        .client
        .stream_metric_spec(model("mineru"))
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::Unimplemented, "{e:?}");
}

#[tokio::test]
async fn stream_sends_at_once_then_only_when_its_own_answer_changes() {
    let mut h = start(example(1)).await;
    h.allowed.send_replace(published(&["mineru"], &[]));
    let mut mineru = h
        .client
        .stream_is_active(model("mineru"))
        .await
        .unwrap()
        .into_inner();
    let mut ocr = h
        .client
        .stream_is_active(class("paperless-ocr"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(next(&mut mineru).await.unwrap(), Some(true));
    assert_eq!(next(&mut ocr).await.unwrap(), Some(false));
    assert_quiet(&mut mineru).await;

    // Another object's answer changes: only that stream hears of it.
    h.allowed
        .send_replace(published(&["mineru"], &["paperless-ocr"]));
    assert_eq!(next(&mut ocr).await.unwrap(), Some(true));
    assert_quiet(&mut mineru).await;

    // A new tick with the same answers: nothing is sent.
    h.allowed
        .send_replace(published(&["mineru"], &["paperless-ocr"]));
    assert_quiet(&mut mineru).await;
    assert_quiet(&mut ocr).await;

    // Its own answer changes, both ways.
    h.allowed.send_replace(published(&[], &[]));
    assert_eq!(next(&mut mineru).await.unwrap(), Some(false));
    assert_eq!(next(&mut ocr).await.unwrap(), Some(false));
    h.allowed.send_replace(published(&["mineru"], &[]));
    assert_eq!(next(&mut mineru).await.unwrap(), Some(true));
    assert_quiet(&mut ocr).await;

    // An unrelated config reload (a new generation, same ids) changes no answer.
    h.config.send_replace(example(2));
    assert_quiet(&mut mineru).await;
    assert_quiet(&mut ocr).await;
}

#[tokio::test]
async fn a_stream_ends_when_its_client_goes_away() {
    let mut h = start(example(1)).await;
    h.allowed.send_replace(published(&["mineru"], &[]));
    // One receiver is the scaler's own; each open stream holds one more.
    let base = h.allowed.receiver_count();
    let mut streams = Vec::new();
    for _ in 0..3 {
        let mut s = h
            .client
            .stream_is_active(model("mineru"))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(next(&mut s).await.unwrap(), Some(true));
        streams.push(s);
    }
    assert_eq!(h.allowed.receiver_count(), base + 3);
    drop(streams);
    // Nothing is published: the stream tasks must notice the client leaving on their own.
    let deadline = tokio::time::Instant::now() + PROMPT;
    while h.allowed.receiver_count() != base {
        assert!(
            tokio::time::Instant::now() < deadline,
            "stream tasks still hold {} receivers",
            h.allowed.receiver_count() - base
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_dropped_publisher_is_unavailable_never_the_last_answer() {
    let mut h = start(example(1)).await;
    h.allowed.send_replace(published(&["mineru"], &[]));
    let mut s = h
        .client
        .stream_is_active(model("mineru"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(next(&mut s).await.unwrap(), Some(true));

    // The scheduler's sender goes away (the harness keeps a fresh one nobody reads).
    drop(std::mem::replace(&mut h.allowed, watch::channel(None).0));
    let e = next(&mut s).await.unwrap_err();
    assert_eq!(e.code(), Code::Unavailable, "{e:?}");
    assert!(e.message().contains(STOPPED), "{e:?}");

    for r in [model("mineru"), model("llm")] {
        assert_all_fail(&mut h, &r, Code::Unavailable, STOPPED).await;
    }
    // Broken metadata still reads as broken.
    assert_all_fail(&mut h, &model("gpt-9"), Code::InvalidArgument, "gpt-9").await;
}

#[tokio::test]
async fn config_reload_adds_and_removes_ids() {
    let mut h = start(example(1)).await;
    h.allowed
        .send_replace(published(&["mineru"], &["paperless-ngx"]));

    // Not configured yet.
    assert_all_fail(
        &mut h,
        &model("extra"),
        Code::InvalidArgument,
        "model \"extra\" is not in the active config (generation 1)",
    )
    .await;
    assert_all_fail(
        &mut h,
        &class("host-gate"),
        Code::InvalidArgument,
        "host-gate",
    )
    .await;
    let mut ngx = h
        .client
        .stream_is_active(class("paperless-ngx"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(next(&mut ngx).await.unwrap(), Some(true));

    // Added: known now, and answered from the allowed set, which does not have them yet.
    h.config.send_replace(example_with_additions(2));
    let u = unary(&mut h.client, &model("extra")).await;
    assert!(!u.is_active.unwrap());
    assert_eq!(
        u.value.unwrap(),
        ("flowmanager-model-extra".to_string(), 0, 0.0)
    );
    assert!(
        !unary(&mut h.client, &class("host-gate"))
            .await
            .is_active
            .unwrap()
    );
    let mut gate = h
        .client
        .stream_is_active(class("host-gate"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(next(&mut gate).await.unwrap(), Some(false));
    assert_quiet(&mut ngx).await;

    // The next tick allows them.
    h.allowed
        .send_replace(published(&["extra"], &["host-gate", "paperless-ngx"]));
    assert_eq!(next(&mut gate).await.unwrap(), Some(true));
    assert!(
        unary(&mut h.client, &model("extra"))
            .await
            .is_active
            .unwrap()
    );

    // Removed: refused even while still in the published set, and an open stream ends.
    h.config.send_replace(example_without_paperless_ngx(3));
    let e = next(&mut ngx).await.unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument, "{e:?}");
    assert!(
        e.message()
            .contains("class \"paperless-ngx\" is not in the active config (generation 3)"),
        "{e:?}"
    );
    assert_all_fail(
        &mut h,
        &class("paperless-ngx"),
        Code::InvalidArgument,
        "paperless-ngx",
    )
    .await;
    let e = next(&mut gate).await.unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument, "{e:?}");
    assert!(e.message().contains("host-gate"), "{e:?}");
    assert_all_fail(&mut h, &model("extra"), Code::InvalidArgument, "extra").await;
    // Ids still configured keep their answers.
    assert!(
        !unary(&mut h.client, &model("mineru"))
            .await
            .is_active
            .unwrap()
    );
}
