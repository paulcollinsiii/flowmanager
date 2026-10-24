//! The KEDA scaler as the binary wires it (`keda_scaler::spawn`), over a real socket: it
//! answers from what this process's ticks publish and validates ids against the config the
//! `ConfigHolder` holds, reloads included.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use sched_keda::proto::ScaledObjectRef;
use sched_keda::proto::external_scaler_client::ExternalScalerClient;
use sched_model::test_support::EXAMPLE_CONFIG;
use sched_model::{Fidelity, Millis, ObservedValue};
use sched_observe::Report;
use sched_service::clock::ManualClock;
use sched_service::config_source::{ConfigHolder, ReloadOutcome};
use sched_service::keda_scaler;
use sched_service::metrics::Metrics;
use sched_service::tick::{Durable, Shared};
use tonic::Code;

const T0: u64 = 50_000_000;

fn class(id: &str) -> ScaledObjectRef {
    ScaledObjectRef {
        name: format!("so-{id}"),
        namespace: "ns".into(),
        scaler_metadata: [("class".to_string(), id.to_string())].into(),
    }
}

/// The example without the `paperless-ngx` class.
fn without_paperless_ngx() -> String {
    let block = "  - id: paperless-ngx\n    requires:\n      - { resource: basestar-nfs }\n    priority: 0\n    preemptibility: none\n";
    assert!(EXAMPLE_CONFIG.contains(block));
    EXAMPLE_CONFIG.replacen(block, "", 1)
}

#[tokio::test]
async fn the_wired_scaler_answers_from_ticks_and_follows_config_reloads() {
    let clock = Arc::new(ManualClock::new(Millis(T0)));
    let holder = Arc::new(ConfigHolder::new(EXAMPLE_CONFIG).unwrap());
    let shared = Arc::new(Shared::new(
        Arc::clone(&holder),
        Arc::new(Metrics::new()),
        clock,
    ));
    let addr = keda_scaler::spawn(&shared, SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let mut client = ExternalScalerClient::connect(format!("http://{addr}"))
        .await
        .unwrap();

    // Before this process's first tick: not ready, never "inactive".
    let e = client.is_active(class("paperless-ngx")).await.unwrap_err();
    assert_eq!(e.code(), Code::Unavailable, "{e:?}");
    let e = client
        .stream_is_active(class("paperless-ngx"))
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::Unavailable, "{e:?}");

    // NFS is up, so the tick allows the NFS-only class.
    shared.ingest(
        Report::Observed {
            source: "basestar-probe".into(),
            fidelity: Fidelity::Polled,
            values: vec![ObservedValue::ResourceUp {
                resource: "basestar-nfs".into(),
                up: true,
            }],
        },
        Millis(T0),
    );
    let mut durable = Durable::default();
    let report = shared.tick(Millis(T0 + 1_000), &mut durable);
    assert!(report.error.is_none(), "{report:?}");
    let published = shared.published_allowed().unwrap();
    assert!(
        published.allowed.classes.contains(&"paperless-ngx".into()),
        "{published:?}"
    );
    assert!(
        client
            .is_active(class("paperless-ngx"))
            .await
            .unwrap()
            .into_inner()
            .result
    );
    assert!(
        !client
            .is_active(class("paperless-ocr"))
            .await
            .unwrap()
            .into_inner()
            .result
    );
    let mut stream = client
        .stream_is_active(class("paperless-ngx"))
        .await
        .unwrap()
        .into_inner();
    let first = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.map(|m| m.result), Some(true));

    // The class is removed by a reload: refused, and the open stream ends with it.
    assert!(matches!(
        holder.apply_text(&without_paperless_ngx()),
        ReloadOutcome::Applied(2)
    ));
    let e = client.is_active(class("paperless-ngx")).await.unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument, "{e:?}");
    assert!(e.message().contains("generation 2"), "{e:?}");
    let e = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument, "{e:?}");

    // Put back: answered again, from the published set.
    assert!(matches!(
        holder.apply_text(EXAMPLE_CONFIG),
        ReloadOutcome::Applied(3)
    ));
    assert!(
        client
            .is_active(class("paperless-ngx"))
            .await
            .unwrap()
            .into_inner()
            .result
    );
}
