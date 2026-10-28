//! The binary itself: started as a process, with no Restate server behind it, it must come
//! up serving everything it is meant to. This is the wiring `main` does and nothing else
//! tests: dropping the `keda_scaler::spawn` call (or the metrics server, or the Restate
//! endpoint) from `main` fails here, while the flag-parsing tests and the tests of
//! `keda_scaler::spawn` itself would still pass.

use std::net::{SocketAddr, TcpListener};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use sched_keda::proto::ScaledObjectRef;
use sched_keda::proto::external_scaler_client::ExternalScalerClient;
use sched_model::test_support::EXAMPLE_CONFIG;
use tonic::Code;

/// The child process and its temp dir, cleaned up when the test ends however it ends.
struct Running(Child, std::path::PathBuf);

impl Drop for Running {
    fn drop(&mut self) {
        drop(self.0.kill());
        drop(self.0.wait());
        drop(std::fs::remove_dir_all(&self.1));
    }
}

/// An address nothing listens on (yet): bind port 0, note it, release it.
fn free_addr() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap()
}

#[tokio::test]
async fn the_binary_serves_the_restate_endpoint_metrics_and_the_keda_scaler() {
    let dir = std::env::temp_dir().join(format!("sched-binary-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let config = dir.join("config.yaml");
    std::fs::write(&config, EXAMPLE_CONFIG).unwrap();
    let (restate, metrics, keda, ingress) = (free_addr(), free_addr(), free_addr(), free_addr());
    let mut child = Running(
        Command::new(env!("CARGO_BIN_EXE_flowmanager"))
            .arg("--config")
            .arg(&config)
            .args(["--listen", &restate.to_string()])
            .args(["--metrics-listen", &metrics.to_string()])
            .args(["--keda-listen", &keda.to_string()])
            // Nothing listens here: no tick ever completes, so the scaler stays not ready.
            .args(["--restate-ingress", &format!("http://{ingress}")])
            // The example config has a kubernetes_watch source: make sure it can never find
            // (and contact) a real cluster from this machine.
            .env_remove("KUBERNETES_SERVICE_HOST")
            .env_remove("KUBERNETES_SERVICE_PORT")
            .env("KUBECONFIG", dir.join("no-such-kubeconfig"))
            .env("HOME", &dir)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start sched-service"),
        dir.clone(),
    );

    // Start-up takes a moment; the deadline covers a slow machine.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut client = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            panic!("sched-service exited early: {status}");
        }
        match ExternalScalerClient::connect(format!("http://{keda}")).await {
            Ok(c) => break c,
            Err(e) => {
                assert!(
                    Instant::now() < deadline,
                    "the KEDA scaler never came up: {e}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };

    // A valid id before the first tick: not ready, never "inactive".
    let e = client
        .is_active(ScaledObjectRef {
            name: "so".into(),
            namespace: "ns".into(),
            scaler_metadata: [("model".to_string(), "mineru".to_string())].into(),
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::Unavailable, "{e:?}");
    // An id the config does not have is refused: the scaler reads the loaded config.
    let e = client
        .is_active(ScaledObjectRef {
            name: "so".into(),
            namespace: "ns".into(),
            scaler_metadata: [("model".to_string(), "no-such-model".to_string())].into(),
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument, "{e:?}");

    // Metrics are served on their own address, with the process's own series.
    let body = reqwest::get(format!("http://{metrics}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.contains("sched_config_generation"), "{body}");
    let health = reqwest::get(format!("http://{metrics}/healthz"))
        .await
        .unwrap();
    assert_eq!(health.status(), 200);

    // The Restate endpoint is bound after the scaler, so allow it a moment.
    loop {
        match tokio::net::TcpStream::connect(restate).await {
            Ok(_) => break,
            Err(e) => {
                assert!(
                    Instant::now() < deadline,
                    "the Restate endpoint never listened: {e}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    assert!(
        child.0.try_wait().unwrap().is_none(),
        "the binary is still running"
    );
}
