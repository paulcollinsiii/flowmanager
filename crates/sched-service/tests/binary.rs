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

/// Start the binary on `yaml` and wait for it to exit: its status and stderr. The child is
/// killed (and the test fails) if it is still running at the deadline.
fn start_and_expect_exit(tag: &str, yaml: &str) -> (std::process::ExitStatus, String) {
    use std::io::Read;
    let dir = std::env::temp_dir().join(format!("sched-binary-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let config = dir.join("config.yaml");
    std::fs::write(&config, yaml).unwrap();
    let (restate, metrics, keda, ingress) = (free_addr(), free_addr(), free_addr(), free_addr());
    let mut child = Running(
        Command::new(env!("CARGO_BIN_EXE_flowmanager"))
            .arg("--config")
            .arg(&config)
            .args(["--listen", &restate.to_string()])
            .args(["--metrics-listen", &metrics.to_string()])
            .args(["--keda-listen", &keda.to_string()])
            .args(["--restate-ingress", &format!("http://{ingress}")])
            .env_remove("KUBERNETES_SERVICE_HOST")
            .env_remove("KUBERNETES_SERVICE_PORT")
            .env("KUBECONFIG", dir.join("no-such-kubeconfig"))
            .env("HOME", &dir)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start sched-service"),
        dir,
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        // The guard kills the child when this panics.
        assert!(
            Instant::now() < deadline,
            "the binary did not exit on an invalid config"
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    let mut stderr = String::new();
    child
        .0
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    (status, stderr)
}

#[test]
fn the_binary_refuses_to_start_on_a_class_whose_failed_source_has_no_recovery() {
    // Break only this rule: take the recovery block off `speakr-transcription`, whose source
    // `speakr-pending` counts failed items.
    // The example has more than one recovery (speakr-summarization has one too); take them
    // all off, so the refusal names `speakr-transcription` among the classes left bare.
    let end_marker = "        timeout: 10s\n";
    let mut yaml = EXAMPLE_CONFIG.to_string();
    let mut removed = 0;
    while let Some(start) = yaml.find("    recovery:\n") {
        let end = start + yaml[start..].find(end_marker).unwrap() + end_marker.len();
        yaml.replace_range(start..end, "");
        removed += 1;
    }
    assert!(removed >= 1, "the example has a recovery");
    assert!(!yaml.contains("recovery:") && yaml.contains("counts_as: failed"));
    let (status, stderr) = start_and_expect_exit("norecovery", &yaml);
    assert!(!status.success(), "{status}: {stderr}");
    assert!(
        stderr.contains("speakr-transcription") && stderr.contains("speakr-pending"),
        "{stderr}"
    );
    assert!(stderr.contains("no recovery"), "{stderr}");
}

#[test]
fn the_binary_refuses_to_start_on_a_with_work_class_without_demand() {
    let anchor = "  - id: wikijs\n";
    assert!(EXAMPLE_CONFIG.contains(anchor));
    let yaml = EXAMPLE_CONFIG.replacen(anchor, &format!("{anchor}    allowed: with_work\n"), 1);
    let (status, stderr) = start_and_expect_exit("nodemand", &yaml);
    assert!(!status.success(), "{status}: {stderr}");
    assert!(
        stderr.contains("wikijs") && stderr.contains("with_work needs a demand source"),
        "{stderr}"
    );
}
