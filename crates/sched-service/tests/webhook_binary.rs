//! The webhook listener as wired by `main`: the flags that start it or refuse to, and a
//! request reaching the observers' nudge registry through the real process. Like
//! `binary.rs` this runs the binary with no Restate server behind it.

use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use sched_model::test_support::EXAMPLE_CONFIG;

struct Running(Child, PathBuf);

impl Drop for Running {
    fn drop(&mut self) {
        drop(self.0.kill());
        drop(self.0.wait());
        drop(std::fs::remove_dir_all(&self.1));
    }
}

fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("sched-webhook-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&d).unwrap();
    std::fs::write(d.join("config.yaml"), EXAMPLE_CONFIG).unwrap();
    d
}

fn command(dir: &Path, metrics: SocketAddr, extra: &[&str]) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_flowmanager"));
    c.arg("--config")
        .arg(dir.join("config.yaml"))
        .args(["--listen", &free_addr().to_string()])
        .args(["--metrics-listen", &metrics.to_string()])
        .args(["--keda-listen", &free_addr().to_string()])
        .args(["--restate-ingress", &format!("http://{}", free_addr())])
        .args(extra)
        .env_remove("KUBERNETES_SERVICE_HOST")
        .env_remove("KUBERNETES_SERVICE_PORT")
        .env_remove("SCHED_WEBHOOK_LISTEN")
        .env_remove("SCHED_WEBHOOK_AUTH")
        .env_remove("SCHED_WEBHOOK_TOKEN_FILE")
        .env("KUBECONFIG", dir.join("no-such-kubeconfig"))
        .env("HOME", dir)
        .stdin(Stdio::null());
    c
}

/// Runs the binary expecting it to refuse to start; returns its stderr.
fn refused(name: &str, extra: &[&str], token: Option<&str>) -> String {
    let d = dir(name);
    if let Some(t) = token {
        std::fs::write(d.join("token"), t).unwrap();
    }
    let out = command(&d, free_addr(), extra)
        .output()
        .expect("run sched-service");
    let _ = std::fs::remove_dir_all(&d);
    assert!(!out.status.success(), "{extra:?} should not start");
    String::from_utf8_lossy(&out.stderr).into_owned()
}

#[test]
fn the_listener_refuses_to_start_without_a_clear_choice_of_authentication() {
    let e = refused("noauth", &["--webhook-listen", "127.0.0.1:0"], None);
    assert!(e.contains("--webhook-auth"), "{e}");
    let e = refused("authonly", &["--webhook-auth", "mesh"], None);
    assert!(e.contains("--webhook-listen"), "{e}");
    let e = refused(
        "notoken",
        &["--webhook-listen", "127.0.0.1:0", "--webhook-auth", "token"],
        None,
    );
    assert!(e.contains("--webhook-token-file"), "{e}");
    let e = refused(
        "meshtoken",
        &[
            "--webhook-listen",
            "127.0.0.1:0",
            "--webhook-auth",
            "mesh",
            "--webhook-token-file",
            "/t",
        ],
        None,
    );
    assert!(e.contains("mesh"), "{e}");
    let e = refused(
        "badauth",
        &["--webhook-listen", "127.0.0.1:0", "--webhook-auth", "none"],
        None,
    );
    assert!(e.contains("mesh") && e.contains("token"), "{e}");
}

#[test]
fn token_mode_refuses_to_start_with_a_missing_or_empty_token_file() {
    let d = dir("emptytoken");
    let missing = d.join("nope").to_string_lossy().into_owned();
    let args = |f: &str| {
        vec![
            "--webhook-listen".to_string(),
            "127.0.0.1:0".to_string(),
            "--webhook-auth".to_string(),
            "token".to_string(),
            "--webhook-token-file".to_string(),
            f.to_string(),
        ]
    };
    for (what, file, content) in [
        ("missing", missing, None),
        ("empty", String::new(), Some("\n")),
    ] {
        let file = match content {
            Some(c) => {
                let p = d.join("token");
                std::fs::write(&p, c).unwrap();
                p.to_string_lossy().into_owned()
            }
            None => file,
        };
        let a = args(&file);
        let a: Vec<&str> = a.iter().map(String::as_str).collect();
        let out = command(&d, free_addr(), &a)
            .output()
            .expect("run sched-service");
        assert!(!out.status.success(), "{what}");
        let e = String::from_utf8_lossy(&out.stderr);
        assert!(e.contains("webhook token is not available"), "{what}: {e}");
    }
    let _ = std::fs::remove_dir_all(&d);
}

async fn status(
    client: &reqwest::Client,
    method: reqwest::Method,
    url: &str,
    token: Option<&str>,
) -> (u16, String) {
    let mut r = client.request(method, url).body("{\"ignored\": true}");
    if let Some(t) = token {
        r = r.bearer_auth(t);
    }
    let resp = r.send().await.expect("the listener answers");
    (resp.status().as_u16(), resp.text().await.unwrap())
}

/// Starts the binary with the webhook listener and waits until it answers.
async fn serving(name: &str, auth: &[&str]) -> (Running, SocketAddr, SocketAddr) {
    let d = dir(name);
    std::fs::write(d.join("token"), "s3cret\n").unwrap();
    let (hook, metrics) = (free_addr(), free_addr());
    let mut args = vec!["--webhook-listen".to_string(), hook.to_string()];
    args.extend(
        auth.iter()
            .map(|a| a.replace("{dir}", &d.to_string_lossy())),
    );
    let a: Vec<&str> = args.iter().map(String::as_str).collect();
    let mut cmd = command(&d, metrics, &a);
    cmd.stdout(Stdio::null()).stderr(Stdio::null());
    let mut child = Running(cmd.spawn().expect("start sched-service"), d);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(s) = child.0.try_wait().unwrap() {
            panic!("sched-service exited early: {s}");
        }
        if tokio::net::TcpStream::connect(hook).await.is_ok() {
            return (child, hook, metrics);
        }
        assert!(
            Instant::now() < deadline,
            "the webhook listener never came up"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Posts until the observers have registered (503 before that), up to the deadline.
async fn post_when_ready(
    client: &reqwest::Client,
    url: &str,
    token: Option<&str>,
) -> (u16, String) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let r = status(client, reqwest::Method::POST, url, token).await;
        if r.0 != 503 || Instant::now() > deadline {
            return r;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn in_mesh_mode_a_post_reaches_the_sources_of_the_path_and_is_counted() {
    let (_child, hook, metrics) = serving("mesh", &["--webhook-auth", "mesh"]).await;
    let client = reqwest::Client::new();
    let url = |p: &str| format!("http://{hook}{p}");

    let (code, body) = post_when_ready(&client, &url("/hooks/paperless"), None).await;
    assert_eq!((code, body.as_str()), (202, r#"{"nudged":2,"sources":2}"#));
    let (code, body) = post_when_ready(&client, &url("/hooks/speakr"), None).await;
    assert_eq!((code, body.as_str()), (202, r#"{"nudged":2,"sources":2}"#));
    assert_eq!(
        status(&client, reqwest::Method::POST, &url("/hooks/other"), None)
            .await
            .0,
        404
    );
    assert_eq!(
        status(
            &client,
            reqwest::Method::GET,
            &url("/hooks/paperless"),
            None
        )
        .await
        .0,
        405
    );

    let text = reqwest::get(format!("http://{metrics}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    for needle in [
        "sched_webhook_requests_total{outcome=\"accepted\"} 2",
        "sched_webhook_requests_total{outcome=\"not_found\"} 1",
        "sched_webhook_requests_total{outcome=\"method_not_allowed\"} 1",
        "sched_webhook_nudges_total 4",
    ] {
        assert!(text.contains(needle), "{needle}\n{text}");
    }
}

#[tokio::test]
async fn in_token_mode_the_token_file_decides_and_a_rotated_token_applies_at_once() {
    let (child, hook, _) = serving(
        "token",
        &[
            "--webhook-auth",
            "token",
            "--webhook-token-file",
            "{dir}/token",
        ],
    )
    .await;
    let client = reqwest::Client::new();
    let url = format!("http://{hook}/hooks/paperless");
    assert_eq!(
        status(&client, reqwest::Method::POST, &url, None).await.0,
        401
    );
    assert_eq!(
        status(&client, reqwest::Method::POST, &url, Some("wrong"))
            .await
            .0,
        401
    );
    assert_eq!(post_when_ready(&client, &url, Some("s3cret")).await.0, 202);
    std::fs::write(child.1.join("token"), "rotated\n").unwrap();
    assert_eq!(
        status(&client, reqwest::Method::POST, &url, Some("s3cret"))
            .await
            .0,
        401
    );
    assert_eq!(
        status(&client, reqwest::Method::POST, &url, Some("rotated"))
            .await
            .0,
        202
    );
}
