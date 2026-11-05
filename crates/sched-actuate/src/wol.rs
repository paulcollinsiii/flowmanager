//! Wake-on-LAN through the OPNsense `os-wol` plugin. A pod's broadcast does not leave the
//! pod network, so the router sends the magic packet on the LAN instead: one authenticated
//! `POST /api/wol/wol/set`.
//!
//! The credentials file (`key:secret` on one line) and the optional CA bundle are read on
//! every call, so a rotated Kubernetes Secret is picked up without a restart. The router
//! URL must be https and verification is never turned off: with a CA bundle only that
//! bundle is trusted, otherwise the platform roots. No redirect is followed, so the
//! credentials cannot be forwarded elsewhere. Every failure is a message for the caller;
//! it never contains the credentials.

use std::path::Path;
use std::time::Duration;

use sched_model::{MacAddr, WakeOnLan};
use serde_json::{Value, json};

/// Bound on the whole request.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const WAKE_PATH: &str = "/api/wol/wol/set";

/// Asks the router named by `wol` to send a magic packet for its MAC out of its interface.
pub async fn wake(wol: &WakeOnLan, credentials: &Path, ca: Option<&Path>) -> Result<(), String> {
    let url = wake_url(&wol.router_url)?;
    let client = client(ca)?;
    send(&client, &url, credentials, &wol.interface, wol.mac).await
}

/// The HTTP client: only the given CA bundle is trusted, if there is one.
fn client(ca: Option<&Path>) -> Result<reqwest::Client, String> {
    let mut builder = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none());
    if let Some(ca) = ca {
        let pem = std::fs::read(ca).map_err(|e| format!("opnsense CA {}: {e}", ca.display()))?;
        let certs = reqwest::Certificate::from_pem_bundle(&pem)
            .map_err(|e| format!("opnsense CA {}: {e}", ca.display()))?;
        if certs.is_empty() {
            return Err(format!(
                "opnsense CA {} holds no certificates",
                ca.display()
            ));
        }
        builder = builder.tls_certs_only(certs);
    }
    builder.build().map_err(|e| format!("opnsense client: {e}"))
}

/// The request itself. `url` is the full endpoint; [`wake`] only ever passes an https one
/// (tests point it at a local server).
async fn send(
    http: &reqwest::Client,
    url: &str,
    credentials: &Path,
    interface: &str,
    mac: MacAddr,
) -> Result<(), String> {
    let text = std::fs::read_to_string(credentials)
        .map_err(|e| format!("opnsense credentials {}: {e}", credentials.display()))?;
    let (key, secret) = parse_credentials(&text)?;
    let response = http
        .post(url)
        .basic_auth(key, Some(secret))
        .json(&wake_body(interface, mac))
        .send()
        .await
        .map_err(|e| format!("opnsense request to {url}: {}", e.without_url()))?;
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .map_err(|e| format!("opnsense response: {}", e.without_url()))?;
    check_response(status, &body)
}

/// Credentials are sent over basic auth, so the URL must be https.
fn wake_url(base: &str) -> Result<String, String> {
    let base = base.trim().trim_end_matches('/');
    if !base.starts_with("https://") {
        return Err(format!("opnsense URL {base:?} must start with https://"));
    }
    Ok(format!("{base}{WAKE_PATH}"))
}

/// The file holds `key:secret` on one line, which is what basic auth sends.
fn parse_credentials(text: &str) -> Result<(&str, &str), String> {
    match text.trim().split_once(':') {
        Some((key, secret)) if !key.is_empty() && !secret.is_empty() => Ok((key, secret)),
        _ => Err("opnsense credentials file must hold key:secret".to_string()),
    }
}

fn wake_body(interface: &str, mac: MacAddr) -> Value {
    json!({ "wake": { "interface": interface, "mac": mac.to_string() } })
}

/// Any non-2xx is a failure. The body is also checked when it carries a `status` that is
/// not "ok" in any case (the router answers `{"status":"OK"}`), since a rejected wake may
/// still come back as HTTP 200.
fn check_response(status: u16, body: &str) -> Result<(), String> {
    let shown: String = body.chars().take(200).collect();
    if !(200..300).contains(&status) {
        return Err(format!("opnsense answered HTTP {status}: {shown}"));
    }
    if let Ok(Value::Object(fields)) = serde_json::from_str::<Value>(body)
        && let Some(Value::String(s)) = fields.get("status")
        && !s.eq_ignore_ascii_case("ok")
    {
        return Err(format!("opnsense reported status {s:?}: {shown}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use sched_observe::testserver::{Reply, TestServer};

    use super::*;

    const MAC: MacAddr = MacAddr([0x70, 0x85, 0xc2, 0xb7, 0x2f, 0x83]);

    fn scratch(name: &str, contents: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("sched-actuate-wol-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, contents).unwrap();
        path
    }

    /// The `Authorization` header reqwest builds for basic auth, for comparison.
    fn basic_header(key: &str, secret: &str) -> String {
        let request = reqwest::Client::new()
            .post("http://localhost/")
            .basic_auth(key, Some(secret))
            .build()
            .unwrap();
        request.headers()["authorization"]
            .to_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn url_must_be_https() {
        assert_eq!(
            wake_url("https://router.example").unwrap(),
            "https://router.example/api/wol/wol/set"
        );
        assert_eq!(
            wake_url(" https://router.example/ ").unwrap(),
            "https://router.example/api/wol/wol/set"
        );
        assert!(wake_url("http://router.example").is_err());
        assert!(wake_url("router.example").is_err());
    }

    #[test]
    fn credentials_are_key_colon_secret() {
        assert_eq!(parse_credentials("abc:def\n"), Ok(("abc", "def")));
        assert_eq!(parse_credentials("abc:d:ef"), Ok(("abc", "d:ef")));
        for bad in ["", "abc", ":def", "abc:", "\n"] {
            assert!(parse_credentials(bad).is_err(), "{bad:?} should fail");
        }
    }

    #[test]
    fn body_shape() {
        assert_eq!(
            wake_body("lan", MAC),
            json!({ "wake": { "interface": "lan", "mac": "70:85:c2:b7:2f:83" } })
        );
    }

    #[test]
    fn response_checks() {
        assert!(check_response(200, r#"{"status":"ok"}"#).is_ok());
        assert!(check_response(200, r#"{"status":"OK"}"#).is_ok());
        assert!(check_response(200, "").is_ok());
        assert!(check_response(200, "<html>").is_ok());
        assert!(check_response(200, r#"{"status":"failed"}"#).is_err());
        assert!(check_response(200, r#"{"status":"FAILED"}"#).is_err());
        assert!(check_response(401, r#"{"status":401}"#).is_err());
        assert!(check_response(500, r#"{"status":"OK"}"#).is_err());
        assert!(check_response(500, "").is_err());
    }

    #[test]
    fn a_bad_ca_is_refused_and_never_falls_back_to_the_platform_roots() {
        assert!(client(None).is_ok());
        assert!(client(Some(Path::new("/nonexistent/ca.pem"))).is_err());
        let junk = scratch("junk-ca.pem", "not a certificate");
        let e = client(Some(&junk)).unwrap_err();
        assert!(
            e.contains("holds no certificates") || e.contains("CA"),
            "{e}"
        );
    }

    #[tokio::test]
    async fn wake_posts_with_basic_auth_read_from_the_file_on_every_call() {
        let server = TestServer::start().await;
        server.set("/api/wol/wol/set", Reply::json(r#"{"status":"OK"}"#));
        let url = server.url("/api/wol/wol/set");
        let creds = scratch("creds-ok", "the-key:the-secret\n");
        let http = client(None).unwrap();
        send(&http, &url, &creds, "lan", MAC).await.unwrap();
        // Rotated: the next call sends the new credentials.
        std::fs::write(&creds, "new-key:new-secret").unwrap();
        send(&http, &url, &creds, "lan", MAC).await.unwrap();
        let auth = server.requests_with_header("authorization");
        assert_eq!(auth.len(), 2);
        assert_eq!(
            auth[0].1.as_deref(),
            Some(basic_header("the-key", "the-secret").as_str())
        );
        assert_eq!(
            auth[1].1.as_deref(),
            Some(basic_header("new-key", "new-secret").as_str())
        );
        assert_eq!(server.methods(), ["POST", "POST"]);
    }

    #[tokio::test]
    async fn router_refusals_and_missing_credentials_are_errors() {
        let server = TestServer::start().await;
        let url = server.url("/api/wol/wol/set");
        let http = client(None).unwrap();
        let creds = scratch("creds-err", "k:s");
        server.set("/api/wol/wol/set", Reply::status(401));
        let e = send(&http, &url, &creds, "lan", MAC).await.unwrap_err();
        assert!(e.contains("HTTP 401"), "{e}");
        server.set("/api/wol/wol/set", Reply::json(r#"{"status":"failed"}"#));
        let e = send(&http, &url, &creds, "lan", MAC).await.unwrap_err();
        assert!(e.contains("status \"failed\""), "{e}");
        let e = send(&http, &url, Path::new("/nonexistent/creds"), "lan", MAC)
            .await
            .unwrap_err();
        assert!(e.contains("opnsense credentials"), "{e}");
        assert!(!e.contains("secret"));
    }

    #[tokio::test]
    async fn a_redirect_is_not_followed_so_credentials_stay_put() {
        let server = TestServer::start().await;
        server.set("/api/wol/wol/set", Reply::redirect("/elsewhere"));
        server.set("/elsewhere", Reply::json("{}"));
        let creds = scratch("creds-redirect", "k:s");
        let http = client(None).unwrap();
        let e = send(&http, &server.url("/api/wol/wol/set"), &creds, "lan", MAC)
            .await
            .unwrap_err();
        assert!(e.contains("HTTP 302"), "{e}");
        assert_eq!(server.count("/elsewhere"), 0);
    }

    #[tokio::test]
    async fn a_plain_http_router_is_refused_before_any_request() {
        let server = TestServer::start().await;
        let wol = WakeOnLan {
            router_url: server.url("").trim_end_matches('/').to_string(),
            interface: "lan".into(),
            mac: MAC,
        };
        let creds = scratch("creds-http", "k:s");
        let e = wake(&wol, &creds, None).await.unwrap_err();
        assert!(e.contains("must start with https://"), "{e}");
        assert!(server.requests().is_empty());
    }
}
