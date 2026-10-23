//! Wake-on-LAN through the OPNsense `os-wol` plugin. A pod's broadcast does not
//! leave the pod network, so the router sends the magic packet on the LAN instead.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{Value, json};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const WAKE_PATH: &str = "/api/wol/wol/set";

/// Calls the router's API. The credentials file is read on every call, so a
/// rotated Secret is picked up without a restart.
pub struct Opnsense {
    http: reqwest::Client,
    wake_url: String,
    credentials: PathBuf,
}

impl Opnsense {
    /// With `ca`, only that bundle is trusted (the cluster CA signs the router's
    /// certificate); otherwise the platform roots are. Verification is never off.
    pub fn new(url: &str, credentials: PathBuf, ca: Option<&Path>) -> Result<Self, String> {
        let wake_url = wake_url(url)?;
        let mut http = reqwest::Client::builder().timeout(REQUEST_TIMEOUT);
        if let Some(ca) = ca {
            let pem = std::fs::read(ca).map_err(|e| format!("opnsense CA {}: {e}", ca.display()))?;
            let certs = reqwest::Certificate::from_pem_bundle(&pem)
                .map_err(|e| format!("opnsense CA {}: {e}", ca.display()))?;
            if certs.is_empty() {
                return Err(format!("opnsense CA {} holds no certificates", ca.display()));
            }
            http = http.tls_certs_only(certs);
        }
        let http = http.build().map_err(|e| format!("opnsense client: {e}"))?;
        Ok(Self { http, wake_url, credentials })
    }

    /// Ask the router to send a magic packet for `mac` out of `interface`.
    pub async fn wake(&self, interface: &str, mac: [u8; 6]) -> Result<(), String> {
        let text = std::fs::read_to_string(&self.credentials)
            .map_err(|e| format!("opnsense credentials {}: {e}", self.credentials.display()))?;
        let (key, secret) = parse_credentials(&text)?;
        let response = self
            .http
            .post(&self.wake_url)
            .basic_auth(key, Some(secret))
            .json(&wake_body(interface, mac))
            .send()
            .await
            .map_err(|e| format!("opnsense request to {}: {e}", self.wake_url))?;
        let status = response.status().as_u16();
        let body = response.text().await.map_err(|e| format!("opnsense response: {e}"))?;
        check_response(status, &body)
    }
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

fn format_mac(mac: [u8; 6]) -> String {
    mac.map(|b| format!("{b:02x}")).join(":")
}

fn wake_body(interface: &str, mac: [u8; 6]) -> Value {
    json!({ "wake": { "interface": interface, "mac": format_mac(mac) } })
}

/// Any non-2xx is a failure. The body is also checked when it carries a `status`
/// that is not "ok", since a rejected wake may still come back as HTTP 200.
fn check_response(status: u16, body: &str) -> Result<(), String> {
    let shown: String = body.chars().take(200).collect();
    if !(200..300).contains(&status) {
        return Err(format!("opnsense answered HTTP {status}: {shown}"));
    }
    if let Ok(Value::Object(fields)) = serde_json::from_str::<Value>(body) {
        if let Some(Value::String(s)) = fields.get("status") {
            if s != "ok" {
                return Err(format!("opnsense reported status {s:?}: {shown}"));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_must_be_https() {
        assert_eq!(wake_url("https://router.kaffi.internal").unwrap(), "https://router.kaffi.internal/api/wol/wol/set");
        assert_eq!(wake_url(" https://router.kaffi.internal/ ").unwrap(), "https://router.kaffi.internal/api/wol/wol/set");
        assert!(wake_url("http://router.kaffi.internal").is_err());
        assert!(wake_url("router.kaffi.internal").is_err());
    }

    #[test]
    fn credentials_are_key_colon_secret() {
        assert_eq!(parse_credentials("abc:def\n"), Ok(("abc", "def")));
        // the secret may itself contain a colon; only the first splits
        assert_eq!(parse_credentials("abc:d:ef"), Ok(("abc", "d:ef")));
        for bad in ["", "abc", ":def", "abc:", "\n"] {
            assert!(parse_credentials(bad).is_err(), "{bad:?} should fail");
        }
    }

    #[test]
    fn client_builds_and_rejects_a_bad_ca() {
        let creds = PathBuf::from("/nonexistent");
        assert!(Opnsense::new("https://router.kaffi.internal", creds.clone(), None).is_ok());
        assert!(Opnsense::new("https://router.kaffi.internal", creds.clone(), Some(Path::new("/nonexistent"))).is_err());
        let empty = std::env::temp_dir().join(format!("flowmanager-empty-ca-{}.pem", std::process::id()));
        std::fs::write(&empty, "not a certificate").unwrap();
        let result = Opnsense::new("https://router.kaffi.internal", creds, Some(&empty));
        std::fs::remove_file(&empty).unwrap();
        assert!(result.is_err(), "a CA file with no certificates must not fall back to the platform roots");
    }

    #[test]
    fn wake_body_shape() {
        assert_eq!(
            wake_body("lan", [0x70, 0x85, 0xc2, 0xb7, 0x2f, 0x83]),
            json!({ "wake": { "interface": "lan", "mac": "70:85:c2:b7:2f:83" } })
        );
        assert_eq!(format_mac([0, 1, 2, 0xaa, 0xbb, 0xff]), "00:01:02:aa:bb:ff");
    }

    #[test]
    fn response_checks() {
        assert!(check_response(200, r#"{"status":"ok"}"#).is_ok());
        assert!(check_response(200, "").is_ok(), "an empty or unparsed body is not a failure");
        assert!(check_response(200, "<html>").is_ok());
        assert!(check_response(200, r#"{"status":"failed"}"#).is_err());
        assert!(check_response(401, r#"{"status":401}"#).is_err());
        assert!(check_response(500, "").is_err());
    }
}
