//! Typed power configuration of a resource: how its host is woken (Wake-on-LAN, sent by
//! the router, since the scheduler runs in the cluster and cannot broadcast on the LAN) and
//! how it is shut down (SSH to a forced-command key). Checked when the configuration is
//! loaded, so a managed resource that lacks what it needs is rejected there rather than
//! failing when the first power step is carried out.
//!
//! Server definitions live here, in the scheduler config. Secrets do not: the router
//! credentials, the optional CA bundle and the SSH private key are files mounted from
//! Kubernetes Secrets, named on the service's command line and read on every call.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::config::{MAX_LEASE_DURATION, PowerControl, PowerSpec};
use crate::time::Millis;

/// A MAC address. Written `aa:bb:cc:dd:ee:ff` (or with `-`), in either case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MacAddr(pub [u8; 6]);

impl FromStr for MacAddr {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        let bad = || format!("{text:?} is not a MAC address (expected aa:bb:cc:dd:ee:ff)");
        let sep = if text.contains(':') { ':' } else { '-' };
        let parts: Vec<&str> = text.split(sep).collect();
        if parts.len() != 6 {
            return Err(bad());
        }
        let mut out = [0u8; 6];
        for (byte, part) in out.iter_mut().zip(parts) {
            if part.len() != 2 || !part.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(bad());
            }
            *byte = u8::from_str_radix(part, 16).map_err(|_| bad())?;
        }
        if out == [0u8; 6] || out == [0xff; 6] {
            return Err(format!("{text:?} is not a usable host address"));
        }
        Ok(MacAddr(out))
    }
}

impl fmt::Display for MacAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let [a, b, c, d, e, g] = self.0;
        write!(f, "{a:02x}:{b:02x}:{c:02x}:{d:02x}:{e:02x}:{g:02x}")
    }
}

impl Serialize for MacAddr {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for MacAddr {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let text = String::deserialize(d)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

/// Power-on: the router sends the magic packet (OPNsense `os-wol` plugin).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WakeOnLan {
    /// Base URL of the router's API. Must be `https://`: the credentials go over basic auth.
    pub router_url: String,
    /// The router interface the packet is sent out of (`lan`, `opt1`, ...). Default `lan`.
    #[serde(default = "default_interface")]
    pub interface: String,
    pub mac: MacAddr,
}

/// Power-off: SSH with a forced-command key, which runs the host's shutdown whatever
/// command is sent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SshShutdown {
    /// Name or address to connect to.
    pub host: String,
    /// Default 22.
    #[serde(default = "default_ssh_port")]
    pub port: u16,
    /// The user the forced-command key belongs to.
    pub user: String,
    /// The host's key, as the SHA256 fingerprint `ssh-keygen -lf` prints (`SHA256:...`). A
    /// host presenting any other key is refused, so the pinned key is required.
    pub host_key: String,
}

pub const DEFAULT_INTERFACE: &str = "lan";
pub const DEFAULT_SSH_PORT: u16 = 22;

fn default_interface() -> String {
    DEFAULT_INTERFACE.to_string()
}
fn default_ssh_port() -> u16 {
    DEFAULT_SSH_PORT
}

impl PowerSpec {
    /// Everything wrong with this power section. For `control: managed` both actuators
    /// are required; for `manual` they are optional (never used) but, if present, must
    /// still be well formed.
    pub fn problems(&self) -> Vec<String> {
        let mut out = Vec::new();
        if self.control == PowerControl::Managed {
            if self.wake_on_lan.is_none() {
                out.push(
                    "control is managed but there is no wake_on_lan section to power the host on"
                        .to_string(),
                );
            }
            if self.shutdown.is_none() {
                out.push(
                    "control is managed but there is no shutdown section to power the host off"
                        .to_string(),
                );
            }
        }
        for (name, value) in [
            ("power_on_timeout", self.power_on_timeout),
            ("shutdown_timeout", self.shutdown_timeout),
            ("mounts_timeout", self.mounts_timeout),
        ] {
            match value {
                Some(Millis::ZERO) => out.push(format!("{name} must be greater than zero")),
                Some(t) if t > MAX_LEASE_DURATION => out.push(format!(
                    "{name} {t} exceeds the maximum of {MAX_LEASE_DURATION} (1 day)"
                )),
                _ => {}
            }
        }
        if let Some(w) = &self.wake_on_lan {
            out.extend(router_url_problem(&w.router_url).map(|m| format!("wake_on_lan.{m}")));
            if w.interface.trim().is_empty() {
                out.push("wake_on_lan.interface must not be empty".to_string());
            } else if !w
                .interface
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_')
            {
                out.push(format!(
                    "wake_on_lan.interface {:?} must be an OPNsense interface name (letters, digits and _)",
                    w.interface
                ));
            }
        }
        if let Some(s) = &self.shutdown {
            if s.host.trim().is_empty() || s.host.contains(char::is_whitespace) {
                out.push("shutdown.host must be a host name or address".to_string());
            }
            if s.port == 0 {
                out.push("shutdown.port must be greater than zero".to_string());
            }
            if s.user.trim().is_empty() || s.user.contains(char::is_whitespace) {
                out.push("shutdown.user must be a user name".to_string());
            }
            out.extend(host_key_problem(&s.host_key).map(|m| format!("shutdown.{m}")));
        }
        out
    }
}

fn router_url_problem(url: &str) -> Option<String> {
    let Some(rest) = url.trim().strip_prefix("https://") else {
        return Some(format!(
            "router_url {url:?} must start with https:// (the credentials are sent with it)"
        ));
    };
    // The wake path is appended to the URL, so a query string or fragment would swallow it.
    if url.contains(['?', '#']) {
        return Some(format!(
            "router_url {url:?} must not contain a query string or fragment (the API path is appended to it)"
        ));
    }
    let authority = rest.split('/').next().unwrap_or("");
    if authority.is_empty() {
        return Some(format!("router_url {url:?} has no host"));
    }
    if authority.contains('@') {
        return Some("router_url must not contain credentials (userinfo)".to_string());
    }
    None
}

fn host_key_problem(key: &str) -> Option<String> {
    let Some(digest) = key.trim().strip_prefix("SHA256:") else {
        return Some(format!(
            "host_key {key:?} must be a SHA256 fingerprint (SHA256:...) as printed by ssh-keygen -lf"
        ));
    };
    // 32 bytes in unpadded base64.
    let ok = digest.len() == 43
        && digest
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'+' || b == b'/');
    (!ok).then(|| {
        format!("host_key {key:?} is not a SHA256 fingerprint (43 base64 characters after SHA256:)")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(yaml: &str) -> PowerSpec {
        serde_yaml_ng::from_str(yaml).unwrap()
    }

    const KEY: &str = "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    fn full(control: &str) -> String {
        format!(
            "control: {control}\nwake_on_lan: {{ router_url: 'https://r.example', mac: 'AA-bb-cc-dd-ee-01' }}\nshutdown: {{ host: h, user: u, host_key: '{KEY}' }}\n"
        )
    }

    #[test]
    fn a_complete_managed_section_is_valid_and_defaults_apply() {
        let p = spec(&full("managed"));
        assert!(p.problems().is_empty(), "{:?}", p.problems());
        let w = p.wake_on_lan.unwrap();
        assert_eq!(w.interface, "lan");
        assert_eq!(w.mac.to_string(), "aa:bb:cc:dd:ee:01");
        assert_eq!(p.shutdown.unwrap().port, 22);
    }

    #[test]
    fn managed_needs_both_actuators() {
        let p = spec("control: managed\n");
        let m = p.problems();
        assert_eq!(m.len(), 2, "{m:?}");
        assert!(m[0].contains("no wake_on_lan"), "{m:?}");
        assert!(m[1].contains("no shutdown"), "{m:?}");
        assert!(spec("control: manual\n").problems().is_empty());
        assert!(spec("{}").problems().is_empty());
    }

    #[test]
    fn rejects_bad_values() {
        let cases = [
            (
                "router_url: 'https://r.example'",
                "router_url: 'http://r.example'",
                "must start with https://",
            ),
            (
                "router_url: 'https://r.example'",
                "router_url: 'https://u:p@r.example'",
                "credentials",
            ),
            (
                "router_url: 'https://r.example'",
                "router_url: 'https://'",
                "has no host",
            ),
            (
                "mac: 'AA-bb-cc-dd-ee-01'",
                "mac: 'AA-bb-cc-dd-ee'",
                "not a MAC",
            ),
            ("host: h,", "host: '',", "shutdown.host"),
            ("user: u,", "user: 'a b',", "shutdown.user"),
            (KEY, "SHA256:short", "not a SHA256 fingerprint"),
            (KEY, "MD5:aa", "must be a SHA256 fingerprint"),
        ];
        for (from, to, want) in cases {
            let yaml = full("managed").replace(from, to);
            assert_ne!(yaml, full("managed"), "{from}");
            match serde_yaml_ng::from_str::<PowerSpec>(&yaml) {
                Ok(p) => {
                    let m = p.problems().join("; ");
                    assert!(m.contains(want), "{want}: {m}");
                }
                Err(e) => assert!(e.to_string().contains(want), "{want}: {e}"),
            }
        }
    }

    #[test]
    fn router_url_with_a_query_or_fragment_is_rejected() {
        for bad in [
            "https://r.example?x=1",
            "https://r.example/?x=1",
            "https://r.example/api#frag",
            "https://r.example#frag",
        ] {
            let yaml = full("managed").replace("https://r.example", bad);
            let p = spec(&yaml);
            let m = p.problems().join("; ");
            assert!(m.contains("query string or fragment"), "{bad}: {m}");
        }
        let yaml = full("managed").replace("https://r.example", "https://r.example:8443/base/");
        assert!(spec(&yaml).problems().is_empty());
    }

    #[test]
    fn timeouts_default_and_are_validated() {
        let p = spec(&full("managed"));
        assert_eq!(p.power_on_timeout(), Millis(600_000));
        assert_eq!(p.shutdown_timeout(), Millis(900_000));
        let p = spec(&format!(
            "{}power_on_timeout: 3m\nshutdown_timeout: 1h\n",
            full("managed")
        ));
        assert!(p.problems().is_empty(), "{:?}", p.problems());
        assert_eq!(p.power_on_timeout(), Millis(180_000));
        assert_eq!(p.shutdown_timeout(), Millis(3_600_000));
        let p = spec(&format!(
            "{}power_on_timeout: 0\nshutdown_timeout: 2d\n",
            full("managed")
        ));
        let m = p.problems().join("; ");
        assert!(
            m.contains("power_on_timeout must be greater than zero"),
            "{m}"
        );
        assert!(
            m.contains("shutdown_timeout 172800000ms exceeds the maximum"),
            "{m}"
        );
    }

    #[test]
    fn rejects_unknown_fields_and_the_old_placeholder_shape() {
        assert!(serde_yaml_ng::from_str::<PowerSpec>("actuator: wol\n").is_err());
        assert!(serde_yaml_ng::from_str::<PowerSpec>("control: managed\nwake_on_lan: { router_url: 'https://r', mac: 'aa:bb:cc:dd:ee:01', password: x }\n").is_err());
    }

    #[test]
    fn mac_parsing() {
        assert_eq!(
            "70:85:C2:B7:2F:83".parse::<MacAddr>().unwrap().0,
            [0x70, 0x85, 0xc2, 0xb7, 0x2f, 0x83]
        );
        for bad in [
            "",
            "70:85:c2:b7:2f",
            "70:85:c2:b7:2f:8g",
            "7085c2b72f83",
            "0:1:2:3:4:5",
            "00:00:00:00:00:00",
            "ff:ff:ff:ff:ff:ff",
        ] {
            assert!(bad.parse::<MacAddr>().is_err(), "{bad:?}");
        }
    }
}
