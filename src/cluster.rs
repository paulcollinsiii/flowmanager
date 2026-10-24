//! Cluster discovery for the power handlers: which servers exist, read from labelled
//! ConfigMaps. (The KEDA group handlers that also used it, and the GROUPS, DRAIN and
//! EXCLUSIVE_ROLES keys they read, were retired in favour of the scheduler's KEDA
//! external scaler; see kubernetes-requirements.md.)

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::{Api, ListParams};
use restate_sdk::prelude::*;
use serde::{Deserialize, Serialize};

/// Label on the ConfigMap that defines a server; its value is the server name.
pub const L_SERVER: &str = "power.kaffi.internal/server";

const DEFAULT_PORT: u16 = 2049;
const DEFAULT_WOL_INTERFACE: &str = "lan";

/// A server definition read from its ConfigMap. Shutdown fields are optional because
/// waking does not need them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Server {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub mac: [u8; 6],
    /// OPNsense interface the router sends the magic packet out of.
    pub wol_interface: String,
    pub shutdown_user: Option<String>,
    /// Expected SSH host key fingerprint, as `ssh-keygen -lf` prints it: SHA256:...
    pub host_key: Option<String>,
}

pub enum ClusterError {
    NotFound(String),
    Invalid(String),
    Api(kube::Error),
}

impl From<kube::Error> for ClusterError {
    fn from(e: kube::Error) -> Self {
        Self::Api(e)
    }
}

/// A missing or malformed definition will not fix itself, so it is terminal; an
/// API failure is worth retrying.
impl From<ClusterError> for HandlerError {
    fn from(e: ClusterError) -> Self {
        match e {
            ClusterError::NotFound(m) => TerminalError::new_with_code(404, m).into(),
            ClusterError::Invalid(m) => TerminalError::new(m).into(),
            ClusterError::Api(e) => e.into(),
        }
    }
}

pub fn parse_mac(s: &str) -> Result<[u8; 6], String> {
    let parts: Vec<&str> = s.split([':', '-']).collect();
    if parts.len() != 6 {
        return Err(format!("MAC {s:?} is not 6 hex octets"));
    }
    let mut mac = [0u8; 6];
    for (byte, part) in mac.iter_mut().zip(parts) {
        *byte = u8::from_str_radix(part, 16).map_err(|_| format!("MAC {s:?} has bad octet {part:?}"))?;
    }
    Ok(mac)
}

pub fn server_from_data(name: &str, data: &BTreeMap<String, String>) -> Result<Server, String> {
    let get = |key: &str| data.get(key).map(|v| v.trim()).filter(|v| !v.is_empty());
    let host = get("HOST").ok_or("HOST is required")?;
    let port = match get("PORT") {
        Some(p) => p.parse().map_err(|_| format!("PORT {p:?} is not a port number"))?,
        None => DEFAULT_PORT,
    };
    let mac = parse_mac(get("MAC").ok_or("MAC is required")?)?;
    let wol_interface = get("WOL_INTERFACE").unwrap_or(DEFAULT_WOL_INTERFACE);
    if !wol_interface.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(format!("WOL_INTERFACE {wol_interface:?} is not an OPNsense interface name"));
    }
    Ok(Server {
        name: name.to_string(),
        host: host.to_string(),
        port,
        mac,
        wol_interface: wol_interface.to_string(),
        shutdown_user: get("SHUTDOWN_USER").map(str::to_string),
        host_key: get("HOST_KEY_FPR").map(str::to_string),
    })
}

fn parse_configmap(name: &str, cm: ConfigMap) -> Result<Server, ClusterError> {
    server_from_data(name, &cm.data.unwrap_or_default())
        .map_err(|e| ClusterError::Invalid(format!("server {name}: {e}")))
}

// Cluster edges, kept thin.

pub async fn find_server(kube: kube::Client, name: &str) -> Result<Server, ClusterError> {
    let api: Api<ConfigMap> = Api::all(kube);
    let found = api.list(&ListParams::default().labels(&format!("{L_SERVER}={name}"))).await?;
    let mut items = found.items.into_iter();
    let cm = items
        .next()
        .ok_or_else(|| ClusterError::NotFound(format!("no ConfigMap labelled {L_SERVER}={name}")))?;
    if items.next().is_some() {
        return Err(ClusterError::Invalid(format!("more than one ConfigMap labelled {L_SERVER}={name}")));
    }
    parse_configmap(name, cm)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn full() -> BTreeMap<String, String> {
        data(&[
            ("HOST", "basestar.kaffi.internal"),
            ("PORT", "2049"),
            ("MAC", "70:85:c2:b7:2f:83"),
        ])
    }

    #[test]
    fn parses_server() {
        let s = server_from_data("basestar", &full()).unwrap();
        assert_eq!(s.host, "basestar.kaffi.internal");
        assert_eq!(s.port, 2049);
        assert_eq!(s.mac, [0x70, 0x85, 0xc2, 0xb7, 0x2f, 0x83]);
        assert_eq!(s.wol_interface, "lan");
    }

    #[test]
    fn port_defaults_to_nfs() {
        let mut d = full();
        d.remove("PORT");
        assert_eq!(server_from_data("basestar", &d).unwrap().port, 2049);
    }

    #[test]
    fn rejects_missing_or_bad_fields() {
        for key in ["HOST", "MAC"] {
            let mut d = full();
            d.remove(key);
            assert!(server_from_data("x", &d).is_err(), "{key} should be required");
        }
        for (key, bad) in [
            ("WOL_INTERFACE", "opt 1"),
            ("MAC", "70:85:c2"),
            ("MAC", "zz:85:c2:b7:2f:83"),
            ("PORT", "ssh"),
        ] {
            let mut d = full();
            d.insert(key.to_string(), bad.to_string());
            assert!(server_from_data("x", &d).is_err(), "{key}={bad} should fail");
        }
    }

    #[test]
    fn parses_wol_interface_and_ignores_the_retired_keda_keys() {
        let mut d = full();
        d.insert("WOL_INTERFACE".into(), "opt1".into());
        // Read only by the retired KEDA group handlers; a ConfigMap that still has them works.
        d.insert("GROUPS".into(), " gpu, idle ,".into());
        d.insert("DRAIN".into(), "tape".into());
        d.insert("EXCLUSIVE_ROLES".into(), "yes".into());
        let s = server_from_data("cuda-central", &d).unwrap();
        assert_eq!(s.wol_interface, "opt1");
    }

    #[test]
    fn mac_accepts_dashes() {
        assert_eq!(parse_mac("d8-5e-d3-86-fd-4a").unwrap(), [0xd8, 0x5e, 0xd3, 0x86, 0xfd, 0x4a]);
    }
}
