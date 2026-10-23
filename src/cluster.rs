//! Cluster discovery shared by the power and KEDA handlers: which servers exist
//! and what they serve, read from labelled ConfigMaps.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::ResourceExt;
use kube::api::{Api, ListParams};
use restate_sdk::prelude::*;
use serde::{Deserialize, Serialize};

/// Label on the ConfigMap that defines a server; its value is the server name.
pub const L_SERVER: &str = "power.kaffi.internal/server";

const DEFAULT_PORT: u16 = 2049;
const DEFAULT_WOL_INTERFACE: &str = "lan";

/// What must stop using a server before it is shut down. Each variant has its own
/// way of finding the pods that still hold it (see keda.rs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Drain {
    Nfs,
    Gpu,
}

/// A server definition read from its ConfigMap. Shutdown and grouping fields are
/// optional because waking does not need them.
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
    /// KEDA groups this server serves.
    pub groups: Vec<String>,
    pub drain: Option<Drain>,
    /// When true, a role mode in one of its groups pauses every other role there.
    pub exclusive_roles: bool,
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
    let groups = get("GROUPS")
        .map(|g| g.split(',').map(str::trim).filter(|g| !g.is_empty()).map(str::to_string).collect())
        .unwrap_or_default();
    let drain = match get("DRAIN") {
        None => None,
        Some("nfs") => Some(Drain::Nfs),
        Some("gpu") => Some(Drain::Gpu),
        Some(other) => return Err(format!("DRAIN {other:?} is not nfs or gpu")),
    };
    let exclusive_roles = match get("EXCLUSIVE_ROLES") {
        None | Some("false") => false,
        Some("true") => true,
        Some(other) => return Err(format!("EXCLUSIVE_ROLES {other:?} is not true or false")),
    };
    Ok(Server {
        name: name.to_string(),
        host: host.to_string(),
        port,
        mac,
        wol_interface: wol_interface.to_string(),
        shutdown_user: get("SHUTDOWN_USER").map(str::to_string),
        host_key: get("HOST_KEY_FPR").map(str::to_string),
        groups,
        drain,
        exclusive_roles,
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

pub async fn list_servers(kube: kube::Client) -> Result<Vec<Server>, ClusterError> {
    let api: Api<ConfigMap> = Api::all(kube);
    let found = api.list(&ListParams::default().labels(L_SERVER)).await?;
    let mut servers: Vec<Server> = Vec::new();
    for cm in found.items {
        let name = cm.labels().get(L_SERVER).cloned().unwrap_or_default();
        if servers.iter().any(|s| s.name == name) {
            return Err(ClusterError::Invalid(format!("more than one ConfigMap labelled {L_SERVER}={name}")));
        }
        servers.push(parse_configmap(&name, cm)?);
    }
    servers.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(servers)
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
        assert!(s.groups.is_empty());
        assert_eq!(s.drain, None);
        assert!(!s.exclusive_roles);
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
            ("DRAIN", "tape"),
            ("EXCLUSIVE_ROLES", "yes"),
        ] {
            let mut d = full();
            d.insert(key.to_string(), bad.to_string());
            assert!(server_from_data("x", &d).is_err(), "{key}={bad} should fail");
        }
    }

    #[test]
    fn parses_grouping_fields() {
        let mut d = full();
        d.insert("GROUPS".into(), " gpu, idle ,".into());
        d.insert("DRAIN".into(), "gpu".into());
        d.insert("EXCLUSIVE_ROLES".into(), "true".into());
        d.insert("WOL_INTERFACE".into(), "opt1".into());
        let s = server_from_data("cuda-central", &d).unwrap();
        assert_eq!(s.wol_interface, "opt1");
        assert_eq!(s.groups, ["gpu", "idle"]);
        assert_eq!(s.drain, Some(Drain::Gpu));
        assert!(s.exclusive_roles);
    }

    #[test]
    fn mac_accepts_dashes() {
        assert_eq!(parse_mac("d8-5e-d3-86-fd-4a").unwrap(), [0xd8, 0x5e, 0xd3, 0x86, 0xfd, 0x4a]);
    }
}
