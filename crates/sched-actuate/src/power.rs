//! The hardware calls behind `PowerOn` and `PowerOff`, behind a trait seam so the service
//! can be tested without a router or a host. [`HostPower`] is the real one: Wake-on-LAN
//! through the router ([`crate::wol`]) and the forced-command SSH shutdown ([`crate::ssh`]).
//!
//! Secrets are files named at startup (mounted from Kubernetes Secrets) and read on every
//! call. An actuator never retries: a failure is returned once, as a message.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;

use sched_model::{SshShutdown, WakeOnLan};

/// A boxed, `Send` future.
pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Sends the two power commands. Errors are messages free of secrets.
pub trait PowerBackend: Send + Sync {
    /// Asks the router to wake the host.
    fn wake<'a>(&'a self, wol: &'a WakeOnLan) -> BoxFut<'a, Result<(), String>>;
    /// Asks the host to shut itself down.
    fn shutdown<'a>(&'a self, target: &'a SshShutdown) -> BoxFut<'a, Result<(), String>>;
}

/// Where the secrets are mounted; each is optional because a deployment with every host
/// under manual power control needs none.
#[derive(Debug, Clone, Default)]
pub struct SecretFiles {
    /// The router API credentials, `key:secret` on one line.
    pub opnsense_credentials: Option<PathBuf>,
    /// A PEM bundle that is the only CA trusted for the router; the platform roots if absent.
    pub opnsense_ca: Option<PathBuf>,
    /// The private key of the forced-command shutdown user.
    pub ssh_key: Option<PathBuf>,
}

impl SecretFiles {
    /// The command-line options still missing for the calls that are needed.
    pub fn missing(&self, needs_wake: bool, needs_shutdown: bool) -> Vec<&'static str> {
        let mut out = Vec::new();
        if needs_wake && self.opnsense_credentials.is_none() {
            out.push("--opnsense-credentials-file");
        }
        if needs_shutdown && self.ssh_key.is_none() {
            out.push("--ssh-key-file");
        }
        out
    }
}

/// The real backend.
#[derive(Debug, Clone)]
pub struct HostPower {
    files: SecretFiles,
}

impl HostPower {
    pub fn new(files: SecretFiles) -> Self {
        HostPower { files }
    }
}

impl PowerBackend for HostPower {
    fn wake<'a>(&'a self, wol: &'a WakeOnLan) -> BoxFut<'a, Result<(), String>> {
        Box::pin(async move {
            let Some(credentials) = &self.files.opnsense_credentials else {
                return Err(
                    "no router credentials file is configured (--opnsense-credentials-file)"
                        .to_string(),
                );
            };
            crate::wol::wake(wol, credentials, self.files.opnsense_ca.as_deref()).await
        })
    }

    fn shutdown<'a>(&'a self, target: &'a SshShutdown) -> BoxFut<'a, Result<(), String>> {
        Box::pin(async move {
            let Some(key) = &self.files.ssh_key else {
                return Err("no SSH key file is configured (--ssh-key-file)".to_string());
            };
            crate::ssh::shutdown(target, key).await
        })
    }
}

#[cfg(test)]
mod tests {
    use sched_model::MacAddr;

    use super::*;

    #[test]
    fn missing_lists_only_what_is_needed() {
        let none = SecretFiles::default();
        assert!(none.missing(false, false).is_empty());
        assert_eq!(none.missing(true, false), ["--opnsense-credentials-file"]);
        assert_eq!(none.missing(false, true), ["--ssh-key-file"]);
        assert_eq!(none.missing(true, true).len(), 2);
        let all = SecretFiles {
            opnsense_credentials: Some("c".into()),
            opnsense_ca: None,
            ssh_key: Some("k".into()),
        };
        assert!(all.missing(true, true).is_empty());
    }

    #[tokio::test]
    async fn an_unconfigured_backend_fails_with_a_clear_message() {
        let backend = HostPower::new(SecretFiles::default());
        let wol = WakeOnLan {
            router_url: "https://r.example".into(),
            interface: "lan".into(),
            mac: MacAddr([2, 0, 0, 0, 0, 1]),
        };
        let e = backend.wake(&wol).await.unwrap_err();
        assert!(e.contains("--opnsense-credentials-file"), "{e}");
        let target = SshShutdown {
            host: "h".into(),
            port: 22,
            user: "u".into(),
            host_key: "SHA256:x".into(),
        };
        let e = backend.shutdown(&target).await.unwrap_err();
        assert!(e.contains("--ssh-key-file"), "{e}");
    }
}
