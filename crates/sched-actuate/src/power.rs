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

/// Where the secrets are mounted.
#[derive(Debug, Clone)]
pub struct SecretFiles {
    /// The router API credentials, `key:secret` on one line.
    pub opnsense_credentials: PathBuf,
    /// A PEM bundle that is the only CA trusted for the router; the platform roots if absent.
    pub opnsense_ca: Option<PathBuf>,
    /// The private key of the forced-command shutdown user.
    pub ssh_key: PathBuf,
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
        Box::pin(crate::wol::wake(
            wol,
            &self.files.opnsense_credentials,
            self.files.opnsense_ca.as_deref(),
        ))
    }

    fn shutdown<'a>(&'a self, target: &'a SshShutdown) -> BoxFut<'a, Result<(), String>> {
        Box::pin(crate::ssh::shutdown(target, &self.files.ssh_key))
    }
}
