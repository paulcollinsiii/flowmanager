use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use restate_sdk::prelude::*;
use russh::client;
use russh::keys::{load_secret_key, HashAlg, PrivateKeyWithHashAlg, PublicKeyOrCertificate};
use russh::{ChannelMsg, Disconnect};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;

use crate::cluster::{Server, find_server};
use crate::opnsense::Opnsense;

const PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const POLL_INTERVAL: Duration = Duration::from_secs(10);
const SSH_PORT: u16 = 22;
/// Connect, authenticate and get the exec accepted within this.
const SSH_TIMEOUT: Duration = Duration::from_secs(30);
/// After the exec is accepted the host is going down, so the link may just hang.
const SSH_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a server may keep answering after the shutdown was accepted.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(120);

/// Powers one server on or off. A Virtual Object keyed by server name: exclusive
/// handlers run one at a time per server, and the durable state records the last
/// power transition so quick cycling of the power state can be refused.
pub struct PowerObject {
    kube: kube::Client,
    wake_timeout: Duration,
    cooldown: Duration,
    shutdown_key: PathBuf,
    opnsense: Opnsense,
}

impl PowerObject {
    pub fn new(
        kube: kube::Client,
        wake_timeout: Duration,
        cooldown: Duration,
        shutdown_key: PathBuf,
        opnsense: Opnsense,
    ) -> Self {
        Self { kube, wake_timeout, cooldown, shutdown_key, opnsense }
    }

    /// Refuse, loudly, if the last power transition was too recent. Returns the
    /// journaled current time so the caller can record the transition it starts.
    /// The error! line is the hook for outside alerting; match on
    /// event="power_cooldown_rejected".
    async fn enforce_cooldown(
        &self,
        ctx: &ObjectContext<'_>,
        server: &Server,
        requested: &str,
    ) -> Result<u64, HandlerError> {
        let now = ctx.run(|| async { Ok(unix_ms()) }).name("now").await?;
        let last = ctx.get::<u64>(LAST_TRANSITION_MS).await?;
        if let Some(remaining) = cooldown_remaining(last, now, self.cooldown) {
            tracing::error!(
                event = "power_cooldown_rejected",
                server = %server.name,
                requested,
                remaining_secs = remaining.as_secs(),
                cooldown_secs = self.cooldown.as_secs(),
                "refusing to power {requested} {}: last power transition was too recent",
                server.name,
            );
            return Err(TerminalError::new_with_code(
                429,
                format!("{}: power cooldown active, {}s remaining", server.name, remaining.as_secs()),
            )
            .into());
        }
        Ok(now)
    }
}

/// State key: unix millis of the last power transition this object started.
const LAST_TRANSITION_MS: &str = "last_transition_ms";

#[derive(Serialize, Deserialize)]
pub struct PowerOutcome {
    pub was_up: bool,
    pub now_up: bool,
}

#[derive(Serialize, Deserialize)]
struct PowerStatus {
    up: bool,
    last_transition_ms: Option<u64>,
}

/// The user and pinned host key a shutdown needs. Without a pinned key we would
/// be trusting whatever answers on the address, so it is required.
pub(crate) fn shutdown_target(server: &Server) -> Result<(&str, &str), String> {
    let user = server.shutdown_user.as_deref().ok_or("SHUTDOWN_USER is required to power off")?;
    let key = server.host_key.as_deref().ok_or("HOST_KEY_FPR is required to power off")?;
    Ok((user, key))
}

/// Time left before another transition is allowed, or None if it is allowed now.
fn cooldown_remaining(last_ms: Option<u64>, now_ms: u64, cooldown: Duration) -> Option<Duration> {
    let elapsed = Duration::from_millis(now_ms.saturating_sub(last_ms?));
    cooldown.checked_sub(elapsed).filter(|d| !d.is_zero())
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

// Network and cluster edges, kept thin.

/// True if host:port accepts a connection. Any failure, DNS included, counts as down.
async fn port_up(host: &str, port: u16) -> bool {
    matches!(tokio::time::timeout(PROBE_TIMEOUT, TcpStream::connect((host, port))).await, Ok(Ok(_)))
}

/// Accepts only the host key whose SHA256 fingerprint was pinned in the ConfigMap.
struct PinnedHostKey {
    expected: String,
}

impl client::Handler for PinnedHostKey {
    type Error = russh::Error;

    async fn check_server_key(&mut self, key: &PublicKeyOrCertificate) -> Result<bool, Self::Error> {
        Ok(match key {
            PublicKeyOrCertificate::PublicKey { key, .. } => {
                fingerprint_matches(&self.expected, &key.fingerprint(HashAlg::Sha256).to_string())
            }
            PublicKeyOrCertificate::Certificate(_) => false,
        })
    }
}

fn fingerprint_matches(expected: &str, actual: &str) -> bool {
    expected.trim() == actual
}

/// Ask the host to shut down. The key on the host is forced-command, so the
/// command sent here is ignored; it only has to be a valid exec request. Once the
/// exec is accepted the host is going down and the link may drop or hang, which
/// is success, not failure.
async fn ssh_shutdown(host: &str, user: &str, host_key: &str, key_path: &Path) -> Result<(), String> {
    let (session, mut channel) = tokio::time::timeout(SSH_TIMEOUT, async {
        let key = load_secret_key(key_path, None).map_err(|e| format!("shutdown key {}: {e}", key_path.display()))?;
        let handler = PinnedHostKey { expected: host_key.to_string() };
        let mut session = client::connect(Arc::new(client::Config::default()), (host, SSH_PORT), handler)
            .await
            .map_err(|e| format!("ssh connect to {host} (host key must match {host_key}): {e}"))?;
        let hash = session.best_supported_rsa_hash().await.map_err(|e| format!("ssh handshake: {e}"))?.flatten();
        let auth = session
            .authenticate_publickey(user, PrivateKeyWithHashAlg::new(Arc::new(key), hash))
            .await
            .map_err(|e| format!("ssh auth as {user}@{host}: {e}"))?;
        if !auth.success() {
            return Err(format!("ssh auth as {user}@{host} was rejected"));
        }
        let channel = session.channel_open_session().await.map_err(|e| format!("ssh open session: {e}"))?;
        channel.exec(true, "poweroff").await.map_err(|e| format!("ssh exec: {e}"))?;
        // The channel only lives as long as the session, so hand back both.
        Ok::<_, String>((session, channel))
    })
    .await
    .map_err(|_| format!("ssh to {host} timed out after {SSH_TIMEOUT:?}"))??;

    let mut status = None;
    let _ = tokio::time::timeout(SSH_DRAIN_TIMEOUT, async {
        while let Some(msg) = channel.wait().await {
            if let ChannelMsg::ExitStatus { exit_status } = msg {
                status = Some(exit_status);
            }
        }
    })
    .await;
    let _ = session.disconnect(Disconnect::ByApplication, "", "English").await;
    match status {
        None | Some(0) => Ok(()),
        Some(n) => Err(format!("shutdown command on {host} exited with status {n}")),
    }
}

#[restate_sdk::object]
impl PowerObject {
    #[handler]
    async fn power_on(&self, ctx: ObjectContext<'_>) -> Result<Json<PowerOutcome>, HandlerError> {
        let name = ctx.key().to_string();
        let kube = self.kube.clone();
        let server = ctx
            .run(|| async { Ok(Json(find_server(kube, &name).await?)) })
            .name("discover_server")
            .await?
            .into_inner();

        let was_up = ctx
            .run(|| async { Ok(port_up(&server.host, server.port).await) })
            .name("check_state")
            .await?;
        if was_up {
            return Ok(Json(PowerOutcome { was_up, now_up: true }));
        }

        // Fail fast rather than queue: a rapid off/on cycle is what this guards
        // against, and the caller should see it.
        let now = self.enforce_cooldown(&ctx, &server, "on").await?;

        // Terminal rather than retried forever, like the shutdown: the caller
        // should see that the router refused or could not be reached. Match on
        // event="wol_request_failed" for alerting.
        ctx.run(|| async {
            self.opnsense.wake(&server.wol_interface, server.mac).await.map_err(|e| {
                tracing::error!(
                    event = "wol_request_failed",
                    server = %server.name,
                    interface = %server.wol_interface,
                    error = %e,
                    "wake request for {} failed",
                    server.name,
                );
                TerminalError::new_with_code(502, format!("{}: {e}", server.name)).into()
            })
        })
        .name("request_wake")
        .await?;
        ctx.set(LAST_TRANSITION_MS, now);

        // Every sleep and probe is journaled, so a restart resumes mid-wait.
        let attempts = (self.wake_timeout.as_secs() / POLL_INTERVAL.as_secs()).max(1);
        for _ in 0..attempts {
            ctx.sleep(POLL_INTERVAL).await?;
            let up = ctx
                .run(|| async { Ok(port_up(&server.host, server.port).await) })
                .name("check_state")
                .await?;
            if up {
                return Ok(Json(PowerOutcome { was_up, now_up: true }));
            }
        }
        tracing::error!(
            event = "power_on_timeout",
            server = %server.name,
            timeout_secs = self.wake_timeout.as_secs(),
            "{} did not answer on {}:{} after wake",
            server.name,
            server.host,
            server.port,
        );
        Err(TerminalError::new_with_code(
            504,
            format!("{} did not answer on {}:{} within {:?}", server.name, server.host, server.port, self.wake_timeout),
        )
        .into())
    }

    /// Asks the host to shut down over the forced-command SSH key, then waits for
    /// its port to stop answering. Draining is the caller's job: make sure nothing
    /// still uses the server (the scheduler's allowed set no longer includes its
    /// workloads, and their pods are gone) before calling this.
    #[handler]
    async fn power_off(&self, ctx: ObjectContext<'_>) -> Result<Json<PowerOutcome>, HandlerError> {
        let name = ctx.key().to_string();
        let kube = self.kube.clone();
        let server = ctx
            .run(|| async { Ok(Json(find_server(kube, &name).await?)) })
            .name("discover_server")
            .await?
            .into_inner();
        let (user, host_key) = shutdown_target(&server)
            .map_err(|e| TerminalError::new_with_code(412, format!("server {}: {e}", server.name)))?;

        let was_up = ctx
            .run(|| async { Ok(port_up(&server.host, server.port).await) })
            .name("check_state")
            .await?;
        if !was_up {
            return Ok(Json(PowerOutcome { was_up, now_up: false }));
        }

        let now = self.enforce_cooldown(&ctx, &server, "off").await?;

        // A failed request is terminal rather than retried forever: the caller
        // has already drained, and should decide whether to try again.
        ctx.run(|| async {
            ssh_shutdown(&server.host, user, host_key, &self.shutdown_key)
                .await
                .map_err(|e| TerminalError::new_with_code(502, format!("{}: {e}", server.name)).into())
        })
        .name("ssh_shutdown")
        .await?;
        ctx.set(LAST_TRANSITION_MS, now);

        let attempts = (SHUTDOWN_TIMEOUT.as_secs() / POLL_INTERVAL.as_secs()).max(1);
        for _ in 0..attempts {
            ctx.sleep(POLL_INTERVAL).await?;
            let up = ctx
                .run(|| async { Ok(port_up(&server.host, server.port).await) })
                .name("check_state")
                .await?;
            if !up {
                return Ok(Json(PowerOutcome { was_up, now_up: false }));
            }
        }
        tracing::error!(
            event = "power_off_timeout",
            server = %server.name,
            timeout_secs = SHUTDOWN_TIMEOUT.as_secs(),
            "{} still answers on {}:{} after shutdown was accepted",
            server.name,
            server.host,
            server.port,
        );
        Err(TerminalError::new_with_code(
            504,
            format!("{} still answers on {}:{} after shutdown", server.name, server.host, server.port),
        )
        .into())
    }

    /// Read-only and shared: does not wait behind a wake in progress.
    #[handler]
    async fn status(&self, ctx: SharedObjectContext<'_>) -> Result<Json<PowerStatus>, HandlerError> {
        let name = ctx.key().to_string();
        let kube = self.kube.clone();
        let server = ctx
            .run(|| async { Ok(Json(find_server(kube, &name).await?)) })
            .name("discover_server")
            .await?
            .into_inner();
        let up = ctx
            .run(|| async { Ok(port_up(&server.host, server.port).await) })
            .name("check_state")
            .await?;
        let last_transition_ms = ctx.get::<u64>(LAST_TRANSITION_MS).await?;
        Ok(Json(PowerStatus { up, last_transition_ms }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cooldown() {
        let c = Duration::from_secs(600);
        assert_eq!(cooldown_remaining(None, 1_000_000, c), None);
        assert_eq!(cooldown_remaining(Some(1_000_000), 1_100_000, c), Some(Duration::from_secs(500)));
        assert_eq!(cooldown_remaining(Some(1_000_000), 1_600_000, c), None);
        // clock moved backwards: still cooling down, never underflows
        assert_eq!(cooldown_remaining(Some(1_000_000), 900_000, c), Some(c));
    }

    fn server(user: Option<&str>, key: Option<&str>) -> Server {
        Server {
            name: "x".into(),
            host: "x.kaffi.internal".into(),
            port: 22,
            mac: [1, 2, 3, 4, 5, 6],
            wol_interface: "lan".into(),
            shutdown_user: user.map(str::to_string),
            host_key: key.map(str::to_string),
        }
    }

    #[test]
    fn shutdown_fields_are_optional_until_powering_off() {
        assert!(shutdown_target(&server(None, None)).is_err());
        assert!(shutdown_target(&server(Some("poweroff"), None)).is_err(), "host key is required too");
        assert!(shutdown_target(&server(None, Some("SHA256:abc"))).is_err());
        assert_eq!(shutdown_target(&server(Some("poweroff"), Some("SHA256:abc"))), Ok(("poweroff", "SHA256:abc")));
    }

    #[test]
    fn host_key_must_match_exactly() {
        assert!(fingerprint_matches("SHA256:abc", "SHA256:abc"));
        assert!(fingerprint_matches(" SHA256:abc\n", "SHA256:abc"));
        assert!(!fingerprint_matches("SHA256:abc", "SHA256:abd"));
        assert!(!fingerprint_matches("", "SHA256:abc"));
    }
}
