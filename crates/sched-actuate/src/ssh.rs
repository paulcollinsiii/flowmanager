//! Power-off over SSH with a forced-command key. The key on the host is restricted to its
//! shutdown command, so the command sent here is ignored; it only has to be a valid exec
//! request. The host's key is pinned by its SHA256 fingerprint (a host presenting any
//! other key is refused before anything is sent), and the private key is a file read on
//! every call, so a rotated Kubernetes Secret is picked up without a restart.
//!
//! Once the exec is accepted the host is going down and the link may drop or hang; that is
//! success, not failure, and the wait after it is bounded.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use russh::client;
use russh::keys::{HashAlg, PrivateKeyWithHashAlg, PublicKeyOrCertificate, load_secret_key};
use russh::{ChannelMsg, Disconnect};
use sched_model::SshShutdown;

/// Connect, authenticate and get the exec accepted within this.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// After the exec is accepted the host is going down, so the link may just hang.
pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// The bounds of one shutdown request.
#[derive(Debug, Clone, Copy)]
pub struct Timeouts {
    pub connect: Duration,
    pub drain: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Timeouts {
            connect: CONNECT_TIMEOUT,
            drain: DRAIN_TIMEOUT,
        }
    }
}

/// Accepts only the host key whose SHA256 fingerprint was pinned in the config.
struct PinnedHostKey {
    expected: String,
}

impl client::Handler for PinnedHostKey {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        key: &PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        Ok(match key {
            PublicKeyOrCertificate::PublicKey { key, .. } => fingerprint_matches(
                &self.expected,
                &key.fingerprint(HashAlg::Sha256).to_string(),
            ),
            PublicKeyOrCertificate::Certificate(_) => false,
        })
    }
}

fn fingerprint_matches(expected: &str, actual: &str) -> bool {
    expected.trim() == actual
}

/// What the command's exit says, once the link is done: no status at all (the link dropped
/// as the host went down) and 0 are success.
fn exit_outcome(host: &str, status: Option<u32>) -> Result<(), String> {
    match status {
        None | Some(0) => Ok(()),
        Some(n) => Err(format!("shutdown command on {host} exited with status {n}")),
    }
}

/// Asks the host to shut down.
pub async fn shutdown(target: &SshShutdown, key_path: &Path) -> Result<(), String> {
    shutdown_with(target, key_path, Timeouts::default()).await
}

/// As [`shutdown`] with explicit bounds.
pub async fn shutdown_with(
    target: &SshShutdown,
    key_path: &Path,
    timeouts: Timeouts,
) -> Result<(), String> {
    let host = target.host.as_str();
    let user = target.user.as_str();
    let (session, mut channel) = tokio::time::timeout(timeouts.connect, async {
        let key = load_secret_key(key_path, None)
            .map_err(|e| format!("shutdown key {}: {e}", key_path.display()))?;
        let handler = PinnedHostKey {
            expected: target.host_key.clone(),
        };
        let mut session = client::connect(
            Arc::new(client::Config::default()),
            (host, target.port),
            handler,
        )
        .await
        .map_err(|e| {
            format!(
                "ssh connect to {host}:{} (host key must match {}): {e}",
                target.port, target.host_key
            )
        })?;
        let hash = session
            .best_supported_rsa_hash()
            .await
            .map_err(|e| format!("ssh handshake with {host}: {e}"))?
            .flatten();
        let auth = session
            .authenticate_publickey(user, PrivateKeyWithHashAlg::new(Arc::new(key), hash))
            .await
            .map_err(|e| format!("ssh auth as {user}@{host}: {e}"))?;
        if !auth.success() {
            return Err(format!("ssh auth as {user}@{host} was rejected"));
        }
        let channel = session
            .channel_open_session()
            .await
            .map_err(|e| format!("ssh open session on {host}: {e}"))?;
        channel
            .exec(true, "poweroff")
            .await
            .map_err(|e| format!("ssh exec on {host}: {e}"))?;
        // The channel only lives as long as the session, so hand back both.
        Ok::<_, String>((session, channel))
    })
    .await
    .map_err(|_| format!("ssh to {host} timed out after {:?}", timeouts.connect))??;

    let mut status = None;
    let _ = tokio::time::timeout(timeouts.drain, async {
        while let Some(msg) = channel.wait().await {
            if let ChannelMsg::ExitStatus { exit_status } = msg {
                status = Some(exit_status);
            }
        }
    })
    .await;
    let _ = session
        .disconnect(Disconnect::ByApplication, "", "English")
        .await;
    exit_outcome(host, status)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_pinned_fingerprint_must_match_exactly() {
        assert!(fingerprint_matches("SHA256:abc", "SHA256:abc"));
        assert!(fingerprint_matches(" SHA256:abc\n", "SHA256:abc"));
        assert!(!fingerprint_matches("SHA256:abc", "SHA256:abd"));
        assert!(!fingerprint_matches("", "SHA256:abc"));
    }

    #[test]
    fn exit_status_outcomes() {
        assert!(exit_outcome("h", None).is_ok());
        assert!(exit_outcome("h", Some(0)).is_ok());
        let e = exit_outcome("h", Some(3)).unwrap_err();
        assert!(e.contains("status 3"), "{e}");
    }
}

#[cfg(test)]
mod server_tests {
    //! Against a real SSH server (russh's own) on a local port: the host key pin, the key
    //! file, the forced-command exec and the bounded wait after it.

    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use russh::keys::{PrivateKey, PublicKey};
    use russh::server::{self, Auth, Msg, Server as _, Session};
    use russh::{Channel, ChannelId};
    use tokio::net::TcpListener;

    use super::*;

    // Throwaway keys made for these tests; they protect nothing.
    const HOST_KEY: &str = "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW\nQyNTUxOQAAACBY3EnK2pPVWanWYe586iMFi/j+ndlXVP0R6QfpmFPVOgAAAJCqdezZqnXs\n2QAAAAtzc2gtZWQyNTUxOQAAACBY3EnK2pPVWanWYe586iMFi/j+ndlXVP0R6QfpmFPVOg\nAAAECt2Dg/hDgi80Egwi4NoRjimOQdcL87GwymwDI8cBXOWVjcScrak9VZqdZh7nzqIwWL\n+P6d2VdU/RHpB+mYU9U6AAAACXRlc3QtaG9zdAECAwQ=\n-----END OPENSSH PRIVATE KEY-----\n";
    const CLIENT_KEY: &str = "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW\nQyNTUxOQAAACB0mCg1vZZnF4iUuCVotiwpsnglJIzPRTaRrqjbVam7SAAAAJA2zu2tNs7t\nrQAAAAtzc2gtZWQyNTUxOQAAACB0mCg1vZZnF4iUuCVotiwpsnglJIzPRTaRrqjbVam7SA\nAAAEAHpwvJf9BlZ5RZey7JrO6+Uw49S44paRwlNlr/fEhL2nSYKDW9lmcXiJS4JWi2LCmy\neCUkjM9FNpGuqNtVqbtIAAAAC3Rlc3QtY2xpZW50AQI=\n-----END OPENSSH PRIVATE KEY-----\n";

    #[derive(Clone, Copy)]
    enum Mode {
        /// Answer the exec with this exit status and close.
        Exit(u32),
        /// Accept the exec and say nothing more, as a host that is going down.
        Silent,
    }

    #[derive(Clone)]
    struct Fake {
        mode: Mode,
        user: &'static str,
        client: PublicKey,
        commands: Arc<Mutex<Vec<String>>>,
    }

    impl server::Server for Fake {
        type Handler = Fake;
        fn new_client(&mut self, _: Option<std::net::SocketAddr>) -> Fake {
            self.clone()
        }
    }

    impl server::Handler for Fake {
        type Error = russh::Error;

        async fn channel_open_session(
            &mut self,
            _channel: Channel<Msg>,
            reply: server::ChannelOpenHandle,
            _session: &mut Session,
        ) -> Result<(), Self::Error> {
            reply.accept().await;
            Ok(())
        }

        async fn auth_publickey(
            &mut self,
            user: &str,
            key: &PublicKey,
        ) -> Result<Auth, Self::Error> {
            Ok(
                if user == self.user && key.key_data() == self.client.key_data() {
                    Auth::Accept
                } else {
                    Auth::reject()
                },
            )
        }

        async fn exec_request(
            &mut self,
            channel: ChannelId,
            data: &[u8],
            session: &mut Session,
        ) -> Result<(), Self::Error> {
            self.commands
                .lock()
                .unwrap()
                .push(String::from_utf8_lossy(data).into_owned());
            session.channel_success(channel)?;
            if let Mode::Exit(n) = self.mode {
                session.exit_status_request(channel, n)?;
                session.eof(channel)?;
                session.close(channel)?;
            }
            Ok(())
        }
    }

    struct Running {
        port: u16,
        commands: Arc<Mutex<Vec<String>>>,
        host_fingerprint: String,
        key_file: PathBuf,
    }

    async fn start(mode: Mode, name: &str) -> Running {
        let host = PrivateKey::from_openssh(HOST_KEY).unwrap();
        let client = PrivateKey::from_openssh(CLIENT_KEY).unwrap();
        let host_fingerprint = host.public_key().fingerprint(HashAlg::Sha256).to_string();
        let commands = Arc::new(Mutex::new(Vec::new()));
        let mut fake = Fake {
            mode,
            user: "poweroff",
            client: client.public_key().clone(),
            commands: Arc::clone(&commands),
        };
        let config = Arc::new(server::Config {
            keys: vec![host],
            auth_rejection_time: Duration::from_millis(10),
            auth_rejection_time_initial: Some(Duration::ZERO),
            ..Default::default()
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = fake.run_on_socket(config, &listener).await;
        });
        let dir = std::env::temp_dir().join(format!("sched-actuate-ssh-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let key_file = dir.join(name);
        std::fs::write(&key_file, CLIENT_KEY).unwrap();
        Running {
            port,
            commands,
            host_fingerprint,
            key_file,
        }
    }

    fn target(r: &Running) -> SshShutdown {
        SshShutdown {
            host: "127.0.0.1".into(),
            port: r.port,
            user: "poweroff".into(),
            host_key: r.host_fingerprint.clone(),
        }
    }

    const QUICK: Timeouts = Timeouts {
        connect: Duration::from_secs(10),
        drain: Duration::from_millis(300),
    };

    #[tokio::test]
    async fn a_clean_exit_is_success_and_one_exec_is_sent() {
        let r = start(Mode::Exit(0), "clean").await;
        shutdown_with(&target(&r), &r.key_file, QUICK)
            .await
            .unwrap();
        assert_eq!(*r.commands.lock().unwrap(), ["poweroff"]);
    }

    #[tokio::test]
    async fn a_host_that_goes_quiet_after_the_exec_is_success_within_the_drain_bound() {
        let r = start(Mode::Silent, "silent").await;
        let started = std::time::Instant::now();
        shutdown_with(&target(&r), &r.key_file, QUICK)
            .await
            .unwrap();
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(r.commands.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_failing_command_is_an_error() {
        let r = start(Mode::Exit(7), "failing").await;
        let e = shutdown_with(&target(&r), &r.key_file, QUICK)
            .await
            .unwrap_err();
        assert!(e.contains("status 7"), "{e}");
    }

    #[tokio::test]
    async fn another_host_key_is_refused_before_anything_is_sent() {
        let r = start(Mode::Exit(0), "pin").await;
        let mut t = target(&r);
        t.host_key = "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".into();
        let e = shutdown_with(&t, &r.key_file, QUICK).await.unwrap_err();
        assert!(e.contains("ssh connect"), "{e}");
        assert!(r.commands.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn the_wrong_user_or_key_is_rejected() {
        let r = start(Mode::Exit(0), "auth").await;
        let mut t = target(&r);
        t.user = "root".into();
        let e = shutdown_with(&t, &r.key_file, QUICK).await.unwrap_err();
        assert!(e.contains("rejected") || e.contains("auth"), "{e}");
        assert!(r.commands.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_missing_key_file_and_a_closed_port_are_errors() {
        let r = start(Mode::Exit(0), "missing").await;
        let e = shutdown_with(&target(&r), Path::new("/nonexistent/key"), QUICK)
            .await
            .unwrap_err();
        assert!(e.contains("shutdown key"), "{e}");
        let closed = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut t = target(&r);
        t.port = closed.local_addr().unwrap().port();
        drop(closed);
        let e = shutdown_with(&t, &r.key_file, QUICK).await.unwrap_err();
        assert!(e.contains("ssh connect"), "{e}");
    }

    #[tokio::test]
    async fn the_key_file_is_read_on_every_call() {
        let r = start(Mode::Exit(0), "rotated").await;
        shutdown_with(&target(&r), &r.key_file, QUICK)
            .await
            .unwrap();
        std::fs::write(&r.key_file, "garbage").unwrap();
        let e = shutdown_with(&target(&r), &r.key_file, QUICK)
            .await
            .unwrap_err();
        assert!(e.contains("shutdown key"), "{e}");
    }
}
