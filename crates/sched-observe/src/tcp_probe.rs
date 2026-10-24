//! The TCP probe adapter: reachability of one resource from a plain TCP connect. Which host,
//! port and resource come from the source's `params`; nothing here knows any particular
//! host or service. It sends no data: a connection is closed as soon as it is made.
//!
//! - The connect succeeds: up.
//! - The connection is refused, or nothing answers within the timeout: down. A refusal
//!   comes from the host itself (it is there, the port is closed); silence is what a host
//!   that is off looks like.
//! - Anything else is a failed read and is reported as one, never as "down": the name does
//!   not resolve or its lookup outlasts the timeout, it resolves to no address, or the
//!   connect fails another way (no route to host, network unreachable, an address that
//!   cannot be connected to, a local error). None of these says the host is off.
//!
//! A name resolving to several addresses is tried address by address, in the resolver's
//! order, within one connect timeout: the first connection is up; otherwise any refusal or
//! the timeout is down; only when every address failed another way is the read failed.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use sched_model::{Fidelity, ObservedValue, SourceSpec, TcpProbeParams};

use crate::kube_watch::Debouncer;
use crate::{Report, ReportTx};

/// One raw (not debounced) probe result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reading {
    Up,
    /// Down, and why: a refusal or the timeout.
    Down(String),
}

fn timeout_of(params: &TcpProbeParams) -> Duration {
    Duration::from_millis(params.timeout.as_u64())
}

/// `host:port` as written in the params (an IPv6 address is bracketed).
fn target(params: &TcpProbeParams) -> String {
    if params.host.contains(':') {
        format!("[{}]:{}", params.host, params.port)
    } else {
        format!("{}:{}", params.host, params.port)
    }
}

/// The addresses `params.host` names. A failed or slow lookup is an error.
async fn resolve(params: &TcpProbeParams) -> Result<Vec<SocketAddr>, String> {
    let at = target(params);
    let lookup = tokio::net::lookup_host((params.host.as_str(), params.port));
    let addrs: Vec<SocketAddr> = match tokio::time::timeout(timeout_of(params), lookup).await {
        Err(_) => {
            return Err(format!(
                "{at}: name lookup did not finish within {}; not treated as down",
                params.timeout
            ));
        }
        Ok(Err(e)) => {
            return Err(format!(
                "{at}: cannot resolve {:?}: {e}; not treated as down",
                params.host
            ));
        }
        Ok(Ok(addrs)) => addrs.collect(),
    };
    if addrs.is_empty() {
        return Err(format!(
            "{at}: {:?} resolves to no address; not treated as down",
            params.host
        ));
    }
    Ok(addrs)
}

/// Whether a connect error says the host is down (it refused), rather than that the probe
/// could not tell.
fn is_down(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::ConnectionRefused | io::ErrorKind::TimedOut
    )
}

/// Connects to each of `addrs` in turn until one connects, all within `timeout`.
pub async fn connect_any(addrs: &[SocketAddr], timeout: Duration) -> Result<Reading, String> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut down = None;
    let mut errors = Vec::new();
    for addr in addrs {
        match tokio::time::timeout_at(deadline, tokio::net::TcpStream::connect(addr)).await {
            // Dropping the stream closes the connection; nothing is sent.
            Ok(Ok(_stream)) => return Ok(Reading::Up),
            Ok(Err(e)) if is_down(&e) => down = Some(format!("{addr}: {e}")),
            Ok(Err(e)) => errors.push(format!("{addr}: {e}")),
            Err(_) => {
                down = Some(format!(
                    "{addr}: no answer within {}ms",
                    timeout.as_millis()
                ));
                break;
            }
        }
    }
    match down {
        Some(why) => Ok(Reading::Down(why)),
        None => Err(format!(
            "{}; not treated as down",
            if errors.is_empty() {
                "no address to connect to".to_string()
            } else {
                errors.join("; ")
            }
        )),
    }
}

/// One probe: the raw (not debounced) reading, or why there is none.
pub async fn read(params: &TcpProbeParams) -> Result<Reading, String> {
    let addrs = resolve(params).await?;
    connect_any(&addrs, timeout_of(params))
        .await
        .map_err(|e| format!("{}: {e}", target(params)))
}

/// One source's observer state: the debounce of its readings.
pub struct Probe {
    spec: SourceSpec,
    params: TcpProbeParams,
    debouncer: Debouncer,
}

impl Probe {
    pub fn new(spec: SourceSpec, params: TcpProbeParams) -> Self {
        let debouncer = Debouncer::new(params.reachability_confirmations);
        Probe {
            spec,
            params,
            debouncer,
        }
    }

    /// Probes once and turns the result into a report. A failed read is a failure and
    /// does not count as a reading for the debounce.
    pub async fn poll(&mut self) -> Report {
        match read(&self.params).await {
            Ok(reading) => {
                if let Reading::Down(why) = &reading {
                    tracing::debug!(source = %self.spec.id, "probe reads down: {why}");
                }
                Report::Observed {
                    source: self.spec.id.clone(),
                    fidelity: Fidelity::Polled,
                    values: vec![ObservedValue::ResourceUp {
                        resource: self.params.resource.clone(),
                        up: self.debouncer.check(reading == Reading::Up),
                    }],
                }
            }
            Err(error) => Report::Failed {
                source: self.spec.id.clone(),
                error,
            },
        }
    }
}

/// Probes forever, every `resync`, until the receiver is gone.
pub async fn run(spec: SourceSpec, params: TcpProbeParams, tx: ReportTx) {
    let mut ticker = tokio::time::interval(Duration::from_millis(spec.resync.as_u64().max(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut probe = Probe::new(spec, params);
    loop {
        ticker.tick().await;
        let report = probe.poll().await;
        if tx.send(report).await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ObserverState, Observers, SpawnOptions, observer_state};
    use sched_model::test_support::{EXAMPLE_CONFIG, example_config};
    use sched_model::{Config, Millis, ResourceId};
    use std::net::{Ipv4Addr, TcpListener as StdListener};
    use std::time::Instant;
    use tokio::net::{TcpListener, TcpSocket, TcpStream};

    const LOCALHOST: Ipv4Addr = Ipv4Addr::LOCALHOST;

    /// The example's probe, pointed at `host:port`, with a short timeout.
    fn example_probe(host: &str, port: u16) -> (SourceSpec, TcpProbeParams) {
        let spec = example_config()
            .source(&"basestar-probe".into())
            .unwrap()
            .clone();
        let mut params = spec.tcp_probe_params().unwrap().unwrap();
        params.host = host.to_string();
        params.port = port;
        params.timeout = Millis(300);
        (spec, params)
    }

    /// A local port that refuses connections: bound (so nothing else can take it) but not
    /// listening, so a connect is answered with a reset.
    fn refusing_port() -> (TcpSocket, u16) {
        let socket = TcpSocket::new_v4().unwrap();
        socket.bind((LOCALHOST, 0).into()).unwrap();
        let port = socket.local_addr().unwrap().port();
        (socket, port)
    }

    /// A local port that never answers a connect: a listener whose accept queue is full and
    /// is never drained, so the kernel drops further connection requests unanswered. The
    /// queue is filled until a connect goes unanswered; the returned connections keep it
    /// full.
    async fn silent_port() -> (TcpListener, Vec<TcpStream>, u16) {
        let socket = TcpSocket::new_v4().unwrap();
        socket.bind((LOCALHOST, 0).into()).unwrap();
        let listener = socket.listen(0).unwrap();
        let addr = listener.local_addr().unwrap();
        let mut held = Vec::new();
        for _ in 0..16 {
            match tokio::time::timeout(Duration::from_millis(200), TcpStream::connect(addr)).await {
                Ok(Ok(s)) => held.push(s),
                Ok(Err(e)) => panic!("filling the accept queue: {e}"),
                Err(_) => return (listener, held, addr.port()),
            }
        }
        panic!("the accept queue of a backlog-0 listener never filled");
    }

    fn up_of(r: &Report) -> bool {
        match r {
            Report::Observed {
                source,
                fidelity,
                values,
            } => {
                assert_eq!(source.as_str(), "basestar-probe");
                assert_eq!(*fidelity, Fidelity::Polled);
                match values.as_slice() {
                    [ObservedValue::ResourceUp { resource, up }] => {
                        assert_eq!(resource, &ResourceId::new("basestar-nfs"));
                        *up
                    }
                    v => panic!("{v:?}"),
                }
            }
            Report::Failed { error, .. } => panic!("failed: {error}"),
        }
    }

    fn error_of(r: Report) -> String {
        match r {
            Report::Failed { source, error } => {
                assert_eq!(source.as_str(), "basestar-probe");
                error
            }
            other => panic!("expected a failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_listening_port_is_up() {
        let listener = TcpListener::bind((LOCALHOST, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (_, params) = example_probe("127.0.0.1", port);
        assert_eq!(read(&params).await, Ok(Reading::Up));
        // By name as well.
        let (_, params) = example_probe("localhost", port);
        assert_eq!(read(&params).await, Ok(Reading::Up));
        // The probe made a connection and closed it without sending anything.
        let (mut conn, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 8];
        let n = tokio::io::AsyncReadExt::read(&mut conn, &mut buf)
            .await
            .unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn a_refused_connection_is_down() {
        let (_held, port) = refusing_port();
        let (_, params) = example_probe("127.0.0.1", port);
        match read(&params).await {
            Ok(Reading::Down(why)) => assert!(why.contains("refused"), "{why}"),
            other => panic!("expected down, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn no_answer_within_the_timeout_is_down() {
        let (_listener, _held, port) = silent_port().await;
        let (_, params) = example_probe("127.0.0.1", port);
        let started = Instant::now();
        match read(&params).await {
            Ok(Reading::Down(why)) => assert!(why.contains("no answer within 300ms"), "{why}"),
            other => panic!("expected down, got {other:?}"),
        }
        // It gave up at the timeout, not at the kernel's own connect timeout.
        let took = started.elapsed();
        assert!(took >= Duration::from_millis(290), "{took:?}");
        assert!(took < Duration::from_secs(2), "{took:?}");
    }

    #[tokio::test]
    async fn an_unresolvable_name_is_a_failed_read_not_down() {
        // `.invalid` never resolves (RFC 6761).
        let (spec, params) = example_probe("basestar.invalid", 2049);
        let e = read(&params).await.unwrap_err();
        assert!(e.contains("basestar.invalid:2049"), "{e}");
        assert!(
            e.contains("cannot resolve") || e.contains("name lookup did not finish"),
            "{e}"
        );
        assert!(e.contains("not treated as down"), "{e}");
        let mut probe = Probe::new(spec, params);
        assert!(!error_of(probe.poll().await).is_empty());
    }

    #[tokio::test]
    async fn an_address_that_cannot_be_connected_to_is_a_failed_read_not_down() {
        // The kernel refuses TCP to a broadcast or multicast address outright ("network
        // unreachable") without sending anything.
        for host in ["255.255.255.255", "224.0.0.1"] {
            let (_, params) = example_probe(host, 2049);
            let e = read(&params).await.unwrap_err();
            assert!(e.contains(&format!("{host}:2049")), "{e}");
            assert!(e.contains("not treated as down"), "{e}");
        }
        // A name the resolver rejects.
        let (_, params) = example_probe("bad host name", 2049);
        assert!(read(&params).await.is_err());
    }

    #[tokio::test]
    async fn several_addresses_are_tried_in_turn() {
        let listener = TcpListener::bind((LOCALHOST, 0)).await.unwrap();
        let open: SocketAddr = listener.local_addr().unwrap();
        let (_held, refusing) = refusing_port();
        let refusing = SocketAddr::from((LOCALHOST, refusing));
        let unusable = SocketAddr::from((Ipv4Addr::BROADCAST, 2049));
        let t = Duration::from_millis(300);
        // Any address connecting is up, whatever the others did.
        assert_eq!(
            connect_any(&[unusable, refusing, open], t).await,
            Ok(Reading::Up)
        );
        // A refusal is down even when another address could not be tried.
        assert!(matches!(
            connect_any(&[unusable, refusing], t).await,
            Ok(Reading::Down(_))
        ));
        // Only unusable addresses: a failed read.
        let e = connect_any(&[unusable, unusable], t).await.unwrap_err();
        assert!(e.contains("not treated as down"), "{e}");
        assert!(connect_any(&[], t).await.is_err());
        // The timeout covers all addresses together: a silent first address uses it up.
        let (_l, _h, silent) = silent_port().await;
        let silent = SocketAddr::from((LOCALHOST, silent));
        let started = Instant::now();
        match connect_any(&[silent, open], t).await {
            Ok(Reading::Down(why)) => assert!(why.contains("no answer"), "{why}"),
            other => panic!("expected down, got {other:?}"),
        }
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn readings_are_debounced_and_failed_reads_are_not_readings() {
        let listener = TcpListener::bind((LOCALHOST, 0)).await.unwrap();
        let open = listener.local_addr().unwrap().port();
        let (_held, refusing) = refusing_port();
        let (spec, mut params) = example_probe("127.0.0.1", open);
        assert_eq!(params.reachability_confirmations, 3);
        let mut probe = Probe::new(spec.clone(), params.clone());
        // The first reading is adopted as is.
        assert!(up_of(&probe.poll().await));
        probe.params.port = refusing;
        assert!(up_of(&probe.poll().await));
        // A failed read in between neither confirms nor resets the pending "down".
        probe.params.host = "basestar.invalid".into();
        error_of(probe.poll().await);
        probe.params.host = "127.0.0.1".into();
        assert!(up_of(&probe.poll().await));
        assert!(!up_of(&probe.poll().await));
        // Back up takes three agreeing readings as well.
        probe.params.port = open;
        assert!(!up_of(&probe.poll().await));
        assert!(!up_of(&probe.poll().await));
        assert!(up_of(&probe.poll().await));
        // With one confirmation every reading counts at once.
        params.reachability_confirmations = 1;
        let mut probe = Probe::new(spec, params);
        assert!(up_of(&probe.poll().await));
        probe.params.port = refusing;
        assert!(!up_of(&probe.poll().await));
    }

    #[tokio::test]
    async fn the_example_probe_has_a_running_observer_that_reports_reachability() {
        let spec = example_config()
            .source(&"basestar-probe".into())
            .unwrap()
            .clone();
        assert_eq!(observer_state(&spec), ObserverState::Running);
        let mut no_params = spec.clone();
        no_params.params = Default::default();
        assert_eq!(observer_state(&no_params), ObserverState::NoParams);

        let listener = StdListener::bind((LOCALHOST, 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let text = EXAMPLE_CONFIG.replace(
            "      host: basestar\n      port: 2049\n",
            &format!("      host: 127.0.0.1\n      port: {port}\n"),
        );
        assert_ne!(text, EXAMPLE_CONFIG);
        let config = Config::from_yaml(&text).unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(256);
        let mut observers = Observers::default();
        let started = observers.update(&config, &tx, &SpawnOptions::default());
        assert!(
            started.iter().any(|s| s.as_str() == "basestar-probe"),
            "{started:?}"
        );
        let report = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let r = rx.recv().await.unwrap();
                if r.source().as_str() == "basestar-probe" {
                    return r;
                }
            }
        })
        .await
        .expect("the probe reports");
        assert!(up_of(&report));
    }
}
