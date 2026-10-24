//! The TCP probe adapter: reachability of one resource from a plain TCP connect. Which host,
//! port and resource come from the source's `params`; nothing here knows any particular
//! host or service. It sends no data: a connection is closed as soon as it is made.
//!
//! - The connect succeeds: up.
//! - The host does not answer, or answers no: down. That is a refused connection, no answer
//!   before the deadline, or "no route to host" (`EHOSTUNREACH`, which is what Linux reports
//!   for an address on the local network that does not answer ARP, as a powered-off host
//!   does, after about 3s, and sooner while an earlier ARP resolution of it is pending).
//! - Anything else is a failed read and is reported as one, never as "down": the name does
//!   not resolve or its lookup outlasts the deadline, it resolves to no address, or the
//!   connect fails another way ("network unreachable", that is no route at all, an address
//!   that cannot be connected to, a local error). None of these says the host is off.
//!
//! One probe takes at most `timeout`: the name lookup and the connects share one deadline.
//! A lookup that is slow leaves the connects less time.
//!
//! A name resolving to several addresses is probed at all of them at once, within that
//! deadline: up as soon as any connects; down only if every address answered down; a
//! failed read if none connected and at least one failed another way. So a probe never
//! reads down while an address that would have connected before the deadline was left
//! untried.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use sched_model::{Fidelity, Millis, ObservedValue, SourceSpec, TcpProbeParams};
use tokio::time::Instant;

use crate::kube_watch::Debouncer;
use crate::{Report, ReportTx};

/// One raw (not debounced) probe result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reading {
    Up,
    /// Down, and why: refused, no answer, or no route to the host.
    Down(String),
}

/// `host:port` as written in the params (an IPv6 address is bracketed).
fn target(params: &TcpProbeParams) -> String {
    if params.host.contains(':') {
        format!("[{}]:{}", params.host, params.port)
    } else {
        format!("{}:{}", params.host, params.port)
    }
}

/// The addresses `params.host` names, looked up before `deadline`. A failed or slow lookup
/// is an error.
async fn resolve(params: &TcpProbeParams, deadline: Instant) -> Result<Vec<SocketAddr>, String> {
    let at = target(params);
    let lookup = tokio::net::lookup_host((params.host.as_str(), params.port));
    match tokio::time::timeout_at(deadline, lookup).await {
        Err(_) => Err(format!(
            "{at}: name lookup did not finish within the probe's {} timeout; not treated as down",
            params.timeout
        )),
        Ok(Err(e)) => Err(format!(
            "{at}: cannot resolve {:?}: {e}; not treated as down",
            params.host
        )),
        Ok(Ok(addrs)) => Ok(addrs.collect()),
    }
}

/// Whether a connect error is the host answering no or not answering (down), rather than
/// the probe being unable to tell (a failed read).
pub fn means_down(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::ConnectionRefused | io::ErrorKind::TimedOut | io::ErrorKind::HostUnreachable
    )
}

/// What one address answered.
enum Answer {
    Connected,
    Down(String),
    Failed(String),
}

async fn answer(addr: SocketAddr, deadline: Instant, timeout: Millis) -> Answer {
    match tokio::time::timeout_at(deadline, tokio::net::TcpStream::connect(addr)).await {
        // Dropping the stream closes the connection; nothing is sent.
        Ok(Ok(_stream)) => Answer::Connected,
        Ok(Err(e)) if means_down(&e) => Answer::Down(e.to_string()),
        Ok(Err(e)) => Answer::Failed(e.to_string()),
        Err(_) => Answer::Down(format!("no answer within the probe's {timeout} timeout")),
    }
}

/// Connects to all of `addrs` at once, each until `deadline`; `target` is what they were
/// resolved from and `timeout` the probe's timeout, both for the messages.
pub async fn connect_all(
    target: &str,
    addrs: &[SocketAddr],
    deadline: Instant,
    timeout: Millis,
) -> Result<Reading, String> {
    if addrs.is_empty() {
        return Err(format!(
            "{target}: resolves to no address; not treated as down"
        ));
    }
    // Name the address only when it is not simply the target itself.
    let alone = addrs.len() == 1 && addrs[0].to_string() == target;
    let line = |addr: SocketAddr, why: String| {
        if alone { why } else { format!("{addr}: {why}") }
    };
    let mut pending: FuturesUnordered<_> = addrs
        .iter()
        .map(|&addr| async move { (addr, answer(addr, deadline, timeout).await) })
        .collect();
    let (mut down, mut failed) = (Vec::new(), Vec::new());
    while let Some((addr, answer)) = pending.next().await {
        match answer {
            // The other attempts are dropped, which closes them.
            Answer::Connected => return Ok(Reading::Up),
            Answer::Down(why) => down.push(line(addr, why)),
            Answer::Failed(why) => failed.push(line(addr, why)),
        }
    }
    if failed.is_empty() {
        Ok(Reading::Down(format!("{target}: {}", down.join("; "))))
    } else {
        failed.extend(down);
        Err(format!(
            "{target}: {}; not treated as down",
            failed.join("; ")
        ))
    }
}

/// One probe, finished within the params' timeout: the raw (not debounced) reading, or
/// why there is none.
pub async fn read(params: &TcpProbeParams) -> Result<Reading, String> {
    let deadline = Instant::now() + Duration::from_millis(params.timeout.as_u64());
    let addrs = resolve(params, deadline).await?;
    connect_all(&target(params), &addrs, deadline, params.timeout).await
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
    use sched_model::{Config, ResourceId};
    use std::net::{Ipv4Addr, TcpListener as StdListener};
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
    async fn a_refused_connection_is_down_and_names_the_target_once() {
        let (_held, port) = refusing_port();
        let (_, params) = example_probe("127.0.0.1", port);
        match read(&params).await {
            Ok(Reading::Down(why)) => {
                assert!(why.contains("refused"), "{why}");
                assert!(why.starts_with(&format!("127.0.0.1:{port}: ")), "{why}");
                assert_eq!(why.matches("127.0.0.1").count(), 1, "{why}");
            }
            other => panic!("expected down, got {other:?}"),
        }
        // Resolved from a name, the address it connected to is named after the target.
        let (_, params) = example_probe("localhost", port);
        match read(&params).await {
            Ok(Reading::Down(why)) => {
                assert!(why.starts_with(&format!("localhost:{port}: ")), "{why}")
            }
            // Where localhost also resolves to ::1 and IPv6 is unavailable, that address
            // fails another way and the read fails; it is still not "up".
            Err(e) => assert!(e.contains("not treated as down"), "{e}"),
            other => panic!("expected down, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn no_answer_within_the_timeout_is_down_and_the_probe_keeps_to_its_timeout() {
        let (_listener, _held, port) = silent_port().await;
        for host in ["127.0.0.1", "localhost"] {
            let (_, params) = example_probe(host, port);
            let started = Instant::now();
            let result = read(&params).await;
            let took = started.elapsed();
            if host == "127.0.0.1" {
                match result {
                    Ok(Reading::Down(why)) => assert_eq!(
                        why,
                        format!("127.0.0.1:{port}: no answer within the probe's 300ms timeout")
                    ),
                    other => panic!("expected down, got {other:?}"),
                }
            }
            // The lookup and the connect share the one deadline: the whole probe gives up at
            // the timeout, not at the kernel's own connect timeout and not later.
            assert!(took >= Duration::from_millis(290), "{host}: {took:?}");
            assert!(took < Duration::from_millis(500), "{host}: {took:?}");
        }
    }

    #[test]
    fn refused_timed_out_and_no_route_to_host_are_down_other_errors_are_not() {
        use io::ErrorKind::*;
        for kind in [ConnectionRefused, TimedOut, HostUnreachable] {
            assert!(means_down(&io::Error::from(kind)), "{kind:?}");
        }
        for kind in [
            NetworkUnreachable,
            AddrNotAvailable,
            PermissionDenied,
            InvalidInput,
            Unsupported,
            Other,
        ] {
            assert!(!means_down(&io::Error::from(kind)), "{kind:?}");
        }
        // The errno values Linux returns for them.
        if cfg!(target_os = "linux") {
            for (errno, down) in [
                (111, true),  // ECONNREFUSED
                (110, true),  // ETIMEDOUT
                (113, true),  // EHOSTUNREACH
                (101, false), // ENETUNREACH
                (99, false),  // EADDRNOTAVAIL
                (97, false),  // EAFNOSUPPORT
                (13, false),  // EACCES
            ] {
                let e = io::Error::from_raw_os_error(errno);
                assert_eq!(means_down(&e), down, "errno {errno}: {e}");
            }
        }
    }

    /// Run with `TCP_PROBE_UNANSWERED_ADDR=<ip> cargo test -p sched-observe -- --ignored`.
    #[tokio::test]
    #[ignore = "needs TCP_PROBE_UNANSWERED_ADDR: an unused address on a directly attached subnet (nothing answers its ARP); takes about 3s"]
    async fn an_address_nothing_answers_arp_for_is_down() {
        let host = std::env::var("TCP_PROBE_UNANSWERED_ADDR")
            .expect("set TCP_PROBE_UNANSWERED_ADDR to an unused address on a local subnet");
        let (_, mut params) = example_probe(&host, 51049);
        // Longer than the kernel's ARP failure (about 3s), so "no route to host" comes first.
        params.timeout = Millis(5_000);
        let started = Instant::now();
        let result = read(&params).await;
        let took = started.elapsed();
        eprintln!("{host}: {result:?} after {took:?}");
        match result {
            Ok(Reading::Down(why)) => assert!(
                why.contains("No route to host") || why.contains("no answer"),
                "{why}"
            ),
            other => panic!("expected down, got {other:?}"),
        }
        assert!(took < Duration::from_millis(5_200), "{took:?}");
    }

    #[tokio::test]
    async fn an_unresolvable_name_is_a_failed_read_not_down() {
        // `.invalid` never resolves (RFC 6761).
        let (spec, params) = example_probe("basestar.invalid", 2049);
        let e = read(&params).await.unwrap_err();
        assert!(e.starts_with("basestar.invalid:2049: "), "{e}");
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
            assert!(e.starts_with(&format!("{host}:2049: ")), "{e}");
            assert_eq!(e.matches(host).count(), 1, "{e}");
            assert!(e.contains("not treated as down"), "{e}");
        }
        // A name the resolver rejects.
        let (_, params) = example_probe("bad host name", 2049);
        assert!(read(&params).await.is_err());
    }

    /// `connect_all` with a deadline `ms` from now.
    async fn connect(addrs: &[SocketAddr], ms: u64) -> Result<Reading, String> {
        let deadline = Instant::now() + Duration::from_millis(ms);
        connect_all("probe-target:1", addrs, deadline, Millis(ms)).await
    }

    #[tokio::test]
    async fn several_addresses_are_probed_at_once() {
        let listener = TcpListener::bind((LOCALHOST, 0)).await.unwrap();
        let open: SocketAddr = listener.local_addr().unwrap();
        let (_held, refusing) = refusing_port();
        let refusing = SocketAddr::from((LOCALHOST, refusing));
        let (_l, _h, silent) = silent_port().await;
        let silent = SocketAddr::from((LOCALHOST, silent));
        let unusable = SocketAddr::from((Ipv4Addr::BROADCAST, 2049));
        // Any address connecting is up, whatever the others did, and in whichever order.
        for addrs in [
            [unusable, refusing, open],
            [silent, refusing, open],
            [open, silent, unusable],
            [silent, silent, open],
        ] {
            let started = Instant::now();
            assert_eq!(connect(&addrs, 300).await, Ok(Reading::Up), "{addrs:?}");
            // It does not wait for a silent address first.
            assert!(
                started.elapsed() < Duration::from_millis(250),
                "{addrs:?}: {:?}",
                started.elapsed()
            );
        }
        // Down only when every address answered down.
        match connect(&[refusing, silent], 300).await {
            Ok(Reading::Down(why)) => {
                assert!(why.starts_with("probe-target:1: "), "{why}");
                assert!(why.contains(&format!("{refusing}: ")), "{why}");
                assert!(why.contains(&format!("{silent}: no answer")), "{why}");
            }
            other => panic!("expected down, got {other:?}"),
        }
        // An address that failed another way makes it a failed read, even beside a refusal.
        for addrs in [
            vec![unusable, refusing],
            vec![silent, unusable],
            vec![unusable, unusable],
        ] {
            let e = connect(&addrs, 300).await.unwrap_err();
            assert!(e.contains("not treated as down"), "{addrs:?}: {e}");
            assert!(e.contains(&format!("{unusable}: ")), "{e}");
        }
        assert!(connect(&[], 300).await.unwrap_err().contains("no address"));
        // All of them within the one deadline: three silent addresses take one timeout.
        let started = Instant::now();
        assert!(matches!(
            connect(&[silent, silent, silent], 300).await,
            Ok(Reading::Down(_))
        ));
        assert!(started.elapsed() < Duration::from_millis(500));
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
