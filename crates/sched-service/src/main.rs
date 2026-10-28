//! The flowmanager scheduler binary: it hosts the scheduler as a Restate virtual object,
//! runs the observers, serves metrics, and serves each tick's allowed set to KEDA as an
//! external scaler (gRPC). Power steps (Wake-on-LAN through the router, SSH shutdown) and the
//! recovery action are carried out in their own Restate objects; kill and pause steps have no
//! actuator and are logged as not executed. The observers only read.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use restate_sdk::endpoint::Endpoint;
use restate_sdk::http_server::HttpServer;
use sched_actuate::{HostPower, SecretFiles};
use sched_model::{Config, Millis};
use sched_observe::Nudges;
use sched_service::clock::SystemClock;
use sched_service::config_source::{ConfigHolder, watch_file};
use sched_service::metrics::Metrics;
use sched_service::scheduler::{Power, Recovery, Scheduler};
use sched_service::tick::Shared;
use sched_service::{keda_scaler, metrics_server, recovery, runtime, toggle, webhook};

fn duration(text: &str) -> Result<Duration, String> {
    Millis::parse(text).map(|m| Duration::from_millis(m.as_u64()))
}

/// A duration that must be greater than zero: a zero interval would spin or panic.
fn positive_duration(text: &str) -> Result<Duration, String> {
    let d = duration(text)?;
    if d.is_zero() {
        return Err(format!("{text:?} must be greater than zero"));
    }
    Ok(d)
}

#[derive(Parser, Debug)]
#[command(
    version,
    about = "GPU scheduler: decides which model runs and publishes the allowed set"
)]
struct Args {
    /// Path of the scheduler configuration; re-read on change (a mounted ConfigMap).
    #[arg(long, env = "SCHED_CONFIG")]
    config: PathBuf,
    /// Address of the Restate service endpoint.
    #[arg(long, env = "SCHED_LISTEN", default_value = "0.0.0.0:9080")]
    listen: SocketAddr,
    /// Address of the Prometheus /metrics listener.
    #[arg(long, env = "SCHED_METRICS_LISTEN", default_value = "0.0.0.0:9090")]
    metrics_listen: SocketAddr,
    /// Address of the KEDA external scaler (plaintext gRPC; in the cluster Istio adds mTLS).
    #[arg(long, env = "SCHED_KEDA_LISTEN", default_value = "0.0.0.0:9095")]
    keda_listen: SocketAddr,
    /// Address of the toggle listener (plain HTTP; Home Assistant's REST switch uses it to
    /// hold and release manual model and host leases). Off when not given. Needs
    /// `--toggle-token-file`. Suggested: 0.0.0.0:9096, like the other listeners, because
    /// the client is another pod or a hostNetwork pod, not a process on this host.
    #[arg(long, env = "SCHED_TOGGLE_LISTEN", requires = "toggle_token_file")]
    toggle_listen: Option<SocketAddr>,
    /// Bearer token the toggle listener requires (a mounted Kubernetes Secret, one line),
    /// read on every request. Needs `--toggle-listen`; the listener refuses to start when
    /// the file is missing or empty.
    #[arg(long, env = "SCHED_TOGGLE_TOKEN_FILE", requires = "toggle_listen")]
    toggle_token_file: Option<PathBuf>,
    /// Address of the webhook listener (plain HTTP): `POST` on a path an `http_json` source
    /// declared with `webhook: { path: ... }` makes that source poll now. Off when not
    /// given. Needs `--webhook-auth`. Suggested: 0.0.0.0:9097, a port of its own (not the
    /// toggle port: the two need different network and mesh policies).
    #[arg(long, env = "SCHED_WEBHOOK_LISTEN")]
    webhook_listen: Option<SocketAddr>,
    /// Who may send webhooks; there is no default. `mesh`: this process authenticates no
    /// one and the service mesh must (STRICT mTLS plus an AuthorizationPolicy on the
    /// port). `token`: `Authorization: Bearer` with the token of `--webhook-token-file`.
    #[arg(long, env = "SCHED_WEBHOOK_AUTH", value_enum)]
    webhook_auth: Option<WebhookAuth>,
    /// Bearer token the webhook listener requires with `--webhook-auth token` (a mounted
    /// Kubernetes Secret, one line), read on every request; the listener refuses to start
    /// when the file is missing or empty. Not allowed with `--webhook-auth mesh`.
    #[arg(long, env = "SCHED_WEBHOOK_TOKEN_FILE")]
    webhook_token_file: Option<PathBuf>,
    /// The least time between two polls of a source that webhooks nudge; nudges that come
    /// sooner are merged into one poll. The source's `resync` still applies.
    #[arg(long, env = "SCHED_WEBHOOK_MIN_INTERVAL", default_value = "10s", value_parser = positive_duration)]
    webhook_min_interval: Duration,
    /// Restate ingress, used to ask for a tick when observations change and to arm the backstop.
    #[arg(
        long,
        env = "SCHED_RESTATE_INGRESS",
        default_value = "http://localhost:8080"
    )]
    restate_ingress: String,
    /// Interval of the periodic backstop tick.
    #[arg(long, env = "SCHED_BACKSTOP", default_value = "30s", value_parser = positive_duration)]
    backstop: Duration,
    /// How often the config file is checked for changes.
    #[arg(long, env = "SCHED_RELOAD_INTERVAL", default_value = "5s", value_parser = positive_duration)]
    reload_interval: Duration,
    /// Delay that merges bursts of changes into one requested tick.
    #[arg(long, env = "SCHED_TRIGGER_DEBOUNCE", default_value = "250ms", value_parser = duration)]
    trigger_debounce: Duration,
    /// Router API credentials, `key:secret` on one line (a mounted Kubernetes Secret), read
    /// on every call. Needed when a resource has `power.control: managed`.
    #[arg(long, env = "SCHED_OPNSENSE_CREDENTIALS_FILE")]
    opnsense_credentials_file: Option<PathBuf>,
    /// PEM bundle that is the only CA trusted for the router (a mounted Secret), read on
    /// every call; the platform roots are used without it. Verification is never off.
    #[arg(long, env = "SCHED_OPNSENSE_CA_FILE")]
    opnsense_ca_file: Option<PathBuf>,
    /// Private key of the forced-command shutdown user (a mounted Secret), read on every
    /// call. Needed when a resource has `power.control: managed`.
    #[arg(long, env = "SCHED_SSH_KEY_FILE")]
    ssh_key_file: Option<PathBuf>,
    /// Log level: error, warn, info, debug or trace.
    #[arg(long, env = "SCHED_LOG", default_value = "info")]
    log_level: tracing::Level,
}

/// How the webhook listener decides who may send (`--webhook-auth`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum WebhookAuth {
    Mesh,
    Token,
}

/// The webhook listener's address and authentication from the flags, or `None` when no
/// webhook flag was given. Choosing no authentication is not an option: a listener needs
/// `--webhook-auth`, and the token file goes with `token` and with nothing else.
fn webhook_settings(args: &Args) -> Result<Option<(SocketAddr, webhook::Auth)>, String> {
    let (listen, auth, file) = (
        args.webhook_listen,
        args.webhook_auth,
        args.webhook_token_file.clone(),
    );
    match (listen, auth, file) {
        (None, None, None) => Ok(None),
        (None, _, _) => Err("--webhook-auth and --webhook-token-file need --webhook-listen".into()),
        (Some(_), None, _) => Err(
            "--webhook-listen needs --webhook-auth mesh or token (there is no default: mesh \
             means this process authenticates no one and the service mesh must)"
                .into(),
        ),
        (Some(addr), Some(WebhookAuth::Mesh), None) => Ok(Some((addr, webhook::Auth::Mesh))),
        (Some(_), Some(WebhookAuth::Mesh), Some(_)) => Err(
            "--webhook-token-file is given with --webhook-auth mesh, where no token is checked; \
             use --webhook-auth token or drop the file"
                .into(),
        ),
        (Some(addr), Some(WebhookAuth::Token), Some(f)) => {
            Ok(Some((addr, webhook::Auth::Token(f))))
        }
        (Some(_), Some(WebhookAuth::Token), None) => {
            Err("--webhook-auth token needs --webhook-token-file".into())
        }
    }
}

fn secret_files(args: &Args) -> SecretFiles {
    SecretFiles {
        opnsense_credentials: args.opnsense_credentials_file.clone(),
        opnsense_ca: args.opnsense_ca_file.clone(),
        ssh_key: args.ssh_key_file.clone(),
    }
}

/// Refuses to start when the config puts a host under managed power control but the secret
/// files its power calls need were not given: the first power step would fail otherwise.
/// (A config reloaded later with a managed host and no files starts fine; its power
/// requests then fail, are counted and logged, and are not retried.)
fn check_power_files(config: &Config, files: &SecretFiles) -> Result<(), String> {
    let managed = config
        .resources()
        .iter()
        .filter(|r| config.power_managed(&r.id))
        .collect::<Vec<_>>();
    let wake = managed
        .iter()
        .any(|r| r.power.as_ref().is_some_and(|p| p.wake_on_lan.is_some()));
    let shutdown = managed
        .iter()
        .any(|r| r.power.as_ref().is_some_and(|p| p.shutdown.is_some()));
    let missing = files.missing(wake, shutdown);
    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "the config has resources under managed power control ({}) but {} not given",
            managed
                .iter()
                .map(|r| r.id.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            missing.join(" and ")
        ))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let webhook_cfg = webhook_settings(&args)?;
    tracing_subscriber::fmt()
        .with_max_level(args.log_level)
        .init();

    let holder = Arc::new(ConfigHolder::load(&args.config)?);
    let files = secret_files(&args);
    check_power_files(&holder.current(), &files)?;
    let backend: Arc<dyn sched_actuate::PowerBackend> = Arc::new(HostPower::new(files));
    let metrics = Arc::new(Metrics::new());
    let shared = Arc::new(Shared::new(
        Arc::clone(&holder),
        Arc::clone(&metrics),
        Arc::new(SystemClock),
    ));
    shared.set_job_backend(Arc::new(sched_actuate::KubeJobs));
    tracing::info!(
        generation = holder.current().generation(),
        path = %args.config.display(),
        "scheduler starting; state is kept in Restate; the recovery action runs in the Recovery object; power steps run in the Power object; kill and pause steps are not executed (no actuator)"
    );

    let (tx, rx) = runtime::report_channel();
    tokio::spawn(runtime::pump(Arc::clone(&shared), rx));
    let nudges = Nudges::default();
    tokio::spawn(runtime::supervise_observers(
        Arc::clone(&shared),
        tx,
        nudges.clone(),
        args.webhook_min_interval,
    ));
    tokio::spawn(watch_file(
        holder,
        args.config.clone(),
        args.reload_interval,
        Arc::clone(&metrics),
    ));
    tokio::spawn(runtime::trigger_ticks(
        Arc::clone(&shared),
        args.restate_ingress.clone(),
        args.trigger_debounce,
    ));
    tokio::spawn(runtime::watch_backstop(
        Arc::clone(&shared),
        args.restate_ingress.clone(),
        args.backstop,
        args.backstop,
    ));
    tokio::spawn(runtime::arm_backstop(
        args.restate_ingress.clone(),
        Duration::from_secs(5),
    ));
    let listener = metrics_server::bind(args.metrics_listen).await?;
    tracing::info!(addr = %args.metrics_listen, "serving /metrics");
    tokio::spawn(metrics_server::serve(listener, metrics));
    let keda = keda_scaler::spawn(&shared, args.keda_listen).await?;
    tracing::info!(addr = %keda, "serving the KEDA external scaler (gRPC)");

    if let (Some(addr), Some(token_file)) = (args.toggle_listen, args.toggle_token_file.clone()) {
        let toggle = toggle::Toggle::new(
            Arc::clone(&shared),
            Arc::new(toggle::RestateIngress::new(&args.restate_ingress)),
            token_file,
        );
        let bound = toggle::spawn(toggle, addr).await?;
        tracing::info!(addr = %bound, "serving the toggle listener (HTTP, bearer token)");
    }
    if let Some((addr, auth)) = webhook_cfg {
        if auth == webhook::Auth::Mesh {
            tracing::warn!(
                "the webhook listener authenticates no one (--webhook-auth mesh): the service mesh must restrict who can reach it"
            );
        }
        let hook = webhook::Webhook::new(Arc::clone(&shared), nudges, auth);
        let bound = webhook::spawn(hook, addr).await?;
        tracing::info!(addr = %bound, min_interval = ?args.webhook_min_interval, "serving the webhook listener (HTTP)");
    } else if !shared.config.current().webhook_paths().is_empty() {
        tracing::info!(
            "sources declare webhook paths but the webhook listener is off (no --webhook-listen); they are polled at their resync only"
        );
    }
    tracing::info!(addr = %args.listen, "serving the Restate endpoint");
    HttpServer::new(
        Endpoint::builder()
            .bind(Power::new(Arc::clone(&shared), backend))
            .bind(Recovery::new(Arc::clone(&shared), recovery::process_env()))
            .bind(Scheduler::new(shared, args.backstop))
            .build(),
    )
    .listen_and_serve(args.listen)
    .await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(extra: &[&str]) -> Result<Args, clap::Error> {
        let mut argv = vec!["flowmanager", "--config", "c.yaml"];
        argv.extend_from_slice(extra);
        Args::try_parse_from(argv)
    }

    #[test]
    fn zero_intervals_are_refused_at_startup_with_a_clear_error() {
        for flag in ["--backstop", "--reload-interval"] {
            for zero in ["0", "0s", "0ms"] {
                let e = parse(&[flag, zero]).unwrap_err().to_string();
                assert!(
                    e.contains("must be greater than zero"),
                    "{flag} {zero}: {e}"
                );
            }
            assert!(parse(&[flag, "1s"]).is_ok());
        }
        assert_eq!(parse(&[]).unwrap().backstop, Duration::from_secs(30));
    }

    #[test]
    fn the_toggle_listener_is_off_unless_given_with_its_token_file() {
        let a = parse(&[]).unwrap();
        assert!(a.toggle_listen.is_none() && a.toggle_token_file.is_none());
        let a = parse(&[
            "--toggle-listen",
            "127.0.0.1:9096",
            "--toggle-token-file",
            "/t",
        ])
        .unwrap();
        assert_eq!(
            a.toggle_listen,
            Some("127.0.0.1:9096".parse::<SocketAddr>().unwrap())
        );
        // Each needs the other: no unauthenticated listener, no token without a listener.
        assert!(parse(&["--toggle-listen", "127.0.0.1:9096"]).is_err());
        assert!(parse(&["--toggle-token-file", "/t"]).is_err());
        assert!(parse(&["--toggle-listen", "nope", "--toggle-token-file", "/t"]).is_err());
    }

    #[test]
    fn the_webhook_listener_is_off_unless_given_and_never_without_an_authentication_choice() {
        let off = |a: &Args| webhook_settings(a).unwrap().is_none();
        assert!(off(&parse(&[]).unwrap()));
        let addr: SocketAddr = "127.0.0.1:9097".parse().unwrap();
        let settings = |extra: &[&str]| webhook_settings(&parse(extra).unwrap());
        assert_eq!(
            settings(&[
                "--webhook-listen",
                "127.0.0.1:9097",
                "--webhook-auth",
                "mesh"
            ]),
            Ok(Some((addr, webhook::Auth::Mesh)))
        );
        assert_eq!(
            settings(&[
                "--webhook-listen",
                "127.0.0.1:9097",
                "--webhook-auth",
                "token",
                "--webhook-token-file",
                "/t"
            ]),
            Ok(Some((addr, webhook::Auth::Token("/t".into()))))
        );
        // No default authentication, and each piece needs the others.
        for bad in [
            &["--webhook-listen", "127.0.0.1:9097"][..],
            &["--webhook-auth", "mesh"],
            &["--webhook-token-file", "/t"],
            &[
                "--webhook-listen",
                "127.0.0.1:9097",
                "--webhook-auth",
                "token",
            ],
            &[
                "--webhook-listen",
                "127.0.0.1:9097",
                "--webhook-auth",
                "mesh",
                "--webhook-token-file",
                "/t",
            ],
        ] {
            assert!(settings(bad).is_err(), "{bad:?}");
        }
        assert!(parse(&["--webhook-auth", "none"]).is_err());
        assert!(parse(&["--webhook-listen", "nope", "--webhook-auth", "mesh"]).is_err());
    }

    #[test]
    fn the_webhook_minimum_interval_defaults_to_ten_seconds_and_must_be_positive() {
        assert_eq!(
            parse(&[]).unwrap().webhook_min_interval,
            Duration::from_secs(10)
        );
        assert!(parse(&["--webhook-min-interval", "0s"]).is_err());
        assert_eq!(
            parse(&["--webhook-min-interval", "30s"])
                .unwrap()
                .webhook_min_interval,
            Duration::from_secs(30)
        );
    }
    #[test]
    fn the_keda_scaler_listens_on_its_own_address() {
        assert_eq!(
            parse(&[]).unwrap().keda_listen,
            "0.0.0.0:9095".parse::<SocketAddr>().unwrap()
        );
        let a = parse(&["--keda-listen", "127.0.0.1:50195"]).unwrap();
        assert_eq!(
            a.keda_listen,
            "127.0.0.1:50195".parse::<SocketAddr>().unwrap()
        );
        assert!(parse(&["--keda-listen", "nope"]).is_err());
    }
}

#[cfg(test)]
mod power_file_tests {
    use super::*;
    use sched_model::test_support::{EXAMPLE_CONFIG, example_yaml_managed};

    fn files(credentials: bool, key: bool) -> SecretFiles {
        SecretFiles {
            opnsense_credentials: credentials.then(|| "/secrets/opnsense".into()),
            opnsense_ca: None,
            ssh_key: key.then(|| "/secrets/ssh".into()),
        }
    }

    #[test]
    fn manual_power_needs_no_secret_files() {
        let config = Config::from_yaml(EXAMPLE_CONFIG).unwrap();
        check_power_files(&config, &files(false, false)).unwrap();
    }

    #[test]
    fn managed_power_refuses_to_start_without_the_files_its_calls_need() {
        let config = Config::from_yaml(&example_yaml_managed()).unwrap();
        check_power_files(&config, &files(true, true)).unwrap();
        let e = check_power_files(&config, &files(false, true)).unwrap_err();
        assert!(
            e.contains("--opnsense-credentials-file") && !e.contains("--ssh-key-file"),
            "{e}"
        );
        assert!(e.contains("cuda-central, basestar-nfs"), "{e}");
        let e = check_power_files(&config, &files(true, false)).unwrap_err();
        assert!(
            e.contains("--ssh-key-file") && !e.contains("--opnsense"),
            "{e}"
        );
    }

    #[test]
    fn the_secret_files_come_from_the_command_line() {
        let a = Args::try_parse_from([
            "flowmanager",
            "--config",
            "c.yaml",
            "--ssh-key-file",
            "/k",
            "--opnsense-credentials-file",
            "/c",
            "--opnsense-ca-file",
            "/ca",
        ])
        .unwrap();
        let f = secret_files(&a);
        assert_eq!(f.ssh_key.as_deref(), Some(std::path::Path::new("/k")));
        assert_eq!(
            f.opnsense_credentials.as_deref(),
            Some(std::path::Path::new("/c"))
        );
        assert_eq!(f.opnsense_ca.as_deref(), Some(std::path::Path::new("/ca")));
    }
}
