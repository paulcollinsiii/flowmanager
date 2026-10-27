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
use sched_service::clock::SystemClock;
use sched_service::config_source::{ConfigHolder, watch_file};
use sched_service::metrics::Metrics;
use sched_service::scheduler::{Power, Recovery, Scheduler};
use sched_service::tick::Shared;
use sched_service::{keda_scaler, metrics_server, recovery, runtime};

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
    tokio::spawn(runtime::supervise_observers(Arc::clone(&shared), tx));
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
        let mut argv = vec!["sched-service", "--config", "c.yaml"];
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
            "sched-service",
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
