//! The flowmanager scheduler binary, in shadow mode: it hosts the scheduler as a Restate
//! virtual object, runs the observers, serves metrics, and logs what it would do. It has no
//! actuator and changes nothing in the cluster, on any host or in any service.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use restate_sdk::endpoint::Endpoint;
use restate_sdk::http_server::HttpServer;
use sched_model::Millis;
use sched_service::clock::SystemClock;
use sched_service::config_source::{ConfigHolder, watch_file};
use sched_service::metrics::Metrics;
use sched_service::scheduler::Scheduler;
use sched_service::tick::Shared;
use sched_service::{metrics_server, runtime};

fn duration(text: &str) -> Result<Duration, String> {
    Millis::parse(text).map(|m| Duration::from_millis(m.as_u64()))
}

#[derive(Parser, Debug)]
#[command(
    version,
    about = "GPU scheduler (shadow mode: observes and logs, never acts)"
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
    /// Restate ingress, used to ask for a tick when observations change and to arm the backstop.
    #[arg(
        long,
        env = "SCHED_RESTATE_INGRESS",
        default_value = "http://localhost:8080"
    )]
    restate_ingress: String,
    /// Interval of the periodic backstop tick.
    #[arg(long, env = "SCHED_BACKSTOP", default_value = "30s", value_parser = duration)]
    backstop: Duration,
    /// How often the config file is checked for changes.
    #[arg(long, env = "SCHED_RELOAD_INTERVAL", default_value = "5s", value_parser = duration)]
    reload_interval: Duration,
    /// Delay that merges bursts of changes into one requested tick.
    #[arg(long, env = "SCHED_TRIGGER_DEBOUNCE", default_value = "250ms", value_parser = duration)]
    trigger_debounce: Duration,
    /// Log level: error, warn, info, debug or trace.
    #[arg(long, env = "SCHED_LOG", default_value = "info")]
    log_level: tracing::Level,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_max_level(args.log_level)
        .init();

    let holder = Arc::new(ConfigHolder::load(&args.config)?);
    let metrics = Arc::new(Metrics::new());
    let shared = Arc::new(Shared::new(
        Arc::clone(&holder),
        Arc::clone(&metrics),
        Arc::new(SystemClock),
    ));
    tracing::info!(
        generation = holder.current().generation(),
        path = %args.config.display(),
        "shadow mode: observing and logging decisions; nothing will be executed"
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
    tokio::spawn(runtime::arm_backstop(
        args.restate_ingress.clone(),
        Duration::from_secs(5),
    ));
    let listener = metrics_server::bind(args.metrics_listen).await?;
    tracing::info!(addr = %args.metrics_listen, "serving /metrics");
    tokio::spawn(metrics_server::serve(listener, metrics));

    tracing::info!(addr = %args.listen, "serving the Restate endpoint");
    HttpServer::new(
        Endpoint::builder()
            .bind(Scheduler::new(shared, args.backstop))
            .build(),
    )
    .listen_and_serve(args.listen)
    .await;
    Ok(())
}
