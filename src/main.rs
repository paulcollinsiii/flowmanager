use clap::{CommandFactory, Parser};
use twelf::{config, Layer};
use restate_sdk::endpoint::Endpoint;
use restate_sdk::http_server::HttpServer;


mod cluster;
mod opnsense;
mod power;


#[config]
#[derive(Parser, Debug, Default)]
#[clap(author, version, about, long_about = None)]
struct Config {
    #[clap(long)]
    restate_key: Option<String>,
    #[clap(long, short, default_value_t = "127.0.0.1".to_string())]
    listen: String,
    
    #[clap(long, short, default_value_t = "9080".to_string())]
    port: String,

    #[clap(long, short)]
    verbose: bool,

    /// How long to wait for a woken server to answer before giving up.
    #[clap(long, default_value_t = 1200)]
    wake_timeout_secs: u64,

    /// Minimum time between power transitions of one server; requests inside it fail fast.
    #[clap(long, default_value_t = 900)]
    power_cooldown_secs: u64,

    /// Private key for the forced-command shutdown user (mounted from a Secret).
    #[clap(long, default_value_t = "/etc/flowmanager/shutdown_key".to_string())]
    shutdown_key_path: String,

    /// OPNsense router whose WoL plugin sends the magic packets. Must be https.
    #[clap(long, default_value_t = "https://router.kaffi.internal".to_string())]
    opnsense_url: String,

    /// File holding `key:secret` for an OPNsense API user (mounted from a Secret).
    #[clap(long, default_value_t = "/etc/flowmanager/opnsense_credentials".to_string())]
    opnsense_credentials_path: String,

    /// PEM bundle of the CA that signed the router's certificate. If unset, the platform roots are used.
    #[clap(long)]
    opnsense_ca_path: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = Config::command().get_matches();
    let config =
        Config::with_layers(&[Layer::Env(Some(String::from("APP_"))), Layer::Clap(matches)])
            .unwrap();

    tracing_subscriber::fmt::init();
    println!("config - {:?}", config);
    let kube = kube::Client::try_default().await?;
    let opnsense = opnsense::Opnsense::new(
        &config.opnsense_url,
        config.opnsense_credentials_path.into(),
        config.opnsense_ca_path.as_deref().map(std::path::Path::new),
    )?;
    HttpServer::new(
        Endpoint::builder()
            .bind(power::PowerObject::new(
                kube,
                std::time::Duration::from_secs(config.wake_timeout_secs),
                std::time::Duration::from_secs(config.power_cooldown_secs),
                config.shutdown_key_path.into(),
                opnsense,
            ))
            .build(),
    )
    .listen_and_serve(format!("{}:{}", config.listen, config.port).parse().unwrap())
    .await;

    Ok(())
}