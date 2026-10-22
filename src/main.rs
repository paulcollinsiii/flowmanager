use clap::{CommandFactory, Parser};
use twelf::{config, Layer};
use restate_sdk::endpoint::Endpoint;
use restate_sdk::http_server::HttpServer;


mod power;


#[config]
#[derive(Parser, Debug, Default)]
#[clap(author, version, about, long_about = None)]
struct Config {
    #[clap(long)]
    restate_key: String,
    #[clap(long, short, default_value_t = "127.0.0.1".to_string())]
    listen: String,
    
    #[clap(long, short, default_value_t = "9080".to_string())]
    port: String,

    #[clap(long, short)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = Config::command().get_matches();
    let config =
        Config::with_layers(&[Layer::Env(Some(String::from("APP_"))), Layer::Clap(matches)])
            .unwrap();

    println!("config - {:?}", config);
    HttpServer::new(
        Endpoint::builder()
            .bind(power::PowerWorkflow)
            .build(),
    )
    .listen_and_serve(format!("{}:{}", config.listen, config.port).parse().unwrap())
    .await;

    Ok(())
}