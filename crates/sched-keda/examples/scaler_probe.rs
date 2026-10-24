//! Calls a running scaler the way KEDA does, and prints what it answers.
//!
//! ```sh
//! cargo run -p sched-keda --example scaler_probe -- 127.0.0.1:9095 model=mineru
//! cargo run -p sched-keda --example scaler_probe -- 127.0.0.1:9095 --watch 30 class=paperless-ngx
//! cargo run -p sched-keda --example scaler_probe -- 127.0.0.1:9095 --stream model=mineru
//! ```
//!
//! Each `key=value` becomes trigger metadata. Without options it calls `IsActive`,
//! `GetMetricSpec` and `GetMetrics` once; `--watch <secs>` repeats that every second for
//! that long, reconnecting as needed; `--stream` prints each `StreamIsActive` message
//! until the stream ends.

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sched_keda::proto::external_scaler_client::ExternalScalerClient;
use sched_keda::proto::{GetMetricsRequest, ScaledObjectRef};

fn now() -> String {
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}.{:03}", t.as_secs(), t.subsec_millis())
}

fn status(s: &tonic::Status) -> String {
    format!("ERROR {:?}: {}", s.code(), s.message())
}

struct Args {
    addr: String,
    watch: Option<u64>,
    stream: bool,
    metadata: HashMap<String, String>,
}

fn parse() -> Result<Args, String> {
    let mut argv = std::env::args().skip(1);
    let addr = argv
        .next()
        .ok_or("usage: scaler_probe <addr> [--watch <secs>] [--stream] key=value...")?;
    let mut args = Args {
        addr,
        watch: None,
        stream: false,
        metadata: HashMap::new(),
    };
    while let Some(a) = argv.next() {
        match a.as_str() {
            "--stream" => args.stream = true,
            "--watch" => {
                let secs = argv.next().ok_or("--watch needs a number of seconds")?;
                args.watch = Some(secs.parse().map_err(|e| format!("--watch {secs}: {e}"))?);
            }
            kv => {
                let (k, v) = kv
                    .split_once('=')
                    .ok_or(format!("{kv:?} is not key=value"))?;
                args.metadata.insert(k.to_string(), v.to_string());
            }
        }
    }
    Ok(args)
}

/// One round of the unary calls, as one line.
async fn round(addr: &str, r: &ScaledObjectRef) -> String {
    let mut client = match ExternalScalerClient::connect(format!("http://{addr}")).await {
        Ok(c) => c,
        Err(e) => return format!("cannot connect: {e}"),
    };
    let active = match client.is_active(r.clone()).await {
        Ok(a) => format!("IsActive={}", a.into_inner().result),
        Err(s) => format!("IsActive {}", status(&s)),
    };
    let (spec, metric) = match client.get_metric_spec(r.clone()).await {
        Ok(s) => {
            let specs = s.into_inner().metric_specs;
            let text = specs
                .iter()
                .map(|m| {
                    format!(
                        "{} target={}/{}",
                        m.metric_name, m.target_size, m.target_size_float
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            (
                format!("GetMetricSpec=[{text}]"),
                specs.first().map(|m| m.metric_name.clone()),
            )
        }
        Err(s) => (format!("GetMetricSpec {}", status(&s)), None),
    };
    let metrics = match client
        .get_metrics(GetMetricsRequest {
            scaled_object_ref: Some(r.clone()),
            metric_name: metric.unwrap_or_default(),
        })
        .await
    {
        Ok(m) => {
            let text = m
                .into_inner()
                .metric_values
                .iter()
                .map(|v| {
                    format!(
                        "{}={}/{}",
                        v.metric_name, v.metric_value, v.metric_value_float
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("GetMetrics=[{text}]")
        }
        Err(s) => format!("GetMetrics {}", status(&s)),
    };
    format!("{active} | {spec} | {metrics}")
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let args = parse()?;
    let r = ScaledObjectRef {
        name: "scaler-probe".to_string(),
        namespace: "default".to_string(),
        scaler_metadata: args.metadata.clone(),
    };
    if args.stream {
        let mut client = ExternalScalerClient::connect(format!("http://{}", args.addr))
            .await
            .map_err(|e| format!("cannot connect: {e}"))?;
        let mut stream = match client.stream_is_active(r).await {
            Ok(s) => s.into_inner(),
            Err(s) => {
                println!("{} StreamIsActive {}", now(), status(&s));
                return Ok(());
            }
        };
        loop {
            match stream.message().await {
                Ok(Some(m)) => println!("{} StreamIsActive message: {}", now(), m.result),
                Ok(None) => {
                    println!("{} StreamIsActive ended", now());
                    return Ok(());
                }
                Err(s) => {
                    println!("{} StreamIsActive {}", now(), status(&s));
                    return Ok(());
                }
            }
        }
    }
    let until = Instant::now() + Duration::from_secs(args.watch.unwrap_or(0));
    loop {
        println!("{} {}", now(), round(&args.addr, &r).await);
        if Instant::now() >= until {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
