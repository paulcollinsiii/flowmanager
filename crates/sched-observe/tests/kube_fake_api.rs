//! The Kubernetes observer against a fake plain-HTTP API server: a real `kube::Client`
//! doing list and watch round trips, with failures injected into the watch stream.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sched_model::test_support::{EXAMPLE_CONFIG, example_config};
use sched_model::{Config, KubeWatchParams, Millis, ObservedValue, SourceSpec};
use sched_observe::kube_watch::{Timing, run_with_client};
use sched_observe::testserver::{Reply, TestServer};
use sched_observe::{Observers, Report, SpawnOptions, supervised};
use tokio::sync::mpsc;

fn pod(name: &str, workload: &str, ready: bool) -> String {
    format!(
        r#"{{"apiVersion":"v1","kind":"Pod","metadata":{{"name":"{name}","namespace":"ai","uid":"{name}","resourceVersion":"5","labels":{{"app.kubernetes.io/name":"{workload}"}}}},"status":{{"phase":"Running","conditions":[{{"type":"Ready","status":"{}"}}]}}}}"#,
        if ready { "True" } else { "False" }
    )
}

struct Fake {
    server: TestServer,
    /// Open watch responses.
    watches: Arc<AtomicUsize>,
    pods: Arc<Mutex<Vec<String>>>,
    /// The next pod watch answers with a garbage line and ends.
    garbage_once: Arc<AtomicBool>,
}

impl Fake {
    async fn start() -> Fake {
        let server = TestServer::start().await;
        let watches = Arc::new(AtomicUsize::new(0));
        let pods = Arc::new(Mutex::new(vec![pod("mineru-1", "mineru", true)]));
        let garbage_once = Arc::new(AtomicBool::new(false));
        let (w, p, g) = (watches.clone(), pods.clone(), garbage_once.clone());
        server.set_dynamic("/api/v1/namespaces/ai/pods", move |query| {
            if query.contains("watch=true") {
                if g.swap(false, Ordering::SeqCst) {
                    return Reply::json("this is not json\n");
                }
                return Reply::hanging("").tracked(&w);
            }
            let items = p.lock().unwrap().join(",");
            Reply::json(&format!(
                r#"{{"apiVersion":"v1","kind":"PodList","metadata":{{"resourceVersion":"10"}},"items":[{items}]}}"#
            ))
        });
        let w = watches.clone();
        server.set_dynamic("/api/v1/nodes", move |query| {
            if query.contains("watch=true") {
                return Reply::hanging("").tracked(&w);
            }
            let status = "True";
            Reply::json(&format!(
                r#"{{"apiVersion":"v1","kind":"NodeList","metadata":{{"resourceVersion":"10"}},"items":[{{"apiVersion":"v1","kind":"Node","metadata":{{"name":"cuda-central","uid":"n1","resourceVersion":"4"}},"status":{{"conditions":[{{"type":"Ready","status":"{status}"}}]}}}}]}}"#
            ))
        });
        Fake {
            server,
            watches,
            pods,
            garbage_once,
        }
    }

    /// A real client talking plain HTTP to the fake. Building it also builds the TLS
    /// machinery, which panics if more than one crypto provider is compiled in.
    fn client(&self) -> kube::Client {
        let config = kube::Config::new(self.server.url("").parse().unwrap());
        kube::Client::try_from(config).expect("client builds")
    }
}

const FAST: Timing = Timing {
    backoff_initial: Duration::from_millis(20),
    backoff_max: Duration::from_millis(100),
    client_retry: Duration::from_millis(20),
};

fn source() -> (SourceSpec, KubeWatchParams, Vec<sched_model::ModelSpec>) {
    let cfg = example_config();
    let mut spec = cfg.source(&"cluster-pods".into()).unwrap().clone();
    spec.resync = Millis(50);
    let mut params = spec.kube_watch_params().unwrap().unwrap();
    params.namespaces = vec!["ai".to_string()];
    params.reachability_confirmations = 1;
    (spec, params, cfg.models().to_vec())
}

fn config_with_one_namespace(label: &str) -> Config {
    let y = EXAMPLE_CONFIG
        .replace("namespaces: [ai, media, docs]", "namespaces: [ai]")
        .replace(
            "workload_label: app.kubernetes.io/name",
            &format!("workload_label: {label}"),
        );
    Config::from_yaml(&y).unwrap()
}

fn pods_of(report: &Report, model: &str) -> Option<(u32, u32)> {
    match report {
        Report::Observed { values, .. } => values.iter().find_map(|v| match v {
            ObservedValue::ModelPods {
                model: m,
                pods,
                ready_pods,
            } if m.as_str() == model => Some((*pods, *ready_pods)),
            _ => None,
        }),
        _ => None,
    }
}

fn up_of(report: &Report) -> Option<bool> {
    match report {
        Report::Observed { values, .. } => values.iter().find_map(|v| match v {
            ObservedValue::ResourceUp { up, .. } => Some(*up),
            _ => None,
        }),
        _ => None,
    }
}

async fn until<T>(rx: &mut mpsc::Receiver<Report>, mut f: impl FnMut(&Report) -> Option<T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let r = rx.recv().await.expect("observer ended");
            if let Some(t) = f(&r) {
                return t;
            }
        }
    })
    .await
    .expect("timed out waiting for a matching report")
}

async fn wait_for(what: &str, mut f: impl FnMut() -> bool) {
    for _ in 0..400 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
async fn a_real_client_lists_and_watches_a_fake_api_and_reports_pods_and_node_readiness() {
    let fake = Fake::start().await;
    let (spec, params, models) = source();
    let (tx, mut rx) = mpsc::channel(64);
    let task = tokio::spawn(run_with_client(
        spec,
        params,
        models,
        fake.client(),
        tx,
        FAST,
    ));

    assert_eq!(until(&mut rx, |r| pods_of(r, "mineru")).await, (1, 1));
    assert_eq!(until(&mut rx, |r| pods_of(r, "llm")).await, (0, 0));
    assert!(until(&mut rx, up_of).await);
    assert!(
        fake.server.count("/api/v1/namespaces/ai/pods") >= 2,
        "list then watch"
    );
    wait_for("both watches open", || {
        fake.watches.load(Ordering::SeqCst) == 2
    })
    .await;

    // (The fake's node list is read once per list, so the reported state only changes
    // after a relist; the debounce itself is covered by unit tests.)
    task.abort();
}

#[tokio::test]
async fn after_a_watch_error_the_observer_relists_and_reports_resume() {
    let fake = Fake::start().await;
    let (spec, params, models) = source();
    let (tx, mut rx) = mpsc::channel(256);
    fake.garbage_once.store(true, Ordering::SeqCst);
    let lists_before = || {
        fake.server
            .requests()
            .iter()
            .filter(|(k, _)| {
                k.starts_with("/api/v1/namespaces/ai/pods?") && !k.contains("watch=true")
            })
            .count()
    };
    let task = tokio::spawn(run_with_client(
        spec,
        params,
        models,
        fake.client(),
        tx,
        FAST,
    ));

    // The garbage line is reported as a failure...
    until(&mut rx, |r| {
        matches!(r, Report::Failed { error, .. } if error.contains("pod")).then_some(())
    })
    .await;
    // ...and a second, different world is picked up by the relist that follows.
    *fake.pods.lock().unwrap() = vec![
        pod("mineru-1", "mineru", true),
        pod("mineru-2", "mineru", false),
    ];
    assert_eq!(
        until(&mut rx, |r| pods_of(r, "mineru").filter(|p| *p == (2, 1))).await,
        (2, 1)
    );
    assert!(lists_before() >= 2, "a fresh list was made after the error");
    task.abort();
}

#[tokio::test]
async fn a_panicking_or_ending_observer_is_reported_and_restarted() {
    let (tx, mut rx) = mpsc::channel(16);
    let runs = Arc::new(AtomicUsize::new(0));
    let r = runs.clone();
    let task = tokio::spawn(supervised(
        "s".into(),
        tx.clone(),
        Duration::from_millis(10),
        move || {
            let n = r.fetch_add(1, Ordering::SeqCst);
            async move {
                if n == 0 {
                    panic!("Could not automatically determine the process-level CryptoProvider");
                }
                if n == 1 {
                    return; // ends on its own
                }
                std::future::pending::<()>().await;
            }
        },
    ));
    let first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the panic must be reported")
        .unwrap();
    assert!(
        matches!(&first, Report::Failed { source, error } if source.as_str() == "s" && error.contains("panicked") && error.contains("CryptoProvider")),
        "{first:?}"
    );
    let second = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the early exit must be reported")
        .unwrap();
    assert!(
        matches!(&second, Report::Failed { error, .. } if error.contains("stopped")),
        "{second:?}"
    );
    wait_for("third start", || runs.load(Ordering::SeqCst) == 3).await;
    task.abort();
}

#[tokio::test]
async fn reloads_do_not_leak_watches_and_unrelated_reloads_keep_observers() {
    let fake = Fake::start().await;
    let (tx, mut rx) = mpsc::channel(4096);
    tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let opts = SpawnOptions {
        kube_client: Some(fake.client()),
        restart_delay: Duration::from_millis(50),
        kube_timing: FAST,
    };
    let mut observers = Observers::default();
    let first = observers.update(
        &config_with_one_namespace("app.kubernetes.io/name"),
        &tx,
        &opts,
    );
    assert!(first.iter().any(|s| s.as_str() == "cluster-pods"));
    wait_for("both watches open", || {
        fake.watches.load(Ordering::SeqCst) == 2
    })
    .await;

    // A reload that changes nothing an observer depends on restarts none of them, so
    // state such as reachability debounce counts survives it.
    let same = Config::from_yaml(
        &EXAMPLE_CONFIG
            .replace("namespaces: [ai, media, docs]", "namespaces: [ai]")
            .replace("idle_grace: 10m", "idle_grace: 20m"),
    )
    .unwrap();
    assert!(observers.update(&same, &tx, &opts).is_empty());
    assert_eq!(fake.watches.load(Ordering::SeqCst), 2);

    // Reloads that do change the Kubernetes source restart only it, and the old watches
    // end rather than piling up.
    let mut alive = Vec::new();
    for i in 1..=5 {
        let started = observers.update(&config_with_one_namespace(&format!("l{i}")), &tx, &opts);
        assert_eq!(
            started.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            vec!["cluster-pods"]
        );
        wait_for("old watches gone and new ones open", || {
            fake.watches.load(Ordering::SeqCst) == 2
        })
        .await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(fake.watches.load(Ordering::SeqCst), 2, "reload {i}");
        alive.push(
            tokio::runtime::Handle::current()
                .metrics()
                .num_alive_tasks(),
        );
    }
    assert!(
        alive.windows(2).all(|w| w[1] <= w[0]),
        "live tasks grew across reloads: {alive:?}"
    );

    drop(observers);
    wait_for("all watches closed after drop", || {
        fake.watches.load(Ordering::SeqCst) == 0
    })
    .await;
}
