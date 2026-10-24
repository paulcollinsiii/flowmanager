//! The Kubernetes observer against a fake plain-HTTP API server: a real `kube::Client`
//! doing list and watch round trips, with failures injected into the watch stream.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sched_model::test_support::{EXAMPLE_CONFIG, example_config};
use sched_model::{Config, KubeWatchParams, Millis, ObservedValue, SourceSpec};
use sched_observe::kube_watch::{
    ModelWorkloads, Timing, model_workloads, run_with_client, run_with_factory,
};
use sched_observe::testserver::{Reply, TestServer};
use sched_observe::{Observers, Report, RestartPolicy, SpawnOptions, shared_backoff, supervised};
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
    /// The next node watch answers with a garbage line and ends.
    garbage_once_nodes: Arc<AtomicBool>,
    /// Every pod watch answers with a garbage line and ends.
    garbage_always: Arc<AtomicBool>,
    /// Pod watches 1-3 fail at once, the 4th lives 450 ms and then fails, later ones hang.
    scripted: Arc<AtomicBool>,
}

impl Fake {
    async fn start() -> Fake {
        let server = TestServer::start().await;
        let watches = Arc::new(AtomicUsize::new(0));
        let pods = Arc::new(Mutex::new(vec![pod("mineru-1", "mineru", true)]));
        let garbage_once = Arc::new(AtomicBool::new(false));
        let garbage_once_nodes = Arc::new(AtomicBool::new(false));
        let garbage_always = Arc::new(AtomicBool::new(false));
        let scripted = Arc::new(AtomicBool::new(false));
        let watch_calls = Arc::new(AtomicUsize::new(0));
        let (w, p, g, ga) = (
            watches.clone(),
            pods.clone(),
            garbage_once.clone(),
            garbage_always.clone(),
        );
        let (sc, wc) = (scripted.clone(), watch_calls.clone());
        server.set_dynamic("/api/v1/namespaces/ai/pods", move |query| {
            if query.contains("watch=true") {
                if sc.load(Ordering::SeqCst) {
                    return match wc.fetch_add(1, Ordering::SeqCst) {
                        0..=2 => Reply::json("this is not json\n"),
                        3 => Reply::json("this is not json\n").delayed(Duration::from_millis(450)),
                        _ => Reply::hanging("").tracked(&w),
                    };
                }
                if g.swap(false, Ordering::SeqCst) || ga.load(Ordering::SeqCst) {
                    return Reply::json("this is not json\n");
                }
                return Reply::hanging("").tracked(&w);
            }
            let items = p.lock().unwrap().join(",");
            Reply::json(&format!(
                r#"{{"apiVersion":"v1","kind":"PodList","metadata":{{"resourceVersion":"10"}},"items":[{items}]}}"#
            ))
        });
        let (w, gn) = (watches.clone(), garbage_once_nodes.clone());
        server.set_dynamic("/api/v1/nodes", move |query| {
            if query.contains("watch=true") {
                if gn.swap(false, Ordering::SeqCst) {
                    return Reply::json("this is not json\n");
                }
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
            garbage_once_nodes,
            garbage_always,
            scripted,
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

fn source() -> (SourceSpec, KubeWatchParams, Vec<ModelWorkloads>) {
    let cfg = example_config();
    let mut spec = cfg.source(&"cluster-pods".into()).unwrap().clone();
    spec.resync = Millis(50);
    let mut params = spec.kube_watch_params().unwrap().unwrap();
    params.namespaces = vec!["ai".to_string()];
    params.reachability_confirmations = 1;
    (spec, params, model_workloads(cfg.models()))
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
        shared_backoff(RestartPolicy {
            initial: Duration::from_millis(10),
            max: Duration::from_millis(10),
            healthy_after: Duration::from_secs(60),
        }),
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
        restart: RestartPolicy {
            initial: Duration::from_millis(50),
            max: Duration::from_millis(200),
            healthy_after: Duration::from_secs(60),
        },
        kube_timing: FAST,
        now: None,
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

const SLOW_RETRY: Timing = Timing {
    backoff_initial: Duration::from_millis(700),
    backoff_max: Duration::from_millis(700),
    client_retry: Duration::from_millis(20),
};

/// Everything received over `window`.
async fn collect(rx: &mut mpsc::Receiver<Report>, window: Duration) -> Vec<Report> {
    let mut out = Vec::new();
    let _ = tokio::time::timeout(window, async {
        while let Some(r) = rx.recv().await {
            out.push(r);
        }
    })
    .await;
    out
}

#[tokio::test]
async fn nothing_is_reported_from_a_store_after_its_watch_failed_until_it_has_relisted() {
    let (spec, params, models) = source();
    // Pods.
    let fake = Fake::start().await;
    fake.garbage_once.store(true, Ordering::SeqCst);
    let (tx, mut rx) = mpsc::channel(512);
    let task = tokio::spawn(run_with_client(
        spec.clone(),
        params.clone(),
        models.clone(),
        fake.client(),
        tx,
        SLOW_RETRY,
    ));
    until(&mut rx, |r| pods_of(r, "mineru")).await;
    until(&mut rx, |r| {
        matches!(r, Report::Failed { error, .. } if error.contains("pod")).then_some(())
    })
    .await;
    // During the backoff the resync ticks (every 50ms) must not report the stale store.
    let during = collect(&mut rx, Duration::from_millis(450)).await;
    assert!(
        during.iter().all(|r| pods_of(r, "mineru").is_none()),
        "pod counts were reported from a store whose watch had failed: {during:?}"
    );
    // After the relist they resume.
    until(&mut rx, |r| pods_of(r, "mineru")).await;
    task.abort();

    // Nodes.
    let fake = Fake::start().await;
    fake.garbage_once_nodes.store(true, Ordering::SeqCst);
    let (tx, mut rx) = mpsc::channel(512);
    let task = tokio::spawn(run_with_client(
        spec,
        params,
        models,
        fake.client(),
        tx,
        SLOW_RETRY,
    ));
    until(&mut rx, |r| pods_of(r, "mineru")).await;
    until(&mut rx, |r| {
        matches!(r, Report::Failed { error, .. } if error.contains("node")).then_some(())
    })
    .await;
    let during = collect(&mut rx, Duration::from_millis(450)).await;
    assert!(
        during.iter().all(|r| up_of(r).is_none()),
        "reachability was reported from a node store whose watch had failed: {during:?}"
    );
    assert!(until(&mut rx, up_of).await);
    task.abort();
}

#[tokio::test]
async fn the_kube_observer_restarts_only_for_changes_that_affect_it() {
    let fake = Fake::start().await;
    let (tx, mut rx) = mpsc::channel(4096);
    tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let opts = SpawnOptions {
        kube_client: Some(fake.client()),
        kube_timing: FAST,
        ..SpawnOptions::default()
    };
    let base = EXAMPLE_CONFIG.replace("namespaces: [ai, media, docs]", "namespaces: [ai]");
    let mut observers = Observers::default();
    observers.update(&Config::from_yaml(&base).unwrap(), &tx, &opts);

    // Not used by the observer: a model's load time, a class's priority, policy.
    let unrelated = base
        .replace(
            "load_time: 120s, workloads: [mineru]",
            "load_time: 130s, workloads: [mineru]",
        )
        .replace("priority: 50", "priority: 51");
    let started = observers.update(&Config::from_yaml(&unrelated).unwrap(), &tx, &opts);
    assert!(
        !started.iter().any(|s| s.as_str() == "cluster-pods"),
        "{started:?}"
    );

    // Used: which workloads make up a model; the namespaces watched; the confirmations.
    for changed in [
        base.replace("workloads: [mineru]", "workloads: [mineru, mineru-b]"),
        base.replace("namespaces: [ai]", "namespaces: [ai, docs]"),
        base.replace(
            "reachability_confirmations: 3",
            "reachability_confirmations: 5",
        ),
    ] {
        let started = observers.update(&Config::from_yaml(&changed).unwrap(), &tx, &opts);
        assert!(
            started.iter().any(|s| s.as_str() == "cluster-pods"),
            "{changed}"
        );
        // Back to the base, which is also a change for the observer.
        observers.update(&Config::from_yaml(&base).unwrap(), &tx, &opts);
    }
}

#[tokio::test]
async fn restarts_wait_the_configured_delay_and_the_delay_grows() {
    let (tx, mut rx) = mpsc::channel(16);
    let starts = Arc::new(Mutex::new(Vec::new()));
    let s = starts.clone();
    let task = tokio::spawn(supervised(
        "s".into(),
        tx,
        shared_backoff(RestartPolicy {
            initial: Duration::from_millis(150),
            max: Duration::from_secs(5),
            healthy_after: Duration::from_secs(60),
        }),
        move || {
            s.lock().unwrap().push(std::time::Instant::now());
            async move { panic!("always") }
        },
    ));
    for _ in 0..3 {
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
    }
    let t = starts.lock().unwrap().clone();
    assert!(t.len() >= 3);
    let (g1, g2) = (t[1] - t[0], t[2] - t[1]);
    assert!(
        g1 >= Duration::from_millis(140),
        "first restart came after {g1:?}"
    );
    assert!(
        g2 >= Duration::from_millis(290),
        "second restart came after {g2:?}, expected about double"
    );
    task.abort();
}

fn gaps(times: &[std::time::Instant]) -> Vec<Duration> {
    times.windows(2).map(|w| w[1] - w[0]).collect()
}

#[tokio::test]
async fn a_watch_that_keeps_failing_is_retried_with_a_growing_wait() {
    let fake = Fake::start().await;
    fake.garbage_always.store(true, Ordering::SeqCst);
    let (spec, params, models) = source();
    let (tx, mut rx) = mpsc::channel(4096);
    tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let timing = Timing {
        backoff_initial: Duration::from_millis(100),
        backoff_max: Duration::from_secs(2),
        client_retry: Duration::from_millis(20),
    };
    let task = tokio::spawn(run_with_client(
        spec,
        params,
        models,
        fake.client(),
        tx,
        timing,
    ));
    // One list per incarnation of the watcher.
    wait_for("four relists", || {
        fake.server
            .requests()
            .iter()
            .filter(|(k, _)| {
                k.starts_with("/api/v1/namespaces/ai/pods?") && !k.contains("watch=true")
            })
            .count()
            >= 4
    })
    .await;
    let times: Vec<_> = fake
        .server
        .requests()
        .iter()
        .zip(fake.server.request_times())
        .filter(|((k, _), _)| {
            k.starts_with("/api/v1/namespaces/ai/pods?") && !k.contains("watch=true")
        })
        .map(|(_, t)| t)
        .collect();
    let g = gaps(&times);
    // Waits of about 100, 200, 400 ms between lists: the third gap is well past the first.
    assert!(g[0] >= Duration::from_millis(90), "{g:?}");
    assert!(
        g[2] >= Duration::from_millis(350),
        "waits did not grow: {g:?}"
    );
    task.abort();
}

#[tokio::test]
async fn client_creation_is_retried_with_a_growing_wait_and_then_proceeds() {
    let fake = Fake::start().await;
    let (spec, params, models) = source();
    let (tx, mut rx) = mpsc::channel(4096);
    let timing = Timing {
        backoff_initial: Duration::from_millis(20),
        backoff_max: Duration::from_secs(2),
        client_retry: Duration::from_millis(60),
    };
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let (a, client) = (attempts.clone(), fake.client());
    let task = tokio::spawn(run_with_factory(
        spec,
        params,
        models,
        tx,
        timing,
        move || {
            let n = {
                let mut g = a.lock().unwrap();
                g.push(std::time::Instant::now());
                g.len()
            };
            let client = client.clone();
            async move {
                if n < 4 {
                    Err("no cluster".to_string())
                } else {
                    Ok(client)
                }
            }
        },
    ));
    // Each failure is reported, then pods flow once a client exists.
    for _ in 0..3 {
        until(&mut rx, |r| {
            matches!(r, Report::Failed { error, .. } if error.contains("cannot create"))
                .then_some(())
        })
        .await;
    }
    assert_eq!(until(&mut rx, |r| pods_of(r, "mineru")).await, (1, 1));
    let g = gaps(&attempts.lock().unwrap());
    assert!(g[0] >= Duration::from_millis(55), "{g:?}");
    assert!(
        g[2] >= Duration::from_millis(220),
        "waits did not grow: {g:?}"
    );
    task.abort();
}

#[tokio::test]
async fn restart_throttling_survives_a_restart_caused_by_a_reload() {
    let fake = Fake::start().await;
    let (tx, mut rx) = mpsc::channel(4096);
    tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let opts = SpawnOptions {
        kube_client: Some(fake.client()),
        restart: RestartPolicy {
            initial: Duration::from_millis(50),
            max: Duration::from_secs(10),
            healthy_after: Duration::from_secs(60),
        },
        kube_timing: FAST,
        now: None,
    };
    let id = sched_model::SourceId::new("cluster-pods");
    let mut observers = Observers::default();
    observers.update(&config_with_one_namespace("l0"), &tx, &opts);
    let before = observers
        .backoff_for(&id)
        .expect("a backoff is kept per source");
    // The observer crash-loops: two quick deaths so far.
    let quick = Duration::from_millis(1);
    assert_eq!(
        before.lock().unwrap().after_exit(quick),
        Duration::from_millis(50)
    );
    assert_eq!(
        before.lock().unwrap().after_exit(quick),
        Duration::from_millis(100)
    );

    // A reload that restarts the observer must not hand it a fresh, unthrottled backoff.
    let started = observers.update(&config_with_one_namespace("l1"), &tx, &opts);
    assert!(started.iter().any(|s| s == &id));
    let after = observers.backoff_for(&id).unwrap();
    assert!(Arc::ptr_eq(&before, &after));
    assert_eq!(
        after.lock().unwrap().after_exit(quick),
        Duration::from_millis(200)
    );
}

#[tokio::test]
async fn the_watch_backoff_starts_over_only_after_a_healthy_run() {
    let fake = Fake::start().await;
    fake.scripted.store(true, Ordering::SeqCst);
    let (spec, params, models) = source();
    let (tx, mut rx) = mpsc::channel(4096);
    tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let timing = Timing {
        backoff_initial: Duration::from_millis(100),
        backoff_max: Duration::from_millis(400),
        client_retry: Duration::from_millis(20),
    };
    let task = tokio::spawn(run_with_client(
        spec,
        params,
        models,
        fake.client(),
        tx,
        timing,
    ));
    let lists = || -> Vec<std::time::Instant> {
        fake.server
            .requests()
            .iter()
            .zip(fake.server.request_times())
            .filter(|((k, _), _)| {
                k.starts_with("/api/v1/namespaces/ai/pods?") && !k.contains("watch=true")
            })
            .map(|(_, t)| t)
            .collect()
    };
    wait_for("five lists", || lists().len() >= 5).await;
    let l = lists();
    // Lists 1-3 fail at once (waits of 100, 200, 400 ms). The 4th watcher lives 450 ms, past
    // backoff_max, then fails: that was a healthy run, so the wait before list 5 starts over
    // at 100 ms (about 550 ms after list 4, not 850).
    let after_healthy = l[4] - l[3];
    assert!(
        after_healthy >= Duration::from_millis(450),
        "{after_healthy:?}"
    );
    assert!(
        after_healthy < Duration::from_millis(750),
        "the backoff was not reset after a healthy run: {after_healthy:?}"
    );
    task.abort();
}

#[tokio::test]
async fn backoff_state_of_a_source_removed_from_the_config_is_dropped() {
    let fake = Fake::start().await;
    let (tx, mut rx) = mpsc::channel(4096);
    tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let opts = SpawnOptions {
        kube_client: Some(fake.client()),
        kube_timing: FAST,
        ..SpawnOptions::default()
    };
    let base = EXAMPLE_CONFIG.replace("namespaces: [ai, media, docs]", "namespaces: [ai]");
    let mut observers = Observers::default();
    observers.update(&Config::from_yaml(&base).unwrap(), &tx, &opts);
    let immich = sched_model::SourceId::new("immich-queues");
    assert!(observers.backoff_for(&immich).is_some());

    // Remove the source (and the class that took its demand from it).
    let start = base.find("  - id: immich-queues\n").unwrap();
    let end = start + base[start..].find("\n\n").unwrap() + 2;
    let without = format!("{}{}", &base[..start], &base[end..])
        .replace("    demand: { source: immich-queues }\n", "");
    assert!(!without.contains("id: immich-queues"));
    assert_ne!(without, base);
    observers.update(&Config::from_yaml(&without).unwrap(), &tx, &opts);
    assert!(observers.backoff_for(&immich).is_none());
    assert!(observers.backoff_for(&"cluster-pods".into()).is_some());
}
