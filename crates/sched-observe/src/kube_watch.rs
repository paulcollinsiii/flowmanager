//! The Kubernetes observer: model pod counts and node readiness from watch streams, each
//! backed by a periodic resync. Read-only: it lists and watches, nothing else.
//!
//! Pod counts are reported on every pod event and on every resync, but only once every
//! watched namespace has completed its initial list, so a half-synced cache can never read
//! as "zero pods". Node readiness is checked once per resync and debounced.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::Api;
use kube::ResourceExt;
use kube::runtime::WatchStreamExt;
use kube::runtime::watcher::{self, Event};
use sched_model::{Config, Fidelity, KubeWatchParams, ObservedValue, ResourceId, SourceSpec};

use crate::{Report, ReportTx};

/// What the observer keeps of a pod.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodInfo {
    /// Value of the workload label, if the pod has it.
    pub workload: Option<String>,
    /// Whether the pod still occupies its node: anything but finished (succeeded or failed).
    pub active: bool,
    pub ready: bool,
}

/// Reduces a pod to what is counted. Terminating pods stay active: they still hold the GPU
/// until they are gone.
pub fn pod_info(pod: &Pod, workload_label: &str) -> PodInfo {
    let status = pod.status.as_ref();
    let phase = status.and_then(|s| s.phase.as_deref()).unwrap_or("");
    let ready = status
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"));
    PodInfo {
        workload: pod.labels().get(workload_label).cloned(),
        active: !matches!(phase, "Succeeded" | "Failed"),
        ready,
    }
}

/// Whether a node's Ready condition is True.
pub fn node_ready(node: &Node) -> bool {
    node.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
}

/// The pods of one namespace as the watch stream describes them.
#[derive(Debug, Default)]
pub struct PodStore {
    pods: HashMap<String, PodInfo>,
    /// Filled during a (re)list, swapped in at `InitDone`.
    relist: Option<HashMap<String, PodInfo>>,
    synced: bool,
}

impl PodStore {
    /// Whether the initial list has completed. After a watch error the store keeps its
    /// contents but a restart re-lists and replaces them atomically.
    pub fn synced(&self) -> bool {
        self.synced
    }

    /// Marks the store unsynced, for example after a watch error: its contents may be
    /// stale, so nothing is reported from it until the next list completes.
    pub fn invalidate(&mut self) {
        self.synced = false;
    }

    pub fn apply(&mut self, event: Event<Pod>, workload_label: &str) {
        match event {
            Event::Init => self.relist = Some(HashMap::new()),
            Event::InitApply(p) => {
                if let Some(r) = &mut self.relist {
                    r.insert(p.name_any(), pod_info(&p, workload_label));
                }
            }
            Event::InitDone => {
                if let Some(r) = self.relist.take() {
                    self.pods = r;
                    self.synced = true;
                }
            }
            Event::Apply(p) => {
                self.pods.insert(p.name_any(), pod_info(&p, workload_label));
            }
            Event::Delete(p) => {
                self.pods.remove(&p.name_any());
            }
        }
    }

    /// `(active pods, ready pods)` per workload label value.
    pub fn counts(&self) -> BTreeMap<String, (u32, u32)> {
        let mut out: BTreeMap<String, (u32, u32)> = BTreeMap::new();
        for p in self.pods.values().filter(|p| p.active) {
            if let Some(w) = &p.workload {
                let e = out.entry(w.clone()).or_default();
                e.0 += 1;
                e.1 += u32::from(p.ready);
            }
        }
        out
    }
}

/// One `ModelPods` value per configured model, summing its workloads over `stores`. A
/// workload with no pods counts as zero: the lists are complete, so absence is a fact.
pub fn model_values(
    config: &Config,
    stores: &[BTreeMap<String, (u32, u32)>],
) -> Vec<ObservedValue> {
    config
        .models()
        .iter()
        .map(|m| {
            let (pods, ready_pods) = m
                .workloads
                .iter()
                .flat_map(|w| stores.iter().filter_map(move |s| s.get(w)))
                .fold((0, 0), |a, c| (a.0 + c.0, a.1 + c.1));
            ObservedValue::ModelPods {
                model: m.id.clone(),
                pods,
                ready_pods,
            }
        })
        .collect()
}

/// Requires several consecutive agreeing checks before a state flips, so one flaky
/// reading cannot change what the scheduler believes about a host. The first reading is
/// adopted as is, since there is no earlier belief to protect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Debouncer {
    confirmations: u32,
    current: Option<bool>,
    /// A differing reading and how many checks in a row have shown it.
    pending: Option<(bool, u32)>,
}

impl Debouncer {
    pub fn new(confirmations: u32) -> Self {
        Debouncer {
            confirmations: confirmations.max(1),
            current: None,
            pending: None,
        }
    }

    /// Feeds one check and returns the state to report.
    pub fn check(&mut self, reading: bool) -> bool {
        let Some(current) = self.current else {
            self.current = Some(reading);
            return reading;
        };
        if reading == current {
            self.pending = None;
            return current;
        }
        let seen = match self.pending {
            Some((r, n)) if r == reading => n + 1,
            _ => 1,
        };
        if seen >= self.confirmations {
            self.current = Some(reading);
            self.pending = None;
            reading
        } else {
            self.pending = Some((reading, seen));
            current
        }
    }
}

/// Shared by the watch tasks and the resync loop.
#[derive(Default)]
struct Shared {
    pods: BTreeMap<String, PodStore>,
    nodes: HashMap<String, bool>,
    nodes_synced: bool,
}

type SharedState = Arc<Mutex<Shared>>;

fn lock(s: &SharedState) -> std::sync::MutexGuard<'_, Shared> {
    s.lock().unwrap_or_else(|e| e.into_inner())
}

/// The pod report, or `None` while any namespace is still unsynced.
fn pod_report(
    spec: &SourceSpec,
    config: &Config,
    shared: &SharedState,
    fidelity: Fidelity,
) -> Option<Report> {
    let g = lock(shared);
    if g.pods.values().any(|s| !s.synced()) {
        return None;
    }
    let counts: Vec<_> = g.pods.values().map(PodStore::counts).collect();
    Some(Report::Observed {
        source: spec.id.clone(),
        fidelity,
        values: model_values(config, &counts),
    })
}

async fn watch_pods(
    namespace: String,
    spec: SourceSpec,
    params: KubeWatchParams,
    config: Arc<Config>,
    shared: SharedState,
    client: kube::Client,
    tx: ReportTx,
) {
    let api: Api<Pod> = Api::namespaced(client, &namespace);
    let mut stream = Box::pin(watcher::watcher(api, watcher::Config::default()).default_backoff());
    while let Some(ev) = stream.next().await {
        match ev {
            Ok(ev) => {
                lock(&shared)
                    .pods
                    .entry(namespace.clone())
                    .or_default()
                    .apply(ev, &params.workload_label);
                if let Some(r) = pod_report(&spec, &config, &shared, Fidelity::Event)
                    && tx.send(r).await.is_err()
                {
                    return;
                }
            }
            Err(e) => {
                lock(&shared)
                    .pods
                    .entry(namespace.clone())
                    .or_default()
                    .invalidate();
                let failed = Report::Failed {
                    source: spec.id.clone(),
                    error: format!("pod watch in namespace {namespace}: {e}"),
                };
                if tx.send(failed).await.is_err() {
                    return;
                }
            }
        }
    }
}

async fn watch_nodes(spec: SourceSpec, shared: SharedState, client: kube::Client, tx: ReportTx) {
    let api: Api<Node> = Api::all(client);
    let mut stream = Box::pin(watcher::watcher(api, watcher::Config::default()).default_backoff());
    let mut relist: Option<HashMap<String, bool>> = None;
    while let Some(ev) = stream.next().await {
        match ev {
            Ok(Event::Init) => relist = Some(HashMap::new()),
            Ok(Event::InitApply(n)) => {
                if let Some(r) = &mut relist {
                    r.insert(n.name_any(), node_ready(&n));
                }
            }
            Ok(Event::InitDone) => {
                if let Some(r) = relist.take() {
                    let mut g = lock(&shared);
                    g.nodes = r;
                    g.nodes_synced = true;
                }
            }
            Ok(Event::Apply(n)) => {
                lock(&shared).nodes.insert(n.name_any(), node_ready(&n));
            }
            Ok(Event::Delete(n)) => {
                lock(&shared).nodes.remove(&n.name_any());
            }
            Err(e) => {
                lock(&shared).nodes_synced = false;
                let failed = Report::Failed {
                    source: spec.id.clone(),
                    error: format!("node watch: {e}"),
                };
                if tx.send(failed).await.is_err() {
                    return;
                }
            }
        }
    }
}

/// The readiness of each mapped resource's node right now; an absent node is not ready.
/// `None` until the node list has synced.
fn reachability_readings(
    params: &KubeWatchParams,
    shared: &SharedState,
) -> Option<Vec<(ResourceId, bool)>> {
    let g = lock(shared);
    g.nodes_synced.then(|| {
        params
            .nodes
            .iter()
            .map(|(res, node)| (res.clone(), g.nodes.get(node).copied().unwrap_or(false)))
            .collect()
    })
}

/// Watches until the receiver is gone. A cluster client that cannot be built is reported
/// as a source failure and retried, so the source goes stale rather than looking healthy.
pub async fn run(spec: SourceSpec, params: KubeWatchParams, config: Arc<Config>, tx: ReportTx) {
    use sched_model::Capability;
    let resync = Duration::from_millis(spec.resync.as_u64().max(1));
    let client = loop {
        match kube::Client::try_default().await {
            Ok(c) => break c,
            Err(e) => {
                let failed = Report::Failed {
                    source: spec.id.clone(),
                    error: format!("cannot create the Kubernetes client: {e}"),
                };
                if tx.send(failed).await.is_err() {
                    return;
                }
                tokio::time::sleep(resync.max(Duration::from_secs(5))).await;
            }
        }
    };
    let shared: SharedState = Arc::default();
    let mut tasks = Vec::new();
    let observes_pods = spec.observe.contains(&Capability::Pods);
    let observes_up = spec.observe.contains(&Capability::Reachability);
    if observes_pods {
        for ns in &params.namespaces {
            lock(&shared).pods.entry(ns.clone()).or_default();
            tasks.push(tokio::spawn(watch_pods(
                ns.clone(),
                spec.clone(),
                params.clone(),
                Arc::clone(&config),
                Arc::clone(&shared),
                client.clone(),
                tx.clone(),
            )));
        }
    }
    if observes_up {
        tasks.push(tokio::spawn(watch_nodes(
            spec.clone(),
            Arc::clone(&shared),
            client.clone(),
            tx.clone(),
        )));
    }
    let mut debouncers: BTreeMap<ResourceId, Debouncer> = BTreeMap::new();
    let mut ticker = tokio::time::interval(resync);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        if observes_pods
            && let Some(r) = pod_report(&spec, &config, &shared, Fidelity::Polled)
            && tx.send(r).await.is_err()
        {
            break;
        }
        if observes_up && let Some(readings) = reachability_readings(&params, &shared) {
            let values = readings
                .into_iter()
                .map(|(resource, up)| {
                    let up = debouncers
                        .entry(resource.clone())
                        .or_insert_with(|| Debouncer::new(params.reachability_confirmations))
                        .check(up);
                    ObservedValue::ResourceUp { resource, up }
                })
                .collect();
            let report = Report::Observed {
                source: spec.id.clone(),
                fidelity: Fidelity::Polled,
                values,
            };
            if tx.send(report).await.is_err() {
                break;
            }
        }
    }
    for t in tasks {
        t.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{NodeCondition, NodeStatus, PodCondition, PodStatus};
    use sched_model::test_support::example_config;

    fn pod(name: &str, workload: Option<&str>, phase: &str, ready: bool) -> Pod {
        let mut p = Pod::default();
        p.metadata.name = Some(name.into());
        if let Some(w) = workload {
            p.metadata.labels =
                Some([("app.kubernetes.io/name".to_string(), w.to_string())].into());
        }
        p.status = Some(PodStatus {
            phase: Some(phase.into()),
            conditions: Some(vec![PodCondition {
                type_: "Ready".into(),
                status: if ready { "True" } else { "False" }.into(),
                ..Default::default()
            }]),
            ..Default::default()
        });
        p
    }

    const LABEL: &str = "app.kubernetes.io/name";

    #[test]
    fn pod_info_reads_label_phase_and_readiness() {
        let i = pod_info(&pod("a", Some("mineru"), "Running", true), LABEL);
        assert_eq!(
            i,
            PodInfo {
                workload: Some("mineru".into()),
                active: true,
                ready: true
            }
        );
        assert!(!pod_info(&pod("a", None, "Succeeded", false), LABEL).active);
        assert!(!pod_info(&pod("a", None, "Failed", false), LABEL).active);
        assert!(pod_info(&pod("a", None, "Pending", false), LABEL).active);
        assert_eq!(
            pod_info(&pod("a", None, "Running", true), LABEL).workload,
            None
        );
    }

    #[test]
    fn node_ready_follows_the_ready_condition() {
        let node = |status: &str| Node {
            status: Some(NodeStatus {
                conditions: Some(vec![NodeCondition {
                    type_: "Ready".into(),
                    status: status.into(),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(node_ready(&node("True")));
        assert!(!node_ready(&node("False")));
        assert!(!node_ready(&node("Unknown")));
        assert!(!node_ready(&Node::default()));
    }

    #[test]
    fn the_store_is_not_synced_until_the_list_completes_and_relists_replace_it() {
        let mut s = PodStore::default();
        assert!(!s.synced());
        s.apply(Event::Init, LABEL);
        s.apply(
            Event::InitApply(pod("a", Some("mineru"), "Running", true)),
            LABEL,
        );
        assert!(!s.synced());
        assert!(s.counts().is_empty());
        s.apply(Event::InitDone, LABEL);
        assert!(s.synced());
        assert_eq!(s.counts()["mineru"], (1, 1));

        s.apply(
            Event::Apply(pod("b", Some("mineru"), "Pending", false)),
            LABEL,
        );
        assert_eq!(s.counts()["mineru"], (2, 1));
        s.apply(
            Event::Delete(pod("a", Some("mineru"), "Running", true)),
            LABEL,
        );
        assert_eq!(s.counts()["mineru"], (1, 0));

        // After an error nothing is trusted until a full relist; the relist replaces all.
        s.invalidate();
        assert!(!s.synced());
        s.apply(Event::Init, LABEL);
        s.apply(
            Event::InitApply(pod("c", Some("ollama"), "Running", true)),
            LABEL,
        );
        s.apply(Event::InitDone, LABEL);
        assert!(s.synced());
        assert!(!s.counts().contains_key("mineru"));
        assert_eq!(s.counts()["ollama"], (1, 1));
    }

    #[test]
    fn finished_and_unlabelled_pods_are_not_counted() {
        let mut s = PodStore::default();
        s.apply(Event::Init, LABEL);
        for p in [
            pod("done", Some("mineru"), "Succeeded", false),
            pod("failed", Some("mineru"), "Failed", false),
            pod("other", None, "Running", true),
        ] {
            s.apply(Event::InitApply(p), LABEL);
        }
        s.apply(Event::InitDone, LABEL);
        assert!(s.counts().is_empty());
    }

    #[test]
    fn model_values_sum_workloads_across_namespaces_and_report_zero_for_absence() {
        let config = example_config();
        let a = BTreeMap::from([("mineru".to_string(), (1, 1))]);
        let b = BTreeMap::from([
            ("mineru".to_string(), (1, 0)),
            ("ollama".to_string(), (2, 2)),
        ]);
        let values = model_values(&config, &[a, b]);
        let get = |m: &str| {
            values
                .iter()
                .find_map(|v| match v {
                    ObservedValue::ModelPods {
                        model,
                        pods,
                        ready_pods,
                    } if model.as_str() == m => Some((*pods, *ready_pods)),
                    _ => None,
                })
                .unwrap()
        };
        assert_eq!(values.len(), config.models().len());
        assert_eq!(get("mineru"), (2, 1));
        assert_eq!(get("llm"), (2, 2));
        assert_eq!(get("whisperx"), (0, 0));
    }

    #[test]
    fn one_flaky_down_does_not_flip_a_running_host() {
        let mut d = Debouncer::new(3);
        assert!(d.check(true));
        assert!(d.check(false));
        assert!(d.check(true));
        assert!(d.check(false));
        assert!(d.check(false));
        // Not consecutive: the up reading in between reset the count.
        assert!(d.check(true));
        assert!(d.check(false));
        assert!(d.check(false));
        assert!(!d.check(false), "third consecutive down flips");
        assert!(!d.check(true), "and a single up does not flip it back");
        assert!(!d.check(true));
        assert!(d.check(true));
    }

    #[test]
    fn the_first_reading_is_adopted_and_one_confirmation_flips_at_once() {
        let mut d = Debouncer::new(3);
        assert!(!d.check(false));
        let mut one = Debouncer::new(1);
        assert!(one.check(true));
        assert!(!one.check(false));
        // Zero is treated as one rather than never flipping.
        let mut zero = Debouncer::new(0);
        assert!(zero.check(true));
        assert!(!zero.check(false));
    }
}
