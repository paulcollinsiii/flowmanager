//! The Kubernetes observer: model pod counts and node readiness from watch streams, each
//! backed by a periodic resync. Read-only: it lists and watches, nothing else.
//!
//! Pod counts are reported on every pod event and on every resync, but only once every
//! watched namespace has completed its initial list, so a half-synced cache can never read
//! as "zero pods". Node readiness is checked once per resync and debounced.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::Api;
use kube::ResourceExt;
use kube::runtime::watcher::{self, Event};
use sched_model::{
    Fidelity, KubeWatchParams, ModelId, ModelSpec, ObservedValue, ResourceId, SourceId, SourceSpec,
};
use serde::de::DeserializeOwned;
use tokio::task::JoinSet;

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

/// A model and the workloads (pod label values) that implement it.
pub type ModelWorkloads = (ModelId, Vec<String>);

/// The part of the models the pod observer uses.
pub fn model_workloads(models: &[ModelSpec]) -> Vec<ModelWorkloads> {
    models
        .iter()
        .map(|m| (m.id.clone(), m.workloads.clone()))
        .collect()
}

/// One `ModelPods` value per configured model, summing its workloads over `stores`. A
/// workload with no pods counts as zero: the lists are complete, so absence is a fact.
pub fn model_values(
    models: &[ModelWorkloads],
    stores: &[BTreeMap<String, (u32, u32)>],
) -> Vec<ObservedValue> {
    models
        .iter()
        .map(|(model, workloads)| {
            let (pods, ready_pods) = workloads
                .iter()
                .flat_map(|w| stores.iter().filter_map(move |s| s.get(w)))
                .fold((0, 0), |a, c| (a.0 + c.0, a.1 + c.1));
            ObservedValue::ModelPods {
                model: model.clone(),
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

/// Delays of the restart loops.
#[derive(Debug, Clone, Copy)]
pub struct Timing {
    /// First wait before re-listing after a watch failure; doubles up to `backoff_max`
    /// and starts over once a watcher has lived `backoff_max` without failing.
    pub backoff_initial: Duration,
    pub backoff_max: Duration,
    /// Wait between attempts to create the cluster client.
    pub client_retry: Duration,
}

impl Default for Timing {
    fn default() -> Self {
        Timing {
            backoff_initial: Duration::from_secs(1),
            backoff_max: Duration::from_secs(30),
            client_retry: Duration::from_secs(5),
        }
    }
}

/// The next backoff after `current`: doubled, up to `max`.
pub fn grow_backoff(current: Duration, max: Duration) -> Duration {
    current.saturating_mul(2).min(max)
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
pub fn pod_report_from(
    source: &SourceId,
    models: &[ModelWorkloads],
    stores: &BTreeMap<String, PodStore>,
    fidelity: Fidelity,
) -> Option<Report> {
    if stores.values().any(|s| !s.synced()) {
        return None;
    }
    let counts: Vec<_> = stores.values().map(PodStore::counts).collect();
    Some(Report::Observed {
        source: source.clone(),
        fidelity,
        values: model_values(models, &counts),
    })
}

fn pod_report(
    spec: &SourceSpec,
    models: &[ModelWorkloads],
    shared: &SharedState,
    fidelity: Fidelity,
) -> Option<Report> {
    pod_report_from(&spec.id, models, &lock(shared).pods, fidelity)
}

/// Watches `K` until the receiver is gone. After any watch error the watcher is thrown
/// away and, after a backoff, started afresh: the new one lists again, so the store is
/// replaced and resumes reporting. (kube-runtime alone would keep watching without a new
/// list and the stores would stay unsynced for good.)
async fn watch_restarting<K>(
    make_api: impl Fn() -> Api<K>,
    timing: Timing,
    source: SourceId,
    what: String,
    tx: ReportTx,
    mut on_event: impl FnMut(Event<K>) -> Option<Report>,
    mut on_error: impl FnMut(),
) where
    K: kube::Resource<DynamicType = ()> + Clone + DeserializeOwned + Debug + Send + 'static,
{
    let mut backoff = timing.backoff_initial;
    loop {
        let started = std::time::Instant::now();
        let mut stream = Box::pin(watcher::watcher(make_api(), watcher::Config::default()));
        while let Some(ev) = stream.next().await {
            match ev {
                Ok(ev) => {
                    if let Some(r) = on_event(ev)
                        && tx.send(r).await.is_err()
                    {
                        return;
                    }
                }
                Err(e) => {
                    on_error();
                    let failed = Report::Failed {
                        source: source.clone(),
                        error: format!("{what} watch: {e}"),
                    };
                    if tx.send(failed).await.is_err() {
                        return;
                    }
                    break;
                }
            }
        }
        drop(stream);
        // A watcher that lived a good while was healthy: its next failure starts the waits
        // over. One that fails right after listing (a bad watch endpoint) keeps backing off.
        if started.elapsed() >= timing.backoff_max {
            backoff = timing.backoff_initial;
        }
        tokio::time::sleep(backoff).await;
        backoff = grow_backoff(backoff, timing.backoff_max);
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

/// Creates the cluster client from the environment and watches until the receiver is gone.
/// A client that cannot be created is reported as a source failure and retried, so the
/// source goes stale rather than looking healthy.
pub async fn run(
    spec: SourceSpec,
    params: KubeWatchParams,
    models: Vec<ModelWorkloads>,
    tx: ReportTx,
    timing: Timing,
) {
    run_with_factory(spec, params, models, tx, timing, || async {
        kube::Client::try_default().await.map_err(|e| e.to_string())
    })
    .await
}

/// As [`run`], with the client made by `make_client`. A failure is reported and retried
/// after a wait that doubles (from `timing.client_retry` up to `timing.backoff_max`), so a
/// cluster that stays unreachable is not hammered.
pub async fn run_with_factory<F, Fut>(
    spec: SourceSpec,
    params: KubeWatchParams,
    models: Vec<ModelWorkloads>,
    tx: ReportTx,
    timing: Timing,
    mut make_client: F,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<kube::Client, String>>,
{
    let mut wait = timing.client_retry;
    let client = loop {
        match make_client().await {
            Ok(c) => break c,
            Err(e) => {
                let failed = Report::Failed {
                    source: spec.id.clone(),
                    error: format!("cannot create the Kubernetes client: {e}"),
                };
                if tx.send(failed).await.is_err() {
                    return;
                }
                tokio::time::sleep(wait).await;
                wait = grow_backoff(wait, timing.backoff_max);
            }
        }
    };
    run_with_client(spec, params, models, client, tx, timing).await
}

/// Watches with `client` until the receiver is gone or a watch task dies. All child tasks
/// belong to a `JoinSet`, so they end with this future however it ends (including when it
/// is aborted).
pub async fn run_with_client(
    spec: SourceSpec,
    params: KubeWatchParams,
    models: Vec<ModelWorkloads>,
    client: kube::Client,
    tx: ReportTx,
    timing: Timing,
) {
    use sched_model::Capability;
    let resync = Duration::from_millis(spec.resync.as_u64().max(1));
    let shared: SharedState = Arc::default();
    let mut tasks: JoinSet<()> = JoinSet::new();
    let observes_pods = spec.observe.contains(&Capability::Pods);
    let observes_up = spec.observe.contains(&Capability::Reachability);
    if observes_pods {
        for ns in params.namespaces.clone() {
            lock(&shared).pods.entry(ns.clone()).or_default();
            let (on_event_state, on_error_state) = (Arc::clone(&shared), Arc::clone(&shared));
            let (ev_ns, err_ns, api_ns) = (ns.clone(), ns.clone(), ns.clone());
            let label = params.workload_label.clone();
            let (source, event_models) = (spec.id.clone(), models.clone());
            let c = client.clone();
            tasks.spawn(watch_restarting(
                move || Api::<Pod>::namespaced(c.clone(), &api_ns),
                timing,
                spec.id.clone(),
                format!("pod (namespace {ns})"),
                tx.clone(),
                move |ev| {
                    let mut g = lock(&on_event_state);
                    g.pods.entry(ev_ns.clone()).or_default().apply(ev, &label);
                    pod_report_from(&source, &event_models, &g.pods, Fidelity::Event)
                },
                move || {
                    lock(&on_error_state)
                        .pods
                        .entry(err_ns.clone())
                        .or_default()
                        .invalidate();
                },
            ));
        }
    }
    if observes_up {
        let (state, state2) = (Arc::clone(&shared), Arc::clone(&shared));
        let c = client.clone();
        let mut relist: Option<HashMap<String, bool>> = None;
        tasks.spawn(watch_restarting(
            move || Api::<Node>::all(c.clone()),
            timing,
            spec.id.clone(),
            "node".to_string(),
            tx.clone(),
            move |ev| {
                let mut g = lock(&state);
                match ev {
                    Event::Init => relist = Some(HashMap::new()),
                    Event::InitApply(n) => {
                        if let Some(r) = &mut relist {
                            r.insert(n.name_any(), node_ready(&n));
                        }
                    }
                    Event::InitDone => {
                        if let Some(r) = relist.take() {
                            g.nodes = r;
                            g.nodes_synced = true;
                        }
                    }
                    Event::Apply(n) => {
                        g.nodes.insert(n.name_any(), node_ready(&n));
                    }
                    Event::Delete(n) => {
                        g.nodes.remove(&n.name_any());
                    }
                }
                None
            },
            move || lock(&state2).nodes_synced = false,
        ));
    }
    let mut debouncers: BTreeMap<ResourceId, Debouncer> = BTreeMap::new();
    let mut ticker = tokio::time::interval(resync);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            ended = tasks.join_next(), if !tasks.is_empty() => {
                let why = match ended {
                    Some(Err(e)) if e.is_panic() => "a watch task panicked",
                    _ => "a watch task ended",
                };
                let _ = tx.send(Report::Failed { source: spec.id.clone(), error: why.to_string() }).await;
                return;
            }
        }
        if observes_pods
            && let Some(r) = pod_report(&spec, &models, &shared, Fidelity::Polled)
            && tx.send(r).await.is_err()
        {
            return;
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
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{NodeCondition, NodeStatus, PodCondition, PodStatus};
    use sched_model::SourceId;
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
        let values = model_values(&model_workloads(config.models()), &[a, b]);
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

    #[test]
    fn nothing_is_reported_while_any_namespace_is_unsynced() {
        let config = example_config();
        let source = SourceId::new("cluster-pods");
        let synced = |names: &[&str]| {
            let mut s = PodStore::default();
            s.apply(Event::Init, LABEL);
            for n in names {
                s.apply(
                    Event::InitApply(pod(n, Some("mineru"), "Running", true)),
                    LABEL,
                );
            }
            s.apply(Event::InitDone, LABEL);
            s
        };
        let mut stores = BTreeMap::from([
            ("a".to_string(), synced(&["m1"])),
            ("b".to_string(), PodStore::default()),
        ]);
        // One namespace has not listed yet: a count from the other alone would be a lie.
        assert!(
            pod_report_from(
                &source,
                &model_workloads(config.models()),
                &stores,
                Fidelity::Polled
            )
            .is_none()
        );
        stores.insert("b".to_string(), synced(&["m2"]));
        let Some(Report::Observed { values, .. }) = pod_report_from(
            &source,
            &model_workloads(config.models()),
            &stores,
            Fidelity::Polled,
        ) else {
            panic!("expected a report once both are synced");
        };
        assert!(values.iter().any(|v| matches!(v, ObservedValue::ModelPods { model, pods: 2, .. } if model.as_str() == "mineru")));
        stores.get_mut("a").unwrap().invalidate();
        assert!(
            pod_report_from(
                &source,
                &model_workloads(config.models()),
                &stores,
                Fidelity::Polled
            )
            .is_none()
        );
    }

    #[test]
    fn watch_backoff_doubles_up_to_the_cap() {
        let (a, max) = (Duration::from_millis(100), Duration::from_millis(350));
        let b = grow_backoff(a, max);
        assert_eq!(b, Duration::from_millis(200));
        assert_eq!(grow_backoff(b, max), max);
        assert_eq!(grow_backoff(max, max), max);
    }
}
