//! Starting the observers a configuration asks for, keeping them running, and restarting
//! only those whose own inputs change.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sched_model::{ClassId, Config, ModelId, SourceId, SourceKind, SourceSpec};
use tokio::task::{AbortHandle, JoinHandle};

use crate::kube_jobs::{self, JobTarget};
use crate::kube_watch::{self, Timing};
use crate::nudge::{DEFAULT_NUDGE_MIN_INTERVAL, Nudge, Nudges};
use crate::{ReportTx, http_json, prometheus, tcp_probe};

/// What a running observer was built from. If it is unchanged by a reload, the observer
/// keeps running (and keeps its state, such as reachability debounce counts).
#[derive(Debug, Clone, PartialEq)]
struct Fingerprint {
    spec: SourceSpec,
    /// Classes taking demand or a switch reading from the source (used by HTTP/JSON only).
    classes: Vec<ClassId>,
    /// Which workloads make up each model (Kubernetes): the only thing about a model the
    /// pod observer uses, so other model changes (load time and the like) do not restart it.
    models: Vec<(ModelId, Vec<String>)>,
    /// The scheduled jobs and their namespaces (a source that observes `jobs` only).
    jobs: Vec<JobTarget>,
}

struct Running {
    fingerprint: Fingerprint,
    handle: JoinHandle<()>,
    /// Whether the observer takes nudges (its handle is in the registry).
    nudged: bool,
}

/// Knobs for starting observers; the defaults suit production.
#[derive(Clone, Default)]
pub struct SpawnOptions {
    /// A cluster client to use instead of creating one from the environment.
    pub kube_client: Option<kube::Client>,
    /// How an observer that died is started again.
    pub restart: RestartPolicy,
    pub kube_timing: Timing,
    /// The clock adapters date `since` windows from; defaults to the system clock.
    pub now: Option<http_json::NowFn>,
    /// Where `http_json` sources with a `webhook:` setting get their "poll now" handle. The
    /// registry outlives the observers, so a restart keeps the handle. Share a clone of it
    /// with whatever delivers the nudges.
    pub nudges: Nudges,
    /// The least time between two polls of a nudged source; defaults to
    /// [`DEFAULT_NUDGE_MIN_INTERVAL`].
    pub nudge_min_interval: Option<Duration>,
}

/// The running observers. Dropping this stops them and everything they started.
#[derive(Default)]
pub struct Observers {
    running: BTreeMap<SourceId, Running>,
    /// Restart delay state per source. It outlives a restart caused by a reload, so an
    /// observer that crash-loops stays throttled however often the config is reapplied.
    backoffs: BTreeMap<SourceId, SharedBackoff>,
}

/// A restart backoff shared between an observer's incarnations.
pub type SharedBackoff = Arc<Mutex<RestartBackoff>>;

pub fn shared_backoff(policy: RestartPolicy) -> SharedBackoff {
    Arc::new(Mutex::new(RestartBackoff::new(policy)))
}

impl Observers {
    pub fn len(&self) -> usize {
        self.running.len()
    }

    pub fn is_empty(&self) -> bool {
        self.running.is_empty()
    }

    /// The restart backoff kept for `source`.
    pub fn backoff_for(&self, source: &SourceId) -> Option<SharedBackoff> {
        self.backoffs.get(source).cloned()
    }

    /// Brings the running set in line with `config`: starts observers for new sources,
    /// stops those whose source is gone, and restarts only those whose own inputs changed.
    /// Returns the ids that were (re)started.
    pub fn update(&mut self, config: &Config, tx: &ReportTx, opts: &SpawnOptions) -> Vec<SourceId> {
        let mut wanted: BTreeMap<SourceId, Fingerprint> = BTreeMap::new();
        for spec in config.sources() {
            if matches!(
                spec.kind,
                SourceKind::HttpJson
                    | SourceKind::KubernetesWatch
                    | SourceKind::Prometheus
                    | SourceKind::TcpProbe
            ) {
                wanted.insert(spec.id.clone(), fingerprint(config, spec));
            } else {
                tracing::info!(source = %spec.id, kind = ?spec.kind, "no observer for this kind yet");
            }
        }
        self.running.retain(|id, r| {
            let keep = wanted.get(id) == Some(&r.fingerprint);
            if !keep {
                r.handle.abort();
            }
            keep
        });
        self.backoffs.retain(|id, _| wanted.contains_key(id));
        let mut started = Vec::new();
        for (id, fp) in wanted {
            if self.running.contains_key(&id) {
                continue;
            }
            let backoff = Arc::clone(
                self.backoffs
                    .entry(id.clone())
                    .or_insert_with(|| shared_backoff(opts.restart)),
            );
            if let Some((handle, nudged)) = start(&fp, tx, opts, backoff) {
                self.running.insert(
                    id.clone(),
                    Running {
                        fingerprint: fp,
                        handle,
                        nudged,
                    },
                );
                started.push(id);
            }
        }
        // Handles of sources that are gone, or no longer take nudges, are dropped; those of
        // sources that merely restarted were kept above.
        opts.nudges.retain_only(
            &self
                .running
                .iter()
                .filter(|(_, r)| r.nudged)
                .map(|(id, _)| id.clone())
                .collect(),
        );
        started
    }
}

impl Drop for Observers {
    fn drop(&mut self) {
        for r in self.running.values() {
            r.handle.abort();
        }
    }
}

fn fingerprint(config: &Config, spec: &SourceSpec) -> Fingerprint {
    let is_kube = spec.kind == SourceKind::KubernetesWatch;
    Fingerprint {
        spec: spec.clone(),
        classes: if is_kube {
            Vec::new()
        } else {
            config
                .classes()
                .iter()
                .filter(|c| {
                    c.demand.as_ref().is_some_and(|d| d.source == spec.id)
                        || c.switch.as_ref().is_some_and(|s| s.source == spec.id)
                })
                .map(|c| c.id.clone())
                .collect()
        },
        models: if is_kube {
            kube_watch::model_workloads(config.models())
        } else {
            Vec::new()
        },
        jobs: if spec.observe.contains(&sched_model::Capability::Jobs) {
            kube_jobs::job_targets(config)
        } else {
            Vec::new()
        },
    }
}

/// Starts the observer for `fp`; the flag says whether it takes nudges.
fn start(
    fp: &Fingerprint,
    tx: &ReportTx,
    opts: &SpawnOptions,
    backoff: SharedBackoff,
) -> Option<(JoinHandle<()>, bool)> {
    let takes_nudges = fp.spec.kind == SourceKind::HttpJson
        && fp
            .spec
            .http_json_params()
            .ok()
            .flatten()
            .is_some_and(|p| p.webhook.is_some());
    let nudge = takes_nudges.then(|| Nudge {
        notify: opts.nudges.handle(&fp.spec.id),
        min_interval: opts
            .nudge_min_interval
            .unwrap_or(DEFAULT_NUDGE_MIN_INTERVAL),
    });
    let handle = start_with(fp, tx, opts, backoff, nudge)?;
    Some((handle, takes_nudges))
}

fn start_with(
    fp: &Fingerprint,
    tx: &ReportTx,
    opts: &SpawnOptions,
    backoff: SharedBackoff,
    nudge: Option<Nudge>,
) -> Option<JoinHandle<()>> {
    let spec = &fp.spec;
    match spec.kind {
        SourceKind::HttpJson => match spec.http_json_params() {
            Ok(Some(params)) => {
                let (spec, classes, tx2) = (spec.clone(), fp.classes.clone(), tx.clone());
                let now = opts.now.clone().unwrap_or_else(http_json::system_now);
                Some(tokio::spawn(supervised(
                    spec.id.clone(),
                    tx.clone(),
                    backoff,
                    move || {
                        http_json::run(
                            spec.clone(),
                            params.clone(),
                            classes.clone(),
                            tx2.clone(),
                            now.clone(),
                            nudge.clone(),
                        )
                    },
                )))
            }
            Ok(None) => {
                tracing::info!(source = %spec.id, "no params: source is not polled");
                None
            }
            Err(e) => {
                tracing::error!(source = %spec.id, "{e}");
                None
            }
        },
        SourceKind::KubernetesWatch => match spec.kube_watch_params() {
            Ok(Some(params)) if spec.observe.contains(&sched_model::Capability::Jobs) => {
                let (spec, targets, tx2) = (spec.clone(), fp.jobs.clone(), tx.clone());
                let (client, timing) = (opts.kube_client.clone(), opts.kube_timing);
                Some(tokio::spawn(supervised(
                    spec.id.clone(),
                    tx.clone(),
                    backoff,
                    move || {
                        let (spec, params, targets, tx) =
                            (spec.clone(), params.clone(), targets.clone(), tx2.clone());
                        let client = client.clone();
                        async move {
                            match client {
                                Some(c) => {
                                    kube_jobs::run_with_client(spec, params, targets, c, tx).await
                                }
                                None => kube_jobs::run(spec, params, targets, tx, timing).await,
                            }
                        }
                    },
                )))
            }
            Ok(Some(params)) => {
                let (spec, models, tx2) = (spec.clone(), fp.models.clone(), tx.clone());
                let (client, timing) = (opts.kube_client.clone(), opts.kube_timing);
                Some(tokio::spawn(supervised(
                    spec.id.clone(),
                    tx.clone(),
                    backoff,
                    move || {
                        let (spec, params, models, tx) =
                            (spec.clone(), params.clone(), models.clone(), tx2.clone());
                        let client = client.clone();
                        async move {
                            match client {
                                Some(c) => {
                                    kube_watch::run_with_client(spec, params, models, c, tx, timing)
                                        .await
                                }
                                None => kube_watch::run(spec, params, models, tx, timing).await,
                            }
                        }
                    },
                )))
            }
            Ok(None) => {
                tracing::info!(source = %spec.id, "no params: source is not watched");
                None
            }
            Err(e) => {
                tracing::error!(source = %spec.id, "{e}");
                None
            }
        },
        SourceKind::Prometheus => match spec.prometheus_params() {
            Ok(Some(params)) => {
                let (spec, tx2) = (spec.clone(), tx.clone());
                Some(tokio::spawn(supervised(
                    spec.id.clone(),
                    tx.clone(),
                    backoff,
                    move || prometheus::run(spec.clone(), params.clone(), tx2.clone()),
                )))
            }
            Ok(None) => {
                tracing::info!(source = %spec.id, "no params: source is not polled");
                None
            }
            Err(e) => {
                tracing::error!(source = %spec.id, "{e}");
                None
            }
        },
        SourceKind::TcpProbe => match spec.tcp_probe_params() {
            Ok(Some(params)) => {
                let (spec, tx2) = (spec.clone(), tx.clone());
                Some(tokio::spawn(supervised(
                    spec.id.clone(),
                    tx.clone(),
                    backoff,
                    move || tcp_probe::run(spec.clone(), params.clone(), tx2.clone()),
                )))
            }
            Ok(None) => {
                tracing::info!(source = %spec.id, "no params: source is not probed");
                None
            }
            Err(e) => {
                tracing::error!(source = %spec.id, "{e}");
                None
            }
        },
        _ => None,
    }
}

/// Restart delays for an observer that keeps dying: exponential up to a cap, starting over
/// once it has run healthily for a while.
#[derive(Debug, Clone, Copy)]
pub struct RestartPolicy {
    pub initial: Duration,
    pub max: Duration,
    /// An observer that ran at least this long before dying is not "flapping".
    pub healthy_after: Duration,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        RestartPolicy {
            initial: Duration::from_secs(5),
            max: Duration::from_secs(300),
            healthy_after: Duration::from_secs(120),
        }
    }
}

/// The delay state of one observer's restarts.
#[derive(Debug, Clone)]
pub struct RestartBackoff {
    policy: RestartPolicy,
    next: Duration,
}

impl RestartBackoff {
    pub fn new(policy: RestartPolicy) -> Self {
        RestartBackoff {
            policy,
            next: policy.initial,
        }
    }

    /// The wait before restarting an observer that ran for `ran_for` and then died.
    pub fn after_exit(&mut self, ran_for: Duration) -> Duration {
        if ran_for >= self.policy.healthy_after {
            self.next = self.policy.initial;
        }
        let wait = self.next;
        self.next = self.next.saturating_mul(2).min(self.policy.max);
        wait
    }
}

/// Aborts a task when dropped, so the task cannot outlive whoever was waiting for it.
struct AbortOnDrop(AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn panic_text(payload: Box<dyn std::any::Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(s) => *s,
        Err(p) => p
            .downcast_ref::<&str>()
            .map_or_else(|| "unknown panic".to_string(), |s| (*s).to_string()),
    }
}

/// Runs the observer made by `make`, and when it panics or returns, says so with a
/// `Report::Failed` (so it shows up as a source error and the source goes stale) and
/// starts it again after a delay that grows while it keeps dying (see [`RestartPolicy`]). Ends when the receiver is gone or this future
/// is dropped, which also stops the observer.
pub async fn supervised<F, Fut>(source: SourceId, tx: ReportTx, backoff: SharedBackoff, make: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    loop {
        let started = Instant::now();
        let handle = tokio::spawn(make());
        let _abort = AbortOnDrop(handle.abort_handle());
        let error = match handle.await {
            Ok(()) => "observer stopped unexpectedly; restarting".to_string(),
            Err(e) if e.is_panic() => {
                format!(
                    "observer panicked: {}; restarting",
                    panic_text(e.into_panic())
                )
            }
            Err(_) => return,
        };
        tracing::error!(source = %source, "{error}");
        if tx
            .send(crate::Report::Failed {
                source: source.clone(),
                error,
            })
            .await
            .is_err()
        {
            return;
        }
        let wait = backoff
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .after_exit(started.elapsed());
        tokio::time::sleep(wait).await;
    }
}

/// Whether something in this build polls or watches a source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObserverState {
    /// An observer runs for it.
    Running,
    /// The kind has an observer, but the source was configured without `params`, so
    /// nothing polls it. It can still be fed by pushes.
    NoParams,
    /// This build has no observer for the kind at all (schedule, session, ...).
    NoImplementation,
}

pub fn observer_state(spec: &SourceSpec) -> ObserverState {
    let has_params = match spec.kind {
        SourceKind::HttpJson => matches!(spec.http_json_params(), Ok(Some(_))),
        SourceKind::KubernetesWatch => matches!(spec.kube_watch_params(), Ok(Some(_))),
        SourceKind::Prometheus => matches!(spec.prometheus_params(), Ok(Some(_))),
        SourceKind::TcpProbe => matches!(spec.tcp_probe_params(), Ok(Some(_))),
        _ => return ObserverState::NoImplementation,
    };
    if has_params {
        ObserverState::Running
    } else {
        ObserverState::NoParams
    }
}

/// Whether an observer runs for `spec`.
pub fn observer_available(spec: &SourceSpec) -> bool {
    observer_state(spec) == ObserverState::Running
}

/// Starts the observers `config` asks for, with default options. See [`Observers::update`].
pub fn spawn_observers(config: &Arc<Config>, tx: &ReportTx) -> Observers {
    let mut o = Observers::default();
    o.update(config, tx, &SpawnOptions::default());
    o
}

#[cfg(test)]
mod tests {
    use super::*;
    use sched_model::test_support::{EXAMPLE_CONFIG, with_switched_class};

    #[test]
    fn a_source_is_routed_the_classes_that_name_it_by_demand_or_by_switch() {
        let config = Config::from_yaml(&with_switched_class(EXAMPLE_CONFIG)).unwrap();
        let fp = |id: &str| fingerprint(&config, config.source(&id.into()).unwrap());
        assert_eq!(fp("ha-wikijs").classes, [ClassId::from("wikijs")]);
        assert_eq!(
            fp("speakr-pending").classes,
            [ClassId::from("speakr-transcription")]
        );
        assert!(
            fp("immich-queues")
                .classes
                .iter()
                .all(|c| c.as_str() != "wikijs")
        );
        // The shipped example has no switched class, so its switch source routes to none.
        let shipped = Config::from_yaml(EXAMPLE_CONFIG).unwrap();
        let source = shipped.source(&"ha-wikijs".into()).unwrap();
        assert!(fingerprint(&shipped, source).classes.is_empty());
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn restart_delays_double_to_a_cap_and_start_over_after_a_healthy_run() {
        let mut b = RestartBackoff::new(RestartPolicy {
            initial: ms(100),
            max: ms(350),
            healthy_after: ms(1000),
        });
        let quick = ms(10);
        assert_eq!(b.after_exit(quick), ms(100));
        assert_eq!(b.after_exit(quick), ms(200));
        assert_eq!(b.after_exit(quick), ms(350));
        assert_eq!(b.after_exit(quick), ms(350));
        // It ran healthily for a while before dying this time: start over.
        assert_eq!(b.after_exit(ms(1000)), ms(100));
        assert_eq!(b.after_exit(quick), ms(200));
    }
}
