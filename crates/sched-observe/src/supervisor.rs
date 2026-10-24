//! Starting the observers a configuration asks for, keeping them running, and restarting
//! only those whose own inputs change.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sched_model::{ClassId, Config, ModelId, SourceId, SourceKind, SourceSpec};
use tokio::task::{AbortHandle, JoinHandle};

use crate::kube_watch::{self, Timing};
use crate::{ReportTx, http_json};

/// What a running observer was built from. If it is unchanged by a reload, the observer
/// keeps running (and keeps its state, such as reachability debounce counts).
#[derive(Debug, Clone, PartialEq)]
struct Fingerprint {
    spec: SourceSpec,
    /// Classes taking demand from the source (HTTP/JSON).
    classes: Vec<ClassId>,
    /// Which workloads make up each model (Kubernetes): the only thing about a model the
    /// pod observer uses, so other model changes (load time and the like) do not restart it.
    models: Vec<(ModelId, Vec<String>)>,
}

struct Running {
    fingerprint: Fingerprint,
    handle: JoinHandle<()>,
}

/// Knobs for starting observers; the defaults suit production.
#[derive(Clone, Default)]
pub struct SpawnOptions {
    /// A cluster client to use instead of creating one from the environment.
    pub kube_client: Option<kube::Client>,
    /// How an observer that died is started again.
    pub restart: RestartPolicy,
    pub kube_timing: Timing,
}

/// The running observers. Dropping this stops them and everything they started.
#[derive(Default)]
pub struct Observers {
    running: BTreeMap<SourceId, Running>,
}

impl Observers {
    pub fn len(&self) -> usize {
        self.running.len()
    }

    pub fn is_empty(&self) -> bool {
        self.running.is_empty()
    }

    /// Brings the running set in line with `config`: starts observers for new sources,
    /// stops those whose source is gone, and restarts only those whose own inputs changed.
    /// Returns the ids that were (re)started.
    pub fn update(&mut self, config: &Config, tx: &ReportTx, opts: &SpawnOptions) -> Vec<SourceId> {
        let mut wanted: BTreeMap<SourceId, Fingerprint> = BTreeMap::new();
        for spec in config.sources() {
            if matches!(
                spec.kind,
                SourceKind::HttpJson | SourceKind::KubernetesWatch
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
        let mut started = Vec::new();
        for (id, fp) in wanted {
            if self.running.contains_key(&id) {
                continue;
            }
            if let Some(handle) = start(&fp, tx, opts) {
                self.running.insert(
                    id.clone(),
                    Running {
                        fingerprint: fp,
                        handle,
                    },
                );
                started.push(id);
            }
        }
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
                .filter(|c| c.demand.as_ref().is_some_and(|d| d.source == spec.id))
                .map(|c| c.id.clone())
                .collect()
        },
        models: if is_kube {
            kube_watch::model_workloads(config.models())
        } else {
            Vec::new()
        },
    }
}

fn start(fp: &Fingerprint, tx: &ReportTx, opts: &SpawnOptions) -> Option<JoinHandle<()>> {
    let spec = &fp.spec;
    match spec.kind {
        SourceKind::HttpJson => match spec.http_json_params() {
            Ok(Some(params)) => {
                let (spec, classes, tx2) = (spec.clone(), fp.classes.clone(), tx.clone());
                Some(tokio::spawn(supervised(
                    spec.id.clone(),
                    tx.clone(),
                    opts.restart,
                    move || {
                        http_json::run(spec.clone(), params.clone(), classes.clone(), tx2.clone())
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
            Ok(Some(params)) => {
                let (spec, models, tx2) = (spec.clone(), fp.models.clone(), tx.clone());
                let (client, timing) = (opts.kube_client.clone(), opts.kube_timing);
                Some(tokio::spawn(supervised(
                    spec.id.clone(),
                    tx.clone(),
                    opts.restart,
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
pub async fn supervised<F, Fut>(source: SourceId, tx: ReportTx, policy: RestartPolicy, make: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let mut backoff = RestartBackoff::new(policy);
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
        tokio::time::sleep(backoff.after_exit(started.elapsed())).await;
    }
}

/// Whether this build has an observer that would run for `spec`: a supported kind with
/// params. Other sources are configured but not (yet) observed; they can still be fed by
/// pushes.
pub fn observer_available(spec: &SourceSpec) -> bool {
    match spec.kind {
        SourceKind::HttpJson => matches!(spec.http_json_params(), Ok(Some(_))),
        SourceKind::KubernetesWatch => matches!(spec.kube_watch_params(), Ok(Some(_))),
        _ => false,
    }
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
