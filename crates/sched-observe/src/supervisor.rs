//! Starting the observers a configuration asks for.

use std::sync::Arc;

use sched_model::{ClassId, Config, SourceKind};
use tokio::task::JoinHandle;

use crate::{ReportTx, http_json, kube_watch};

/// The running observers. Dropping this stops them.
#[derive(Default)]
pub struct Observers {
    handles: Vec<JoinHandle<()>>,
}

impl Observers {
    pub fn len(&self) -> usize {
        self.handles.len()
    }

    pub fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }
}

impl Drop for Observers {
    fn drop(&mut self) {
        for h in &self.handles {
            h.abort();
        }
    }
}

/// Starts one task per source this crate has an adapter for and that has `params`. Other
/// sources are skipped; they can still be fed by pushes. Must run inside a tokio runtime.
pub fn spawn_observers(config: &Arc<Config>, tx: &ReportTx) -> Observers {
    let mut handles = Vec::new();
    for spec in config.sources() {
        match spec.kind {
            SourceKind::HttpJson => match spec.http_json_params() {
                Ok(Some(params)) => {
                    let classes: Vec<ClassId> = config
                        .classes()
                        .iter()
                        .filter(|c| c.demand.as_ref().is_some_and(|d| d.source == spec.id))
                        .map(|c| c.id.clone())
                        .collect();
                    handles.push(tokio::spawn(http_json::run(
                        spec.clone(),
                        params,
                        classes,
                        tx.clone(),
                    )));
                }
                Ok(None) => tracing::info!(source = %spec.id, "no params: source is not polled"),
                Err(e) => tracing::error!(source = %spec.id, "{e}"),
            },
            SourceKind::KubernetesWatch => match spec.kube_watch_params() {
                Ok(Some(params)) => handles.push(tokio::spawn(kube_watch::run(
                    spec.clone(),
                    params,
                    Arc::clone(config),
                    tx.clone(),
                ))),
                Ok(None) => tracing::info!(source = %spec.id, "no params: source is not watched"),
                Err(e) => tracing::error!(source = %spec.id, "{e}"),
            },
            other => {
                tracing::info!(source = %spec.id, kind = ?other, "no observer for this kind yet")
            }
        }
    }
    Observers { handles }
}
