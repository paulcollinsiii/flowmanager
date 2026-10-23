//! Delivering configuration from a file. In production the file is a mounted ConfigMap,
//! which Kubernetes updates in place, so the path is re-read on an interval. Every attempt
//! goes through [`ConfigStore`]: a config is validated in full before it replaces the
//! last good one, and a rejected or unreadable one changes nothing.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sched_model::{Config, ConfigStore};
use tokio::sync::watch;

use crate::metrics::Metrics;

/// What one reload attempt did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReloadOutcome {
    /// A new config became active, with this generation.
    Applied(u64),
    /// The text is the one already applied, so the generation did not move.
    Unchanged,
    /// The text is not a valid config; the previous one stays active.
    Rejected(String),
    /// The file could not be read; the previous config stays active.
    Unreadable(String),
}

impl ReloadOutcome {
    /// The metric label.
    pub fn label(&self) -> &'static str {
        match self {
            ReloadOutcome::Applied(_) => "applied",
            ReloadOutcome::Unchanged => "unchanged",
            ReloadOutcome::Rejected(_) => "rejected",
            ReloadOutcome::Unreadable(_) => "unreadable",
        }
    }
}

struct Inner {
    store: ConfigStore,
    text: String,
}

/// The active configuration, shared. Ticks take a [`current`](ConfigHolder::current) handle
/// once and use it throughout, so each tick sees exactly one generation.
pub struct ConfigHolder {
    inner: Mutex<Inner>,
    tx: watch::Sender<Arc<Config>>,
}

impl ConfigHolder {
    /// Starts from `text`; fails if it is not a valid config, since there is no earlier one.
    pub fn new(text: &str) -> Result<ConfigHolder, String> {
        let store = ConfigStore::new(text).map_err(|e| e.to_string())?;
        let (tx, _) = watch::channel(Arc::new(store.active().clone()));
        Ok(ConfigHolder {
            inner: Mutex::new(Inner {
                store,
                text: text.to_string(),
            }),
            tx,
        })
    }

    pub fn load(path: &Path) -> Result<ConfigHolder, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        Self::new(&text).map_err(|e| format!("{}: {e}", path.display()))
    }

    pub fn current(&self) -> Arc<Config> {
        Arc::clone(&self.tx.borrow())
    }

    /// Notified whenever a new config becomes active.
    pub fn subscribe(&self) -> watch::Receiver<Arc<Config>> {
        self.tx.subscribe()
    }

    /// Validates `text` and, only if it is valid and differs from the active text, applies it.
    pub fn apply_text(&self, text: &str) -> ReloadOutcome {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if g.text == text {
            return ReloadOutcome::Unchanged;
        }
        match g.store.apply(text) {
            Ok(generation) => {
                g.text = text.to_string();
                self.tx.send_replace(Arc::new(g.store.active().clone()));
                ReloadOutcome::Applied(generation)
            }
            Err(e) => ReloadOutcome::Rejected(e.to_string()),
        }
    }

    pub async fn reload_from(&self, path: &Path) -> ReloadOutcome {
        match tokio::fs::read_to_string(path).await {
            Ok(text) => self.apply_text(&text),
            Err(e) => ReloadOutcome::Unreadable(format!("cannot read {}: {e}", path.display())),
        }
    }
}

/// Re-reads `path` every `every` for as long as the task lives, logging each outcome that
/// is not "unchanged" and counting all of them.
pub async fn watch_file(
    holder: Arc<ConfigHolder>,
    path: PathBuf,
    every: Duration,
    metrics: Arc<Metrics>,
) {
    let mut ticker = tokio::time::interval(every);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // the first tick is immediate; the file was just loaded
    // A bad file is re-read every interval; log each distinct problem once.
    let mut last_problem: Option<String> = None;
    loop {
        ticker.tick().await;
        let outcome = holder.reload_from(&path).await;
        metrics.config_reload(outcome.label());
        match &outcome {
            ReloadOutcome::Applied(g) => {
                last_problem = None;
                metrics.set_config_generation(*g);
                tracing::info!(generation = g, path = %path.display(), "configuration reloaded");
            }
            ReloadOutcome::Unchanged => last_problem = None,
            ReloadOutcome::Rejected(why) | ReloadOutcome::Unreadable(why) => {
                if last_problem.as_ref() != Some(why) {
                    tracing::error!(
                        generation = holder.current().generation(),
                        path = %path.display(),
                        outcome = outcome.label(),
                        "configuration not applied; keeping the last good one: {why}"
                    );
                    last_problem = Some(why.clone());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sched_model::test_support::EXAMPLE_CONFIG;

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("sched-config-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("config.yaml")
    }

    #[test]
    fn valid_changes_apply_invalid_ones_keep_the_last_good() {
        let h = ConfigHolder::new(EXAMPLE_CONFIG).unwrap();
        let rx = h.subscribe();
        assert_eq!(h.current().generation(), 1);

        let changed = EXAMPLE_CONFIG.replace("idle_grace: 10m", "idle_grace: 20m");
        assert_eq!(h.apply_text(&changed), ReloadOutcome::Applied(2));
        assert!(rx.has_changed().unwrap());
        assert_eq!(h.current().policy().idle_grace.as_u64(), 20 * 60_000);

        // Invalid: unparseable, then failing validation. Generation and config are untouched.
        let before = h.current();
        for bad in [
            "policy: [".to_string(),
            EXAMPLE_CONFIG.replace("min_dwell: 5m", "min_dwell: 3h"),
        ] {
            assert!(matches!(h.apply_text(&bad), ReloadOutcome::Rejected(_)));
            assert_eq!(h.current(), before);
        }
        // The same text again does not bump the generation.
        assert_eq!(h.apply_text(&changed), ReloadOutcome::Unchanged);
        assert_eq!(h.current().generation(), 2);
        // And a good one still applies after rejections.
        assert_eq!(h.apply_text(EXAMPLE_CONFIG), ReloadOutcome::Applied(3));
    }

    #[test]
    fn a_ticks_handle_keeps_its_generation_across_a_reload() {
        let h = ConfigHolder::new(EXAMPLE_CONFIG).unwrap();
        let pinned = h.current();
        h.apply_text(&EXAMPLE_CONFIG.replace("idle_grace: 10m", "idle_grace: 20m"));
        assert_eq!(pinned.generation(), 1);
        assert_eq!(h.current().generation(), 2);
    }

    #[test]
    fn the_first_config_must_be_valid() {
        assert!(ConfigHolder::new("policy: [").is_err());
        assert!(ConfigHolder::load(Path::new("/nonexistent/config.yaml")).is_err());
    }

    #[tokio::test]
    async fn reloads_from_a_file_on_disk() {
        let path = scratch("reload");
        std::fs::write(&path, EXAMPLE_CONFIG).unwrap();
        let h = ConfigHolder::load(&path).unwrap();
        assert_eq!(h.reload_from(&path).await, ReloadOutcome::Unchanged);

        std::fs::write(
            &path,
            EXAMPLE_CONFIG.replace("idle_grace: 10m", "idle_grace: 15m"),
        )
        .unwrap();
        assert_eq!(h.reload_from(&path).await, ReloadOutcome::Applied(2));

        std::fs::write(&path, "policy: [").unwrap();
        assert!(matches!(
            h.reload_from(&path).await,
            ReloadOutcome::Rejected(_)
        ));
        assert_eq!(h.current().generation(), 2);
        assert_eq!(h.current().policy().idle_grace.as_u64(), 15 * 60_000);

        std::fs::remove_file(&path).unwrap();
        assert!(matches!(
            h.reload_from(&path).await,
            ReloadOutcome::Unreadable(_)
        ));
        assert_eq!(h.current().generation(), 2);
    }

    #[tokio::test]
    async fn the_watcher_applies_good_files_counts_outcomes_and_ignores_bad_ones() {
        let path = scratch("watch");
        std::fs::write(&path, EXAMPLE_CONFIG).unwrap();
        let h = Arc::new(ConfigHolder::load(&path).unwrap());
        let metrics = Arc::new(Metrics::new());
        let task = tokio::spawn(watch_file(
            Arc::clone(&h),
            path.clone(),
            Duration::from_millis(20),
            Arc::clone(&metrics),
        ));
        std::fs::write(&path, "policy: [").unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(h.current().generation(), 1);
        std::fs::write(
            &path,
            EXAMPLE_CONFIG.replace("idle_grace: 10m", "idle_grace: 15m"),
        )
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(h.current().generation(), 2);
        task.abort();
        let text = metrics.render();
        assert!(
            text.contains("sched_config_reloads_total{outcome=\"rejected\"}"),
            "{text}"
        );
        assert!(
            text.contains("sched_config_reloads_total{outcome=\"applied\"} 1"),
            "{text}"
        );
        assert!(text.contains("sched_config_generation 2"), "{text}");
    }
}
