//! The recovery action of a class, run by the `Recovery` object (one instance per class).
//!
//! **Why its own object.** A `Scheduler` handler holds the object's exclusive lock, so an
//! action that makes HTTP requests there would queue every tick and lease request behind
//! it for as long as the service answers slowly. The `Recovery` object has its own lock and
//! its own state: the tick only sends it a one-way request after publishing the allowed
//! set, and the action runs beside the ticks. A hung or failing action costs the tick
//! nothing; every request is bounded by the action's `timeout`.
//!
//! **What a run does** (`requeue_failed`; see `sched_actuate`):
//! 1. A cheap *gate*, one journaled step: the class must have a runnable action, must be in
//!    the allowed set the tick published, and `min_interval` must have passed since the last
//!    run. Otherwise it stops there (a request per tick while items are failed is cheap).
//! 2. One journaled `list` request.
//! 3. For each item that has attempts left, at most `max_per_run`: re-check that the class
//!    is still allowed (a journaled step), **record the attempt and make it durable**, and
//!    only then send the re-queue request (a journaled step). A crash between recording and
//!    sending can therefore only waste an attempt, never allow a third one; a replay reuses
//!    journaled results and cannot send again. A crash after a request was sent but before
//!    its result was journaled makes the retry send it once more for the same recorded
//!    attempt (Restate steps are at least once); that is the one case with more requests
//!    than recorded attempts, and it is bounded to one extra per crash.
//!
//! The state is the object's `recovery_attempts` key ([`sched_actuate::AttemptBook`],
//! versioned). Failures of any request are values, counted and logged, never errors of the
//! invocation: Restate must not retry a handler that talks to a service that is down.

use std::sync::Arc;

use restate_sdk::errors::HandlerError;
use sched_actuate::{AttemptBook, Listed, PostOutcome, requeue};
use sched_model::{ClassId, Millis, RequeueFailedParams};
use serde::{Deserialize, Serialize};

use crate::object::ObjectCtx;
use crate::persist::{self, RECOVERY_KEY};
use crate::tick::Shared;

/// Looks up an environment variable (injected so tests need not touch the process
/// environment).
pub type Env = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

/// The process environment.
pub fn process_env() -> Env {
    Arc::new(|name| std::env::var(name).ok())
}

/// What a run did, for the caller and for `last_run` style inspection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunAck {
    pub class: String,
    /// `ran`, `skipped_interval`, `skipped_not_allowed`, `not_wired`, `list_failed` or
    /// `state_error`.
    pub outcome: String,
    pub listed: usize,
    /// Requests sent (an attempt was recorded for each).
    pub attempted: usize,
    pub queued: usize,
    pub failed: usize,
    /// Items that had used all their attempts.
    pub capped: usize,
    /// Items with attempts left that `max_per_run` did not reach.
    pub deferred: usize,
    /// Items not attempted because the class left the allowed set during the run.
    pub not_allowed: usize,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Gate {
    Go {
        now: Millis,
        params: Box<RequeueFailedParams>,
    },
    Stop {
        outcome: String,
    },
}

/// Runs the recovery action of `class` once.
pub async fn run<C: ObjectCtx>(
    shared: &Arc<Shared>,
    env: &Env,
    ctx: &C,
    class: &str,
) -> Result<RunAck, HandlerError> {
    let metrics = &shared.metrics;
    let mut ack = RunAck {
        class: class.to_string(),
        ..RunAck::default()
    };
    let stop = |mut ack: RunAck, outcome: &str, error: Option<String>| {
        metrics.recovery_run(class, outcome);
        ack.outcome = outcome.to_string();
        ack.error = error;
        ack
    };

    let mut book = match persist::decode_attempts(ctx.get_raw(RECOVERY_KEY).await?.as_deref()) {
        Ok(b) => b,
        Err(e) => {
            metrics.persisted_state_error(&e.key);
            tracing::error!(
                class,
                "recovery attempts cannot be read; the action does not run: {e}"
            );
            return Ok(stop(ack, "state_error", Some(e.to_string())));
        }
    };

    // The gate. Everything it reads from the process (config, allowed set, clock) is read
    // inside the journaled step, so a replay takes the same road.
    let (s, id, b) = (Arc::clone(shared), ClassId::new(class), book.clone());
    let gate = ctx
        .run_journaled("recovery_gate", move || gate(&s, &id, &b))
        .await?;
    let (now, params) = match gate {
        Gate::Stop { outcome } => return Ok(stop(ack, &outcome, None)),
        Gate::Go { now, params } => (now, *params),
    };
    book.mark_run(now);

    let client = match sched_actuate::client(&params) {
        Ok(c) => c,
        Err(e) => {
            return Ok(stop(
                ack,
                "list_failed",
                Some(format!("cannot build the HTTP client: {e}")),
            ));
        }
    };

    // The list.
    let (c, p, e) = (client.clone(), params.clone(), Arc::clone(env));
    let listed: Result<Listed, String> = ctx
        .run_journaled_async("recovery_list", move || {
            Box::pin(async move {
                let auth = sched_actuate::requeue::auth(&p, &*e).await?;
                sched_actuate::list(&c, &p, auth.as_ref(), now).await
            })
        })
        .await?;
    let listed = match listed {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(
                class,
                "recovery: the list of failed items could not be read: {e}"
            );
            ctx.set_raw(RECOVERY_KEY, persist::encode_attempts(&book));
            return Ok(stop(ack, "list_failed", Some(e)));
        }
    };
    if listed.truncated {
        tracing::warn!(
            class,
            "recovery: the list is a full page; the rest follows on later runs"
        );
    }
    if listed.invalid > 0 {
        tracing::warn!(
            class,
            invalid = listed.invalid,
            "recovery: listed items without a usable identifier skipped"
        );
    }
    ack.listed = listed.ids.len();
    let pruned = book.prune(now, params.window, &listed.ids);
    let plan = book.plan(&params, &listed.ids);
    ack.capped = plan.capped;
    ack.deferred = plan.deferred;
    metrics.recovery_requeue(class, "skipped_cap", plan.capped as u64);
    metrics.recovery_requeue(class, "deferred", plan.deferred as u64);
    if pruned > 0 {
        tracing::debug!(
            class,
            pruned,
            "recovery: forgot attempts older than the window"
        );
    }

    // The requests, one item at a time: allowed? record durably, then send.
    for (i, id) in plan.send.iter().enumerate() {
        let (s, cid) = (Arc::clone(shared), ClassId::new(class));
        let allowed = ctx
            .run_journaled("recovery_allowed", move || s.class_is_allowed(&cid))
            .await?;
        if !allowed {
            let left = plan.send.len() - i;
            ack.not_allowed = left;
            metrics.recovery_requeue(class, "skipped_not_allowed", left as u64);
            tracing::info!(
                class,
                left,
                "recovery: the class left the allowed set; the rest waits"
            );
            break;
        }
        book.record(id, now);
        ctx.record_durably(RECOVERY_KEY, persist::encode_attempts(&book))
            .await?;
        ack.attempted += 1;
        metrics.recovery_requeue(class, "attempted", 1);
        let (c, p, e, item) = (client.clone(), params.clone(), Arc::clone(env), id.clone());
        let outcome: PostOutcome = ctx
            .run_journaled_async("recovery_requeue", move || {
                Box::pin(async move {
                    match sched_actuate::requeue::auth(&p, &*e).await {
                        Ok(auth) => requeue(&c, &p, auth.as_ref(), &item).await,
                        Err(e) => PostOutcome::Failed(e),
                    }
                })
            })
            .await?;
        match outcome {
            PostOutcome::Queued => {
                ack.queued += 1;
                metrics.recovery_requeue(class, "queued", 1);
                tracing::info!(class, id = %id, attempt = book.count(id), "recovery: re-queued");
            }
            PostOutcome::Failed(why) => {
                ack.failed += 1;
                metrics.recovery_requeue(class, "failed", 1);
                tracing::warn!(class, id = %id, attempt = book.count(id), "recovery: re-queue failed: {why}");
            }
        }
    }
    ctx.set_raw(RECOVERY_KEY, persist::encode_attempts(&book));
    metrics.recovery_run(class, "ran");
    ack.outcome = "ran".to_string();
    Ok(ack)
}

/// The cheap checks before a run: one journaled step.
fn gate(shared: &Shared, class: &ClassId, book: &AttemptBook) -> Gate {
    let now = shared.clock.now();
    let config = shared.config.current();
    let params = config
        .class(class)
        .and_then(|c| c.recovery.as_ref())
        .and_then(|r| r.requeue_failed_params().ok());
    let Some(params) = params else {
        return Gate::Stop {
            outcome: "not_wired".to_string(),
        };
    };
    if !shared.class_is_allowed(class) {
        return Gate::Stop {
            outcome: "skipped_not_allowed".to_string(),
        };
    }
    if !book.run_due(now, params.min_interval) {
        return Gate::Stop {
            outcome: "skipped_interval".to_string(),
        };
    }
    Gate::Go {
        now,
        params: Box::new(params),
    }
}

/// `discard_attempts` request.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscardAttempts {
    /// Also discard a document that reads fine (it gives every item fresh attempts).
    #[serde(default)]
    pub force: bool,
}

/// Removes the attempts document, for one that cannot be read; a readable one only with
/// `force`. Returns the stored text.
pub async fn discard_attempts<C: ObjectCtx>(
    ctx: &C,
    req: DiscardAttempts,
) -> Result<Option<String>, HandlerError> {
    let raw = ctx.get_raw(RECOVERY_KEY).await?;
    if persist::decode_attempts(raw.as_deref()).is_ok() && !req.force {
        return Err(restate_sdk::errors::TerminalError::new_with_code(
            409,
            format!("{RECOVERY_KEY} reads fine; pass \"force\": true to discard it anyway"),
        )
        .into());
    }
    ctx.clear_key(RECOVERY_KEY);
    tracing::warn!("recovery attempts discarded");
    Ok(raw.map(|b| String::from_utf8_lossy(&b).into_owned()))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use sched_model::test_support::EXAMPLE_CONFIG;
    use sched_model::{Fidelity, ObservedValue};
    use sched_observe::Report;
    use sched_observe::testserver::{Reply, TestServer};

    use super::*;
    use crate::clock::{Clock, ManualClock};
    use crate::config_source::ConfigHolder;
    use crate::metrics::Metrics;
    use crate::object::fake::{Crash, FakeCtx};
    use crate::tick::Durable;

    const T0: u64 = 10_000_000;
    const CLASS: &str = "speakr-transcription";
    const MIN: u64 = 60_000;

    /// A stand-in for the service's recordings API: failed recordings with their summary
    /// flag, and what a re-queue request does.
    #[derive(Default)]
    struct Mock {
        /// (id, has_summary) of the recordings the list returns.
        failed: Vec<(u64, bool)>,
        /// POSTs received, by id.
        posts: Vec<u64>,
        /// A queued recording leaves the failed list (it did not fail again).
        queued_leaves: bool,
        /// What a POST answers: 0 queued, 1 success false, 2 status 500, 3 not JSON.
        mode: u8,
    }

    struct World {
        server: TestServer,
        mock: Arc<Mutex<Mock>>,
        shared: Arc<Shared>,
        clock: Arc<ManualClock>,
        env: Env,
        ctx: FakeCtx,
        durable: Durable,
    }

    const LIST_PATH: &str = "/api/v1/recordings";

    async fn world(yaml_edit: impl Fn(String) -> String) -> World {
        let server = TestServer::start().await;
        let mock = Arc::new(Mutex::new(Mock {
            queued_leaves: true,
            ..Mock::default()
        }));
        let m = Arc::clone(&mock);
        server.set_dynamic(LIST_PATH, move |_q| {
            let g = m.lock().unwrap();
            let items: Vec<String> = g
                .failed
                .iter()
                .map(|(id, has_summary)| {
                    format!(
                        r#"{{"id": {id}, "status": "FAILED", "has_transcription": true, "has_summary": {has_summary}, "updated_at": "2023-11-14T21:00:00", "error_message": "boom"}}"#
                    )
                })
                .collect();
            Reply::json(&format!(r#"{{"recordings": [{}]}}"#, items.join(",")))
        });
        for id in 1..=20u64 {
            let m = Arc::clone(&mock);
            server.set_dynamic(&format!("{LIST_PATH}/{id}/transcribe"), move |_q| {
                let mut g = m.lock().unwrap();
                g.posts.push(id);
                match g.mode {
                    1 => Reply::json(r#"{"success": false, "error": "no"}"#),
                    2 => Reply::status(500),
                    3 => Reply::json("<html>"),
                    _ => {
                        if g.queued_leaves {
                            g.failed.retain(|(i, _)| *i != id);
                        }
                        Reply::json(r#"{"success": true, "job_id": "j", "status": "QUEUED"}"#)
                    }
                }
            });
        }
        let yaml =
            yaml_edit(EXAMPLE_CONFIG.replace("http://speakr.media.svc:8899", &server.url("")));
        let clock = Arc::new(ManualClock::new(Millis(T0)));
        let holder = Arc::new(ConfigHolder::new(&yaml).unwrap());
        let shared = Arc::new(Shared::new(holder, Arc::new(Metrics::new()), clock.clone()));
        let env: Env = Arc::new(|n| (n == "SPEAKR_API_TOKEN").then(|| "tok".to_string()));
        World {
            server,
            mock,
            shared,
            clock,
            env,
            ctx: FakeCtx::default(),
            durable: Durable::default(),
        }
    }

    fn push(s: &Shared, source: &str, values: Vec<ObservedValue>) {
        s.ingest_now(Report::Observed {
            source: source.into(),
            fidelity: Fidelity::Polled,
            values,
        });
    }

    impl World {
        /// A fresh process view of the same cluster: hosts up and `failed` failed items.
        /// Ticks, which allows whisperx (and publishes the class as allowed).
        fn tick_with_failed(&mut self, failed: u64) {
            let mut v: Vec<ObservedValue> = ["mineru", "llm", "whisperx", "immich-ml"]
                .iter()
                .map(|m| ObservedValue::ModelPods {
                    model: (*m).into(),
                    pods: 0,
                    ready_pods: 0,
                })
                .collect();
            v.push(ObservedValue::ResourceUp {
                resource: "cuda-central".into(),
                up: true,
            });
            push(&self.shared, "cluster-pods", v);
            push(
                &self.shared,
                "basestar-probe",
                vec![ObservedValue::ResourceUp {
                    resource: "basestar-nfs".into(),
                    up: true,
                }],
            );
            push(
                &self.shared,
                "speakr-pending",
                vec![ObservedValue::Demand {
                    class: CLASS.into(),
                    ready: 0,
                    in_flight: 0,
                    failed,
                    oldest_ready_at: None,
                }],
            );
            let now = self.clock.now();
            self.shared.tick(now, &mut self.durable);
        }

        async fn run(&self) -> RunAck {
            run(&self.shared, &self.env, &self.ctx, CLASS)
                .await
                .unwrap()
        }

        /// Ends the invocation, as a completed handler does.
        fn finish(&self) {
            self.ctx.commit();
        }

        async fn run_commit(&self) -> RunAck {
            let a = self.run().await;
            self.finish();
            a
        }

        fn advance(&self, ms: u64) {
            self.clock.set(Millis(self.clock.now().as_u64() + ms));
        }

        fn failed_ids(&self, ids: &[u64]) {
            self.mock.lock().unwrap().failed = ids.iter().map(|i| (*i, false)).collect();
        }

        fn posts(&self) -> Vec<u64> {
            self.mock.lock().unwrap().posts.clone()
        }

        fn book(&self) -> AttemptBook {
            persist::decode_attempts(self.ctx.stored(RECOVERY_KEY).as_deref()).unwrap()
        }

        fn metric(&self, name: &str, class: &str, outcome: &str) -> f64 {
            let text = self.shared.metrics.render();
            let needle = format!("{name}{{class=\"{class}\",outcome=\"{outcome}\"}} ");
            text.lines()
                .find_map(|l| l.strip_prefix(&needle))
                .map_or(0.0, |v| v.parse().unwrap())
        }

        fn requeues(&self, outcome: &str) -> f64 {
            self.metric("sched_recovery_requeues_total", CLASS, outcome)
        }

        fn runs(&self, outcome: &str) -> f64 {
            self.metric("sched_recovery_runs_total", CLASS, outcome)
        }
    }

    #[tokio::test]
    async fn a_failed_item_is_requeued_once_and_leaves_the_list() {
        let mut w = world(|y| y).await;
        // Two failed transcriptions and one failed summary (has_summary true).
        w.mock.lock().unwrap().failed = vec![(3, false), (4, true), (5, false)];
        w.tick_with_failed(2);
        let a = w.run_commit().await;
        assert_eq!(a.outcome, "ran");
        assert_eq!(
            (a.listed, a.attempted, a.queued, a.failed),
            (2, 2, 2, 0),
            "{a:?}"
        );
        assert_eq!(w.posts(), [3, 5]);
        // The request carried the token, the list the window and the filter's field.
        assert!(
            w.server
                .requests()
                .iter()
                .all(|(_, auth)| auth.as_deref() == Some("Bearer tok"))
        );
        assert_eq!(w.server.methods()[0], "GET");
        assert!(w.server.methods()[1..].iter().all(|m| m == "POST"));
        let b = w.book();
        assert_eq!(b.count("3"), 1);
        assert_eq!(b.count("5"), 1);
        assert_eq!(b.count("4"), 0, "the failed summary is not re-queued");
        assert_eq!((w.requeues("attempted"), w.requeues("queued")), (2.0, 2.0));
        assert_eq!(w.runs("ran"), 1.0);
        // The recording left the list, so the next run (after the interval) sends nothing.
        w.advance(MIN);
        let a = w.run_commit().await;
        assert_eq!((a.listed, a.attempted), (0, 0), "{a:?}");
        assert_eq!(w.posts(), [3, 5]);
    }

    #[tokio::test]
    async fn a_recording_that_keeps_failing_gets_two_requests_and_then_none() {
        let mut w = world(|y| y).await;
        w.mock.lock().unwrap().queued_leaves = false;
        w.failed_ids(&[7]);
        w.tick_with_failed(1);
        for i in 0..5 {
            let a = w.run_commit().await;
            assert_eq!(a.outcome, "ran");
            assert_eq!(a.attempted, usize::from(i < 2), "run {i}: {a:?}");
            assert_eq!(a.capped, usize::from(i >= 2), "run {i}: {a:?}");
            w.advance(MIN);
        }
        assert_eq!(w.posts(), [7, 7], "exactly two POSTs");
        assert_eq!(w.book().count("7"), 2);
        assert_eq!(w.requeues("skipped_cap"), 3.0);
        assert_eq!(w.requeues("attempted"), 2.0);
    }

    #[tokio::test]
    async fn runs_are_spaced_by_min_interval_and_cost_no_request_in_between() {
        let mut w = world(|y| y).await;
        w.mock.lock().unwrap().queued_leaves = false;
        w.failed_ids(&[1]);
        w.tick_with_failed(1);
        assert_eq!(w.run_commit().await.outcome, "ran");
        let seen = w.server.requests().len();
        w.advance(MIN - 1);
        let a = w.run_commit().await;
        assert_eq!(a.outcome, "skipped_interval");
        assert_eq!(w.server.requests().len(), seen, "no request at all");
        assert_eq!(w.runs("skipped_interval"), 1.0);
        w.advance(1);
        assert_eq!(w.run_commit().await.outcome, "ran");
    }

    #[tokio::test]
    async fn at_most_max_per_run_requests_go_out_per_run() {
        let mut w = world(|y| y).await;
        w.failed_ids(&[1, 2, 3, 4, 5, 6, 7]);
        w.tick_with_failed(7);
        let a = w.run_commit().await;
        assert_eq!((a.attempted, a.deferred), (5, 2), "{a:?}");
        assert_eq!(w.posts().len(), 5);
        assert_eq!(w.requeues("deferred"), 2.0);
        w.advance(MIN);
        let a = w.run_commit().await;
        assert_eq!((a.attempted, a.deferred), (2, 0), "{a:?}");
        assert_eq!(w.posts().len(), 7);
    }

    #[tokio::test]
    async fn attempts_are_forgotten_only_when_old_and_no_longer_listed() {
        let mut w = world(|y| y).await;
        w.mock.lock().unwrap().queued_leaves = false;
        w.failed_ids(&[1, 2]);
        w.tick_with_failed(2);
        w.run_commit().await;
        assert_eq!(w.book().attempts.len(), 2);
        // 2 leaves the list (it recovered); an hour later 2's attempts are forgotten but 1,
        // still listed (still failing), keeps its count, so it cannot be given fresh ones.
        w.failed_ids(&[1]);
        w.advance(60 * MIN);
        w.run_commit().await;
        let b = w.book();
        assert_eq!(b.count("1"), 2, "{b:?}");
        assert_eq!(b.attempts.len(), 1, "{b:?}");
        assert_eq!(
            w.posts(),
            [1, 2, 1],
            "1 got its second attempt, nothing more"
        );
    }

    #[tokio::test]
    async fn nothing_runs_while_the_class_is_not_in_the_published_allowed_set() {
        let w = world(|y| y).await;
        w.failed_ids(&[1]);
        // No tick has published anything yet.
        let a = w.run_commit().await;
        assert_eq!(a.outcome, "skipped_not_allowed");
        assert!(w.server.requests().is_empty());
        assert_eq!(w.runs("skipped_not_allowed"), 1.0);
    }

    #[tokio::test]
    async fn the_allowed_set_is_checked_again_before_every_request() {
        let mut w = world(|y| y).await;
        w.failed_ids(&[1, 2, 3]);
        w.tick_with_failed(3);
        // While the first request is answered the model is withdrawn (a tick publishes a set
        // without the class): the rest must not be sent.
        let withdrawn = {
            let mut o = w.shared.decide(w.clock.now(), &w.durable);
            o.report.allowed_classes.clear();
            o.report.allowed_models.clear();
            o
        };
        let shared = Arc::clone(&w.shared);
        let m = Arc::clone(&w.mock);
        w.server
            .set_dynamic(&format!("{LIST_PATH}/1/transcribe"), move |_q| {
                m.lock().unwrap().posts.push(1);
                shared.publish(&withdrawn, true);
                Reply::json(r#"{"success": true, "status": "QUEUED"}"#)
            });
        let a = w.run_commit().await;
        assert_eq!((a.attempted, a.queued, a.not_allowed), (1, 1, 2), "{a:?}");
        assert_eq!(w.posts(), [1]);
        assert_eq!(w.requeues("skipped_not_allowed"), 2.0);
        assert_eq!(
            w.book().count("2"),
            0,
            "no attempt is recorded for what is not sent"
        );
    }

    #[tokio::test]
    async fn failures_are_counted_logged_and_retried_by_the_next_run_within_the_cap() {
        for (mode, label) in [(1u8, "success false"), (2, "HTTP 500"), (3, "not JSON")] {
            let mut w = world(|y| y).await;
            w.mock.lock().unwrap().mode = mode;
            w.failed_ids(&[9]);
            w.tick_with_failed(1);
            for run in 0..3 {
                let a = w.run_commit().await;
                assert_eq!(a.outcome, "ran", "{label}");
                assert_eq!(a.failed, usize::from(run < 2), "{label} run {run}: {a:?}");
                assert_eq!(a.queued, 0, "{label}");
                w.advance(MIN);
            }
            assert_eq!(
                w.posts(),
                [9, 9],
                "{label}: the failed request consumed its attempt"
            );
            assert_eq!(w.requeues("failed"), 2.0, "{label}");
            assert_eq!(w.requeues("queued"), 0.0, "{label}");
            assert_eq!(w.requeues("skipped_cap"), 1.0, "{label}");
        }
    }

    #[tokio::test]
    async fn a_failed_list_sends_nothing_and_does_not_touch_the_attempts() {
        let mut w = world(|y| y).await;
        w.server.set_dynamic(LIST_PATH, |_q| Reply::status(503));
        w.tick_with_failed(1);
        let a = w.run_commit().await;
        assert_eq!(a.outcome, "list_failed");
        assert!(a.error.unwrap().contains("HTTP 503"));
        assert!(w.posts().is_empty());
        assert!(w.book().attempts.is_empty());
        assert_eq!(w.runs("list_failed"), 1.0);
        // The run still counts toward the interval, so a down service is asked once a minute.
        assert_eq!(w.run_commit().await.outcome, "skipped_interval");
    }

    #[tokio::test]
    async fn a_missing_token_fails_the_list_clearly_and_never_panics() {
        let mut w = world(|y| y).await;
        w.env = Arc::new(|_| None);
        w.failed_ids(&[1]);
        w.tick_with_failed(1);
        let a = w.run_commit().await;
        assert_eq!(a.outcome, "list_failed");
        assert!(a.error.unwrap().contains("SPEAKR_API_TOKEN is not set"));
        assert!(w.server.requests().is_empty());
    }

    #[tokio::test]
    async fn a_hung_service_costs_only_the_timeout_and_nothing_else() {
        let mut w =
            world(|y| y.replace("        timeout: 10s\n", "        timeout: 400ms\n")).await;
        w.server.set_dynamic(LIST_PATH, |_q| {
            Reply::json("{}").delayed(std::time::Duration::from_secs(5))
        });
        w.tick_with_failed(1);
        let started = std::time::Instant::now();
        let a = w.run_commit().await;
        assert_eq!(a.outcome, "list_failed", "{a:?}");
        assert!(started.elapsed() < std::time::Duration::from_secs(3));
    }

    #[tokio::test]
    async fn classes_without_a_runnable_action_do_nothing() {
        let w = world(|y| y).await;
        let a = run(&w.shared, &w.env, &w.ctx, "paperless-ocr")
            .await
            .unwrap();
        assert_eq!(a.outcome, "not_wired");
        let a = run(&w.shared, &w.env, &w.ctx, "no-such-class")
            .await
            .unwrap();
        assert_eq!(a.outcome, "not_wired");
        assert!(w.server.requests().is_empty());
    }

    #[tokio::test]
    async fn an_unreadable_attempts_document_stops_the_action_and_is_counted() {
        let mut w = world(|y| y).await;
        w.failed_ids(&[1]);
        w.tick_with_failed(1);
        w.ctx.put(RECOVERY_KEY, br#"{"version":9,"data":{}}"#);
        let a = w.run_commit().await;
        assert_eq!(a.outcome, "state_error");
        assert!(w.server.requests().is_empty());
        assert!(
            w.shared
                .metrics
                .render()
                .contains("sched_persisted_state_errors_total{key=\"recovery_attempts\"} 1")
        );
        // The recovery path: discard it (a readable one only with force).
        let ok = discard_attempts(&w.ctx, DiscardAttempts::default()).await;
        assert!(ok.is_ok());
        w.finish();
        assert!(w.ctx.stored(RECOVERY_KEY).is_none());
        w.ctx.put(
            RECOVERY_KEY,
            &persist::encode_attempts(&AttemptBook::default()),
        );
        assert!(
            discard_attempts(&w.ctx, DiscardAttempts::default())
                .await
                .is_err()
        );
        assert!(
            discard_attempts(&w.ctx, DiscardAttempts { force: true })
                .await
                .is_ok()
        );
    }

    // Durability.

    #[tokio::test]
    async fn the_attempt_count_survives_a_restart_of_the_process_and_of_the_object() {
        let mut w = world(|y| y).await;
        w.mock.lock().unwrap().queued_leaves = false;
        w.failed_ids(&[4]);
        w.tick_with_failed(1);
        assert_eq!(w.run_commit().await.attempted, 1);
        // The process restarts: a new Shared (no in-memory state at all), the same Restate
        // state (the ctx store), a tick that publishes again.
        let yaml = EXAMPLE_CONFIG.replace("http://speakr.media.svc:8899", &w.server.url(""));
        let holder = Arc::new(ConfigHolder::new(&yaml).unwrap());
        w.shared = Arc::new(Shared::new(
            holder,
            Arc::new(Metrics::new()),
            w.clock.clone(),
        ));
        w.durable = Durable::default();
        w.tick_with_failed(1);
        w.advance(MIN);
        assert_eq!(w.run_commit().await.attempted, 1);
        // A second restart: the count (2) was read back, so nothing more goes out.
        let holder = Arc::new(ConfigHolder::new(&yaml).unwrap());
        w.shared = Arc::new(Shared::new(
            holder,
            Arc::new(Metrics::new()),
            w.clock.clone(),
        ));
        w.durable = Durable::default();
        w.tick_with_failed(1);
        w.advance(MIN);
        let a = w.run_commit().await;
        assert_eq!((a.attempted, a.capped), (0, 1), "{a:?}");
        assert_eq!(w.posts(), [4, 4]);
    }

    #[tokio::test]
    async fn a_crash_between_recording_the_attempt_and_sending_never_allows_a_third_attempt() {
        let mut w = world(|y| y).await;
        w.mock.lock().unwrap().queued_leaves = false;
        w.failed_ids(&[6]);
        w.tick_with_failed(1);
        // Attempt 1 completes normally.
        assert_eq!(w.run_commit().await.attempted, 1);
        w.advance(MIN);
        // Attempt 2: the process dies after the attempt was made durable and before the
        // request was sent.
        w.ctx.crash_at("recovery_requeue", 1, Crash::Before);
        assert!(run(&w.shared, &w.env, &w.ctx, CLASS).await.is_err());
        assert_eq!(w.posts(), [6], "the request was never sent");
        assert_eq!(w.book().count("6"), 2, "but the attempt is on record");
        // Restate retries the invocation: it replays the journal and carries on; the retry
        // sends the one request that attempt was for.
        w.ctx.crash_and_retry();
        let a = run(&w.shared, &w.env, &w.ctx, CLASS).await.unwrap();
        w.finish();
        assert_eq!((a.attempted, a.queued), (1, 1), "{a:?}");
        assert_eq!(w.book().count("6"), 2, "the retry did not record a third");
        assert_eq!(w.posts(), [6, 6]);
        // Whatever happens afterwards, nothing more is sent for this item.
        for _ in 0..3 {
            w.advance(MIN);
            let a = w.run_commit().await;
            assert_eq!(a.attempted, 0, "{a:?}");
        }
        assert_eq!(w.posts(), [6, 6]);
    }

    #[tokio::test]
    async fn a_crash_after_the_request_was_sent_is_bounded_by_the_recorded_attempts() {
        let mut w = world(|y| y).await;
        w.mock.lock().unwrap().queued_leaves = false;
        w.failed_ids(&[8]);
        w.tick_with_failed(1);
        // The request goes out, then the process dies before the result is journaled.
        w.ctx.crash_at("recovery_requeue", 1, Crash::After);
        assert!(run(&w.shared, &w.env, &w.ctx, CLASS).await.is_err());
        assert_eq!(w.posts(), [8]);
        assert_eq!(w.book().count("8"), 1);
        w.ctx.crash_and_retry();
        let a = run(&w.shared, &w.env, &w.ctx, CLASS).await.unwrap();
        w.finish();
        // The one case with more requests than recorded attempts: the retry repeats the
        // request of the same attempt (steps are at least once). It records nothing new.
        assert_eq!(a.attempted, 1);
        assert_eq!(w.posts(), [8, 8]);
        assert_eq!(w.book().count("8"), 1);
        // Later runs add the second attempt and then stop.
        for _ in 0..3 {
            w.advance(MIN);
            w.run_commit().await;
        }
        assert_eq!(w.book().count("8"), 2);
        assert_eq!(w.posts().len(), 3);
    }

    #[tokio::test]
    async fn a_replay_of_a_finished_run_sends_nothing_again() {
        let mut w = world(|y| y).await;
        w.failed_ids(&[2]);
        w.tick_with_failed(1);
        let a = w.run().await;
        assert_eq!(a.queued, 1);
        // The invocation fails after its steps were journaled (for example the final state
        // write was lost): the retry replays them all.
        w.ctx.crash_and_retry();
        let before = w.server.requests().len();
        let again = w.run().await;
        assert_eq!(again.queued, 1, "the journaled outcome is reused");
        assert_eq!(
            w.server.requests().len(),
            before,
            "no request is sent twice"
        );
        assert_eq!(w.posts(), [2]);
    }
}
