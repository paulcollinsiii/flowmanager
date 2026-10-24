//! The logic of the `Scheduler` object's handlers, written against [`ObjectCtx`]: the few
//! things a handler does with its Restate context (read and write its state, run a
//! journaled step, ask for a tick). `scheduler` implements it for Restate's context; the
//! tests here implement it with a fake that records the journal and replays it, so the
//! durable behaviour (what is stored, what is journaled, what a retry sees) is tested
//! without a Restate server.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use restate_sdk::errors::{HandlerError, TerminalError};
use sched_model::{Lease, LeaseId, Millis, ModelId};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::leases::{LeaseBook, LeaseError, LeaseGrant};
use crate::persist::{self, CoreState, LEASES_KEY, STATE_KEY, StateError};
use crate::tick::{Durable, Shared, TickReport};

/// A boxed, `Send` future: what the context's methods return. Boxed rather than `impl
/// Future`, which runs into the compiler's higher-ranked `Send` limits with Restate's
/// context lifetimes.
pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// What a handler needs from its context.
pub trait ObjectCtx: Sync {
    /// The stored bytes of `key`, as of this invocation.
    fn get_raw<'a>(&'a self, key: &'a str) -> BoxFut<'a, Result<Option<Vec<u8>>, HandlerError>>;
    /// Stores `value` under `key` when the invocation completes.
    fn set_raw(&self, key: &str, value: Vec<u8>);
    /// Removes `key` when the invocation completes.
    fn clear_key(&self, key: &str);
    /// Runs `f` as a journaled step: on a retry or replay the recorded result is returned
    /// and `f` is not run again.
    fn run_journaled<T, F>(&self, name: &'static str, f: F) -> BoxFut<'_, Result<T, HandlerError>>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        F: FnOnce() -> T + Send + 'static;
    /// Asks for a `tick` once this invocation is done.
    fn request_tick(&self);
}

/// `acquire_model_lease` request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcquireModelLease {
    pub model: ModelId,
}

/// `release_lease` request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseLease {
    pub id: LeaseId,
}

/// `discard_state` request: the recovery path for a persisted document that cannot be read.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscardState {
    /// `scheduler_state` or `lease_book`.
    pub key: String,
    /// Also discard a document that reads fine.
    #[serde(default)]
    pub force: bool,
}

/// What `discard_state` removed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Discarded {
    pub key: String,
    /// The stored text (lossy UTF-8), to keep a copy; absent if nothing was stored.
    pub previous: Option<String>,
}

/// What `tick` and `backstop` answer; the whole report is in `last_decision`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickAck {
    pub config_generation: u64,
    pub at: Millis,
    pub kinds: Vec<String>,
    pub error: Option<String>,
}

impl From<&TickReport> for TickAck {
    fn from(r: &TickReport) -> Self {
        TickAck {
            config_generation: r.config_generation,
            at: r.at,
            kinds: r.kinds.clone(),
            error: r.error.clone(),
        }
    }
}

fn lease_refused(e: &LeaseError) -> HandlerError {
    tracing::warn!("lease request refused: {e}");
    TerminalError::new_with_code(e.status(), e.to_string()).into()
}

fn unreadable(shared: &Shared, e: &StateError) -> HandlerError {
    shared.metrics.persisted_state_error(&e.key);
    tracing::error!("persisted state cannot be read: {e}");
    TerminalError::new_with_code(
        503,
        format!(
            "the persisted {e}; see the README (Recovering the persisted state), e.g. the discard_state handler"
        ),
    )
    .into()
}

async fn read_leases<C: ObjectCtx>(ctx: &C) -> Result<Result<LeaseBook, StateError>, HandlerError> {
    Ok(persist::decode_leases(
        ctx.get_raw(LEASES_KEY).await?.as_deref(),
    ))
}

/// One tick: load the durable state, decide inside a journaled step, store the new state,
/// and only then publish in process. A persisted document that cannot be read makes an
/// error tick that stores and publishes nothing; the invocation still succeeds, so Restate
/// does not retry it forever.
pub async fn tick<C: ObjectCtx>(shared: &Arc<Shared>, ctx: &C) -> Result<TickReport, HandlerError> {
    let core = persist::decode_state(ctx.get_raw(STATE_KEY).await?.as_deref());
    let book = read_leases(ctx).await?;
    let (core, book) = match (core, book) {
        (Ok(core), Ok(book)) => (core, book),
        (core, book) => {
            let problems: Vec<(String, String)> = [core.err(), book.err()]
                .into_iter()
                .flatten()
                .map(|e| (e.key, e.problem))
                .collect();
            let clock = Arc::clone(&shared.clock);
            let now = ctx
                .run_journaled("clock", move || clock.now().as_u64())
                .await?;
            return Ok(shared.state_error(Millis(now), &problems));
        }
    };
    // Deterministic from the stored state, so it is done on a replay too.
    shared.seed_reported(&core.reported);
    let durable = Durable {
        state: core.state,
        leases: book.leases,
        reported: core.reported,
    };
    let ran = Arc::new(AtomicBool::new(false));
    let (s, r) = (Arc::clone(shared), Arc::clone(&ran));
    let outcome = ctx
        .run_journaled("decide", move || {
            r.store(true, Ordering::SeqCst);
            let now = s.clock.now();
            s.decide(now, &durable)
        })
        .await?;
    ctx.set_raw(
        STATE_KEY,
        persist::encode_state(&CoreState {
            state: outcome.report.state.clone(),
            reported: outcome.reported.clone(),
        }),
    );
    shared.publish(&outcome, !ran.load(Ordering::SeqCst));
    Ok(outcome.report)
}

/// Acquires a manual model lease (see `leases`). The config and the clock are read inside
/// the journaled step, so a replay gets the same answer even if the config was reloaded.
pub async fn acquire_model_lease<C: ObjectCtx>(
    shared: &Arc<Shared>,
    ctx: &C,
    req: AcquireModelLease,
) -> Result<LeaseGrant, HandlerError> {
    let book = read_leases(ctx)
        .await?
        .map_err(|e| unreadable(shared, &e))?;
    let s = Arc::clone(shared);
    let result = ctx
        .run_journaled("acquire_model_lease", move || {
            let mut book = book;
            let config = s.config.current();
            let now = s.clock.now();
            book.acquire_manual_model(&config, &req.model, now)
                .map(|grant| (grant, book))
        })
        .await?;
    let (grant, book) = result.map_err(|e| lease_refused(&e))?;
    if grant.created {
        ctx.set_raw(LEASES_KEY, persist::encode_leases(&book));
        ctx.request_tick();
        tracing::info!(lease = %grant.lease.id, resource = %grant.lease.resource, "manual model lease acquired");
    }
    Ok(grant)
}

/// Releases a lease by id.
pub async fn release_lease<C: ObjectCtx>(
    shared: &Arc<Shared>,
    ctx: &C,
    req: ReleaseLease,
) -> Result<Lease, HandlerError> {
    let mut book = read_leases(ctx)
        .await?
        .map_err(|e| unreadable(shared, &e))?;
    let lease = book.release(&req.id).map_err(|e| lease_refused(&e))?;
    ctx.set_raw(LEASES_KEY, persist::encode_leases(&book));
    ctx.request_tick();
    tracing::info!(lease = %lease.id, resource = %lease.resource, "lease released");
    Ok(lease)
}

/// Every lease held, from the stored lease book.
pub fn list_leases(shared: &Shared, raw: Option<&[u8]>) -> Result<Vec<Lease>, HandlerError> {
    persist::decode_leases(raw)
        .map(|b| b.leases)
        .map_err(|e| unreadable(shared, &e))
}

/// Removes a persisted document so the next tick (or lease request) starts it afresh.
/// Refused for an unknown key, and for a document that reads fine unless `force` is set.
pub async fn discard_state<C: ObjectCtx>(
    ctx: &C,
    req: DiscardState,
) -> Result<Discarded, HandlerError> {
    if !persist::DOCUMENT_KEYS.contains(&req.key.as_str()) {
        return Err(TerminalError::new_with_code(
            400,
            format!(
                "unknown key {:?}; one of {}",
                req.key,
                persist::DOCUMENT_KEYS.join(", ")
            ),
        )
        .into());
    }
    let raw = ctx.get_raw(&req.key).await?;
    let readable = if req.key == STATE_KEY {
        persist::decode_state(raw.as_deref()).is_ok()
    } else {
        persist::decode_leases(raw.as_deref()).is_ok()
    };
    if readable && !req.force {
        return Err(TerminalError::new_with_code(
            409,
            format!(
                "{} reads fine; pass \"force\": true to discard it anyway",
                req.key
            ),
        )
        .into());
    }
    ctx.clear_key(&req.key);
    ctx.request_tick();
    tracing::warn!(key = %req.key, "persisted state discarded");
    Ok(Discarded {
        key: req.key,
        previous: raw.map(|b| String::from_utf8_lossy(&b).into_owned()),
    })
}

#[cfg(test)]
pub(crate) mod fake {
    //! A fake context: a committed store, the pending writes of the current attempt, and a
    //! journal of step results that a retried attempt replays.

    use std::collections::{BTreeMap, VecDeque};
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

    use super::*;

    #[derive(Default)]
    pub struct FakeCtx {
        pub store: Mutex<BTreeMap<String, Vec<u8>>>,
        pending: Mutex<BTreeMap<String, Option<Vec<u8>>>>,
        /// Steps recorded in this attempt: (name, JSON result).
        pub journal: Mutex<Vec<(String, String)>>,
        replay: Mutex<VecDeque<(String, String)>>,
        /// Names of the steps whose closures ran in this attempt.
        pub ran: Mutex<Vec<String>>,
        pub ticks_requested: AtomicUsize,
    }

    impl FakeCtx {
        pub fn ran(&self) -> Vec<String> {
            self.ran.lock().unwrap().clone()
        }

        pub fn ticks(&self) -> usize {
            self.ticks_requested.load(Ordering::SeqCst)
        }

        pub fn put(&self, key: &str, value: &[u8]) {
            self.store
                .lock()
                .unwrap()
                .insert(key.to_string(), value.to_vec());
        }
    }

    impl FakeCtx {
        /// Applies the attempt's writes, as a completed invocation does, and starts afresh.
        pub fn commit(&self) {
            let mut store = self.store.lock().unwrap();
            for (k, v) in std::mem::take(&mut *self.pending.lock().unwrap()) {
                match v {
                    Some(v) => store.insert(k, v),
                    None => store.remove(&k),
                };
            }
            self.journal.lock().unwrap().clear();
            self.replay.lock().unwrap().clear();
            self.ran.lock().unwrap().clear();
        }

        /// The attempt failed after its steps were journaled: its writes are lost, and the
        /// retry replays the journal.
        pub fn crash_and_retry(&self) {
            self.pending.lock().unwrap().clear();
            let j = std::mem::take(&mut *self.journal.lock().unwrap());
            *self.replay.lock().unwrap() = j.into();
            self.ran.lock().unwrap().clear();
        }

        pub fn stored(&self, key: &str) -> Option<Vec<u8>> {
            self.store.lock().unwrap().get(key).cloned()
        }
    }

    impl ObjectCtx for FakeCtx {
        fn get_raw<'a>(
            &'a self,
            key: &'a str,
        ) -> BoxFut<'a, Result<Option<Vec<u8>>, HandlerError>> {
            let v = match self.pending.lock().unwrap().get(key) {
                Some(v) => v.clone(),
                None => self.store.lock().unwrap().get(key).cloned(),
            };
            Box::pin(async move { Ok(v) })
        }

        fn set_raw(&self, key: &str, value: Vec<u8>) {
            self.pending
                .lock()
                .unwrap()
                .insert(key.to_string(), Some(value));
        }

        fn clear_key(&self, key: &str) {
            self.pending.lock().unwrap().insert(key.to_string(), None);
        }

        fn run_journaled<T, F>(
            &self,
            name: &'static str,
            f: F,
        ) -> BoxFut<'_, Result<T, HandlerError>>
        where
            T: Serialize + DeserializeOwned + Send + 'static,
            F: FnOnce() -> T + Send + 'static,
        {
            let replayed = self.replay.lock().unwrap().pop_front();
            let json = match replayed {
                Some((n, json)) => {
                    assert_eq!(
                        n, name,
                        "replay out of order: journal has {n}, code runs {name}"
                    );
                    json
                }
                None => {
                    self.ran.lock().unwrap().push(name.to_string());
                    serde_json::to_string(&f()).unwrap()
                }
            };
            self.journal
                .lock()
                .unwrap()
                .push((name.to_string(), json.clone()));
            let v: T = serde_json::from_str(&json).unwrap();
            Box::pin(async move { Ok(v) })
        }

        fn request_tick(&self) {
            self.ticks_requested.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fake::FakeCtx;
    use super::*;
    use crate::clock::ManualClock;
    use crate::config_source::ConfigHolder;
    use crate::metrics::Metrics;
    use sched_model::test_support::EXAMPLE_CONFIG;
    use sched_model::{Fidelity, ModePhase, ObservedValue};
    use sched_observe::Report;

    const T0: u64 = 10_000_000;

    fn process() -> (Arc<Shared>, Arc<ManualClock>) {
        let clock = Arc::new(ManualClock::new(Millis(T0)));
        let holder = Arc::new(ConfigHolder::new(EXAMPLE_CONFIG).unwrap());
        let s = Arc::new(Shared::new(holder, Arc::new(Metrics::new()), clock.clone()));
        (s, clock)
    }

    fn push(s: &Shared, source: &str, values: Vec<ObservedValue>) {
        s.ingest_now(Report::Observed {
            source: source.into(),
            fidelity: Fidelity::Polled,
            values,
        });
    }

    /// Fresh zero pods, both hosts up, and `ready` OCR items.
    fn world(s: &Shared, ready: u64) {
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
        push(s, "cluster-pods", v);
        push(
            s,
            "basestar-probe",
            vec![ObservedValue::ResourceUp {
                resource: "basestar-nfs".into(),
                up: true,
            }],
        );
        push(
            s,
            "paperless-ocr-pending",
            vec![ObservedValue::Demand {
                class: "paperless-ocr".into(),
                ready,
                in_flight: 0,
                oldest_ready_at: None,
            }],
        );
    }

    fn stored_state(ctx: &FakeCtx) -> CoreState {
        persist::decode_state(ctx.stored(STATE_KEY).as_deref()).unwrap()
    }

    fn gpu_mode(ctx: &FakeCtx) -> Option<ModePhase> {
        stored_state(ctx)
            .state
            .resource(&"cuda-central".into())
            .map(|r| r.mode.clone())
    }

    #[tokio::test]
    async fn the_sequencer_state_is_stored_and_the_next_tick_continues_from_it() {
        let (s, clock) = process();
        let ctx = FakeCtx::default();
        world(&s, 3);
        let r1 = tick(&s, &ctx).await.unwrap();
        ctx.commit();
        assert!(r1.kinds.contains(&"allow_model".to_string()), "{r1:?}");
        assert!(matches!(gpu_mode(&ctx), Some(ModePhase::Loading { .. })));
        assert!(stored_state(&ctx).reported.contains(&"cluster-pods".into()));
        // A new process, empty cache: the stored state is what carries the tick on.
        let (s2, clock2) = process();
        clock2.set(Millis(T0 + 1_000));
        let _ = clock;
        world(&s2, 3);
        let r2 = tick(&s2, &ctx).await.unwrap();
        ctx.commit();
        assert!(!r2.kinds.contains(&"allow_model".to_string()), "{r2:?}");
        assert!(r2.reason.contains("still loading"), "{}", r2.reason);
    }

    #[tokio::test]
    async fn a_retried_tick_reuses_the_journaled_decision_and_publishes_it_again() {
        let (s, _) = process();
        let ctx = FakeCtx::default();
        world(&s, 3);
        let first = tick(&s, &ctx).await.unwrap();
        assert_eq!(ctx.ran(), vec!["decide".to_string()]);
        assert!(first.allowed_models.contains(&"mineru".into()));
        // The process dies before the invocation completes: nothing was stored.
        ctx.crash_and_retry();
        assert_eq!(ctx.stored(STATE_KEY), None);
        // The retry runs in a new process that sees a different world (no demand, and a
        // later clock). It must not decide again: it publishes and stores the journaled
        // decision.
        let (s2, clock2) = process();
        clock2.set(Millis(T0 + 60_000));
        world(&s2, 0);
        let retried = tick(&s2, &ctx).await.unwrap();
        assert!(ctx.ran().is_empty(), "the decision ran again");
        assert_eq!(retried, first);
        let published = s2.published_allowed().expect("published by the retry");
        assert_eq!(published.at, first.at);
        assert!(published.allowed.models.contains(&"mineru".into()));
        assert_eq!(s2.last_report(), Some(first.clone()));
        ctx.commit();
        assert!(matches!(gpu_mode(&ctx), Some(ModePhase::Loading { .. })));
        // A replayed tick is not counted again.
        assert!(
            !s2.metrics.render().contains("sched_tick_decisions_total{"),
            "{}",
            s2.metrics.render()
        );
        assert!(
            s.metrics
                .render()
                .contains("sched_tick_decisions_total{kind=\"allow_model\"} 1")
        );
    }

    #[tokio::test]
    async fn an_acquired_lease_is_stored_listed_and_seen_by_the_next_tick() {
        let (s, _) = process();
        let ctx = FakeCtx::default();
        world(&s, 3);
        let g = acquire_model_lease(
            &s,
            &ctx,
            AcquireModelLease {
                model: "llm".into(),
            },
        )
        .await
        .unwrap();
        ctx.commit();
        assert!(g.created);
        assert_eq!(ctx.ticks(), 1);
        let listed = list_leases(&s, ctx.stored(LEASES_KEY).as_deref()).unwrap();
        assert_eq!(listed, vec![g.lease.clone()]);
        let r = tick(&s, &ctx).await.unwrap();
        ctx.commit();
        assert_eq!(r.leases, vec![g.lease.clone()]);
        assert_eq!(r.forced, vec!["cuda-central".into()]);
        assert!(r.reason.contains("forced [cuda-central]"), "{}", r.reason);
        assert!(r.allowed_models.contains(&"llm".into()), "{r:?}");
        // Again: the same lease, nothing written.
        let again = acquire_model_lease(
            &s,
            &ctx,
            AcquireModelLease {
                model: "llm".into(),
            },
        )
        .await
        .unwrap();
        assert!(!again.created && again.lease == g.lease);
        assert_eq!(ctx.ticks(), 1);
        // Released: stored, and asks for a tick.
        let released = release_lease(
            &s,
            &ctx,
            ReleaseLease {
                id: g.lease.id.clone(),
            },
        )
        .await
        .unwrap();
        ctx.commit();
        assert_eq!(released, g.lease);
        assert!(
            list_leases(&s, ctx.stored(LEASES_KEY).as_deref())
                .unwrap()
                .is_empty()
        );
        assert_eq!(ctx.ticks(), 2);
    }

    #[tokio::test]
    async fn a_retried_acquire_reuses_the_journaled_grant() {
        let (s, clock) = process();
        let ctx = FakeCtx::default();
        let g = acquire_model_lease(
            &s,
            &ctx,
            AcquireModelLease {
                model: "llm".into(),
            },
        )
        .await
        .unwrap();
        ctx.crash_and_retry();
        clock.set(Millis(T0 + 5_000));
        let again = acquire_model_lease(
            &s,
            &ctx,
            AcquireModelLease {
                model: "llm".into(),
            },
        )
        .await
        .unwrap();
        assert!(ctx.ran().is_empty());
        assert_eq!(again, g);
        ctx.commit();
        assert_eq!(
            list_leases(&s, ctx.stored(LEASES_KEY).as_deref()).unwrap(),
            vec![g.lease]
        );
    }

    #[tokio::test]
    async fn an_unreadable_state_makes_an_error_tick_that_stores_and_publishes_nothing() {
        let (s, _) = process();
        let ctx = FakeCtx::default();
        world(&s, 3);
        let newer = br#"{"version":99,"data":{}}"#.to_vec();
        ctx.put(STATE_KEY, &newer);
        let r = tick(&s, &ctx)
            .await
            .expect("the invocation itself succeeds");
        ctx.commit();
        assert_eq!(r.kinds, vec!["error".to_string()]);
        assert!(r.error.as_deref().unwrap().contains("version 99"), "{r:?}");
        assert_eq!(s.published_allowed(), None);
        assert_eq!(ctx.stored(STATE_KEY), Some(newer), "left untouched");
        assert!(
            s.metrics
                .render()
                .contains("sched_persisted_state_errors_total{key=\"scheduler_state\"} 1")
        );
        assert_eq!(s.last_report(), Some(r));
        // The lease handlers keep working.
        let g = acquire_model_lease(
            &s,
            &ctx,
            AcquireModelLease {
                model: "llm".into(),
            },
        )
        .await
        .unwrap();
        ctx.commit();
        assert!(g.created);
        // Recovery: a readable document is not discarded by mistake, the unreadable one is.
        let e = discard_state(
            &ctx,
            DiscardState {
                key: LEASES_KEY.into(),
                force: false,
            },
        )
        .await
        .unwrap_err();
        assert!(format!("{e:?}").contains("reads fine"), "{e:?}");
        let d = discard_state(
            &ctx,
            DiscardState {
                key: STATE_KEY.into(),
                force: false,
            },
        )
        .await
        .unwrap();
        ctx.commit();
        assert!(d.previous.unwrap().contains("\"version\":99"));
        let r = tick(&s, &ctx).await.unwrap();
        ctx.commit();
        assert_eq!(r.error, None, "{r:?}");
        assert!(s.published_allowed().is_some());
    }

    #[tokio::test]
    async fn an_unreadable_lease_book_fails_the_tick_and_the_lease_requests_clearly() {
        let (s, _) = process();
        let ctx = FakeCtx::default();
        world(&s, 3);
        ctx.put(LEASES_KEY, br#"{"next":1,"leases":[]}"#);
        let r = tick(&s, &ctx).await.unwrap();
        assert!(r.error.as_deref().unwrap().contains("lease_book"), "{r:?}");
        assert_eq!(s.published_allowed(), None);
        let e = acquire_model_lease(
            &s,
            &ctx,
            AcquireModelLease {
                model: "llm".into(),
            },
        )
        .await
        .unwrap_err();
        assert!(format!("{e:?}").contains("discard_state"), "{e:?}");
        assert!(list_leases(&s, ctx.stored(LEASES_KEY).as_deref()).is_err());
    }
}
