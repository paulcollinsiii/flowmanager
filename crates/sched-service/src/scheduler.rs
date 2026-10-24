//! The scheduler as a Restate virtual object, invoked directly by Restate. There is one
//! instance (key [`KEY`]).
//!
//! Handlers:
//! - `observe` (ingress-private): pushed observations; they are dated on receipt. It can
//!   only be called from other Restate handlers, not through the ingress: the in-process
//!   observers write to the cache directly, and a forged push (`pods: 0`, say) could
//!   otherwise let a second model load.
//! - `tick`: one decision. It loads the durable state, decides inside a journaled step
//!   (`ctx.run`), stores the new state, and only then publishes the allowed set in process.
//!   A retried or replayed invocation gets the journaled decision back instead of deciding
//!   again, so what is published and what is stored always agree.
//! - `arm_backstop` / `backstop`: the periodic backstop. `backstop` runs a tick and
//!   re-schedules itself with a delayed send to itself; `arm_backstop` starts a new chain
//!   and bumps an epoch kept in the object's state, so a chain started earlier (before a
//!   restart, say) sees a stale epoch and ends instead of doubling the ticks.
//! - `acquire_model_lease` / `release_lease` / `leases`: manual model leases (see
//!   [`crate::leases`]). A change asks for a tick.
//! - `last_decision`: the most recent tick, read without blocking behind a tick.
//!
//! Durable state (the object's Restate state): the sequencer state, the lease book, which
//! sources have ever reported, the last decision and the backstop epoch. The observation
//! cache lives in this process, not in Restate: observations are cheap to re-read and must
//! be fresh anyway, so after a restart the scheduler continues from its persisted state
//! and waits for fresh observations before any safety-critical step.
//!
//! The wall clock and the active config are read inside `ctx.run`, so a replayed handler
//! sees what it first saw.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use restate_sdk::prelude::*;
use sched_model::{Fidelity, Lease, LeaseId, Millis, ModelId, ObservedValue, SchedState, SourceId};
use sched_observe::Report;
use serde::{Deserialize, Serialize};

use crate::leases::{LeaseBook, LeaseError, LeaseGrant};
use crate::tick::{Durable, Shared, TickReport};

/// The key of the single scheduler instance.
pub const KEY: &str = "main";

const LAST_DECISION: &str = "last_decision";
const EPOCH: &str = "backstop_epoch";
/// The sequencer state ([`SchedState`]).
pub const STATE: &str = "sched_state";
/// The lease book ([`LeaseBook`]).
pub const LEASES: &str = "leases";
/// Sources that have ever reported values.
pub const REPORTED: &str = "reported_sources";

/// A push of observations (or of a failure) from an external observer. It carries no
/// timestamps: the scheduler dates it on receipt.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserveRequest {
    pub source: SourceId,
    /// Defaults to `event`.
    #[serde(default)]
    pub fidelity: Option<Fidelity>,
    #[serde(default)]
    pub values: Vec<ObservedValue>,
    /// If set, the source failed to be read; `values` is ignored.
    #[serde(default)]
    pub error: Option<String>,
}

impl ObserveRequest {
    pub fn into_report(self) -> Report {
        match self.error {
            Some(error) => Report::Failed {
                source: self.source,
                error,
            },
            None => Report::Observed {
                source: self.source,
                fidelity: self.fidelity.unwrap_or(crate::cache::PUSH_FIDELITY),
                values: self.values,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObserveAck {
    /// False if the source is not in the active config and the push was dropped.
    pub accepted: bool,
    /// Whether the push changed what the scheduler knows.
    pub changed: bool,
    /// Values dropped because the config has no place for them.
    pub dropped: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackstopArgs {
    pub epoch: u64,
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

fn refused(e: &LeaseError) -> HandlerError {
    tracing::warn!("lease request refused: {e}");
    TerminalError::new_with_code(e.status(), e.to_string()).into()
}

pub struct Scheduler {
    shared: Arc<Shared>,
    backstop_every: Duration,
}

impl Scheduler {
    pub fn new(shared: Arc<Shared>, backstop_every: Duration) -> Scheduler {
        Scheduler {
            shared,
            backstop_every,
        }
    }

    async fn now(&self, ctx: &ObjectContext<'_>) -> Result<Millis, HandlerError> {
        let clock = Arc::clone(&self.shared.clock);
        let ms = ctx.run(|| async move { Ok(clock.now().as_u64()) }).await?;
        Ok(Millis(ms))
    }

    /// Takes in a push, dated `now`: the receipt time read by the handler from the
    /// scheduler's clock, never anything the sender says.
    pub fn handle_observe(&self, now: Millis, req: ObserveRequest) -> ObserveAck {
        match self.shared.ingest(req.into_report(), now) {
            crate::cache::Ingest::Taken { changed, dropped } => ObserveAck {
                accepted: true,
                changed,
                dropped,
            },
            crate::cache::Ingest::UnknownSource => ObserveAck {
                accepted: false,
                changed: false,
                dropped: 0,
            },
        }
    }

    async fn lease_book(ctx: &ObjectContext<'_>) -> Result<LeaseBook, HandlerError> {
        Ok(ctx
            .get::<Json<LeaseBook>>(LEASES)
            .await?
            .map(Json::into_inner)
            .unwrap_or_default())
    }

    /// Asks for a tick once this invocation is done (the object runs one at a time).
    fn request_tick(ctx: &ObjectContext<'_>) {
        ctx.object_client::<SchedulerClient>(ctx.key())
            .tick()
            .send();
    }

    async fn run_tick(&self, ctx: &ObjectContext<'_>) -> Result<TickReport, HandlerError> {
        let durable = Durable {
            state: ctx
                .get::<Json<SchedState>>(STATE)
                .await?
                .map(Json::into_inner)
                .unwrap_or_default(),
            leases: Self::lease_book(ctx).await?.leases,
            reported: ctx
                .get::<Json<BTreeSet<SourceId>>>(REPORTED)
                .await?
                .map(Json::into_inner)
                .unwrap_or_default(),
        };
        let shared = Arc::clone(&self.shared);
        // The decision is journaled: on a retry or replay this returns the recorded outcome
        // without running the closure again.
        let Json(outcome) = ctx
            .run(move || async move {
                let now = shared.clock.now();
                Ok(Json(shared.decide(now, &durable)))
            })
            .name("decide")
            .await?;
        ctx.set(STATE, Json(outcome.report.state.clone()));
        ctx.set(REPORTED, Json(outcome.reported.clone()));
        ctx.set(LAST_DECISION, Json(outcome.report.clone()));
        self.shared.publish(&outcome);
        Ok(outcome.report)
    }
}

#[object]
impl Scheduler {
    /// Takes in pushed observations. Ingress-private: only other Restate handlers may call it.
    #[handler(ingress_private)]
    async fn observe(
        &self,
        ctx: ObjectContext<'_>,
        Json(req): Json<ObserveRequest>,
    ) -> Result<Json<ObserveAck>, HandlerError> {
        let now = self.now(&ctx).await?;
        let ack = self.handle_observe(now, req);
        Ok(Json(ack))
    }

    /// One decision from the cached observations and the persisted state.
    #[handler]
    async fn tick(&self, ctx: ObjectContext<'_>) -> Result<Json<TickReport>, HandlerError> {
        Ok(Json(self.run_tick(&ctx).await?))
    }

    /// Starts a backstop chain, superseding any earlier one.
    #[handler]
    async fn arm_backstop(
        &self,
        ctx: ObjectContext<'_>,
    ) -> Result<Json<BackstopArgs>, HandlerError> {
        let epoch = ctx.get::<u64>(EPOCH).await?.unwrap_or(0) + 1;
        ctx.set(EPOCH, epoch);
        let now = self.now(&ctx).await?;
        self.shared.note_backstop(now);
        ctx.object_client::<SchedulerClient>(ctx.key())
            .backstop(Json(BackstopArgs { epoch }))
            .send();
        Ok(Json(BackstopArgs { epoch }))
    }

    /// A periodic tick that re-schedules itself while its epoch is current.
    #[handler]
    async fn backstop(
        &self,
        ctx: ObjectContext<'_>,
        Json(args): Json<BackstopArgs>,
    ) -> Result<(), HandlerError> {
        if ctx.get::<u64>(EPOCH).await?.unwrap_or(0) != args.epoch {
            tracing::info!(epoch = args.epoch, "backstop chain superseded; ending");
            return Ok(());
        }
        let report = self.run_tick(&ctx).await?;
        self.shared.note_backstop(report.at);
        ctx.object_client::<SchedulerClient>(ctx.key())
            .backstop(Json(args))
            .send_after(self.backstop_every);
        Ok(())
    }

    /// Acquires a manual lease on a model, which pins it on its resource until released and
    /// cuts batch work short. Asking again for the leased model returns the same lease; a
    /// different model of the same resource is refused (409); an unknown model too (400).
    #[handler]
    async fn acquire_model_lease(
        &self,
        ctx: ObjectContext<'_>,
        Json(req): Json<AcquireModelLease>,
    ) -> Result<Json<LeaseGrant>, HandlerError> {
        let book = Self::lease_book(&ctx).await?;
        let shared = Arc::clone(&self.shared);
        // The config and the clock are read inside the journaled step, so a replay gets
        // the same answer even if the config was reloaded meanwhile.
        let Json(result) = ctx
            .run(move || async move {
                let mut book = book;
                let config = shared.config.current();
                let now = shared.clock.now();
                Ok(Json(
                    book.acquire_manual_model(&config, &req.model, now)
                        .map(|grant| (grant, book)),
                ))
            })
            .name("acquire_model_lease")
            .await?;
        let (grant, book) = result.map_err(|e| refused(&e))?;
        if grant.created {
            ctx.set(LEASES, Json(book));
            Self::request_tick(&ctx);
            tracing::info!(lease = %grant.lease.id, resource = %grant.lease.resource, "manual model lease acquired");
        }
        Ok(Json(grant))
    }

    /// Releases a lease by id and returns it; an unknown id is refused (404).
    #[handler]
    async fn release_lease(
        &self,
        ctx: ObjectContext<'_>,
        Json(req): Json<ReleaseLease>,
    ) -> Result<Json<Lease>, HandlerError> {
        let mut book = Self::lease_book(&ctx).await?;
        let lease = book.release(&req.id).map_err(|e| refused(&e))?;
        ctx.set(LEASES, Json(book));
        Self::request_tick(&ctx);
        tracing::info!(lease = %lease.id, resource = %lease.resource, "lease released");
        Ok(Json(lease))
    }

    /// Every lease held.
    #[handler]
    async fn leases(&self, ctx: SharedObjectContext<'_>) -> Result<Json<Vec<Lease>>, HandlerError> {
        Ok(Json(
            ctx.get::<Json<LeaseBook>>(LEASES)
                .await?
                .map(|j| j.into_inner().leases)
                .unwrap_or_default(),
        ))
    }

    /// The most recent tick, if any.
    #[handler]
    async fn last_decision(
        &self,
        ctx: SharedObjectContext<'_>,
    ) -> Result<Json<Option<TickReport>>, HandlerError> {
        Ok(Json(
            ctx.get::<Json<TickReport>>(LAST_DECISION)
                .await?
                .map(|j| j.into_inner()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ManualClock;
    use crate::config_source::ConfigHolder;
    use crate::metrics::Metrics;
    use sched_model::test_support::EXAMPLE_CONFIG;

    fn scheduler(clock_at: u64) -> Scheduler {
        let holder = Arc::new(ConfigHolder::new(EXAMPLE_CONFIG).unwrap());
        let clock = Arc::new(ManualClock::new(Millis(clock_at)));
        let shared = Arc::new(Shared::new(holder, Arc::new(Metrics::new()), clock));
        Scheduler::new(shared, Duration::from_secs(30))
    }

    fn request(json: &str) -> ObserveRequest {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn a_push_is_dated_with_the_receipt_time_not_with_anything_the_sender_says() {
        // The clock reads 9_999 but the handler was given 5_000 (the journaled receipt time).
        let s = scheduler(9_999);
        let ack = s.handle_observe(
            Millis(5_000),
            request(
                r#"{"source": "paperless-ocr-pending", "values": [
                    {"type": "demand", "class": "paperless-ocr", "ready": 2,
                     "oldest_ready_at": 18446744073709551615}]}"#,
            ),
        );
        assert!(ack.accepted && ack.changed && ack.dropped == 0);
        let config = s.shared.config.current();
        let snap = s
            .shared
            .cache
            .lock()
            .unwrap()
            .snapshot(&config, Millis(5_100));
        let o = &snap.observations[0];
        assert_eq!(o.observed_at, Millis(5_000));
        assert!(matches!(
            o.value,
            ObservedValue::Demand {
                oldest_ready_at: Some(Millis(5_000)),
                ..
            }
        ));
    }

    #[test]
    fn only_observe_is_ingress_private_in_the_registered_metadata() {
        use restate_sdk::service::Discoverable;
        let d = <Scheduler as Discoverable>::discover();
        let mut seen: Vec<(String, Option<bool>)> = d
            .handlers
            .iter()
            .map(|h| (h.name.to_string(), h.ingress_private))
            .collect();
        seen.sort();
        assert_eq!(
            seen,
            vec![
                ("acquire_model_lease".to_string(), None),
                ("arm_backstop".to_string(), None),
                ("backstop".to_string(), None),
                ("last_decision".to_string(), None),
                ("leases".to_string(), None),
                ("observe".to_string(), Some(true)),
                ("release_lease".to_string(), None),
                ("tick".to_string(), None),
            ]
        );
        assert_eq!(d.ingress_private, None);
    }

    #[test]
    fn a_push_cannot_carry_its_own_timestamp_and_unknown_sources_are_refused() {
        assert!(
            serde_json::from_str::<ObserveRequest>(
                r#"{"source": "s", "observed_at": 1, "values": []}"#
            )
            .is_err()
        );
        let s = scheduler(1);
        let ack = s.handle_observe(Millis(1), request(r#"{"source": "nowhere"}"#));
        assert!(!ack.accepted);
        // An error push is recorded as a failure of the source, not as values.
        let ack = s.handle_observe(
            Millis(2),
            request(r#"{"source": "paperless-ocr-pending", "error": "boom"}"#),
        );
        assert!(ack.accepted && ack.changed);
    }
}
