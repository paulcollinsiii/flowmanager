//! The scheduler as a Restate virtual object, invoked directly by Restate. There is one
//! instance (key [`KEY`]).
//!
//! Handlers:
//! - `observe`: an external pusher hands in observations; they are dated on receipt.
//! - `tick`: one decision from the cached snapshot. The decision is logged and kept as
//!   `last_decision`; nothing is executed.
//! - `arm_backstop` / `backstop`: the periodic backstop. `backstop` runs a tick and
//!   re-schedules itself with a delayed send to itself; `arm_backstop` starts a new chain
//!   and bumps an epoch kept in the object's state, so a chain started earlier (before a
//!   restart, say) sees a stale epoch and ends instead of doubling the ticks.
//! - `last_decision`: the most recent tick, read without blocking behind a tick.
//!
//! The wall clock is read inside `ctx.run`, so a replayed handler sees the time it first
//! saw. The observation cache lives in this process, not in Restate state: observations are
//! cheap to re-read and must be fresh anyway.

use std::sync::Arc;
use std::time::Duration;

use restate_sdk::prelude::*;
use sched_model::{Fidelity, Millis, ObservedValue, SourceId};
use sched_observe::Report;
use serde::{Deserialize, Serialize};

use crate::tick::{Shared, TickReport};

/// The key of the single scheduler instance.
pub const KEY: &str = "main";

const LAST_DECISION: &str = "last_decision";
const EPOCH: &str = "backstop_epoch";

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

    async fn run_tick(&self, ctx: &ObjectContext<'_>) -> Result<TickReport, HandlerError> {
        let now = self.now(ctx).await?;
        let report = self.shared.tick(now);
        ctx.set(LAST_DECISION, Json(report.clone()));
        Ok(report)
    }
}

#[object]
impl Scheduler {
    /// Takes in pushed observations.
    #[handler]
    async fn observe(
        &self,
        ctx: ObjectContext<'_>,
        Json(req): Json<ObserveRequest>,
    ) -> Result<Json<ObserveAck>, HandlerError> {
        let now = self.now(&ctx).await?;
        let ack = self.handle_observe(now, req);
        Ok(Json(ack))
    }

    /// One decision. Logged and remembered; never executed.
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
