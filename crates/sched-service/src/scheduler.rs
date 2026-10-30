//! The scheduler as a Restate virtual object, invoked directly by Restate. There is one
//! instance (key [`KEY`]). The handlers' logic lives in [`crate::object`], written against
//! a small context trait that this module implements for Restate's context.
//!
//! Handlers:
//! - `observe` (ingress-private): pushed observations; they are dated on receipt. It can
//!   only be called from other Restate handlers, not through the ingress: the in-process
//!   observers write to the cache directly, and a forged push (a host reading `up`, or
//!   queue demand, say) could otherwise steer decisions.
//! - `tick`: one decision. It loads the durable state, decides inside a journaled step
//!   (`ctx.run`), stores the new state, and only then publishes the allowed set in process.
//!   A retried or replayed invocation gets the journaled decision back instead of deciding
//!   again, so what is published and what is stored always agree. Answers a short
//!   [`TickAck`]; the full report is `last_decision`.
//! - `arm_backstop` / `backstop`: the periodic backstop. `backstop` runs a tick and
//!   re-schedules itself with a delayed send to itself; `arm_backstop` starts a new chain
//!   and bumps an epoch kept in the object's state, so a chain started earlier (before a
//!   restart, say) sees a stale epoch and ends instead of doubling the ticks.
//! - `acquire_host_lease` / `release_host_lease`: a manual lease on a host with a power section (an NFS
//!   server, or a GPU host): it powers a managed host on and keeps it up. `reclaim` / `unreclaim`: take an
//!   exclusive resource with a power section back, and give it up again. `resume`: for a
//!   person who fixed a host by hand, clears what flowmanager stopped at on one resource (a
//!   failed power transition, or a shutdown stopped by mounting pods) so it follows
//!   observation again. Like the lease handlers they are reachable through the ingress, which
//!   is trusted-network only.
//! - `acquire_model_lease` / `release_lease` / `leases`: manual model leases (see
//!   [`crate::leases`]). A change asks for a tick.
//! - `discard_state`: the recovery path for a persisted document that cannot be read.
//! - `last_decision`: the latest tick report this process published (kept in memory, so
//!   the report is journaled once per tick; `null` until the first tick of the process).
//!
//! Two more objects carry out actions beside the ticks: `Recovery` (one instance per class,
//! below) runs recovery actions, and `Power` (one instance per resource) makes the power calls;
//! the `tick` handler only sends them one-way requests.
//!
//! Durable state (the object's Restate state, see [`crate::persist`]): `scheduler_state`
//! (the sequencer state and which sources have ever reported), `lease_book`, and
//! `backstop_epoch`. The observation cache lives in this process, not in Restate:
//! observations are cheap to re-read and must be fresh anyway, so after a restart the
//! scheduler continues from its persisted state and waits for fresh observations before
//! any safety-critical step.

use std::sync::Arc;
use std::time::Duration;

use restate_sdk::prelude::*;
use sched_actuate::PowerBackend;
use sched_model::{Fidelity, Lease, Millis, ObservedValue, SourceId};
use sched_observe::Report;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::jobs::JobSummary;
use crate::leases::LeaseGrant;
use crate::object::{
    self, AcquireModelLease, BoxFut, DiscardState, Discarded, DrainAck, DrainHost, ObjectCtx,
    ReleaseHostLease, ReleaseLease, ResourceRequest, TickAck, TriggerJob,
};
use crate::persist::{EPOCH_KEY, JOBS_KEY, LEASES_KEY, POWER_KEY, RECOVERY_KEY};
use crate::power::{self, PowerAck, PowerAction, PowerRecord, PowerRequest};
use crate::recovery::{self, DiscardAttempts, Env, RunAck};
use crate::resume::{Resume, ResumeAck};
use crate::tick::{Shared, TickReport};

/// The key of the single scheduler instance.
pub const KEY: &str = "main";

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

/// [`ObjectCtx`] over Restate's exclusive-handler context.
struct Rctx<'a, 'b>(&'a ObjectContext<'b>);

impl ObjectCtx for Rctx<'_, '_> {
    fn get_raw<'a>(&'a self, key: &'a str) -> BoxFut<'a, Result<Option<Vec<u8>>, HandlerError>> {
        Box::pin(async move { Ok(self.0.get::<Vec<u8>>(key).await?) })
    }

    fn set_raw(&self, key: &str, value: Vec<u8>) {
        self.0.set(key, value);
    }

    fn clear_key(&self, key: &str) {
        self.0.clear(key);
    }

    fn run_journaled<T, F>(&self, name: &'static str, f: F) -> BoxFut<'_, Result<T, HandlerError>>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        Box::pin(async move {
            let Json(v) = self
                .0
                .run(move || async move { Ok(Json(f())) })
                .name(name)
                .await?;
            Ok(v)
        })
    }

    fn run_journaled_async<T, F>(
        &self,
        name: &'static str,
        f: F,
    ) -> BoxFut<'_, Result<T, HandlerError>>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        F: FnOnce() -> BoxFut<'static, T> + Send + 'static,
    {
        Box::pin(async move {
            let Json(v) = self
                .0
                .run(move || async move { Ok(Json(f().await)) })
                .name(name)
                .await?;
            Ok(v)
        })
    }

    fn record_durably<'a>(
        &'a self,
        key: &'a str,
        value: Vec<u8>,
    ) -> BoxFut<'a, Result<(), HandlerError>> {
        // A journaled step completes only after every command before it (the state write)
        // is stored, so once it has returned the write survives a crash.
        Box::pin(async move {
            self.0.set(key, value);
            self.0
                .run(|| async { Ok(Json(0u8)) })
                .name("durable_barrier")
                .await?;
            Ok(())
        })
    }

    fn request_tick(&self) {
        self.0
            .object_client::<SchedulerClient>(self.0.key())
            .tick()
            .send();
    }

    fn request_recovery(&self, class: &str) {
        self.0.object_client::<RecoveryClient>(class).run().send();
    }

    fn request_power(&self, resource: &str, action: PowerAction) {
        self.0
            .object_client::<PowerClient>(resource)
            .run(Json(PowerRequest { action }))
            .send();
    }
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
        let ms = Rctx(ctx)
            .run_journaled("clock", move || clock.now().as_u64())
            .await?;
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
    async fn tick(&self, ctx: ObjectContext<'_>) -> Result<Json<TickAck>, HandlerError> {
        let report = object::tick(&self.shared, &Rctx(&ctx)).await?;
        Ok(Json(TickAck::from(&report)))
    }

    /// Starts a backstop chain, superseding any earlier one.
    #[handler]
    async fn arm_backstop(
        &self,
        ctx: ObjectContext<'_>,
    ) -> Result<Json<BackstopArgs>, HandlerError> {
        let epoch = ctx.get::<u64>(EPOCH_KEY).await?.unwrap_or(0) + 1;
        ctx.set(EPOCH_KEY, epoch);
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
        if ctx.get::<u64>(EPOCH_KEY).await?.unwrap_or(0) != args.epoch {
            tracing::info!(epoch = args.epoch, "backstop chain superseded; ending");
            return Ok(());
        }
        let report = object::tick(&self.shared, &Rctx(&ctx)).await?;
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
        Ok(Json(
            object::acquire_model_lease(&self.shared, &Rctx(&ctx), req).await?,
        ))
    }

    /// Acquires a manual lease on a host with a power section, shared (an NFS server) or
    /// exclusive (a GPU): a managed host that is off is powered on and everything gated on it
    /// is allowed once it is up; the host is kept up until the lease is released (on an
    /// exclusive host the queues still choose the model). Asking again returns the same
    /// lease; a resource without a power section or an unknown one is refused (400).
    #[handler]
    async fn acquire_host_lease(
        &self,
        ctx: ObjectContext<'_>,
        Json(req): Json<ResourceRequest>,
    ) -> Result<Json<LeaseGrant>, HandlerError> {
        Ok(Json(
            object::acquire_host_lease(&self.shared, &Rctx(&ctx), req).await?,
        ))
    }

    /// Releases the manual host lease on a resource and returns it; none held is refused
    /// (404). The host is then shut down only once nothing holds it, idle grace has passed and
    /// no pod mounts it. With `"drain": "graceful"` or `"immediate"` (a shared host only; an
    /// exclusive host has no drain: its release is always plain) the host is taken down
    /// even though work is queued or in flight: its consumers are scaled down (at once, or
    /// after at most `drain_timeout` for graceful) and it is shut down once no lease holds it
    /// and no pod mounts it; a new manual host lease cancels the drain.
    #[handler]
    async fn release_host_lease(
        &self,
        ctx: ObjectContext<'_>,
        Json(req): Json<ReleaseHostLease>,
    ) -> Result<Json<Lease>, HandlerError> {
        Ok(Json(
            object::release_host_lease(&self.shared, &Rctx(&ctx), req).await?,
        ))
    }

    /// Takes a shared host down on request (an exclusive one is refused: release its host lease), with or without a manual lease held: releases
    /// the lease if there is one and records a drain (`graceful` or `immediate`; see
    /// `release_host_lease`). Asking again changes the mode of the drain under way
    /// (`immediate` escalates, `graceful` leaves an immediate one). Answers the drain recorded,
    /// absent when the host is not up under managed control.
    #[handler]
    async fn drain_host(
        &self,
        ctx: ObjectContext<'_>,
        Json(req): Json<DrainHost>,
    ) -> Result<Json<DrainAck>, HandlerError> {
        Ok(Json(
            object::drain_host(&self.shared, &Rctx(&ctx), req).await?,
        ))
    }

    /// Reclaims an exclusive resource with a power section: intake stops, its work is cut
    /// off after the reclaim timeout, the host is shut down (if its power is managed) and it
    /// is not woken until `unreclaim`. Asking again returns the same lease; a resource that
    /// cannot be reclaimed (shared, or without a power section) or an unknown one is refused
    /// (400).
    #[handler]
    async fn reclaim(
        &self,
        ctx: ObjectContext<'_>,
        Json(req): Json<ResourceRequest>,
    ) -> Result<Json<LeaseGrant>, HandlerError> {
        Ok(Json(object::reclaim(&self.shared, &Rctx(&ctx), req).await?))
    }

    /// Releases the reclaim on a resource and returns it; none held is refused (404).
    #[handler]
    async fn unreclaim(
        &self,
        ctx: ObjectContext<'_>,
        Json(req): Json<ResourceRequest>,
    ) -> Result<Json<Lease>, HandlerError> {
        Ok(Json(
            object::unreclaim(&self.shared, &Rctx(&ctx), req).await?,
        ))
    }

    /// For a person who has fixed a host by hand: clears the failed power transition (or the
    /// shutdown stopped by mounting pods) of one resource, so it follows observation again
    /// (found running means up and not managed). Other resources' state is untouched. A
    /// resource that is not stopped is refused (409), an unknown one or one without a power
    /// section (400).
    #[handler]
    async fn resume(
        &self,
        ctx: ObjectContext<'_>,
        Json(req): Json<Resume>,
    ) -> Result<Json<ResumeAck>, HandlerError> {
        Ok(Json(object::resume(&self.shared, &Rctx(&ctx), req).await?))
    }

    /// Releases a lease by id and returns it; an unknown id is refused (404).
    #[handler]
    async fn release_lease(
        &self,
        ctx: ObjectContext<'_>,
        Json(req): Json<ReleaseLease>,
    ) -> Result<Json<Lease>, HandlerError> {
        Ok(Json(
            object::release_lease(&self.shared, &Rctx(&ctx), req).await?,
        ))
    }

    /// Every lease held.
    #[handler]
    async fn leases(&self, ctx: SharedObjectContext<'_>) -> Result<Json<Vec<Lease>>, HandlerError> {
        let raw = ctx.get::<Vec<u8>>(LEASES_KEY).await?;
        Ok(Json(object::list_leases(&self.shared, raw.as_deref())?))
    }

    /// Starts a run of a scheduled job now (`{"id": "backup"}`): it waits for the job's needs
    /// and then the Job is created, like a scheduled run. Refused (409) while the previous
    /// instance is pending or running; an unknown job is 404.
    #[handler]
    async fn trigger_job(
        &self,
        ctx: ObjectContext<'_>,
        Json(req): Json<TriggerJob>,
    ) -> Result<Json<JobSummary>, HandlerError> {
        Ok(Json(
            object::trigger_job(&self.shared, &Rctx(&ctx), req).await?,
        ))
    }

    /// Every configured scheduled job with its state: next time, the run in progress, the
    /// last outcome and the last success.
    #[handler]
    async fn jobs(
        &self,
        ctx: SharedObjectContext<'_>,
    ) -> Result<Json<Vec<JobSummary>>, HandlerError> {
        let raw = ctx.get::<Vec<u8>>(JOBS_KEY).await?;
        Ok(Json(object::list_jobs(&self.shared, raw.as_deref())?))
    }

    /// Discards a persisted document (`scheduler_state` or `lease_book`) that cannot be
    /// read, so the next tick starts it afresh; a readable one only with `force`. Returns
    /// what was stored.
    #[handler]
    async fn discard_state(
        &self,
        ctx: ObjectContext<'_>,
        Json(req): Json<DiscardState>,
    ) -> Result<Json<Discarded>, HandlerError> {
        Ok(Json(object::discard_state(&Rctx(&ctx), req).await?))
    }

    /// The latest tick report this process published, if any.
    #[handler]
    async fn last_decision(
        &self,
        _ctx: SharedObjectContext<'_>,
    ) -> Result<Json<Option<TickReport>>, HandlerError> {
        Ok(Json(self.shared.last_report()))
    }
}

/// The recovery action of one class (the object key is the class id), kept apart from the
/// `Scheduler` so that its requests never hold up a tick (see [`crate::recovery`]). Its state
/// is the key `recovery_attempts` (versioned).
///
/// Handlers:
/// - `run` (ingress-private): one run of the class's recovery action. Sent by the `tick`
///   handler, one way, whenever the decision carries a `RunRecovery` step for the class.
/// - `discard_attempts`: the recovery path for an attempts document that cannot be read.
/// - `attempts`: the stored document, as text (`null` if none).
pub struct Recovery {
    shared: Arc<Shared>,
    env: Env,
}

impl Recovery {
    pub fn new(shared: Arc<Shared>, env: Env) -> Recovery {
        Recovery { shared, env }
    }
}

#[object]
impl Recovery {
    /// Runs the class's recovery action once (the key is the class id).
    #[handler(ingress_private)]
    async fn run(&self, ctx: ObjectContext<'_>) -> Result<Json<RunAck>, HandlerError> {
        let class = ctx.key().to_string();
        Ok(Json(
            recovery::run(&self.shared, &self.env, &Rctx(&ctx), &class).await?,
        ))
    }

    /// Discards an attempts document that cannot be read; a readable one only with `force`.
    #[handler]
    async fn discard_attempts(
        &self,
        ctx: ObjectContext<'_>,
        Json(req): Json<DiscardAttempts>,
    ) -> Result<Json<Option<String>>, HandlerError> {
        Ok(Json(recovery::discard_attempts(&Rctx(&ctx), req).await?))
    }

    /// The stored attempts document, as text.
    #[handler]
    async fn attempts(
        &self,
        ctx: SharedObjectContext<'_>,
    ) -> Result<Json<Option<String>>, HandlerError> {
        let raw = ctx.get::<Vec<u8>>(RECOVERY_KEY).await?;
        Ok(Json(raw.map(|b| String::from_utf8_lossy(&b).into_owned())))
    }
}

/// The power actions of one resource (the object key is the resource id), kept apart from
/// the `Scheduler` so that a slow router or host never holds up a tick (see
/// [`crate::power`]). Its state is the key `power_last_action` (versioned).
///
/// Handlers:
/// - `run` (ingress-private): one power action. Sent by the `tick` handler, one way, once
///   per `PowerOn` or `PowerOff` step of the decision.
/// - `last_action`: the last action, when it ran and how it ended (`null` if none).
pub struct Power {
    shared: Arc<Shared>,
    backend: Arc<dyn PowerBackend>,
}

impl Power {
    pub fn new(shared: Arc<Shared>, backend: Arc<dyn PowerBackend>) -> Power {
        Power { shared, backend }
    }
}

#[object]
impl Power {
    /// Carries out one power action (the key is the resource id). It never fails the
    /// invocation: a failed call is the answer, and nothing retries it.
    #[handler(ingress_private)]
    async fn run(
        &self,
        ctx: ObjectContext<'_>,
        Json(req): Json<PowerRequest>,
    ) -> Result<Json<PowerAck>, HandlerError> {
        let resource = ctx.key().to_string();
        Ok(Json(
            power::run(&self.shared, &self.backend, &Rctx(&ctx), &resource, req).await?,
        ))
    }

    /// The last action of the resource.
    #[handler]
    async fn last_action(
        &self,
        ctx: SharedObjectContext<'_>,
    ) -> Result<Json<Option<PowerRecord>>, HandlerError> {
        let raw = ctx.get::<Vec<u8>>(POWER_KEY).await?;
        Ok(Json(power::stored_record(raw.as_deref())?))
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
                ("acquire_host_lease".to_string(), None),
                ("acquire_model_lease".to_string(), None),
                ("arm_backstop".to_string(), None),
                ("backstop".to_string(), None),
                ("discard_state".to_string(), None),
                ("drain_host".to_string(), None),
                ("jobs".to_string(), None),
                ("last_decision".to_string(), None),
                ("leases".to_string(), None),
                ("observe".to_string(), Some(true)),
                ("reclaim".to_string(), None),
                ("release_host_lease".to_string(), None),
                ("release_lease".to_string(), None),
                ("resume".to_string(), None),
                ("tick".to_string(), None),
                ("trigger_job".to_string(), None),
                ("unreclaim".to_string(), None),
            ]
        );
        assert_eq!(d.ingress_private, None);
    }

    #[test]
    fn the_recovery_object_is_registered_with_its_own_handlers() {
        use restate_sdk::service::Discoverable;
        let d = <Recovery as Discoverable>::discover();
        let mut seen: Vec<String> = d.handlers.iter().map(|h| h.name.to_string()).collect();
        seen.sort();
        assert_eq!(seen, ["attempts", "discard_attempts", "run"]);
        // `run` is called only by the tick; `attempts` and `discard_attempts` stay public,
        // like the Scheduler's `discard_state`.
        let private: Vec<_> = d
            .handlers
            .iter()
            .filter(|h| h.ingress_private == Some(true))
            .map(|h| h.name.to_string())
            .collect();
        assert_eq!(private, ["run"]);
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
