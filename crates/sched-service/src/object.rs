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
use sched_model::{Config, Lease, LeaseId, Millis, ModelId, ResourceId};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::leases::{LeaseBook, LeaseError, LeaseGrant};
use crate::persist::{self, CoreState, LEASES_KEY, STATE_KEY, StateError};
use crate::power::PowerAction;
use crate::resume::{self, Resume, ResumeAck};
use crate::tick::{Durable, Shared, TickOutcome, TickReport};

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
    /// As [`ObjectCtx::run_journaled`] for a step that does I/O: the recorded result is
    /// returned on a retry or replay and the future is not run again. The step's own errors
    /// belong in `T`: a step that fails is recorded, never retried by Restate.
    fn run_journaled_async<T, F>(
        &self,
        name: &'static str,
        f: F,
    ) -> BoxFut<'_, Result<T, HandlerError>>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        F: FnOnce() -> BoxFut<'static, T> + Send + 'static;
    /// Stores `value` under `key` and does not return before it is durable: whatever runs
    /// after this call may rely on it, even across a crash (a plain `set_raw` only counts
    /// once the invocation completes or a later step is journaled).
    fn record_durably<'a>(
        &'a self,
        key: &'a str,
        value: Vec<u8>,
    ) -> BoxFut<'a, Result<(), HandlerError>>;
    /// Asks for a `tick` once this invocation is done.
    fn request_tick(&self);
    /// Sends the `Recovery` object of `class` a one-way request to run its action. It never
    /// blocks this invocation: the `Recovery` object has its own lock.
    fn request_recovery(&self, class: &str);
    /// Sends the `Power` object of `resource` a one-way request to carry out `action`. It
    /// never blocks this invocation, and Restate journals the send, so a retried invocation
    /// does not send it again.
    fn request_power(&self, resource: &str, action: PowerAction);
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

/// A request about one resource: `acquire_host_lease`, `release_host_lease`, `reclaim`,
/// `unreclaim`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRequest {
    pub resource: ResourceId,
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
            "the persisted {e}; see the README section 'Versions, and recovering the persisted state' (the discard_state handler)"
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
    // The jobs' state is read apart: an unreadable document switches the job step off for
    // this tick (loudly) instead of stopping the GPU and NFS scheduling with it.
    let jobs_raw = ctx.get_raw(persist::JOBS_KEY).await?;
    let (jobs, jobs_off) = match persist::decode_jobs(jobs_raw.as_deref()) {
        Ok(b) => (b, false),
        Err(e) => {
            shared.metrics.persisted_state_error(&e.key);
            tracing::error!(
                event = "job_book_unreadable",
                "ALERT: the scheduled jobs' state cannot be read ({e}); jobs and their holds are left as they are until it is discarded (the discard_state handler)"
            );
            (Default::default(), true)
        }
    };
    let next_lease = book.next;
    let durable = Durable {
        state: core.state,
        leases: book.leases,
        reported: core.reported,
        jobs,
        jobs_off,
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
    let mut outcome = outcome;
    ctx.set_raw(
        STATE_KEY,
        persist::encode_state(&CoreState {
            state: outcome.report.state.clone(),
            reported: outcome.reported.clone(),
        }),
    );
    if !jobs_off {
        ctx.set_raw(
            LEASES_KEY,
            persist::encode_leases(&LeaseBook {
                next: next_lease,
                leases: outcome.leases.clone(),
            }),
        );
        ctx.set_raw(persist::JOBS_KEY, persist::encode_jobs(&outcome.jobs));
        create_jobs(shared, ctx, &mut outcome).await?;
    }
    shared.publish(&outcome, !ran.load(Ordering::SeqCst));
    // Recovery runs in its own object, after the allowed set is published (it re-checks it).
    // Asked for on every tick that emits the step: the action is idempotent and gates itself.
    // Power steps are asked for once per step: the sequencer emits one per transition (it
    // records the host as powering on or shutting down), and the decision is journaled.
    for step in &outcome.report.steps {
        match step {
            sched_model::Step::RunRecovery { class } => ctx.request_recovery(class.as_str()),
            sched_model::Step::PowerOn { resource } => {
                ctx.request_power(resource.as_str(), PowerAction::On)
            }
            sched_model::Step::PowerOff { resource } => {
                ctx.request_power(resource.as_str(), PowerAction::Off)
            }
            _ => {}
        }
    }
    Ok(outcome.report)
}

/// Creates the Jobs the job step asked for, each in its own journaled step (the call is made
/// once; a retried invocation gets the recorded result). The name is deterministic and a Job
/// that exists counts as made, so even a crash between the call and its journal entry is
/// safe. The results go back through the state machine, and the new state is stored.
async fn create_jobs<C: ObjectCtx>(
    shared: &Arc<Shared>,
    ctx: &C,
    outcome: &mut TickOutcome,
) -> Result<(), HandlerError> {
    if outcome.creates.is_empty() {
        return Ok(());
    }
    let config = shared.config.current();
    let backend = shared.job_backend();
    let decided_at = outcome.report.at;
    for c in outcome.creates.clone() {
        let Some(spec) = config.job(&c.job).cloned() else {
            continue;
        };
        let (b, cronjob, job, name, scheduled_at) = (
            backend.clone(),
            spec.cronjob.clone(),
            c.job.clone(),
            c.name.clone(),
            c.scheduled_at,
        );
        let clock = Arc::clone(&shared.clock);
        // The time is taken when the call returns and journaled with its result. The decision
        // was made earlier (the calls take up to 15 s), and a reading received in between
        // does not show the Job: the run must be stamped with a time at which the Job really
        // existed, or such a reading would later prove it deleted.
        let (result, returned_at): (Result<(), String>, Millis) = ctx
            .run_journaled_async("create_job", move || {
                Box::pin(async move {
                    let r = match b {
                        Some(b) => b.create(cronjob, job, name, scheduled_at).await.map(|_| ()),
                        None => Err("no Kubernetes client is configured for jobs".to_string()),
                    };
                    (r, clock.now())
                })
            })
            .await?;
        let at = returned_at.max(decided_at);
        let rec = outcome.jobs.jobs.entry(c.job.clone()).or_default();
        let events = sched_sequencer::jobs::created(
            rec,
            &spec,
            at,
            match result {
                Ok(()) => sched_sequencer::jobs::Created::Made,
                Err(e) => sched_sequencer::jobs::Created::Failed(e),
            },
        );
        outcome.report.job_events.extend(events);
    }
    outcome.report.jobs = crate::jobs::summaries(&config, &outcome.jobs);
    ctx.set_raw(persist::JOBS_KEY, persist::encode_jobs(&outcome.jobs));
    Ok(())
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

/// Acquires a lease on a resource with `grant` (see `leases`). As for a model lease, the
/// config and the clock are read inside the journaled step, so a replay gets the same answer
/// even if the config was reloaded, and an existing lease is returned without a write.
async fn acquire_resource_lease<C: ObjectCtx>(
    shared: &Arc<Shared>,
    ctx: &C,
    step: &'static str,
    what: &'static str,
    req: ResourceRequest,
    grant: fn(&mut LeaseBook, &Config, &ResourceId, Millis) -> Result<LeaseGrant, LeaseError>,
) -> Result<LeaseGrant, HandlerError> {
    let book = read_leases(ctx)
        .await?
        .map_err(|e| unreadable(shared, &e))?;
    let s = Arc::clone(shared);
    let result = ctx
        .run_journaled(step, move || {
            let mut book = book;
            let config = s.config.current();
            let now = s.clock.now();
            grant(&mut book, &config, &req.resource, now).map(|grant| (grant, book))
        })
        .await?;
    let (grant, book) = result.map_err(|e| lease_refused(&e))?;
    if grant.created {
        ctx.set_raw(LEASES_KEY, persist::encode_leases(&book));
        ctx.request_tick();
        tracing::info!(lease = %grant.lease.id, resource = %grant.lease.resource, "{what} lease acquired");
    }
    Ok(grant)
}

/// Acquires a manual lease on a shared host (see `LeaseBook::acquire_manual_host`).
pub async fn acquire_host_lease<C: ObjectCtx>(
    shared: &Arc<Shared>,
    ctx: &C,
    req: ResourceRequest,
) -> Result<LeaseGrant, HandlerError> {
    acquire_resource_lease(
        shared,
        ctx,
        "acquire_host_lease",
        "manual host",
        req,
        LeaseBook::acquire_manual_host,
    )
    .await
}

/// Reclaims an exclusive resource (see `LeaseBook::acquire_reclaim`).
pub async fn reclaim<C: ObjectCtx>(
    shared: &Arc<Shared>,
    ctx: &C,
    req: ResourceRequest,
) -> Result<LeaseGrant, HandlerError> {
    acquire_resource_lease(
        shared,
        ctx,
        "reclaim",
        "reclaim",
        req,
        LeaseBook::acquire_reclaim,
    )
    .await
}

/// Releases the lease of one kind a resource holds, the way `release_lease` does by id.
async fn release_resource_lease<C: ObjectCtx>(
    shared: &Arc<Shared>,
    ctx: &C,
    req: ResourceRequest,
    release: fn(&mut LeaseBook, &ResourceId) -> Result<Lease, LeaseError>,
) -> Result<Lease, HandlerError> {
    let mut book = read_leases(ctx)
        .await?
        .map_err(|e| unreadable(shared, &e))?;
    let lease = release(&mut book, &req.resource).map_err(|e| lease_refused(&e))?;
    ctx.set_raw(LEASES_KEY, persist::encode_leases(&book));
    ctx.request_tick();
    tracing::info!(lease = %lease.id, resource = %lease.resource, "lease released");
    Ok(lease)
}

/// Releases the manual host lease on a resource.
pub async fn release_host_lease<C: ObjectCtx>(
    shared: &Arc<Shared>,
    ctx: &C,
    req: ResourceRequest,
) -> Result<Lease, HandlerError> {
    release_resource_lease(shared, ctx, req, LeaseBook::release_manual_host).await
}

/// Releases the reclaim on a resource.
pub async fn unreclaim<C: ObjectCtx>(
    shared: &Arc<Shared>,
    ctx: &C,
    req: ResourceRequest,
) -> Result<Lease, HandlerError> {
    release_resource_lease(shared, ctx, req, LeaseBook::release_reclaim).await
}

/// Clears what flowmanager stopped at on one resource (see `resume`): its failed power
/// transition, or its shutdown stopped by mounting pods. The state is read, the rule and the
/// config are applied inside a journaled step (a replay gets the same answer even if the
/// config was reloaded), and the new state is stored; other resources' state and the lease
/// book are left as they are.
pub async fn resume<C: ObjectCtx>(
    shared: &Arc<Shared>,
    ctx: &C,
    req: Resume,
) -> Result<ResumeAck, HandlerError> {
    let core = persist::decode_state(ctx.get_raw(STATE_KEY).await?.as_deref())
        .map_err(|e| unreadable(shared, &e))?;
    let s = Arc::clone(shared);
    let result = ctx
        .run_journaled("resume", move || {
            let mut core = core;
            let config = s.config.current();
            resume::resume(&mut core, &config, &req.resource).map(|ack| (ack, core))
        })
        .await?;
    let (ack, core) = result.map_err(|e| {
        tracing::warn!("resume refused: {e}");
        HandlerError::from(TerminalError::new_with_code(e.status(), e.to_string()))
    })?;
    ctx.set_raw(STATE_KEY, persist::encode_state(&core));
    ctx.request_tick();
    tracing::warn!(
        event = "power_resumed",
        resource = %ack.resource,
        cleared = %ack.cleared,
        "a person resumed {}: its state ({}) is cleared and it follows observation again",
        ack.resource,
        ack.cleared
    );
    Ok(ack)
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

/// `trigger_job` request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TriggerJob {
    pub id: sched_model::JobId,
}

/// What the journaled step of `trigger_job` decided.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum Triggered {
    Unknown(Vec<sched_model::JobId>),
    Refused(sched_model::JobEvent),
    Started(sched_model::JobBook, sched_model::JobEvent),
}

/// Starts a run of a job now, on a person's request: the same machine as a scheduled run
/// (it waits for the needs, then the Job is created), subject to the overlap rule: while the
/// previous instance is pending or running (or may still be) the request is refused with 409
/// and nothing is queued. The config and the clock are read inside the journaled step.
pub async fn trigger_job<C: ObjectCtx>(
    shared: &Arc<Shared>,
    ctx: &C,
    req: TriggerJob,
) -> Result<crate::jobs::JobSummary, HandlerError> {
    let book = persist::decode_jobs(ctx.get_raw(persist::JOBS_KEY).await?.as_deref())
        .map_err(|e| unreadable(shared, &e))?;
    let s = Arc::clone(shared);
    let ran = Arc::new(AtomicBool::new(false));
    let r = Arc::clone(&ran);
    let id = req.id.clone();
    let result = ctx
        .run_journaled("trigger_job", move || {
            r.store(true, Ordering::SeqCst);
            let config = s.config.current();
            let now = s.clock.now();
            let Some(spec) = config.job(&id) else {
                return Triggered::Unknown(config.jobs().iter().map(|j| j.id.clone()).collect());
            };
            let mut book = book;
            let rec = book.jobs.entry(id.clone()).or_default();
            match sched_sequencer::jobs::trigger(rec, spec, now) {
                Ok(e) => Triggered::Started(book, e),
                Err(e) => Triggered::Refused(e),
            }
        })
        .await?;
    let fresh = ran.load(Ordering::SeqCst);
    let count = |e: &sched_model::JobEvent| {
        if fresh {
            shared.metrics.job_event(e.job.as_str(), e.kind.counter());
        }
    };
    match result {
        Triggered::Unknown(known) => {
            let known: Vec<&str> = known.iter().map(|j| j.as_str()).collect();
            Err(TerminalError::new_with_code(
                404,
                format!(
                    "unknown job {:?} (configured: {})",
                    req.id.as_str(),
                    known.join(", ")
                ),
            )
            .into())
        }
        Triggered::Refused(e) => {
            count(&e);
            tracing::error!(
                event = "job_skipped_overlap",
                job = %e.job,
                "ALERT: a person asked for a run of scheduled job {} while the previous instance is pending or running; refused, not queued",
                e.job
            );
            Err(TerminalError::new_with_code(
                409,
                format!(
                    "job {:?} already has a run pending or running (or a Job past its deadline that may still be active); it is not queued",
                    req.id.as_str()
                ),
            )
            .into())
        }
        Triggered::Started(book, e) => {
            count(&e);
            ctx.set_raw(persist::JOBS_KEY, persist::encode_jobs(&book));
            ctx.request_tick();
            tracing::info!(event = "job_triggered", job = %e.job, "a person started a run of scheduled job {}", e.job);
            let config = shared.config.current();
            Ok(crate::jobs::summaries(&config, &book)
                .into_iter()
                .find(|j| j.id == req.id)
                .expect("a configured job has a summary"))
        }
    }
}

/// Every configured job with its state, from the stored job book.
pub fn list_jobs(
    shared: &Shared,
    raw: Option<&[u8]>,
) -> Result<Vec<crate::jobs::JobSummary>, HandlerError> {
    let book = persist::decode_jobs(raw).map_err(|e| unreadable(shared, &e))?;
    Ok(crate::jobs::summaries(&shared.config.current(), &book))
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
    let readable = match req.key.as_str() {
        STATE_KEY => persist::decode_state(raw.as_deref()).is_ok(),
        persist::JOBS_KEY => persist::decode_jobs(raw.as_deref()).is_ok(),
        _ => persist::decode_leases(raw.as_deref()).is_ok(),
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
        /// Classes whose recovery was requested, in order.
        pub recoveries: Mutex<Vec<String>>,
        /// Power requests sent, in order: (resource, action).
        pub powers: Mutex<Vec<(String, PowerAction)>>,
        /// Values `get_raw` returned in this attempt, replayed on a retry as Restate does.
        gets: Mutex<Vec<(String, Option<Vec<u8>>)>>,
        get_replay: Mutex<VecDeque<(String, Option<Vec<u8>>)>>,
        /// Simulated crash: at the n-th execution (1-based) of the named step in this
        /// invocation the attempt fails, before the step runs (`Before`) or after it ran but
        /// before its result was journaled (`After`).
        crash: Mutex<Option<(String, usize, Crash)>>,
        executions: Mutex<BTreeMap<String, usize>>,
        /// Number of times each key was made durable by `record_durably`.
        pub durable_writes: Mutex<Vec<String>>,
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    pub enum Crash {
        Before,
        After,
    }

    impl FakeCtx {
        pub fn ran(&self) -> Vec<String> {
            self.ran.lock().unwrap().clone()
        }

        pub fn ticks(&self) -> usize {
            self.ticks_requested.load(Ordering::SeqCst)
        }

        /// Makes the `n`-th execution (1-based) of step `name` fail (see [`Crash`]).
        pub fn crash_at(&self, name: &str, n: usize, when: Crash) {
            *self.crash.lock().unwrap() = Some((name.to_string(), n, when));
            self.executions.lock().unwrap().clear();
        }

        fn should_crash(&self, name: &str) -> Option<Crash> {
            let n = {
                let mut e = self.executions.lock().unwrap();
                let c = e.entry(name.to_string()).or_insert(0);
                *c += 1;
                *c
            };
            match &*self.crash.lock().unwrap() {
                Some((nm, at, when)) if nm == name && *at == n => Some(*when),
                _ => None,
            }
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
            self.gets.lock().unwrap().clear();
            self.get_replay.lock().unwrap().clear();
            *self.crash.lock().unwrap() = None;
        }

        /// The attempt failed after its steps were journaled: its writes are lost, and the
        /// retry replays the journal.
        pub fn crash_and_retry(&self) {
            self.pending.lock().unwrap().clear();
            let j = std::mem::take(&mut *self.journal.lock().unwrap());
            *self.replay.lock().unwrap() = j.into();
            self.ran.lock().unwrap().clear();
            let g = std::mem::take(&mut *self.gets.lock().unwrap());
            *self.get_replay.lock().unwrap() = g.into();
            self.executions.lock().unwrap().clear();
            // A crash is simulated once: the retry runs without it.
            *self.crash.lock().unwrap() = None;
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
            // A retry sees what the first attempt read (Restate journals reads), plus
            // nothing newer; a first attempt reads the pending write, else the store.
            let replayed = self.get_replay.lock().unwrap().pop_front();
            let v = match replayed {
                Some((k, v)) => {
                    assert_eq!(k, key, "replay out of order: journal has a read of {k}");
                    v
                }
                None => match self.pending.lock().unwrap().get(key) {
                    Some(v) => v.clone(),
                    None => self.store.lock().unwrap().get(key).cloned(),
                },
            };
            self.gets.lock().unwrap().push((key.to_string(), v.clone()));
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

        fn run_journaled_async<T, F>(
            &self,
            name: &'static str,
            f: F,
        ) -> BoxFut<'_, Result<T, HandlerError>>
        where
            T: Serialize + DeserializeOwned + Send + 'static,
            F: FnOnce() -> BoxFut<'static, T> + Send + 'static,
        {
            let replayed = self.replay.lock().unwrap().pop_front();
            let crash = match replayed {
                Some(_) => None,
                None => self.should_crash(name),
            };
            Box::pin(async move {
                let json = match replayed {
                    Some((n, json)) => {
                        assert_eq!(
                            n, name,
                            "replay out of order: journal has {n}, code runs {name}"
                        );
                        json
                    }
                    None => {
                        if crash == Some(Crash::Before) {
                            return Err(crash_error(name));
                        }
                        self.ran.lock().unwrap().push(name.to_string());
                        let value = f().await;
                        if crash == Some(Crash::After) {
                            return Err(crash_error(name));
                        }
                        serde_json::to_string(&value).unwrap()
                    }
                };
                self.journal
                    .lock()
                    .unwrap()
                    .push((name.to_string(), json.clone()));
                Ok(serde_json::from_str(&json).unwrap())
            })
        }

        fn record_durably<'a>(
            &'a self,
            key: &'a str,
            value: Vec<u8>,
        ) -> BoxFut<'a, Result<(), HandlerError>> {
            // The write is applied to the store at once (it survives a crash of this
            // attempt) and a replay applies the same write again.
            let crash = self.should_crash(&format!("record:{key}"));
            Box::pin(async move {
                if crash == Some(Crash::Before) {
                    return Err(crash_error("record"));
                }
                self.store.lock().unwrap().insert(key.to_string(), value);
                self.durable_writes.lock().unwrap().push(key.to_string());
                Ok(())
            })
        }

        fn request_tick(&self) {
            self.ticks_requested.fetch_add(1, Ordering::SeqCst);
        }

        fn request_recovery(&self, class: &str) {
            self.recoveries.lock().unwrap().push(class.to_string());
        }

        fn request_power(&self, resource: &str, action: PowerAction) {
            self.powers
                .lock()
                .unwrap()
                .push((resource.to_string(), action));
        }
    }

    fn crash_error(at: &str) -> HandlerError {
        HandlerError::from(std::io::Error::other(format!("simulated crash at {at}")))
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
    use sched_model::{Fidelity, ModePhase, ModelId, ObservedValue};
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

    /// Both hosts freshly up (and zero pods, which no decision reads), and `ready` OCR items.
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
                failed: 0,
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
        assert!(matches!(gpu_mode(&ctx), Some(ModePhase::Serving { .. })));
        let allowed_at = gpu_mode(&ctx);
        assert!(stored_state(&ctx).reported.contains(&"cluster-pods".into()));
        // A new process, empty cache: the stored state is what carries the tick on.
        let (s2, clock2) = process();
        clock2.set(Millis(T0 + 1_000));
        let _ = clock;
        world(&s2, 3);
        let r2 = tick(&s2, &ctx).await.unwrap();
        ctx.commit();
        assert!(!r2.kinds.contains(&"allow_model".to_string()), "{r2:?}");
        assert!(r2.allowed_models.contains(&"mineru".into()), "{r2:?}");
        // Serving since the first tick: the dwell clock carried across the restart.
        assert_eq!(gpu_mode(&ctx), allowed_at);
    }

    fn failed_transcriptions(s: &Shared, failed: u64) {
        push(
            s,
            "speakr-pending",
            vec![ObservedValue::Demand {
                class: "speakr-transcription".into(),
                ready: 0,
                in_flight: 0,
                failed,
                oldest_ready_at: None,
            }],
        );
    }

    #[tokio::test]
    async fn a_tick_with_failed_items_asks_the_recovery_object_after_publishing() {
        let (s, _) = process();
        let ctx = FakeCtx::default();
        world(&s, 0);
        failed_transcriptions(&s, 2);
        let r = tick(&s, &ctx).await.unwrap();
        ctx.commit();
        assert_eq!(r.allowed_models, vec![ModelId::from("whisperx")], "{r:?}");
        // The step is reported as carried out (not in `not_executed`), the object asked once.
        assert!(
            r.steps
                .iter()
                .any(|st| matches!(st, sched_model::Step::RunRecovery { .. })),
            "{:?}",
            r.steps
        );
        assert!(r.not_executed.is_empty(), "{:?}", r.not_executed);
        assert_eq!(*ctx.recoveries.lock().unwrap(), ["speakr-transcription"]);
        // A recovery request is made on every tick that emits the step (it is cheap and
        // gates itself), and never from a tick without failed items.
        let r = tick(&s, &ctx).await.unwrap();
        ctx.commit();
        assert!(r.error.is_none());
        assert_eq!(ctx.recoveries.lock().unwrap().len(), 2);
        failed_transcriptions(&s, 0);
        tick(&s, &ctx).await.unwrap();
        ctx.commit();
        assert_eq!(ctx.recoveries.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_retried_tick_does_not_decide_again_and_the_request_follows_the_journaled_decision() {
        let (s, _) = process();
        let ctx = FakeCtx::default();
        world(&s, 0);
        failed_transcriptions(&s, 2);
        tick(&s, &ctx).await.unwrap();
        ctx.crash_and_retry();
        // The retry reads the same state, replays the decision, and asks again for the same
        // class (Restate journals the send, so the call is made once).
        let before = ctx.recoveries.lock().unwrap().len();
        let r = tick(&s, &ctx).await.unwrap();
        assert_eq!(ctx.ran().len(), 0, "decided once");
        assert!(
            r.steps
                .iter()
                .any(|st| matches!(st, sched_model::Step::RunRecovery { .. }))
        );
        assert_eq!(ctx.recoveries.lock().unwrap().len(), before + 1);
    }

    /// `scheduler_state` as the build before the queue-only change stored it: version 1,
    /// with the `loading` and `draining` phases it no longer writes.
    fn legacy_state(mode: &str) -> Vec<u8> {
        format!(
            r#"{{"version":1,"data":{{"state":{{"resources":{{"cuda-central":{{"power":{{"phase":"on","since":1}},"mode":{mode},"managed":false,"host_idle_since":null,"pending_recovery":[]}},"basestar-nfs":{{"power":{{"phase":"on","since":1}},"mode":{{"phase":"idle"}},"managed":false,"host_idle_since":null,"pending_recovery":[]}}}}}},"reported":["cluster-pods"]}}}}"#
        )
        .into_bytes()
    }

    #[tokio::test]
    async fn a_state_persisted_with_loading_or_draining_loads_and_converges() {
        // Draining (waiting for mineru's pods) under the old build: the next model is allowed
        // at once, with no error and no wait on pods.
        let (s, _) = process();
        let ctx = FakeCtx::default();
        ctx.put(
            STATE_KEY,
            &legacy_state(r#"{"phase":"draining","model":"mineru","since":5}"#),
        );
        world(&s, 0);
        push(
            &s,
            "speakr-pending",
            vec![ObservedValue::Demand {
                failed: 0,
                class: "speakr-transcription".into(),
                ready: 2,
                in_flight: 0,
                oldest_ready_at: None,
            }],
        );
        let r = tick(&s, &ctx).await.unwrap();
        ctx.commit();
        assert_eq!(r.error, None, "{r:?}");
        assert_eq!(r.allowed_models, vec![ModelId::from("whisperx")], "{r:?}");
        let text = String::from_utf8(ctx.stored(STATE_KEY).unwrap()).unwrap();
        assert!(text.starts_with("{\"version\":1,"), "{text}");
        assert!(
            !text.contains("draining") && !text.contains("loading"),
            "{text}"
        );
        // Loading under the old build: serving since the same instant, its classes allowed.
        let (s, _) = process();
        let ctx = FakeCtx::default();
        ctx.put(
            STATE_KEY,
            &legacy_state(r#"{"phase":"loading","model":"mineru","since":5}"#),
        );
        world(&s, 3);
        let r = tick(&s, &ctx).await.unwrap();
        ctx.commit();
        assert_eq!(r.error, None, "{r:?}");
        assert!(r.steps.is_empty(), "{r:?}");
        assert!(r.allowed_classes.contains(&"paperless-ocr".into()), "{r:?}");
        assert!(matches!(
            gpu_mode(&ctx),
            Some(ModePhase::Serving {
                since: Millis(5),
                ..
            })
        ));
    }

    #[tokio::test]
    async fn a_persisted_state_naming_an_unconfigured_model_does_not_block_the_resource() {
        for mode in [
            r#"{"phase":"serving","model":"retired","since":5}"#,
            r#"{"phase":"draining","model":"retired","since":5}"#,
            r#"{"phase":"loading","model":"retired","since":5}"#,
        ] {
            let (s, _) = process();
            let ctx = FakeCtx::default();
            ctx.put(STATE_KEY, &legacy_state(mode));
            world(&s, 3);
            let r = tick(&s, &ctx).await.unwrap();
            ctx.commit();
            assert_eq!(r.error, None, "{mode}: {r:?}");
            assert_eq!(
                r.allowed_models,
                vec![ModelId::from("mineru")],
                "{mode}: {r:?}"
            );
            assert!(!r.allowed_models.contains(&"retired".into()));
        }
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
        assert!(matches!(gpu_mode(&ctx), Some(ModePhase::Serving { .. })));
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

    fn process_managed() -> (Arc<Shared>, Arc<ManualClock>) {
        let clock = Arc::new(ManualClock::new(Millis(T0)));
        let holder = Arc::new(
            ConfigHolder::new(&sched_model::test_support::example_yaml_managed()).unwrap(),
        );
        let s = Arc::new(Shared::new(holder, Arc::new(Metrics::new()), clock.clone()));
        (s, clock)
    }

    fn req(resource: &str) -> ResourceRequest {
        ResourceRequest {
            resource: resource.into(),
        }
    }

    fn refused(e: &HandlerError) -> String {
        format!("{e:?}")
    }

    #[tokio::test]
    async fn a_manual_host_lease_is_stored_idempotent_and_wakes_a_managed_host() {
        let (s, _) = process_managed();
        let ctx = FakeCtx::default();
        world(&s, 0);
        // basestar is observed down: the lease asks for it to be powered on.
        push(
            &s,
            "basestar-probe",
            vec![ObservedValue::ResourceUp {
                resource: "basestar-nfs".into(),
                up: false,
            }],
        );
        let g = acquire_host_lease(&s, &ctx, req("basestar-nfs"))
            .await
            .unwrap();
        ctx.commit();
        assert!(g.created);
        assert_eq!(g.lease.id.as_str(), "manual-host-1");
        assert_eq!(g.lease.kind, sched_model::LeaseKind::ManualHost);
        assert_eq!(ctx.ticks(), 1);
        let listed = list_leases(&s, ctx.stored(LEASES_KEY).as_deref()).unwrap();
        assert_eq!(listed, vec![g.lease.clone()]);
        let r = tick(&s, &ctx).await.unwrap();
        ctx.commit();
        assert!(r.kinds.contains(&"power_on".to_string()), "{r:?}");
        assert_eq!(
            *ctx.powers.lock().unwrap(),
            [("basestar-nfs".to_string(), PowerAction::On)]
        );
        // Again: the same lease, nothing written, no tick asked for.
        let again = acquire_host_lease(&s, &ctx, req("basestar-nfs"))
            .await
            .unwrap();
        assert!(!again.created && again.lease == g.lease);
        assert_eq!(ctx.ticks(), 1);
        // Released by resource: stored, a tick asked for, and a second release is a 404.
        let released = release_host_lease(&s, &ctx, req("basestar-nfs"))
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
        let e = release_host_lease(&s, &ctx, req("basestar-nfs"))
            .await
            .unwrap_err();
        assert!(
            refused(&e).contains("holds no manual host lease"),
            "{}",
            refused(&e)
        );
    }

    #[tokio::test]
    async fn host_lease_and_reclaim_requests_are_validated_against_the_active_config() {
        let (s, _) = process_managed();
        let ctx = FakeCtx::default();
        for (resource, text) in [
            ("nowhere", "unknown resource"),
            ("cuda-central", "not a shared resource"),
        ] {
            let e = acquire_host_lease(&s, &ctx, req(resource))
                .await
                .unwrap_err();
            assert!(refused(&e).contains(text), "{resource}: {}", refused(&e));
        }
        for (resource, text) in [
            ("nowhere", "unknown resource"),
            ("basestar-nfs", "cannot be reclaimed"),
        ] {
            let e = reclaim(&s, &ctx, req(resource)).await.unwrap_err();
            assert!(refused(&e).contains(text), "{resource}: {}", refused(&e));
        }
        ctx.commit();
        assert_eq!(ctx.stored(LEASES_KEY), None, "nothing was stored");
        assert_eq!(ctx.ticks(), 0);
    }

    #[tokio::test]
    async fn a_reclaim_is_stored_stops_intake_and_is_released_by_resource() {
        let (s, _) = process_managed();
        let ctx = FakeCtx::default();
        world(&s, 3);
        let g = reclaim(&s, &ctx, req("cuda-central")).await.unwrap();
        ctx.commit();
        assert!(g.created);
        assert_eq!(g.lease.id.as_str(), "reclaim-1");
        assert_eq!(g.lease.kind, sched_model::LeaseKind::Reclaim);
        let again = reclaim(&s, &ctx, req("cuda-central")).await.unwrap();
        assert!(!again.created && again.lease == g.lease);
        let r = tick(&s, &ctx).await.unwrap();
        ctx.commit();
        assert_eq!(r.leases, vec![g.lease.clone()]);
        // Work is waiting (3 OCR items) but the reclaimed resource allows no model, and a
        // managed host is shut down (the one power request of the tick).
        assert!(r.allowed_models.is_empty(), "{r:?}");
        assert_eq!(
            *ctx.powers.lock().unwrap(),
            [("cuda-central".to_string(), PowerAction::Off)]
        );
        let released = unreclaim(&s, &ctx, req("cuda-central")).await.unwrap();
        ctx.commit();
        assert_eq!(released, g.lease);
        let r = tick(&s, &ctx).await.unwrap();
        ctx.commit();
        // The lease is gone; the host is still going down, so nothing is allowed on it yet.
        assert!(r.leases.is_empty() && r.allowed_models.is_empty(), "{r:?}");
        let e = unreclaim(&s, &ctx, req("cuda-central")).await.unwrap_err();
        assert!(
            refused(&e).contains("holds no reclaim lease"),
            "{}",
            refused(&e)
        );
    }

    #[tokio::test]
    async fn a_retried_host_lease_request_reuses_the_journaled_grant() {
        let (s, clock) = process_managed();
        let ctx = FakeCtx::default();
        let g = acquire_host_lease(&s, &ctx, req("basestar-nfs"))
            .await
            .unwrap();
        ctx.crash_and_retry();
        clock.set(Millis(T0 + 5_000));
        let again = acquire_host_lease(&s, &ctx, req("basestar-nfs"))
            .await
            .unwrap();
        assert!(ctx.ran().is_empty());
        assert_eq!(again, g);
        ctx.commit();
        assert_eq!(
            list_leases(&s, ctx.stored(LEASES_KEY).as_deref()).unwrap(),
            vec![g.lease]
        );
        // The same for a reclaim.
        let ctx = FakeCtx::default();
        let g = reclaim(&s, &ctx, req("cuda-central")).await.unwrap();
        ctx.crash_and_retry();
        let again = reclaim(&s, &ctx, req("cuda-central")).await.unwrap();
        assert!(ctx.ran().is_empty());
        assert_eq!(again, g);
    }

    /// cuda-central failed in a shutdown, basestar up and managed: what a person finds after
    /// a shutdown that never completed and a host fixed by hand.
    fn failed_gpu_state() -> Vec<u8> {
        br#"{"version":1,"data":{"state":{"resources":{"cuda-central":{"power":{"phase":"failed","since":5,"during":"shutting_down"},"mode":{"phase":"idle"}},"basestar-nfs":{"power":{"phase":"on","since":1},"mode":{"phase":"idle"},"managed":true,"host_idle_since":null}}},"reported":["cluster-pods"]}}"#.to_vec()
    }

    #[tokio::test]
    async fn resume_clears_one_resource_and_it_then_follows_observation() {
        let (s, _) = process_managed();
        let ctx = FakeCtx::default();
        ctx.put(STATE_KEY, &failed_gpu_state());
        world(&s, 0);
        let r = tick(&s, &ctx).await.unwrap();
        ctx.commit();
        assert_eq!(
            r.power_failed.len(),
            1,
            "stays failed while the host is up: {r:?}"
        );
        let before = stored_state(&ctx);
        let ack = resume(
            &s,
            &ctx,
            Resume {
                resource: "cuda-central".into(),
            },
        )
        .await
        .unwrap();
        ctx.commit();
        assert_eq!(ack.cleared, "shutdown_failed");
        assert_eq!(ctx.ticks(), 1);
        let after = stored_state(&ctx);
        assert!(after.state.resource(&"cuda-central".into()).is_none());
        assert_eq!(
            after.state.resource(&"basestar-nfs".into()),
            before.state.resource(&"basestar-nfs".into()),
            "another resource's state is untouched"
        );
        assert_eq!(after.reported, before.reported);
        // The next tick finds it running: up, and not ours to power off.
        world(&s, 0);
        let r = tick(&s, &ctx).await.unwrap();
        ctx.commit();
        assert!(r.power_failed.is_empty(), "{r:?}");
        let gpu = r.state.resource(&"cuda-central".into()).unwrap();
        assert!(gpu.is_on() && !gpu.managed, "{gpu:?}");
        assert!(
            ctx.powers.lock().unwrap().is_empty(),
            "resuming powers nothing by itself"
        );
        // Nothing left to resume now.
        let e = resume(
            &s,
            &ctx,
            Resume {
                resource: "cuda-central".into(),
            },
        )
        .await
        .unwrap_err();
        assert!(refused(&e).contains("is not stopped"), "{}", refused(&e));
    }

    #[tokio::test]
    async fn a_retried_resume_reuses_the_journaled_answer_and_writes_the_same_state() {
        let (s, _) = process_managed();
        let ctx = FakeCtx::default();
        ctx.put(STATE_KEY, &failed_gpu_state());
        let ack = resume(
            &s,
            &ctx,
            Resume {
                resource: "cuda-central".into(),
            },
        )
        .await
        .unwrap();
        ctx.crash_and_retry();
        // The retry reads the state the first attempt read (not what it wrote) and gets the
        // journaled answer without applying the rule again.
        let again = resume(
            &s,
            &ctx,
            Resume {
                resource: "cuda-central".into(),
            },
        )
        .await
        .unwrap();
        assert!(ctx.ran().is_empty());
        assert_eq!(again, ack);
        ctx.commit();
        assert!(
            stored_state(&ctx)
                .state
                .resource(&"cuda-central".into())
                .is_none()
        );
    }

    #[tokio::test]
    async fn resume_refuses_what_is_not_stopped_and_writes_nothing() {
        let (s, _) = process_managed();
        let ctx = FakeCtx::default();
        ctx.put(STATE_KEY, &failed_gpu_state());
        for (resource, status) in [("basestar-nfs", "409"), ("nowhere", "400")] {
            let e = resume(
                &s,
                &ctx,
                Resume {
                    resource: resource.into(),
                },
            )
            .await
            .unwrap_err();
            let text = refused(&e);
            assert!(text.contains(status), "{resource}: {text}");
        }
        let before = ctx.stored(STATE_KEY);
        ctx.commit();
        assert_eq!(ctx.stored(STATE_KEY), before);
        assert_eq!(ctx.ticks(), 0);
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
