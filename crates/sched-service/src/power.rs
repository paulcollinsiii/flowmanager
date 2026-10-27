//! Carries out `PowerOn` and `PowerOff` steps, in the `Power` object (one instance per
//! resource, keyed by the resource id).
//!
//! **Why its own object.** As for recovery: a `Scheduler` handler holds the object's
//! exclusive lock, and the router call or the SSH session can take tens of seconds. The
//! tick only sends the resource's `Power` object a one-way request after storing its
//! decision. Restate journals that send, so a retried or replayed tick sends it once, and
//! the sequencer emits the step once per transition, so each decision step is carried out
//! once.
//!
//! **What a run does.**
//! 1. A journaled *gate*: the active config must still have the resource under managed
//!    power control with the section the action needs (the config may have been reloaded
//!    since the tick). Otherwise nothing is sent (`not_wired`).
//! 2. One journaled step that makes the call: Wake-on-LAN through the router for `on`, the
//!    forced-command SSH shutdown for `off` ([`sched_actuate::PowerBackend`]). A replay
//!    reuses the journaled result and cannot send again. (A crash after the call but before
//!    its result is journaled makes Restate run the step once more; both calls are
//!    harmless to repeat, and this is the one case with more than one call.)
//! 3. The outcome is recorded, counted, and logged. The result of the call is a value:
//!    a failure is **never retried** by this code or, because the handler returns normally,
//!    by Restate. A host that does not come up or go down is beyond the scope of the
//!    scheduler, and a retry risks flapping the host. The sequencer has already recorded the
//!    step as taken; it follows the host's observed reachability from there.
//!
//! The record is the object's `power_last_action` key (versioned, informational): the
//! last action, when it ran, and whether the request was accepted. The `last_action`
//! handler returns it.

use std::sync::Arc;

use restate_sdk::errors::HandlerError;
use sched_actuate::PowerBackend;
use sched_model::{Millis, ResourceId, SshShutdown, WakeOnLan};
use serde::{Deserialize, Serialize};

use crate::object::ObjectCtx;
use crate::persist::{self, POWER_KEY, POWER_VERSION};
use crate::tick::Shared;

/// Which way to switch the host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PowerAction {
    On,
    Off,
}

impl PowerAction {
    pub fn as_str(self) -> &'static str {
        match self {
            PowerAction::On => "on",
            PowerAction::Off => "off",
        }
    }
}

/// The request the tick sends (the resource is the object key).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PowerRequest {
    pub action: PowerAction,
}

/// What a run did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PowerAck {
    pub resource: String,
    pub action: PowerAction,
    /// `requested`, `failed` or `not_wired`.
    pub outcome: String,
    pub error: Option<String>,
}

/// The last action of a resource, as stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PowerRecord {
    pub action: PowerAction,
    pub at: Millis,
    /// `requested` or `failed`.
    pub outcome: String,
    pub error: Option<String>,
}

/// The stored document.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PowerLog {
    #[serde(default)]
    pub last: Option<PowerRecord>,
}

/// The call a gate decided on.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum Gate {
    Wake {
        now: Millis,
        wol: Box<WakeOnLan>,
    },
    Shutdown {
        now: Millis,
        target: Box<SshShutdown>,
    },
    Stop,
}

/// Carries out one power action of `resource` (see the module docs).
pub async fn run<C: ObjectCtx>(
    shared: &Arc<Shared>,
    backend: &Arc<dyn PowerBackend>,
    ctx: &C,
    resource: &str,
    req: PowerRequest,
) -> Result<PowerAck, HandlerError> {
    let metrics = &shared.metrics;
    let action = req.action;
    // The key is whatever the caller invoked; only a configured resource gets a series.
    let label = shared
        .config
        .current()
        .resource(&ResourceId::new(resource))
        .map_or("unknown", |_| resource);
    let ack = |outcome: &str, error: Option<String>| PowerAck {
        resource: resource.to_string(),
        action,
        outcome: outcome.to_string(),
        error,
    };

    let (s, rid) = (Arc::clone(shared), ResourceId::new(resource));
    let gate = ctx
        .run_journaled("power_gate", move || gate(&s, &rid, action))
        .await?;
    let now = match &gate {
        Gate::Stop => {
            metrics.power_request(label, action.as_str(), "not_wired");
            tracing::warn!(
                event = "power_not_wired",
                resource,
                action = action.as_str(),
                "power {} of {resource} not carried out: the active config does not have it under managed power control with an actuator for this action",
                action.as_str(),
            );
            return Ok(ack("not_wired", None));
        }
        Gate::Wake { now, .. } | Gate::Shutdown { now, .. } => *now,
    };

    let backend = Arc::clone(backend);
    let result: Result<(), String> = ctx
        .run_journaled_async("power_request", move || {
            Box::pin(async move {
                match gate {
                    Gate::Wake { wol, .. } => backend.wake(&wol).await,
                    Gate::Shutdown { target, .. } => backend.shutdown(&target).await,
                    Gate::Stop => Ok(()),
                }
            })
        })
        .await?;

    let (outcome, error) = match result {
        Ok(()) => ("requested", None),
        Err(e) => ("failed", Some(e)),
    };
    metrics.power_request(label, action.as_str(), outcome);
    shared.note_power_request(&ResourceId::new(resource), action, now, error.as_deref());
    match &error {
        None => tracing::info!(
            event = "power_requested",
            resource,
            action = action.as_str(),
            "power {} of {resource} requested; the host's state follows from its reachability",
            action.as_str(),
        ),
        Some(e) => tracing::error!(
            event = "power_request_failed",
            resource,
            action = action.as_str(),
            error = %e,
            "power {} of {resource} failed; it is not retried automatically",
            action.as_str(),
        ),
    }
    ctx.set_raw(
        POWER_KEY,
        persist::encode(
            POWER_VERSION,
            &PowerLog {
                last: Some(PowerRecord {
                    action,
                    at: now,
                    outcome: outcome.to_string(),
                    error: error.clone(),
                }),
            },
        ),
    );
    Ok(ack(outcome, error))
}

/// The cheap checks before a call: one journaled step.
fn gate(shared: &Shared, resource: &ResourceId, action: PowerAction) -> Gate {
    let now = shared.clock.now();
    let config = shared.config.current();
    let Some(power) = config.resource(resource).and_then(|r| r.power.as_ref()) else {
        return Gate::Stop;
    };
    if !config.power_managed(resource) {
        return Gate::Stop;
    }
    match action {
        PowerAction::On => power
            .wake_on_lan
            .clone()
            .map_or(Gate::Stop, |wol| Gate::Wake {
                now,
                wol: Box::new(wol),
            }),
        PowerAction::Off => power
            .shutdown
            .clone()
            .map_or(Gate::Stop, |target| Gate::Shutdown {
                now,
                target: Box::new(target),
            }),
    }
}

/// The stored record (`None` if there is none): what the `last_action` handler answers.
pub fn stored_record(raw: Option<&[u8]>) -> Result<Option<PowerRecord>, HandlerError> {
    persist::decode::<PowerLog>(POWER_KEY, POWER_VERSION, raw)
        .map(|l| l.last)
        .map_err(|e| restate_sdk::errors::TerminalError::new_with_code(503, e.to_string()).into())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use sched_actuate::BoxFut;
    use sched_model::test_support::{EXAMPLE_CONFIG, example_yaml_managed};
    use sched_model::{Fidelity, ObservedValue};
    use sched_observe::Report;

    use super::*;
    use crate::clock::ManualClock;
    use crate::config_source::ConfigHolder;
    use crate::metrics::Metrics;
    use crate::object::{
        self,
        fake::{Crash, FakeCtx},
    };
    use crate::persist::STATE_KEY;

    const T0: u64 = 10_000_000;

    /// A backend that records its calls and fails on demand.
    #[derive(Default)]
    struct Fake {
        calls: Mutex<Vec<String>>,
        fail: Mutex<Option<String>>,
    }

    impl Fake {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
        fn answer(&self) -> Result<(), String> {
            match &*self.fail.lock().unwrap() {
                Some(e) => Err(e.clone()),
                None => Ok(()),
            }
        }
    }

    impl PowerBackend for Fake {
        fn wake<'a>(&'a self, wol: &'a WakeOnLan) -> BoxFut<'a, Result<(), String>> {
            self.calls.lock().unwrap().push(format!(
                "wake {} {} {}",
                wol.router_url, wol.interface, wol.mac
            ));
            Box::pin(async move { self.answer() })
        }
        fn shutdown<'a>(&'a self, target: &'a SshShutdown) -> BoxFut<'a, Result<(), String>> {
            self.calls.lock().unwrap().push(format!(
                "shutdown {}@{}:{} {}",
                target.user, target.host, target.port, target.host_key
            ));
            Box::pin(async move { self.answer() })
        }
    }

    fn process(yaml: &str) -> (Arc<Shared>, Arc<ManualClock>) {
        let clock = Arc::new(ManualClock::new(Millis(T0)));
        let holder = Arc::new(ConfigHolder::new(yaml).unwrap());
        let s = Shared::new(holder, Arc::new(Metrics::new()), clock.clone());
        (Arc::new(s), clock)
    }

    fn backend() -> (Arc<Fake>, Arc<dyn PowerBackend>) {
        let fake = Arc::new(Fake::default());
        (Arc::clone(&fake), fake)
    }

    fn on() -> PowerRequest {
        PowerRequest {
            action: PowerAction::On,
        }
    }

    fn off() -> PowerRequest {
        PowerRequest {
            action: PowerAction::Off,
        }
    }

    fn record(ctx: &FakeCtx) -> Option<PowerRecord> {
        stored_record(ctx.stored(POWER_KEY).as_deref()).unwrap()
    }

    fn metric(s: &Shared, resource: &str, action: &str, outcome: &str) -> bool {
        let text = s.metrics.render();
        text.lines().any(|l| {
            l.starts_with("sched_power_requests_total")
                && l.contains(&format!("action=\"{action}\""))
                && l.contains(&format!("outcome=\"{outcome}\""))
                && l.contains(&format!("resource=\"{resource}\""))
                && l.ends_with(" 1")
        })
    }

    #[tokio::test]
    async fn power_on_asks_the_router_once_with_the_configured_server() {
        let (s, _) = process(&example_yaml_managed());
        let (fake, backend) = backend();
        let ctx = FakeCtx::default();
        let ack = run(&s, &backend, &ctx, "cuda-central", on()).await.unwrap();
        ctx.commit();
        assert_eq!(ack.outcome, "requested");
        assert_eq!(ack.error, None);
        assert_eq!(
            fake.calls(),
            ["wake https://router.example.internal lan 02:00:00:00:00:01"]
        );
        assert_eq!(ctx.ran(), Vec::<String>::new());
        let r = record(&ctx).unwrap();
        assert_eq!(
            (r.action, r.at, r.outcome.as_str()),
            (PowerAction::On, Millis(T0), "requested")
        );
        assert!(metric(&s, "cuda-central", "on", "requested"));
    }

    #[tokio::test]
    async fn power_off_runs_the_forced_command_shutdown_once() {
        let (s, _) = process(&example_yaml_managed());
        let (fake, backend) = backend();
        let ctx = FakeCtx::default();
        let ack = run(&s, &backend, &ctx, "basestar-nfs", off())
            .await
            .unwrap();
        ctx.commit();
        assert_eq!(ack.outcome, "requested");
        assert_eq!(
            fake.calls(),
            ["shutdown poweroff@basestar:22 SHA256:BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"]
        );
        assert!(metric(&s, "basestar-nfs", "off", "requested"));
    }

    #[tokio::test]
    async fn a_failed_request_is_recorded_counted_and_never_retried() {
        let (s, _) = process(&example_yaml_managed());
        let (fake, backend) = backend();
        *fake.fail.lock().unwrap() = Some("router answered HTTP 503".into());
        let ctx = FakeCtx::default();
        let ack = run(&s, &backend, &ctx, "cuda-central", on()).await.unwrap();
        assert_eq!(ack.outcome, "failed");
        assert_eq!(ack.error.as_deref(), Some("router answered HTTP 503"));
        assert!(metric(&s, "cuda-central", "on", "failed"));
        // The invocation itself succeeded (Restate must not retry it); a replay of it
        // reuses the journaled failure and does not call the router again.
        ctx.crash_and_retry();
        let again = run(&s, &backend, &ctx, "cuda-central", on()).await.unwrap();
        ctx.commit();
        assert_eq!(again.outcome, "failed");
        assert_eq!(fake.calls().len(), 1, "{:?}", fake.calls());
        let r = record(&ctx).unwrap();
        assert_eq!(r.outcome, "failed");
        assert_eq!(r.error.as_deref(), Some("router answered HTTP 503"));
    }

    #[tokio::test]
    async fn a_replay_after_success_does_not_call_again() {
        let (s, _) = process(&example_yaml_managed());
        let (fake, backend) = backend();
        let ctx = FakeCtx::default();
        run(&s, &backend, &ctx, "basestar-nfs", off())
            .await
            .unwrap();
        ctx.crash_and_retry();
        run(&s, &backend, &ctx, "basestar-nfs", off())
            .await
            .unwrap();
        assert_eq!(fake.calls().len(), 1);
        assert!(ctx.ran().is_empty(), "{:?}", ctx.ran());
    }

    #[tokio::test]
    async fn a_crash_before_the_call_leaves_nothing_sent_and_the_retry_sends_once() {
        let (s, _) = process(&example_yaml_managed());
        let (fake, backend) = backend();
        let ctx = FakeCtx::default();
        ctx.crash_at("power_request", 1, Crash::Before);
        assert!(run(&s, &backend, &ctx, "cuda-central", on()).await.is_err());
        assert!(fake.calls().is_empty());
        ctx.crash_and_retry();
        run(&s, &backend, &ctx, "cuda-central", on()).await.unwrap();
        assert_eq!(fake.calls().len(), 1);
    }

    #[tokio::test]
    async fn nothing_is_sent_for_a_resource_under_manual_control_or_an_unknown_one() {
        let (s, _) = process(EXAMPLE_CONFIG);
        let (fake, backend) = backend();
        let ctx = FakeCtx::default();
        let ack = run(&s, &backend, &ctx, "cuda-central", on()).await.unwrap();
        assert_eq!(ack.outcome, "not_wired");
        let ack = run(&s, &backend, &ctx, "no-such-resource", off())
            .await
            .unwrap();
        assert_eq!(ack.outcome, "not_wired");
        assert!(fake.calls().is_empty());
        assert!(metric(&s, "cuda-central", "on", "not_wired"));
        assert!(metric(&s, "unknown", "off", "not_wired"));
        assert!(record(&ctx).is_none());
    }

    #[tokio::test]
    async fn a_reload_that_hands_the_host_back_to_a_person_cancels_a_pending_request() {
        let (s, _) = process(&example_yaml_managed());
        let (fake, backend) = backend();
        // The tick decided on a power-on; before the request ran the config was reloaded
        // with the host under manual control.
        assert_eq!(s.config.apply_text(EXAMPLE_CONFIG).label(), "applied");
        let ctx = FakeCtx::default();
        let ack = run(&s, &backend, &ctx, "cuda-central", on()).await.unwrap();
        assert_eq!(ack.outcome, "not_wired");
        assert!(fake.calls().is_empty());
    }

    fn down_with_lease(s: &Shared) {
        s.ingest_now(Report::Observed {
            source: "cluster-pods".into(),
            fidelity: Fidelity::Polled,
            values: vec![ObservedValue::ResourceUp {
                resource: "cuda-central".into(),
                up: false,
            }],
        });
    }

    #[tokio::test]
    async fn a_tick_that_powers_a_host_on_sends_one_request_that_reaches_the_router() {
        let (s, _) = process(&example_yaml_managed());
        let ctx = FakeCtx::default();
        // A manual model lease on a host that is down asks for it to be powered on.
        ctx.put(
            crate::persist::LEASES_KEY,
            &persist::encode_leases(&crate::leases::LeaseBook::default()),
        );
        let grant = object::acquire_model_lease(
            &s,
            &ctx,
            object::AcquireModelLease {
                model: "llm".into(),
            },
        )
        .await
        .unwrap();
        assert!(grant.created);
        ctx.commit();
        down_with_lease(&s);
        let report = object::tick(&s, &ctx).await.unwrap();
        ctx.commit();
        assert!(report.not_executed.is_empty(), "{:?}", report.not_executed);
        assert_eq!(
            *ctx.powers.lock().unwrap(),
            [("cuda-central".to_string(), PowerAction::On)]
        );
        // Carry the request out, as Restate would for the sent invocation.
        let (fake, backend) = backend();
        let pctx = FakeCtx::default();
        run(&s, &backend, &pctx, "cuda-central", on())
            .await
            .unwrap();
        assert_eq!(fake.calls().len(), 1);
        // The next tick sees the host as powering on and asks for nothing more, however
        // long the host takes (within its timeout): nothing retries a power-on.
        for _ in 0..3 {
            down_with_lease(&s);
            object::tick(&s, &ctx).await.unwrap();
            ctx.commit();
        }
        assert_eq!(ctx.powers.lock().unwrap().len(), 1);
        let state = persist::decode_state(ctx.stored(STATE_KEY).as_deref()).unwrap();
        assert!(matches!(
            state.state.resource(&"cuda-central".into()).unwrap().power,
            sched_model::PowerPhase::PoweringOn { .. }
        ));
    }

    #[tokio::test]
    async fn a_retried_tick_sends_each_power_request_from_the_journaled_decision() {
        let (s, _) = process(&example_yaml_managed());
        let ctx = FakeCtx::default();
        object::acquire_model_lease(
            &s,
            &ctx,
            object::AcquireModelLease {
                model: "llm".into(),
            },
        )
        .await
        .unwrap();
        ctx.commit();
        down_with_lease(&s);
        object::tick(&s, &ctx).await.unwrap();
        ctx.crash_and_retry();
        let before = ctx.powers.lock().unwrap().len();
        object::tick(&s, &ctx).await.unwrap();
        // Not decided again; the request is the same one (Restate journals the send, so the
        // object is invoked once).
        assert_eq!(ctx.ran().len(), 0);
        assert_eq!(ctx.powers.lock().unwrap().len(), before + 1);
        assert_eq!(
            ctx.powers.lock().unwrap().last().unwrap().1,
            PowerAction::On
        );
    }

    #[tokio::test]
    async fn an_idle_managed_host_past_its_grace_is_shut_down_by_one_request() {
        let (s, _) = process(&example_yaml_managed());
        let ctx = FakeCtx::default();
        // basestar-nfs is up and managed, idle since long before the grace period.
        ctx.put(
            STATE_KEY,
            br#"{"version":1,"data":{"state":{"resources":{"cuda-central":{"power":{"phase":"on","since":1},"mode":{"phase":"idle"},"managed":false,"host_idle_since":null,"pending_recovery":[]},"basestar-nfs":{"power":{"phase":"on","since":1},"mode":{"phase":"idle"},"managed":true,"host_idle_since":1,"pending_recovery":[]}}},"reported":["cluster-pods"]}}"#,
        );
        for (source, resource) in [
            ("cluster-pods", "cuda-central"),
            ("basestar-probe", "basestar-nfs"),
        ] {
            s.ingest_now(Report::Observed {
                source: source.into(),
                fidelity: Fidelity::Polled,
                values: vec![ObservedValue::ResourceUp {
                    resource: resource.into(),
                    up: true,
                }],
            });
        }
        // Nothing is waiting anywhere: every demand source reports zero.
        for (class, source) in [
            ("paperless-ocr", "paperless-ocr-pending"),
            ("paperless-tagging", "paperless-post-ocr"),
            ("speakr-transcription", "speakr-pending"),
            ("speakr-summarization", "speakr-summaries"),
            ("immich-ml", "immich-queues"),
        ] {
            s.ingest_now(Report::Observed {
                source: source.into(),
                fidelity: Fidelity::Polled,
                values: vec![ObservedValue::Demand {
                    class: class.into(),
                    ready: 0,
                    in_flight: 0,
                    failed: 0,
                    oldest_ready_at: None,
                }],
            });
        }
        let report = object::tick(&s, &ctx).await.unwrap();
        ctx.commit();
        assert!(report.not_executed.is_empty(), "{report:?}");
        assert_eq!(
            *ctx.powers.lock().unwrap(),
            [("basestar-nfs".to_string(), PowerAction::Off)],
            "{report:?}"
        );
        // The next tick (state: shutting down) does not ask again.
        s.ingest_now(Report::Observed {
            source: "basestar-probe".into(),
            fidelity: Fidelity::Polled,
            values: vec![ObservedValue::ResourceUp {
                resource: "basestar-nfs".into(),
                up: true,
            }],
        });
        object::tick(&s, &ctx).await.unwrap();
        assert_eq!(ctx.powers.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn under_manual_control_a_tick_never_asks_for_power() {
        let (s, _) = process(EXAMPLE_CONFIG);
        let ctx = FakeCtx::default();
        object::acquire_model_lease(
            &s,
            &ctx,
            object::AcquireModelLease {
                model: "llm".into(),
            },
        )
        .await
        .unwrap();
        ctx.commit();
        down_with_lease(&s);
        object::tick(&s, &ctx).await.unwrap();
        assert!(ctx.powers.lock().unwrap().is_empty());
    }

    #[test]
    fn the_stored_record_reads_back_and_a_foreign_version_is_an_error() {
        assert_eq!(stored_record(None).unwrap(), None);
        let bytes = persist::encode(
            POWER_VERSION,
            &PowerLog {
                last: Some(PowerRecord {
                    action: PowerAction::Off,
                    at: Millis(5),
                    outcome: "failed".into(),
                    error: Some("x".into()),
                }),
            },
        );
        assert_eq!(stored_record(Some(&bytes)).unwrap().unwrap().at, Millis(5));
        assert!(stored_record(Some(br#"{"version":9,"data":{}}"#)).is_err());
    }

    fn gauge(s: &Shared, resource: &str, during: &str) -> Option<String> {
        s.metrics
            .render()
            .lines()
            .find(|l| {
                l.starts_with("sched_resource_power_failed{")
                    && l.contains(&format!("resource=\"{resource}\""))
                    && l.contains(&format!("during=\"{during}\""))
            })
            .and_then(|l| l.rsplit(' ').next().map(str::to_string))
    }

    fn timeouts_counted(s: &Shared) -> Option<String> {
        s.metrics
            .render()
            .lines()
            .find(|l| {
                l.starts_with("sched_power_timeouts_total{")
                    && l.contains("resource=\"cuda-central\"")
                    && l.contains("during=\"powering_on\"")
            })
            .and_then(|l| l.rsplit(' ').next().map(str::to_string))
    }

    #[tokio::test]
    async fn a_host_that_never_comes_up_shows_the_failed_request_then_alerts_once_and_is_never_woken_again()
     {
        let (s, clock) = process(&example_yaml_managed());
        let ctx = FakeCtx::default();
        ctx.put(
            crate::persist::LEASES_KEY,
            &persist::encode_leases(&crate::leases::LeaseBook::default()),
        );
        object::acquire_model_lease(
            &s,
            &ctx,
            object::AcquireModelLease {
                model: "llm".into(),
            },
        )
        .await
        .unwrap();
        ctx.commit();
        down_with_lease(&s);
        object::tick(&s, &ctx).await.unwrap();
        ctx.commit();
        assert_eq!(ctx.powers.lock().unwrap().len(), 1);

        // The router refuses: the call fails, and the next tick says so, not only the log.
        let (fake, backend) = backend();
        *fake.fail.lock().unwrap() = Some("router answered HTTP 503".to_string());
        let ack = run(&s, &backend, &FakeCtx::default(), "cuda-central", on())
            .await
            .unwrap();
        assert_eq!(ack.outcome, "failed");
        clock.advance(Millis(60_000));
        down_with_lease(&s);
        let r = object::tick(&s, &ctx).await.unwrap();
        ctx.commit();
        assert_eq!(r.power_request_failures.len(), 1, "{r:?}");
        assert_eq!(r.power_request_failures[0].action, PowerAction::On);
        assert!(
            r.reason.contains("power on of cuda-central FAILED")
                && r.reason.contains("router answered HTTP 503"),
            "{}",
            r.reason
        );
        assert!(r.power_failed.is_empty() && r.power_timeouts.is_empty());
        assert_eq!(
            gauge(&s, "cuda-central", "powering_on").as_deref(),
            Some("0")
        );

        // Past the 10 minute power-on timeout the host is still down: the alert fires once.
        clock.advance(Millis(10 * 60_000));
        down_with_lease(&s);
        let r = object::tick(&s, &ctx).await.unwrap();
        ctx.commit();
        assert_eq!(r.power_timeouts.len(), 1, "{r:?}");
        assert_eq!(r.power_timeouts[0].resource.as_str(), "cuda-central");
        assert_eq!(r.power_failed.len(), 1);
        assert!(
            r.kinds.contains(&"power_failed".to_string()),
            "{:?}",
            r.kinds
        );
        assert!(
            r.reason.contains("FAILED power state") && r.reason.contains("a person acts"),
            "{}",
            r.reason
        );
        assert!(r.allowed_models.is_empty());
        assert_eq!(
            gauge(&s, "cuda-central", "powering_on").as_deref(),
            Some("1")
        );
        assert_eq!(
            gauge(&s, "cuda-central", "shutting_down").as_deref(),
            Some("0")
        );
        assert_eq!(timeouts_counted(&s).as_deref(), Some("1"));
        // The failed request stays in the report while the resource is failed.
        assert_eq!(r.power_request_failures.len(), 1);

        // Hours later, lease still held, host still down, observations fresh: no timeout
        // again, no request, the failed state stays.
        for _ in 0..360 {
            clock.advance(Millis(60_000));
            down_with_lease(&s);
            let r = object::tick(&s, &ctx).await.unwrap();
            ctx.commit();
            assert!(r.power_timeouts.is_empty());
            assert_eq!(r.power_failed.len(), 1);
            assert!(
                !r.steps.iter().any(|st| matches!(
                    st,
                    sched_model::Step::PowerOn { .. } | sched_model::Step::PowerOff { .. }
                )),
                "{:?}",
                r.steps
            );
        }
        assert_eq!(
            ctx.powers.lock().unwrap().len(),
            1,
            "no further power request"
        );
        assert_eq!(timeouts_counted(&s).as_deref(), Some("1"));

        // The host is observed up (a person fixed it): the failed state clears.
        s.ingest_now(Report::Observed {
            source: "cluster-pods".into(),
            fidelity: Fidelity::Polled,
            values: vec![ObservedValue::ResourceUp {
                resource: "cuda-central".into(),
                up: true,
            }],
        });
        let r = object::tick(&s, &ctx).await.unwrap();
        ctx.commit();
        assert!(r.power_failed.is_empty(), "{r:?}");
        assert!(r.power_request_failures.is_empty(), "{r:?}");
        assert_eq!(
            gauge(&s, "cuda-central", "powering_on").as_deref(),
            Some("0")
        );
        assert_eq!(ctx.powers.lock().unwrap().len(), 1);
    }
}
