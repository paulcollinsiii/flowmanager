//! Carries out decisions: power on and off and recovery actions. The recovery action
//! `requeue_failed` and the two power actuators exist so far.
//!
//! I/O crate: actuators execute steps chosen elsewhere and never decide. Nothing here knows
//! a service: endpoints, JSON pointers, the response that means "queued" and the limits all
//! come from the action's configured params (`sched_model::RequeueFailedParams`).
//!
//! `requeue_failed` is split into pieces the service sequences inside Restate steps, so
//! that every side effect can be journaled and the bookkeeping that bounds it is durable:
//! [`requeue::list`] (a GET), [`attempts::AttemptBook::plan`] (pure), then for each item
//! [`attempts::AttemptBook::record`] (the caller persists it) and only then
//! [`requeue::requeue`] (the POST).
//!
//! Power is two calls behind the [`PowerBackend`] trait, which the service runs as one
//! journaled step each: [`wol::wake`] (Wake-on-LAN through the OPNsense router API) and
//! [`ssh::shutdown`] (a forced-command SSH key). Neither retries; a failure is a value.
//! The reason to keep them here rather than in a new crate is that they are the same kind
//! of thing as the requeue action (a configured outbound call made on the service's
//! behalf) and share its test server.

pub mod attempts;
pub mod power;
pub mod requeue;
pub mod ssh;
pub mod wol;

pub use attempts::{AttemptBook, Attempts, Plan};
pub use power::{BoxFut, HostPower, PowerBackend, SecretFiles};
pub use requeue::{Listed, PostOutcome, client, list, requeue};
