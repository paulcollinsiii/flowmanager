//! Carries out decisions: power on and off, recovery actions, and the backup procedure.
//! Only the recovery action `requeue_failed` exists so far.
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

pub mod attempts;
pub mod requeue;

pub use attempts::{AttemptBook, Attempts, Plan};
pub use requeue::{Listed, PostOutcome, client, list, requeue};
