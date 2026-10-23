//! The wall clock. This is the only place in the service that reads it; everything else
//! receives the time as a [`Millis`].

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use sched_model::Millis;

pub trait Clock: Send + Sync {
    fn now(&self) -> Millis;
}

/// The system clock, as milliseconds since the unix epoch.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Millis {
        let ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        Millis(u64::try_from(ms).unwrap_or(u64::MAX))
    }
}

/// A clock that only moves when told to, for tests.
#[derive(Default)]
pub struct ManualClock(AtomicU64);

impl ManualClock {
    pub fn new(start: Millis) -> Self {
        ManualClock(AtomicU64::new(start.as_u64()))
    }

    pub fn set(&self, to: Millis) {
        self.0.store(to.as_u64(), Ordering::SeqCst);
    }

    pub fn advance(&self, by: Millis) {
        self.0.fetch_add(by.as_u64(), Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Millis {
        Millis(self.0.load(Ordering::SeqCst))
    }
}
