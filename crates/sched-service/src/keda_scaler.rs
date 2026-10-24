//! Serving the KEDA external scaler (`sched-keda`) from this process: it reads the allowed
//! set each tick publishes and the active config, and nothing else. It runs on its own
//! task, and a lookup only borrows the two `watch` channels for a moment, so serving never
//! waits on a tick and a tick never waits on a scaler call.

use std::net::SocketAddr;
use std::sync::Arc;

use sched_keda::Scaler;

use crate::tick::Shared;

/// A scaler over what `shared` publishes and the config it holds.
pub fn scaler(shared: &Shared) -> Scaler {
    Scaler::new(shared.subscribe_allowed(), shared.config.subscribe())
}

/// Binds `addr` (so a taken port fails startup) and serves the scaler on its own task.
/// If serving stops with an error, it is logged; the scheduler keeps running and KEDA,
/// unable to reach the scaler, leaves every replica count as it is.
pub async fn spawn(shared: &Arc<Shared>, addr: SocketAddr) -> std::io::Result<SocketAddr> {
    let listener = sched_keda::bind(addr).await?;
    let local = listener.local_addr()?;
    let scaler = scaler(shared);
    tokio::spawn(async move {
        if let Err(e) = sched_keda::serve(listener, scaler).await {
            tracing::error!(addr = %local, "the KEDA scaler stopped serving: {e}");
        }
    });
    Ok(local)
}
