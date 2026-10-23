//! Observer adapter kinds (Kubernetes watch, HTTP/JSON, SQL, Prometheus, webhook
//! receiver) and their registry. Each adapter translates one external source into
//! `sched-model` observations with a timestamp and fidelity.
//!
//! I/O crate: observers never decide anything and never write to services.
