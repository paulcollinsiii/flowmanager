//! Policy strategies that turn a snapshot and leases into an intent.
//!
//! Pure: depends only on `sched-model`; no I/O, no async runtime, no wall-clock access.
//! Deterministic: the same snapshot and config always produce the same intent.

#[cfg(test)]
mod tests {
    #[test]
    fn can_load_shared_fixtures() {
        let cfg = sched_model::test_support::example_config();
        let f = sched_model::test_support::fixture("idle");
        assert!(f.check(&cfg).is_empty());
    }
}
