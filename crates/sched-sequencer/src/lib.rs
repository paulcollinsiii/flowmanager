//! Per-resource state machines and safety rules that turn an intent and current
//! state into the next safe step.
//!
//! Pure: depends only on `sched-model`; no I/O, no async runtime, no wall-clock access.
//! Deterministic: the same inputs always produce the same step.

#[cfg(test)]
mod tests {
    #[test]
    fn can_load_shared_fixtures() {
        let cfg = sched_model::test_support::example_config();
        let f = sched_model::test_support::fixture("idle");
        assert!(f.check(&cfg).is_empty());
    }
}
