//! `allowed: with_work` (`docs/configuration/workloads.md`, invariant 22): a demand class is
//! allowed only while it has work, or within `idle_grace` after a tick last saw it with work.

use super::*;
use sched_model::Millis;

const GRACE: u64 = 10 * MIN; // policy.idle_grace of the example

/// The managed example with `paperless-tagging` and `speakr-transcription` declaring
/// `allowed: with_work`.
fn gated_yaml() -> String {
    let mut yaml = managed_yaml();
    for class in ["paperless-tagging", "speakr-transcription"] {
        let anchor = format!("  - id: {class}\n");
        assert!(yaml.contains(&anchor));
        yaml = yaml.replacen(&anchor, &format!("{anchor}    allowed: with_work\n"), 1);
    }
    yaml
}

fn gated_config() -> Config {
    Config::from_yaml(&gated_yaml()).unwrap_or_else(|e| panic!("{e}"))
}

/// The LLM serving, so `paperless-tagging`'s requirements hold; its queue reads
/// `ready`/`in_flight`.
fn llm(ready: u64, in_flight: u64) -> Case {
    let mut c = Case::new()
        .gpu(&serving("llm", 10 * MIN))
        .target(Some("llm"))
        .obs(pods("llm", 1, 1, 0))
        .obs(demand(
            "paperless-tagging",
            "paperless-post-ocr",
            ready,
            in_flight,
            0,
        ));
    c.config = gated_config();
    c
}

fn stamped(case: &Case, class: &str, ago: u64) -> SchedState {
    let mut st = case.state();
    st.work_seen.insert(cid(class), Millis(T - ago));
    st
}

fn stamp(d: &Decision, class: &str) -> Option<Millis> {
    d.state.work_seen.get(&cid(class)).copied()
}

#[test]
fn a_class_with_work_is_allowed_and_stamped_and_without_the_key_nothing_is_stamped() {
    for (ready, in_flight) in [(1, 0), (0, 1)] {
        let d = llm(ready, in_flight).run();
        assert!(allowed(&d, "paperless-tagging"), "{:?}", d.allowed);
        assert_eq!(stamp(&d, "paperless-tagging"), Some(Millis(T)));
    }
    // The other class of the model has no key: allowed as before, no stamp.
    let d = llm(0, 0).run();
    assert!(allowed(&d, "speakr-summarization"));
    assert_eq!(stamp(&d, "speakr-summarization"), None);
    assert_eq!(d.state.work_seen.len(), 0);
}

#[test]
fn known_zero_work_past_the_grace_is_not_allowed_while_the_model_stays_allowed() {
    let c = llm(0, 0);
    let d = c.run();
    assert!(!allowed(&d, "paperless-tagging"), "{:?}", d.allowed);
    assert_eq!(stamp(&d, "paperless-tagging"), None);
    // The model stays allowed for the class without the key.
    assert!(d.allowed.models.contains(&mid("llm")), "{:?}", d.allowed);
    assert!(allowed(&d, "speakr-summarization"));
}

#[test]
fn the_class_stays_allowed_to_one_millisecond_before_the_grace_and_leaves_at_it() {
    let c = llm(0, 0);
    let d = c.run_at(&stamped(&c, "paperless-tagging", GRACE - 1), T);
    assert!(allowed(&d, "paperless-tagging"), "{:?}", d.allowed);
    // The old stamp is carried forward, not renewed.
    assert_eq!(stamp(&d, "paperless-tagging"), Some(Millis(T - GRACE + 1)));
    let d = c.run_at(&stamped(&c, "paperless-tagging", GRACE), T);
    assert!(!allowed(&d, "paperless-tagging"), "{:?}", d.allowed);
    assert_eq!(stamp(&d, "paperless-tagging"), None, "dropped once over");
    // A stamp in the future (the clock went back) counts as just made.
    let mut st = c.state();
    st.work_seen
        .insert(cid("paperless-tagging"), Millis(T + MIN));
    assert!(allowed(&c.run_at(&st, T), "paperless-tagging"));
}

#[test]
fn work_that_arrives_before_the_model_is_stamped_whether_or_not_the_requirements_hold() {
    // The GPU idle: the class is not allowed (its model is not serving) but its work is seen.
    let mut c = Case::new().obs(demand("paperless-tagging", "paperless-post-ocr", 3, 0, 0));
    c.config = gated_config();
    let d = c.run();
    assert!(!allowed(&d, "paperless-tagging"));
    assert_eq!(stamp(&d, "paperless-tagging"), Some(Millis(T)));
}

#[test]
fn an_unknown_count_keeps_the_class_allowed() {
    for case in [
        llm(0, 0).drop_obs("paperless-post-ocr"),
        llm(0, 0).obs(demand(
            "paperless-tagging",
            "paperless-post-ocr",
            0,
            0,
            10 * MIN + 1,
        )),
    ] {
        let d = case.run();
        assert!(allowed(&d, "paperless-tagging"), "{:?}", d.allowed);
        assert_eq!(stamp(&d, "paperless-tagging"), Some(Millis(T)));
    }
}

#[test]
fn a_host_freshly_seen_down_is_no_work() {
    // Nothing reports, and the NFS host is seen down: offline, not unknown.
    let c = llm(0, 0)
        .drop_obs("paperless-post-ocr")
        .obs(up(N, false, 0));
    let d = c.run();
    assert_eq!(stamp(&d, "paperless-tagging"), None);
    assert!(!allowed(&d, "paperless-tagging"));
}

#[test]
fn a_dependent_of_a_switched_off_class_has_no_work() {
    use sched_model::Withdrawal::Done;
    // `paperless-ocr` reads its source's demand from `nfs-app`: once that is off (wait over)
    // its ready work is offline, so it has none.
    let yaml = sched_model::test_support::example_yaml_switched_dependents().replacen(
        "  - id: paperless-ocr\n",
        "  - id: paperless-ocr\n    allowed: with_work\n",
        1,
    );
    let config = Config::from_yaml(&yaml).unwrap_or_else(|e| panic!("{e}"));
    let case = |switch: &str| {
        let mut c = Case::new().obs(sw("nfs-app", switch)).obs(demand(
            "paperless-ocr",
            "paperless-ocr-pending",
            5,
            0,
            0,
        ));
        c.config = config.clone();
        c
    };
    let c = case("on");
    assert_eq!(stamp(&c.run(), "paperless-ocr"), Some(Millis(T)));
    let c = case("off");
    let d = c.run_at(&wait_state_of(&c, 10 * MIN, Done), T);
    assert_eq!(stamp(&d, "paperless-ocr"), None, "{:?}", d.state.work_seen);
}

#[test]
fn failed_only_work_keeps_the_class_allowed_and_its_recovery_runs() {
    let mut c = Case::new()
        .gpu(&serving("whisperx", 10 * MIN))
        .target(Some("whisperx"))
        .obs(pods("whisperx", 1, 1, 0))
        .obs(failed_transcriptions(2));
    c.config = gated_config();
    let d = c.run();
    assert!(allowed(&d, "speakr-transcription"), "{:?}", d.allowed);
    assert_eq!(stamp(&d, "speakr-transcription"), Some(Millis(T)));
    assert!(d.steps.contains(&recovery()), "{:?}", d.steps);
    // Nothing failed any more, and the grace is over: it leaves, with no recovery.
    c = c.obs(failed_transcriptions(0));
    let d = c.run_at(&stamped(&c, "speakr-transcription", GRACE), T);
    assert!(!allowed(&d, "speakr-transcription"), "{:?}", d.allowed);
    assert_eq!(recoveries(&d), 0);
}

#[test]
fn stamps_of_classes_without_the_key_or_no_longer_configured_are_dropped() {
    let c = llm(1, 0);
    let mut st = c.state();
    st.work_seen.insert(cid("speakr-summarization"), Millis(T));
    st.work_seen.insert(cid("gone"), Millis(T));
    let d = c.run_at(&st, T);
    assert_eq!(
        d.state.work_seen.keys().collect::<Vec<_>>(),
        vec![&cid("paperless-tagging")]
    );
}

#[test]
fn with_idle_grace_zero_work_allows_the_class_and_the_next_tick_without_work_removes_it() {
    let mut c = llm(2, 0);
    c.config = Config::from_yaml(&gated_yaml().replace("idle_grace: 10m", "idle_grace: 0"))
        .unwrap_or_else(|e| panic!("{e}"));
    let d = c.run();
    assert!(allowed(&d, "paperless-tagging"), "{:?}", d.allowed);
    assert_eq!(stamp(&d, "paperless-tagging"), Some(Millis(T)));
    // No work any more: the stamp of the previous tick is not within a grace of 0.
    let c = c.obs(demand("paperless-tagging", "paperless-post-ocr", 0, 0, 0));
    let d2 = c.run_at(&d.state, T + 1);
    assert!(!allowed(&d2, "paperless-tagging"), "{:?}", d2.allowed);
    assert_eq!(stamp(&d2, "paperless-tagging"), None);
}

#[test]
fn a_dependent_of_a_graceful_off_has_work_only_in_flight_during_the_wait() {
    use sched_model::Withdrawal::Pending;
    let yaml = sched_model::test_support::example_yaml_switched_dependents().replacen(
        "  - id: paperless-ocr\n",
        "  - id: paperless-ocr\n    allowed: with_work\n",
        1,
    );
    let config = Config::from_yaml(&yaml).unwrap_or_else(|e| panic!("{e}"));
    let run = |ready: u64, in_flight: u64| {
        let mut c = Case::new().obs(sw("nfs-app", "off")).obs(demand(
            "paperless-ocr",
            "paperless-ocr-pending",
            ready,
            in_flight,
            0,
        ));
        c.config = config.clone();
        // The wait began a minute ago and is still running.
        c.run_at(&wait_state_of(&c, MIN, Pending), T)
    };
    // Ready work is withheld while the wait runs: not work.
    assert_eq!(stamp(&run(5, 0), "paperless-ocr"), None);
    // In-flight work is.
    assert_eq!(stamp(&run(0, 2), "paperless-ocr"), Some(Millis(T)));
    assert_eq!(stamp(&run(5, 2), "paperless-ocr"), Some(Millis(T)));
}
