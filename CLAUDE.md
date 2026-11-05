# Instructions for agents working in this repository

Start with [README.md](README.md) for the layout. This file covers only what an
agent would otherwise get wrong, and points into `docs/` rather than restating it.

## Three-role orchestration

Substantial work here is run as three roles that deliberately cannot see each
other's state:

- **Orchestrator** — the hub. Owns sequencing, holds the cross-phase state, and
  is the only role that talks to both of the others.
- **Implementor** — executes one phase from a dispatch of verified facts.
  **Must not receive the acceptance criteria**, and never asserts that they are
  met. Produces local commits and a factual report. Never pushes.
- **Reviewer** — verifies a phase against its acceptance criteria, adversarially.
  **Must not receive the Implementor's report.** Runs every command itself.
  Proposes no fixes.

All coordination goes through the Orchestrator; Implementor and Reviewer never
exchange state directly.

**Why, since the shape alone will not survive a deadline:** a visible acceptance
criterion gets written to rather than satisfied, steering the work instead of
testing it. An implementation report biases a reviewer toward confirming it, so
the Reviewer re-derives outcomes from the repo and the cluster rather than
trusting a summary. This is *verify before asserting*, enforced structurally.

This is not ceremony: it repeatedly caught work reported as complete and passing
that was in fact broken — hooks reported green while failing, and a check
reported as "no issues" that could not execute at all in the image it runs in.

**Reviewers also check fit to the model, not only the phase's criteria.** Some
areas have an agreed model document (host power: `planning/power-model.md`
sections 2–3; overall design and the invariant list: `planning/architecture.md`).
The Orchestrator includes the relevant one in every Reviewer dispatch. The
Reviewer reports a model-fit section with a verdict for each item below (found /
none found), so an empty section cannot be mistaken for a skipped one:

- state, a flag, a concept or a special case that the model document does not
  describe;
- shared and exclusive resources handled differently where the model says they
  behave the same;
- decision logic outside the pure crates (planner, sequencer), or HTTP or
  Restate glue that decides policy;
- every "current" / "target" / "enforced" marker in changed docs, checked
  against the code;
- the model document, the invariant list or its mapping out of step with the
  code (enumerate every `out.push` in `crates/sched-service/src/invariants.rs`
  rather than trusting a count).

Known deviations the model already records as current behaviour, still to be
fixed by a later phase, are not findings. New code or text must not add to them.
A phase that passes its criteria but does not fit the model is not accepted
until the code or the model document changes, and changing the model is the
user's decision.

**Why:** five power features were each accepted against their own criteria and
together drifted from any single design: six coupled state flags, two records of
host ownership, and different shutdown verbs for shared and exclusive hosts
(architecture review, Oct 30, 2026).

## Verify before asserting

Prefer running a command over reasoning about what a file probably says. Claims
that looked obviously safe here have been wrong on checking — counts, which
tools were on `PATH`, whether a hook was blocking.

**`$?` after a pipeline is the last stage's exit code.** `cmd | tail; echo $?`
reports `tail`, and has produced false "it passes" reports here more than once.
Redirect to a file and read `$?` from the bare command.

# General Guidelines
- Do not use python for file edits.