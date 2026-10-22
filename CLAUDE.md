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

## Verify before asserting

Prefer running a command over reasoning about what a file probably says. Claims
that looked obviously safe here have been wrong on checking — counts, which
tools were on `PATH`, whether a hook was blocking.

**`$?` after a pipeline is the last stage's exit code.** `cmd | tail; echo $?`
reports `tail`, and has produced false "it passes" reports here more than once.
Redirect to a file and read `$?` from the bare command.