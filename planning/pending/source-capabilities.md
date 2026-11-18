# Pending: source kinds that validate but never report

Status: **feature gap, not started.** Found 2026-11-18 while writing `docs/configuration/sources.md`.

## Problem

`SourceKind::can_supply` (`crates/sched-model/src/config.rs`) decides which `observe`
capabilities a source of each kind may declare. It is wider than what the observers in
`crates/sched-observe/src/` emit:

| Kind | `can_supply` accepts | Observer emits |
| --- | --- | --- |
| `kubernetes_watch` | pods, reachability, mounts, jobs | the same |
| `http_json` | reachability, demand, switch | demand, switch |
| `prometheus` | reachability, demand | reachability |
| `tcp_probe` | reachability | reachability |
| `sql`, `session` | demand | no observer |
| `webhook` | nothing | (a nudge receiver, by design) |

A config using a gap loads, and can even satisfy "a `power` section needs a reachability
source", but the declared capability never gets a reading. A demand source with no reading
counts as no demand, not unknown demand, so the class reads as idle instead of unknown.

## Wanted

Both gaps are real uses, not mistakes in `can_supply`:

- `http_json` reachability: a host's health endpoint (an HTTP 200 with a JSON field) as its
  up/down reading.
- `prometheus` demand: a queue-depth query (for example a consumer-lag or pending-items
  metric) as a class's demand.

`sql` and `session` demand are the same shape of gap; `session` backs the inert
`interactive-llm` class in `config/example.yaml`.

## Until built

Either tighten `can_supply` to what the observers emit (and widen it again with each
observer), or keep it and reject the combination in `validate`. Either way a config that
cannot get readings should fail to load, rather than load and read idle.

## Open questions (user's decision)

- Which of the four gaps to build, and in what order.
- For `http_json` reachability: what counts as down versus a failed read (a non-2xx, a
  field value, a timeout), matching the `tcp_probe` and `prometheus` rules in
  `docs/configuration/sources.md`.
- For `prometheus` demand: which of `ready`, `in_flight` and `failed` a query maps to, as
  `http_json` demand does with `counts_as`.

When built, update `docs/configuration/sources.md` (the kind table and the warning under it)
and the `can_supply` pitfall in `docs/agents.md`.
