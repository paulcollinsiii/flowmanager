# Pending: a demand class allowed only while it has work

Status: **approved 2026-11-19 (decisions 1–6); phase 1 is built (flowmanager); the section 5 text
in the model documents describes current behaviour. The homesvr follow-up is not done.** Facts marked *(read)* were checked by reading the code, not by running it.

## 1. Problem

A class is allowed whenever its requirements hold, whatever its demand *(read:
`class_allowed` in `crates/sched-sequencer/src/lib.rs` checks `requirements_hold` and the
switch phase only)*. Demand decides only which model is wanted *(read: `candidate` in
`crates/sched-planner/src/fifo.rs`)*. The KEDA scaler reports 1 while a class is allowed and 0
otherwise *(read: `get_metrics` in `crates/sched-keda/src/lib.rs`)*.

homesvr's `lightrag-indexing` (requires `cuda-central` + `llm`, and `blade-runner`; demand
`glueon-lightrag`) therefore scales LightRAG and its embedders up whenever the LLM is allowed
for any class (speakr, chat, paperless tagging), not only when glueon has documents to index.
The cost that matters is `llama-embed-cuda-central`, which takes memory on cuda-central next to
the LLM.

Not a problem: LightRAG being down while glueon has work. glueon's count includes deliveries
waiting for LightRAG and the LLM (homesvr `glueon-lightrag` source comment), and the sink polls
`documents/track_status` until a document is indexed *(read: glueon
`src/glueon/integrations/lightrag/workflow.py`, `wait_until_indexed`)*. So demand stays above 0
until indexing really ends: gating on it cannot deadlock or cut an index short.

## 2. Decisions

| # | Decision |
|---|----------|
| 1 | The rule is a property of the class, not of a KEDA trigger. It is validated at load, decided in the sequencer and checked by the invariant checker; `sched-keda` stays a lookup of the allowed set and does not change. |
| 2 | The class stays allowed for `policy.idle_grace` after it last had work, the same linger a host gets, so a queue that empties between documents does not cold-start the pods each time. |
| 3 | The target is cuda-central's memory. bladerunner's memory is not a driver. |
| 4 | Key `allowed: with_work`; the default is `allowed: always` (today's behaviour). |
| 5 | Work is `ClassWork::wanted()` (ready, in-flight or failed items) or an unknown count. Offline demand is no work. `failed` is included because recovery runs only for an allowed class (invariant 17). Failed counts are bounded by the source's `since` window and the recovery's `max_attempts`, not by this rule. |
| 6 | A demand source that counts `failed` (a request or an `also` entry with `counts_as: failed`) may be the `demand` of a class only if that class declares a `recovery`; otherwise the config is rejected. Without a recovery, failed items hold the model (and, with `with_work`, the class) with nothing to re-queue them. Today nothing rejects this *(read: no such check in `crates/sched-model/src/validate.rs` or `source_params.rs`)*. homesvr already complies: its two failed counts (`speakr-pending`, `speakr-summaries`) feed classes that have a recovery. |

## 3. The model

A demand class may declare `allowed: with_work`. It is allowed when the existing rules allow it
(every requirement holds; a lockout or requested shutdown first) **and** it has work, or less
than `idle_grace` has passed since a tick last saw it with work.

- **Work** is `class_work(...).wanted() || !known` (`ready > 0`, `in_flight > 0`,
  `failed > 0`, or an unknown count). Offline (a host freshly seen down, or the source's `reads_from`
  switched class off) is no work, as everywhere else ([Offline demand]).
- **Stored fact:** `SchedState.work_seen: BTreeMap<ClassId, Millis>`, the time of the last tick
  that saw the class with work. It is written whether or not the requirements hold, so work
  that arrives before the LLM does makes the class allowed the moment the LLM is up. An entry
  is removed once its grace is over or the class no longer declares `with_work`. Missing (first
  run, or a restart before any work) reads as "no recent work". `#[serde(default)]`, so state
  persisted earlier still loads.
- **What it does not change:** model choice, host holds, power, wakers and recovery. The
  planner never reads the allowed classes *(read: `runnable` checks hosts only)*, so there is no
  cycle between choosing the model and allowing the class. In-flight work counts as work, so a
  class never leaves with work in flight. Leaving needs no wind-down: nothing is running.
- **Rejected at load:** `with_work` on a class without `demand` (a gate, plain or switched
  class has no work to read).

Config, homesvr shape:

```yaml
- id: lightrag-indexing
  requires:
    - { resource: cuda-central, model: llm }
    - { resource: blade-runner }
  priority: 40
  preemptibility: kill
  allowed: with_work
  demand: { source: glueon-lightrag }
```

## 4. Alternatives rejected

- **`when: demand` on a KEDA trigger** (homesvr's first proposal): the rule would sit in
  ScaledObject metadata, which config validation and the invariant checker cannot see, and it
  would put a decision into `sched-keda`, which is documented as a lookup only. It also had no
  linger.
- **KEDA `scalingModifiers`** that combine the class metric with a `metrics-api` trigger on
  glueon: the scheduling decision would sit outside flowmanager, a failed read would follow
  KEDA's error handling rather than "unknown holds", and it is untested with `external-push`.
- **A switched class driven from glueon's count:** a switch means a person's intent. A missing
  reading is `off-now`, which would kill an index in progress, and the switch could not require
  the LLM.
- **A long `cooldownPeriod` on the ScaledObjects as the only linger:** it works without code,
  but `idle_grace` keeps one idea of "idle" across hosts and classes (decision 2).

## 5. Model-document changes (approved; written, marked *target* until built)

Also written: the `failed` rule (decision 6) in `docs/configuration/sources.md` (`counts_as`)
and `docs/configuration/workloads.md` (`recovery`).


- `docs/concepts.md`, Workload class: add `allowed` to the optional keys, and after the kinds
  list: "A demand class with `allowed: with_work` is allowed only while it has work (ready,
  in-flight, failed or unknown) or within `idle_grace` after it last had, so the workloads that
  scale on it follow its own queue rather than its model."
- `docs/architecture.md`, invariant 22: "A class with `allowed: with_work` is allowed only while
  it has work (ready, in flight, failed or unknown; offline is none) or less than `idle_grace`
  has passed since a tick last saw it with work. The class's other rules come first. Only a
  demand class may declare it" — checked by `invariants.rs` (allowed) and the config loader
  (demand only).
- `docs/architecture.md`, invariant 3: no change. It states what an allowed class needs, and 22
  adds a condition.
- `docs/configuration/workloads.md`: the key, its default, the load error, and the homesvr
  example.
- `docs/architecture.md` state description: `work_seen`, next to the switched class's
  `SwitchOff`.

## 6. Phases

**Phase 0 (Orchestrator, after approval):** write section 5 into the model documents, marked
*target*.

**Phase 1 (Implementor, then Reviewer):**
- `sched-model`: `ClassSpec.allowed` (enum, default `always`), load validation, the
  `failed`-needs-recovery check (decision 6), and `SchedState.work_seen`. Example and fixture
  configs that break the new check are fixed, not exempted.
- `sched-sequencer`: update `work_seen` each tick; `class_allowed` applies the gate.
- `sched-service`: the invariant 22 check (enumerate the new `out.push` lines), the status
  reason for a class held out for lack of work (`reason.rs`), and no persist break.
- Tests: unit tests for the gate, the grace boundary, unknown and offline work, failed-only
  work with recovery, and a restart with and without a stored stamp. Add the key to the
  property-test generator.
- Flip the section 5 markers to *enforced*.

Reviewer dispatch: the criteria (section 7), `docs/architecture.md` and its invariant list, and
the model-fit checklist from `CLAUDE.md`.

**Follow-up in homesvr (not flowmanager):**
- `lightrag-indexing` gets `allowed: with_work`.
- Triggers per the table below.

| Workload | Triggers |
|----------|----------|
| lightrag, llama-embed | `class: lightrag` and `class: lightrag-indexing` |
| llama-embed-cuda-central | `class: lightrag-indexing` |
| llama-rerank | `class: lightrag` |

- A Home Assistant "Query LightRAG" script that sets the switch `on` and takes the LLM lease,
  and a "done" script that undoes both.

## 7. Acceptance criteria (for the Reviewer only; not sent to the Implementor)

1. With a fresh reading of `ready > 0` and its requirements held, a `with_work` class is in
   the published allowed set. With known zero work for longer than `idle_grace`, it is not,
   while its model stays allowed for another class.
2. It stays allowed at `idle_grace − 1 ms` after the last tick with work and leaves at
   `idle_grace`.
3. A missing or stale reading (unknown) keeps it allowed. A host freshly seen down, or a
   `reads_from` switch off, is no work.
4. Failed-only work keeps it allowed, and its recovery runs.
5. A class without the key behaves exactly as before: the existing test suite passes
   unchanged.
6. `allowed: with_work` on a class without `demand` is rejected at load, and a reload with it
   keeps the last good config.
7. State persisted before the change loads; a restart with a stored stamp keeps the grace.
8. The invariant checker flags a `with_work` class allowed with no work past its grace, and
   the property test runs with the key in its generator.
9. `sched-keda` and the planner are unchanged.
10. A class whose demand source counts `failed` (in a request, and separately in an `also`
    entry) and which has no `recovery` is rejected at load, with an error naming the class and
    the source. With a recovery, it loads. Every config shipped in the repo still loads.
11. Workspace build, clippy and tests pass, each exit code read from the bare command.

[Offline demand]: ../../docs/architecture/power.md#offline-demand
