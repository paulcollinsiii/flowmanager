# Pending: apps switched on and off by a person

Status: **proposal, nothing built; decisions agreed with the user 2026-11-11.** It replaces
the first draft of this file, which treated a person's request as demand. Facts marked
*(read)* were checked by reading the code, not by running it. The model-document changes in
section 8 still need the user's approval in the form they are finally written.

## 1. Problem

blade-runner (a k0s node) has more memory *requested* than it can schedule when every app is
up. Some apps (YouTrack, Wiki.js, possibly Vikunja; Paperless-ngx and Speakr) are idle most
of the time and should be off by default and switched on by a person from a Home Assistant
(HA) control.

Today flowmanager can gate a class only on host state: a gate class is allowed whenever its
hosts are up; manual leases attach to resources with a `power:` section, not to classes. There
is no way to say "this app runs because a person switched it on".

Not solved here: memory *capacity*. A person can still switch everything on. This decides
availability, not a budget.

## 2. Decisions

| # | Decision |
|---|----------|
| D1 | The on/off state lives in HA. Flowmanager reads it; it does not store the request. |
| D2 | The reading has three values: `on`, `off` (graceful) and `off-now` (immediate), chosen per press in HA, the same effect as a host's `off` and `off-now`. |
| D3 | It is a **switch reading**, not demand. A demand reading is counts; off and off-now are both zero demand, so demand cannot carry D2. |
| D4 | Fail closed: a missing, stale or unmapped switch reading is `off-now`. |
| D5 | When an app is switched off (once its graceful wait is over), the demand sources that read *from* that app are **offline** for their classes, exactly as when a host they require is down. With basestar up for Immich only, Paperless's OCR queues hold neither cuda-central nor basestar. |
| D6 | The http_json adapter gains a string-to-value map (D3's strings to switch states; also strings to numbers for demand, for example `"true"` to 1). |
| D7 | An app that needs no share requires `blade-runner`, a shared resource with no `power:` section. `requires: []` stays invalid. |
| D8 | `may_change_power` is set per switched class in config, as for any class. |

## 3. The model

### 3.1 Switched class

A **switched class** names no model and has a `switch: { source: <id> }` instead of `demand:`.
It is the third kind of class:

- a class that names a model (demand class) gates nothing: its consumers are not gated on it;
- a **gate class** (hosts only, no demand) is allowed while its hosts are up;
- a **switched class** (hosts only, a switch source) is allowed while its hosts are up and its
  switch is `on`, or while a graceful off is still waiting (3.3).

A ScaledObject gates an app on a gate or switched class with `class: <id>` metadata. KEDA and
the external scaler are unchanged *(read: `sched-keda/src/lib.rs:132` answers from the
allowed set)*.

### 3.2 Switch reading

A new capability `switch` and observation value `Switch { class, state: on | off | off_now }`.
Freshness uses `policy.queue_reading_stale_after` (to confirm: a separate setting may be
wanted, since an HA blip longer than the window switches every app off under D4).

| Reading | Allowed | Holds its hosts (invariant 10) | Wakes its hosts |
|---------|---------|--------------------------------|-----------------|
| `on` | yes (hosts up, no lockout) | yes | if `may_change_power` |
| `off` | during the graceful wait only (3.3) | no (the dependents' own in-flight work does) | no |
| `off_now`, stale, missing, unmapped | no | no | no |

A switched class has no demand of its own, so the first draft's asymmetry (unknown demand
keeping basestar up while fail-closed kept the app down) does not arise.

### 3.3 Graceful and immediate off

A switched class's **dependents** are the classes whose demand source declares
`reads_from: <switched class>` (3.4).

- **`off` (graceful).** The class stays allowed while a dependent reports in-flight work,
  bounded by `drain_timeout` from when `off` was first read. The dependents' *ready* work does
  not count during the wait: it wants no model and wakes no host. Their in-flight work keeps
  holding its model and hosts, as now; unknown dependent demand counts as in flight (as
  everywhere in the power model), bounded by the same timeout. When no dependent has work in
  flight, or the timeout runs out, the class leaves the allowed set. With no dependents, or
  none with work (Wiki.js; Paperless while cuda-central is off), graceful is immediate.
- **`off-now` (immediate).** The class leaves the allowed set at once.
- **Changes during the wait.** `on` cancels it; `off-now` (or a stale reading, D4) ends it at
  once. This mirrors `off` then `off-now` on a host.
- **Overrides.** A lockout or a requested shutdown of a host the class requires withdraws it
  by that host's rules, whatever the switch says.

**State.** The bounded wait needs a start time, which no reading carries, so this is the one
stored addition: `SchedState.classes: BTreeMap<ClassId, SwitchOff>`, with
`SwitchOff { since, withdrawal }` reusing `Withdrawal` from the host `Shutdown`
(`sched-model/src/state.rs:293`) rather than a new flag. It is created when `off` is first
read, kept while the reading stays off or off-now (so the next tick does not start a new wait),
marked done when the class leaves, and removed when the reading is `on`. State lost on a
restart starts a new bounded wait from the restart, which is safe.

### 3.4 Offline dependents

A demand source may declare `reads_from: <switched class>`: the service it polls is that app.
Offline (`power-model.md` 2.1c) widens to: *a fresh reading shows a host the class requires
is down, **or** the switched class its demand source reads from is off and its graceful wait
is over.* Unlike host-offline, a fresh demand reading does **not** win against a switch-off:
the person turned the app off, and the graceful wait is how in-flight work is honoured.

Whether a switched class is off is decided by one pure function in `sched-model` (next to
`class_work`, `src/work.rs:64`), read by both planner and sequencer so they cannot disagree.
It reads the switch observation and `SchedState.classes`.

## 4. Configuration (homesvr side, not built)

```yaml
resources:
  - id: blade-runner          # D7: the node the apps run on; no power section, always on
    kind: shared

sources:
  - id: ha-wikijs
    kind: http_json
    resync: 30s
    observe: [switch]
    params:
      requests:
        - url: http://homeassistant.../api/states/input_select.app_wikijs
          value: /state
          map: { "on": on, "off": off, "off-now": off_now }   # D6; anything else fails the poll
      auth: { ... read-only long-lived token from env ... }
      webhook: { path: /ha/wikijs }   # HA automation calls it on change: poll now
  - id: paperless-ocr-pending
    # ... as today, plus:
    reads_from: paperless-ngx         # 3.4

classes:
  - id: wikijs
    requires: [{ resource: blade-runner }]
    priority: 0
    preemptibility: kill
    switch: { source: ha-wikijs }
  - id: paperless-ngx                 # today a gate class on basestar-nfs
    requires: [{ resource: basestar-nfs }]
    may_change_power: true            # D8: switching it on wakes basestar
    switch: { source: ha-paperless }
```

The exact placement of `value`/`map` (per request, alongside `count`/`length`) and of
`reads_from` (source level) is for the implementer to settle against `source_params.rs`; the
config above shows the intent.

In HA: one `input_select.app_<name>` with options `on`, `off`, `off-now`; an auto-off
timer sets `off`; an automation calls the webhook path on change; a read-only token.

In each app's namespace: a ScaledObject with one external trigger to
`flowmanager.flowmanager.svc.cluster.local:9095`, metadata `class: <id>`,
`minReplicaCount: 0`. The Deployment declares no `replicas`.

Order of apps: Wiki.js and YouTrack first (no dependents), then Paperless-ngx and Speakr
(dependents, D5), then maybe Vikunja.

## 5. Validation rules to add

- A class has at most one of `demand:` and `switch:`.
- A switched class names no model on any requirement and requires at least one resource.
- A `switch:` source must declare the `switch` capability; only http_json (and, if wanted,
  the adapters that can map a string) can supply it.
- `reads_from` names an existing switched class, and only on a source that declares `demand`.
- `may_change_power` still needs a required resource with a power section (existing rule):
  so `false` on a class that requires only `blade-runner`.
- A map value that is not a valid switch state (for a switch source) or not a number (for a
  demand source) is rejected at load.

## 6. Code areas (pure logic stays in the pure crates)

- `sched-model`: `Capability::Switch`, `ObservedValue::Switch`, the class `switch:` and source
  `reads_from:` config, validation, `SchedState.classes`, the switch-off function, the offline
  widening in `class_work`.
- `sched-observe/src/http_json.rs`: the `value` + `map` reading for both capabilities; unmapped
  values fail the poll.
- `sched-planner/src/fifo.rs`: a switched class `on` wants its powered hosts up if
  `may_change_power`; during a graceful wait, dependents' ready work wants nothing.
- `sched-sequencer`: `class_allowed` (`lib.rs:223`) gains the switch condition; the
  `SwitchOff` lifecycle; invariant 10 counts a switched class `on` as a hold.
- `sched-service`: reason text and status for each cause of an app being down, kept distinct:
  switched off, switching off (waiting for in-flight work in <class>, until <time>), switch
  reading stale or unmapped, host down, lockout. No policy here.
- `sched-service/src/invariants.rs`: a check that a switched class is allowed only while its
  switch is `on` or its graceful wait is within `drain_timeout`.

## 7. Suggested phases

1. **Switch reading.** http_json `value` + `map`, the capability, the observation, validation.
   Nothing acts on it yet.
2. **Switched class, immediate only.** `switch:` on a class, `class_allowed`, `off` treated as
   `off-now` for now, holds and wakes for `on`, status reasons, invariant check, docs. Enough
   for Wiki.js and YouTrack.
3. **Graceful off and dependents.** `SwitchOff` state, `reads_from`, the offline widening,
   the graceful wait. Enough for Paperless-ngx and Speakr.

## 8. Model-document changes (user's decision; present as drafts)

- `planning/architecture.md`: the gate class paragraph (line 86) gains the switched class and
  the three-kind summary in 3.1; invariant 3 ("A workload class is allowed only while every
  requirement holds") adds the switch condition; invariant 10 adds a switched class `on` as a
  hold; a new invariant for 3.3's bound, with its row in the mapping table.
- `planning/power-model.md` 2.1c: offline widened as in 3.4, including that a fresh reading
  does not win against a switch-off.
- Unchanged and still true: "consumers are not gated on their demand class"
  (`sched-model/src/plan.rs:127`, `sched-service/src/reason.rs:231` and `:262`); they describe
  `PauseIntake` for classes that name a model, which a switched class never gets.

## 9. Corner cases kept as known behaviour

- **Switching on, before the app answers.** Its dependents' sources fail while pods start, so
  their demand is unknown: it keeps a loaded model and keeps basestar from idling, but wakes
  nothing (`ClassWork::busy()` is false for unknown, `fifo.rs:87`, *read*). The same gap exists
  today when basestar boots.
- **An HA outage** switches every app off (D4), after the staleness window.
- **paperless-gpt** (a gate class on cuda-central and NFS) can run with Paperless-ngx switched
  off, with nothing to talk to. Harmless; fixing it needs a class to require a class, which is
  not proposed.
- **A stale reading during a graceful wait** ends it at once (D4).

## 10. Out of scope

A memory budget resource; leases on classes; TTLs in flowmanager (HA's auto-off is the TTL);
a ScaledObject naming a demand class.

## 11. Alternatives rejected

- **The request as demand** (this file's first draft): cannot carry graceful versus immediate
  per press (D2), and unknown demand from a stale HA reading would have held hosts while the
  app was down.
- **A switch resource** up while a manual lease is held: a new resource concept bent to mean
  one app, plus a TTL to add.
- **A manual-control exclusive resource with one model per app**: bends "exclusive resource"
  to mean "one app"; unconfirmed that an unobserved manual host counts as up.
- **HA patches ScaledObjects directly**: gives HA cluster RBAC and loses flowmanager's view,
  dashboard and alerts, and cannot take the dependents offline.
