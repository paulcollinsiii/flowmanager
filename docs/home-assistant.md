# Home Assistant

Two optional listeners connect flowmanager to other services. The **toggle listener** lets
Home Assistant (HA) take and release leases, shut a host down and read each switch's state.
The **webhook listener** lets a service say "poll now" (*untested*). Terms are in
[concepts.md](concepts.md). Flags, ports, secrets and network policy are in
[install.md](install.md#3-flags-and-environment); this page covers what the listeners answer.

Neither listener decides anything: a request is turned into the Restate handler a person would
call ([operations.md](operations.md#restate-handlers)) and the answer is worded from the last
tick ([architecture/power.md](architecture/power.md#who-decides-what)).

## Toggle listener

Plain HTTP, started by `--toggle-listen`. Every request needs the bearer token
([install.md](install.md#4-secrets)). Names in paths are config ids, percent-decoded. A `POST`
takes its wish from the path (`/on`, `/off`) or from a body of at most 1024 bytes.

### Routes

| Method and path | What it does |
| --- | --- |
| `GET /v1/leases` | Every model, powered resource and lockout with its state, and `needs_attention` (a failed power phase or stopped shutdown) |
| `GET /v1/models/{model}` | One model's state |
| `POST /v1/models/{model}/on`, `/off` | Take or release the manual model lease (`acquire_model_lease`, `release_lease`). `off` with none held is a plain `200` |
| `GET /v1/resources/{resource}` | One host's state. A host found running reads `on` |
| `POST /v1/resources/{resource}/on` | Manual host lease: power the host on and keep it up |
| `POST /v1/resources/{resource}/off` | Graceful requested shutdown, also with no lease held (`drain_host`). Refused while the host has wakers |
| `POST /v1/resources/{resource}/off-now` | Immediate requested shutdown. Never refused |
| `POST /v1/resources/{resource}/release` | Release the lease (an adopted one too) with no drain; the host goes down only when nothing holds it. `404` if no lease is held |
| `POST /v1/resources/{resource}/resume` | Clear a failed power phase or stopped shutdown after fixing the host (`resume`). Powers nothing |
| `GET /v1/lockouts/{resource}` | One host's lockout state |
| `POST /v1/lockouts/{resource}/on`, `/off` | `lockout`, `clear_lockout` |

`POST /v1/{models,resources,lockouts}/{name}` with no suffix takes the wish from the body: `on`,
`off`, `true`, `false` (any case), `{"state": "on"}` or `{"held": true}`. This is the form an HA
`rest` switch sends. Any other body is `400`. `release`, `resume` and `off-now` exist only for
resources. The meaning of each action on a host is defined once in
[Actions](architecture/power.md#actions).

### Answers

| Status | When |
| --- | --- |
| `200` | The state as JSON, after the action |
| `400` | A body that is not a wish or is over 1024 bytes; a handler's refusal (a resource with no `power` section, a model on a shared resource) |
| `401` | Missing or wrong token (with `WWW-Authenticate`) |
| `404` | An unknown route, model or resource (the message lists what is configured); `release` with no lease held |
| `405` | A known path, another method (with `Allow`) |
| `409` | A model lease while another model of the resource is leased; `resume` of a host that is not stopped; a graceful `off` refused for wakers |
| `502`, `503`, `504` | The scheduler answered an unexpected status, could not be reached (or the token file is unreadable), or did not answer within 15 s |

Errors are `{"error": "..."}`. The wakers refusal adds the classes, and nothing was changed:

```json
{"error": "basestar-nfs was not turned off: it has wakers that would power it straight back on: paperless-ocr (work). Nothing was changed. Turn them off or wait for their work, or use off-now to reboot it",
 "wakers": [{"class": "paperless-ocr", "why": "work"}]}
```

`why` is `work` (queued or in-flight work) or `switched_on` (a switched class reading `on`).

### State

Every switch carries `kind` (`model`, `resource`, `lockout`), `id`, `held`, `state` (`on` or
`off`; a host also `draining` or `locked_out`) and, when held, `origin`, `held_since_ms` and
`held_for_seconds`. The rest is from the last tick and is absent before the first.

| Switch | Further fields |
| --- | --- |
| Model | `resource`; `status` and `detail`: `active`, `waiting_for_host`, `host_failed`, `draining`, `locked_out` or `pending` |
| Host | `resource_kind`, `lease_supported`, `power` (`{"phase": "on", "since": ..., "ours": false}`, [State](architecture/power.md#state)), `needs_attention`, `wakers` (always present), `locked_out`, `drain` (`mode`, `cause`, `since_ms`, `scaled_down`) while one is under way, `wait`, `warning` |
| Lockout | `supported`, `power`, `needs_attention`, `status` and `detail` (`pending`, `host_failed`, `powered_off`, `shutting_down`, `draining`, `left_alone`, `left_for_person`), `tick_reason` |

A `wait` entry has a `reason`: `shutdown_held_by_veto` (with `readings`, `blocking`, `stopped`),
`min_off` (with `remaining_seconds`) or `locked_out`. `warning` appears on an `off-now` of a
host with wakers: they will power it back on after `min_off`. A host that is locked out records
no `drain`: the lockout decides. The `detail` text is in `crates/sched-service/src/toggle.rs`,
from `crates/sched-sequencer/src/switch.rs`.

Each `POST` is logged and counted ([operations.md](operations.md#logs)).

### A Home Assistant setup (*untested*)

Generalised from a configuration in use, but not run in this form. The token is the HA
environment variable `FLOWMANAGER_TOGGLE_AUTH`, holding the whole header value `Bearer <token>`.

Two commands call the listener (`configuration.yaml`):

```yaml
rest_command:
  flowmanager_set:             # kind: models | resources | lockouts; state: on | off
    url: "http://flowmanager.flowmanager.svc.cluster.local:9096/v1/{{ kind }}/{{ name }}/{{ state }}"
    method: post
    headers:
      Authorization: !env_var FLOWMANAGER_TOGGLE_AUTH Bearer unset
    timeout: 20
  flowmanager_resume:
    url: "http://flowmanager.flowmanager.svc.cluster.local:9096/v1/resources/{{ resource }}/resume"
    method: post
    headers:
      Authorization: !env_var FLOWMANAGER_TOGGLE_AUTH Bearer unset
    timeout: 20
```

One poll of `/v1/leases` feeds every sensor. Use the `rest` integration for the reads and
template switches for the controls, not REST switches: HA drops a REST switch for good when
its first read is an HTTP error (as when flowmanager is restarting), while the `rest`
integration keeps retrying.

```yaml
rest:
  - resource: http://flowmanager.flowmanager.svc.cluster.local:9096/v1/leases
    headers:
      Authorization: !env_var FLOWMANAGER_TOGGLE_AUTH Bearer unset
    scan_interval: 30
    binary_sensor:
      - name: Flowmanager needs attention
        device_class: problem
        value_template: "{{ value_json.needs_attention }}"
      - name: Flowmanager LLM lease held
        value_template: >-
          {% set m = value_json.models | selectattr('id', 'eq', 'llm') | list %}
          {{ m[0].held if m | count == 1 else None }}
    sensor:
      - name: Flowmanager GPU host wakers
        value_template: >-
          {% set r = value_json.resources | selectattr('id', 'eq', 'cuda-central') | list %}
          {% if r | count != 1 %}{{ None }}{% else %}
          {% set out = namespace(items=[]) %}
          {% for w in r[0].wakers | default([]) %}
          {% set why = 'switched on' if w.why == 'switched_on' else 'work' %}
          {% set out.items = out.items + [w['class'] ~ ' (' ~ why ~ ')'] %}
          {% endfor %}
          {{ out.items | join(', ') if out.items else 'none' }}{% endif %}
```

The switch and the resume button (`templates.yaml`); `update_entity` re-reads the lease at once:

```yaml
- switch:
    - name: Flowmanager LLM lease
      state: "{{ is_state('binary_sensor.flowmanager_llm_lease_held', 'on') }}"
      availability: "{{ states('binary_sensor.flowmanager_llm_lease_held') in ['on', 'off'] }}"
      turn_on:
        - action: rest_command.flowmanager_set
          data: { kind: models, name: llm, state: "on" }
        - action: homeassistant.update_entity
          target: { entity_id: binary_sensor.flowmanager_llm_lease_held }
      turn_off:
        - action: rest_command.flowmanager_set
          data: { kind: models, name: llm, state: "off" }
        - action: homeassistant.update_entity
          target: { entity_id: binary_sensor.flowmanager_llm_lease_held }
- button:
    - name: Flowmanager resume cuda-central
      press:
        - action: rest_command.flowmanager_resume
          data: { resource: cuda-central }
```

A host switch is the same with `kind: resources`. Its `off` is graceful, so a `409` from a host
with wakers makes the switch snap back to its sensor; show the wakers sensor beside it, and
give `off-now` its own button (`rest_command` with the path `/off-now`).

### Switched apps (*untested*)

A [switched class](concepts.md#workloads) reads its switch from an HA entity, not from the
toggle listener. Make the entity an `input_select` with the options `off`, `on` and `off-now`
(no `initial`, so a restart keeps the last choice), a template switch that sets it `on` or
`off`, and a "stop now" button that sets `off-now`. This snippet is a rewrite and has not been
run:

```yaml
input_select:
  app_wikijs:
    name: Wiki.js
    options: ["off", "on", "off-now"]
```

flowmanager polls it with an `http_json` source observing `switch`. The `map` must list the
options exactly: a value it does not list fails the poll, and a failed or stale reading is
`off-now`. The keys are in [sources.md](configuration/sources.md#a-switch-source). The token is
an HA long-lived access token, mounted as a file for `auth` ([install.md](install.md#4-secrets)):

```yaml
  - id: ha-wikijs
    kind: http_json
    observe: [switch]
    resync: 30s
    params:
      auth: { header: Authorization, prefix: "Bearer ", file: /run/flowmanager/ha/token }
      requests:
        - url: http://homeassistant.example.internal:8123/api/states/input_select.app_wikijs
          value: /state
          map: { "on": on, "off": off, "off-now": off_now }
```

The class names it with `switch: { source: ha-wikijs }`. What `off` and `off-now` do is in
[concepts.md](concepts.md#workloads).

## Webhook listener (*untested*)

Started by `--webhook-listen`. It makes an `http_json` source poll now, for a service that can
send a `POST` when its queue changes, instead of waiting for `resync`. A source opts in with
`webhook: { path: /hooks/paperless }` in its params
([sources.md](configuration/sources.md#http_json)).

| Request | Answer |
| --- | --- |
| `POST <path>` of some source | `202` `{"sources": n, "nudged": m}`: `n` sources declared the path, `m` had a running observer |
| Token mode, missing or wrong token | `401`. Checked first, so a caller learns nothing about which paths exist |
| A path no source declared | `404` |
| Another method on a known path | `405` with `Allow: POST` |
| A body over 64 KiB, or unreadable | `413`, `400` |
| No observer running yet for the path, or the token file unreadable | `503` |

Authentication (`mesh` or `token`), the token file and `--webhook-min-interval` are in
[install.md](install.md#3-flags-and-environment).

A webhook only nudges. The body is ignored, and the poll's result enters the pipeline like a
scheduled one, so nothing about a decision depends on who called. Each observer merges the
nudges it gets and starts a poll no sooner than the minimum interval after the previous one
began, so a flood costs one poll per source per interval. A nudge during a poll causes one more
poll, because that poll may have read before the change; a nudged poll restarts the `resync`
clock. The nudge handle survives a config reload and is lost when its source is removed. A lost
webhook costs only delay: `resync` still polls. Requests are logged and counted
([operations.md](operations.md#logs)).
