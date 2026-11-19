# Models and classes

`models` name what an exclusive host can serve; `classes` name the work that needs a host or a
model. Terms are in [concepts.md](../concepts.md#workloads). Ids of both follow the rule in
[requirements.md](../requirements.md#workloads), because the scaler names its metrics from
them.

## Models

A model is a mode of an exclusive resource, implemented by one or more Kubernetes workloads.

| Key | Default | Meaning and limits |
| --- | --- | --- |
| `id` | required | Unique |
| `resource` | required | An existing resource of `kind: exclusive` |
| `load_time` | required | Estimated load time, above `0`. A tie-break between candidates ([priority](../concepts.md#workloads)) |
| `workloads` | required | At least one non-empty name. A workload may belong to only one model, and be listed once |
| `namespaces` | `[]` | Where the workloads run. For a managed host with a `pods` source, every one must be in that source's `namespaces` ([power.md](power.md#what-a-powered-resource-needs)) |

Every exclusive resource needs at least one model. The workload names are matched to pods by
the `kubernetes_watch` source's `workload_label`.

```yaml
models:
  - { id: mineru, resource: cuda-central, load_time: 120s, workloads: [mineru], namespaces: [ai] }
```

## Classes

| Key | Default | Meaning and limits |
| --- | --- | --- |
| `id` | required | Unique |
| `requires` | required | At least one entry, [below](#requires) |
| `priority` | required | A non-negative integer; the higher tier wins ([priority](../concepts.md#workloads)) |
| `preemptibility` | required | [`none`, `pause_intake` or `kill`](#preemptibility) |
| `may_change_power` | `false` | The class may cause a required host to be powered on. Needs a power section on at least one required resource. See [wakers](../architecture/power.md#wakers) |
| `demand` | none | `{ source: <id> }`: the demand source. The source must declare `demand` |
| `switch` | none | `{ source: <id> }`: the switch source. The source must declare `switch` |
| `recovery` | none | [Re-queue of failed items](#recovery) |
| `allowed` | `always` | `always`: allowed whenever its requirements hold. `with_work`: also only while the class has work, or within `idle_grace` after it last had ([concepts](../concepts.md#workloads)). `with_work` needs `demand`; on any other class the config is rejected |
| `kill_timeout` | see below | How long this class's in-flight work may run once a wind-down starts. Above `0`, at most `1d`. Which timeout applies when it is not set, and where it is ignored, is the [timeouts table](../architecture/power.md#timeouts) |

### Class kinds

The kind follows from the keys; there is no `kind` field.

| Kind | `requires` | `demand` | `switch` |
| --- | --- | --- | --- |
| Demand | Each exclusive entry names a `model` | Set | Forbidden |
| Gate | At least one exclusive entry with no `model` (only that the host is up) | Forbidden | Not set |
| Plain | Shared resources | Not set | Not set |
| Switched | Hosts only: no `model` on any entry | Forbidden | Required |

A class with `switch` is switched whatever it requires. A class that names a model but sets no `demand` loads and never has work. `recovery` needs `preemptibility: kill`. What each kind does: [concepts](../concepts.md#workloads).

```yaml
classes:
  - id: paperless-ocr              # demand
    requires:
      - { resource: cuda-central, model: mineru }
      - { resource: basestar-nfs }
    priority: 50
    preemptibility: kill
    demand: { source: paperless-ocr-pending }

  - id: paperless-gpt              # gate
    requires:
      - { resource: cuda-central }
      - { resource: basestar-nfs }
    priority: 0
    preemptibility: none

  - id: immich-server              # plain
    requires:
      - { resource: basestar-nfs }
    priority: 0
    preemptibility: none

  - id: wikijs                     # switched
    requires:
      - { resource: blade-runner }
    priority: 0
    preemptibility: kill
    switch: { source: ha-wikijs }
```

### `requires`

Each entry is `{ resource, model }`.

- The list is not empty and names each resource once. Every resource and model must exist.
- On an `exclusive` resource `model` is optional. Given, it must be a model of that resource;
  absent, the entry only needs the host up.
- On a `shared` resource `model` is forbidden.

### `preemptibility`

One of `none`, `pause_intake` or `kill`; what they do is in [concepts](../concepts.md#workloads).
`kill` is required for a class with a `recovery`; the scheduler treats `pause_intake` and `kill`
alike.

### `switch` and `demand`

Both name a source by id. The source's `observe` must include the matching capability
(`switch` or `demand`), and the source kind must supply it
([sources.md](sources.md#kinds-and-capabilities)). A switched class has `switch` and no
`demand`. A demand source may set `reads_from` to the id of a switched class
([sources.md](sources.md#common-fields)).

## `recovery`

```yaml
recovery:
  action: requeue_failed
  source: speakr-pending
  params: { list: ..., requeue: ... }
```

| Key | Required | Meaning |
| --- | --- | --- |
| `action` | yes | `requeue_failed`, the only action |
| `source` | no | An existing source, usually the class's demand source |
| `params` | yes | Below |

The class must have `preemptibility: kill`.

The reverse also holds. A class whose demand source counts `failed`
(a request or an `also` entry with `counts_as: failed`) must declare a `recovery`, or the config
is rejected: without one, failed items would hold the model with nothing to re-queue them.

### `requeue_failed` params

| Key | Default | Meaning and limits |
| --- | --- | --- |
| `list.url` | required | The request that lists failed items. `http://` or `https://`, no `user@` |
| `list.items` | required | JSON pointer to the array of items |
| `list.id` | required | JSON pointer, relative to one item, to its id (a string or an integer). Not empty |
| `list.where` | none | Keep only matching items; exactly one of `contains` or `equals` ([filters](sources.md#filters-and-counts)) |
| `list.since` | none | Only items changed in a window ending now ([since](sources.md#since)) |
| `list.max_items` | none | The most items one response can hold (the page size in the URL). Above `0`. A full page is re-queued now and the rest on a later run |
| `requeue.url` | required | `http://` or `https://`, with `{id}` exactly once and no other braces. An id that is not letters, digits and `-_.` is skipped |
| `requeue.method` | `POST` | `POST`, `PUT` or `PATCH` |
| `requeue.body` | none | A JSON body, sent as `application/json`. Absent: no body |
| `requeue.expect` | `[]` | Conditions `{ field, equals }` on the JSON answer that must all hold for the request to count as queued. A non-2xx answer never counts |
| `auth` | none | [Credentials](sources.md#auth) |
| `max_attempts` | `2` | Re-queue requests one item may be given within `window`. 1 to 10 |
| `window` | `15m` | The list's `since` window: how long a failure stays listed. Above `0`, at most 365 days |
| `retention` | `24h` | How long an item's attempts are remembered after the last. At least `window`, at most 365 days |
| `min_interval` | `60s` | The least time between two runs. At most `1h`; `0` allowed |
| `max_per_run` | `5` | The most re-queue requests one run sends. 1 to 100 |
| `timeout` | `10s` | Bound on each HTTP request. Above `0`, at most `5m` |

`window` is a separate key from `list.since.ago`, not derived from it: set both to the same
value. The action runs while the
class's model is allowed ([concepts](../concepts.md#workloads)).

```yaml
    recovery:
      action: requeue_failed
      source: speakr-pending
      params:
        list:
          url: http://speakr.media.svc:8899/api/v1/recordings?status=failed&per_page=100&sort_by=updated_at&sort_order=desc
          items: /recordings
          id: /id
          where: { field: /has_summary, equals: false }
          since: { param: updated_since, ago: 15m }
          max_items: 100
        requeue:
          method: POST
          url: http://speakr.media.svc:8899/api/v1/recordings/{id}/transcribe
          body: {}
          expect:
            - { field: /success, equals: true }
            - { field: /status, equals: QUEUED }
        auth: { header: Authorization, prefix: "Bearer ", env: SPEAKR_API_TOKEN }
        max_attempts: 2
        window: 15m
        retention: 24h
        min_interval: 60s
        max_per_run: 5
        timeout: 10s
```
