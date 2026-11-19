# Sources

A source is an observer adapter: it reads one thing from the cluster or from a service and
reports it. Terms are in [concepts.md](../concepts.md#demand-and-observation). RBAC for the
Kubernetes reads is in [install.md](../install.md#6-rbac). *Untested* marks what no real
deployment has exercised.

## Common fields

| Key | Default | Meaning and limits |
| --- | --- | --- |
| `id` | required | Unique |
| `kind` | required | One of the [kinds below](#kinds-and-capabilities) |
| `resync` | required | How often the source is polled. Above `0`; see [staleness](#staleness) |
| `observe` | `[]` | The capabilities the source reports; only those are accepted from it. Each must be one its kind can supply |
| `reads_from` | none | The id of a switched class whose app this source polls ([concepts](../concepts.md#workloads)). Only on a source that observes `demand`; the class must have a `switch` |
| `params` | none | The kind's own keys, below. A source with no `params` is accepted but never polled |

## Kinds and capabilities

| Kind | Supplies |
| --- | --- |
| `kubernetes_watch` | `pods`, `reachability`, `mounts`, `jobs` |
| `http_json` | `demand`, `switch` |
| `prometheus` | `reachability` (*untested*) |
| `tcp_probe` | `reachability` |

Validation also accepts `reachability` on `http_json` and `demand` on `prometheus`, but
neither produces a reading (`crates/sched-observe/src/http_json.rs`, `prometheus.rs`), so do
not use them. The kinds `sql`, `session` and `webhook` parse but have no observer in this
build. A demand source of such a kind, or with no `params`, never reports, and its class counts
as no demand, not unknown demand ([concepts](../concepts.md#demand-and-observation);
`crates/sched-model/src/work.rs`, `crates/sched-observe/src/supervisor.rs`).

A source that observes `jobs` or `switch` must observe nothing else, and at most one source
may observe `jobs`.

## Staleness

A reading older than its limit is unknown. `resync` may not exceed
[`policy.queue_reading_stale_after`](README.md#policy), and for a source that supplies `pods`,
`reachability`, `mounts` or `jobs` not
[`policy.host_reading_stale_after`](README.md#policy) either.

## `kubernetes_watch`

Reads the Kubernetes API: node readiness, pods, NFS mounts and the Jobs flowmanager made.

| Key | Default | Needed for | Meaning and limits |
| --- | --- | --- | --- |
| `namespaces` | `[]` | `pods` (not empty) | Namespaces whose pods are watched. For `mounts`, empty means every namespace, which a safety check wants |
| `workload_label` | `app.kubernetes.io/name` | `pods` | The pod label whose value is the workload name in `models[].workloads`. Not empty |
| `nodes` | `{}` | `reachability` (not empty) | Resource id to Kubernetes node name. Only listed resources are reported |
| `reachability_confirmations` | `3` | | Consecutive agreeing readings before up or down flips. 1 to 100 |
| `nfs_servers` | `{}` | `mounts` | Resource id to the names or addresses of its NFS server, as pod volumes and PersistentVolumes write them (compared ignoring case); list both name and address if both are used. No empty list, no whitespace |
| `job_namespaces` | `[]` | `jobs` (not empty) | Namespaces whose Jobs are listed; each scheduled job's CronJob must be in one |

Resource ids in `nodes` and `nfs_servers` must exist. A failed or stale `mounts` reading is
"possibly mounted", never "no mounts".

```yaml
sources:
  - id: cluster-pods
    kind: kubernetes_watch
    resync: 10s
    observe: [pods, reachability]
    params:
      namespaces: [ai, media, docs]
      nodes: { cuda-central: cuda-central }

  - id: basestar-mounts
    kind: kubernetes_watch
    observe: [mounts]
    resync: 10s
    params:
      nfs_servers: { basestar-nfs: [basestar] }

  - id: scheduled-jobs
    kind: kubernetes_watch
    observe: [jobs]
    resync: 10s
    params:
      job_namespaces: [backups]
```

## `tcp_probe`

One TCP connect per resync says whether a host outside the cluster is up. A connection is up;
a refusal, no answer within `timeout` or "no route to host" is down; any other error (name
does not resolve, network unreachable, unusable address) is a failed read, never down. It
checks that the port accepts connections, not that a service behind it works. Several
addresses are tried at once: up if any connects, down only if every one answered down.

| Key | Default | Meaning and limits |
| --- | --- | --- |
| `host` | required | A host name or IP address (IPv6 without brackets). No scheme, path, space, control character, `@`, `[` or `]`; a `:` only if the whole value is an IPv6 address |
| `port` | required | 1 to 65535 |
| `resource` | required | The resource whose reachability this is. Must exist |
| `timeout` | `2s` | Name lookup and connect together. `100ms` to `60s`, and at most `resync` |
| `reachability_confirmations` | `3` | As for `kubernetes_watch` |

`observe` must be `[reachability]`.

```yaml
  - id: basestar-probe
    kind: tcp_probe
    observe: [reachability]
    resync: 10s
    params: { host: basestar, port: 2049, resource: basestar-nfs, timeout: 2s }
```

## `prometheus` (*untested*)

One instant query per resync, `GET <url>/api/v1/query`, says whether a resource's host is up.
The query must return exactly one series whose value is `1` (up) or `0` (down); anything else
is a failed read, never down. `observe` must be `[reachability]`.

| Key | Default | Meaning and limits |
| --- | --- | --- |
| `url` | required | The server's base URL, `http://` or `https://`, with no query or fragment and not ending in `/api/v1/query` |
| `query` | required | A PromQL instant query, for example a blackbox exporter's `probe_success{...}`. Not empty |
| `resource` | required | The resource whose reachability the value is. Must exist |
| `timeout` | `10s` | Above `0`, at most `5m` |
| `auth` | none | [Credentials](#auth); *untested* |
| `reachability_confirmations` | `3` | As for `kubernetes_watch` |

```yaml
  - id: some-host-probe
    kind: prometheus
    observe: [reachability]
    resync: 10s
    params:
      url: http://prometheus.monitoring.svc:9090
      query: probe_success{instance="some-host", job="blackbox-ssh"}
      resource: some-resource
```

## `http_json`

Makes HTTP `GET` requests and turns each JSON answer into numbers (demand) or one reading (a
switch). Every request is made on each poll.

| Key | Default | Meaning and limits |
| --- | --- | --- |
| `requests` | required | A non-empty list of [requests](#requests). Identical requests are refused |
| `auth` | none | [Credentials](#auth) |
| `timeout` | `10s` | Per request. Above `0`, at most `5m` |
| `webhook` | none | `{ path }`: a `POST` to this path of the webhook listener polls this source now ([install.md](../install.md#3-flags-and-environment); *untested*). The body is ignored, and sources may share a path. The path starts with `/`, has no trailing `/` or `//`, and uses only letters, digits and `-._~/` |

All JSON pointers are RFC 6901: empty, or starting with `/`.

### Requests

A demand request sets exactly one of `count` and `length`. Counts of requests with the same
`counts_as` are summed. At least one request must count as `ready`.

| Key | Default | Meaning |
| --- | --- | --- |
| `url` | required | `http://` or `https://` |
| `count` | | Pointer to a number in the answer |
| `length` | | Pointer to an array; its length, after `where`, is the number |
| `counts_as` | `ready` | `ready` (waiting), `in_flight` (running) or `failed` (waiting for [recovery](workloads.md#recovery), never ages; *target:* a class with this source as its `demand` must then declare a `recovery`) |
| `where` | none | With `length`: [filter](#filters-and-counts) the elements |
| `max_items` | none | With `length`: the most elements one answer holds (the page size in the URL). Above `0`. A full answer may be truncated, so the poll fails rather than under-count; a `failed` request instead counts the matching items on the page |
| `also` | `[]` | More numbers from the same answer: `{ count, map, counts_as }`. A pointer and `counts_as` already counted is refused |
| `when` | none | `{ field, equals }`: count the answer only if the field has the value (a paused queue, for example). Otherwise the request, and its `also`, contribute zero. A missing field or another JSON type fails the poll |
| `since` | none | [A time window](#since) |
| `map` | none | With `count`, or on an `also` entry: turns text or a boolean at the pointer into a number. Values must be non-negative integers |

```yaml
  - id: paperless-ocr-pending
    kind: http_json
    observe: [demand]
    resync: 60s
    params:
      auth: { header: Authorization, prefix: "Token ", env: PAPERLESS_API_TOKEN }
      requests:
        - url: http://paperless-ngx.docs.svc:8000/api/documents/?tags__name__iexact=ocr-pending&page_size=1
          count: /count
      webhook: { path: /hooks/paperless }
```

### Filters and counts

`where: { field, contains | equals }` keeps the elements whose `field` (a pointer relative to
one element) matches; set exactly one of the two.

| Key | Matches |
| --- | --- |
| `contains` | A case-insensitive substring of a string field |
| `equals` | A string, boolean or number field exactly. An unquoted YAML scalar keeps its type: `equals: false` is the boolean, `equals: "false"` the text, `equals: 3` the number (so `3.0` matches) |

A `null` or missing field is no match. A non-null field of another JSON type than `equals`
fails the poll, so the source's demand becomes unknown instead of reading low.

This example counts ready and in-flight work from one answer, and failed items from a second,
filtered and bounded list:

```yaml
  - id: speakr-pending
    kind: http_json
    observe: [demand]
    resync: 60s
    params:
      auth: { header: Authorization, prefix: "Bearer ", env: SPEAKR_API_TOKEN }
      requests:
        - url: http://speakr.media.svc:8899/api/v1/stats
          count: /recordings/pending
          also:
            - { count: /queue/jobs_queued, counts_as: ready }
            - { count: /queue/jobs_processing, counts_as: in_flight }
        - url: http://speakr.media.svc:8899/api/v1/recordings?status=failed&per_page=100&sort_by=updated_at&sort_order=desc
          length: /recordings
          where: { field: /has_summary, equals: false }
          max_items: 100
          since: { param: updated_since, ago: 15m }
          counts_as: failed
```

### `since`

`since: { param, ago }` adds `param=<now - ago>` to the request's query, as an ISO 8601 UTC
time, so only items changed in that window come back. `param` is a plain query parameter name
(letters, digits, `_`, `-`, `.`) that the URL does not already carry. `ago` is above `0` and at
most 365 days.

### A switch source

A source that observes `switch` has exactly one request, and it takes only `url`, `value` and
`map`:

| Key | Meaning |
| --- | --- |
| `value` | Pointer to a string or boolean in the answer |
| `map` | Text of the value (`true` or `false` for a boolean) to `on`, `off` or `off_now`. Not empty. A value not listed, a number, `null` or a missing field fails the poll, and the reading goes stale |

```yaml
  - id: ha-wikijs
    kind: http_json
    observe: [switch]
    resync: 30s
    params:
      auth: { header: Authorization, prefix: "Bearer ", env: HA_TOKEN }
      requests:
        - url: http://homeassistant.example.internal:8123/api/states/input_select.app_wikijs
          value: /state
          map: { "on": on, "off": off, "off-now": off_now }
```

### `auth`

| Key | Default | Meaning |
| --- | --- | --- |
| `header` | required | A plain header name: letters, digits and `-` |
| `prefix` | `""` | Text put before the secret, for example `"Bearer "` or `"Token "` |
| `file` | | Path of a mounted file holding the secret; surrounding whitespace is trimmed; re-read on every poll |
| `env` | | Name of an environment variable holding the secret; fixed when the pod starts. *Untested* |

Set exactly one of `file` and `env`. A missing token is not a load error: each poll reports
an error and the demand reads as unknown ([install.md](../install.md#4-secrets)).
