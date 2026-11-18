# Configuration

The config is one YAML file. It names the hosts, models, workloads and sources of your cluster
and holds the scheduling policy. `config/example.yaml` is a complete, valid example; the pages
here say how to write each key. What a key means is in [concepts.md](../concepts.md).

| Key | Holds | Page |
| --- | --- | --- |
| `policy` | Scheduling and timing knobs (below) | this page |
| `resources` | Hosts, and how a host is powered | [power.md](power.md) |
| `models` | Modes of an exclusive resource | [workloads.md](workloads.md#models) |
| `classes` | Units of demand, gates, plain and switched apps | [workloads.md](workloads.md#classes) |
| `sources` | Where demand, host state, pods, mounts, jobs and switches are read | [sources.md](sources.md) |
| `jobs` | Scheduled jobs | [jobs.md](jobs.md) |

Only `policy` is required; the other lists default to empty.

## General rules

- **Unknown keys are refused.** Every section and entry rejects a key the schema does not
  have (`deny_unknown_fields`), so a misspelt key fails the load. A source's or a recovery
  action's `params` are checked the same way against the kind's or action's own keys.
- **Every reference must resolve.** A model's `resource`, a class's `requires`, `demand`,
  `switch` and `recovery.source`, a source's `reads_from`, `nodes`, `nfs_servers` and
  `resource`, and a job's `needs` must name an entry that exists. All problems are reported
  together, and none is applied.
- **Ids.** Entries are unique within their section and without leading or trailing space.
  Model and class ids have a further rule: [requirements.md](../requirements.md#workloads). A
  job id's rule is on [jobs.md](jobs.md).
- **Durations** (`crates/sched-model/src/time.rs`) are a whole number of milliseconds, a
  plain integer or a string: digits followed by `ms`, `s`, `m`, `h` or `d`, for example
  `1500`, `30s`, `10m`, `2h`, `1d`. No spaces, signs, fractions or combined units
  (`1h30m` is refused; write `90m`). A bare number is milliseconds, so `timeout: 2` is two
  milliseconds.
- **Secrets are not in the file.** Credentials and tokens are files or environment variables
  named by the config or by a flag ([install.md](../install.md#4-secrets)).

## Validation and applying

The file is parsed and validated whole at start and on every reload; an invalid file stops
the start, or is ignored on a reload while the last good one keeps running
([install.md](../install.md#2-configuration), [architecture.md](../architecture.md#configuration-and-extensibility)).

## `policy`

Every key is required except `lockout_timeout`. Durations are above zero unless the table
says otherwise.

| Key | Default | Meaning and limits |
| --- | --- | --- |
| `strategy` | required | The planner strategy by name. The only one is `fifo-exhaustive` (`crates/sched-planner/src/lib.rs`, `fifo.rs`); any other name loads (only an empty name is refused) and then makes every tick fail, keeping the previous state and allowed set |
| `min_dwell` | required | How long a model keeps its place before it may be switched away from, counted from its allow tick. May be `0`; at most `max_dwell`. See [dwell](../concepts.md#choosing-and-serving) |
| `max_dwell` | required | The longest a model serves while other queues wait. See [dwell](../concepts.md#choosing-and-serving) |
| `idle_grace` | required | How long a model or host with nothing to do waits before it is released. May be `0`. See [dwell and idle grace](../concepts.md#choosing-and-serving) |
| `age_threshold` | required | The age of another queue's oldest ready item at which it counts as starved. See [priority](../concepts.md#workloads) |
| `queue_reading_stale_after` | required | The oldest a demand or switch reading may be and still count. At least `host_reading_stale_after`; each source's `resync` must not exceed it. See [staleness](../concepts.md#demand-and-observation) |
| `host_reading_stale_after` | required | The oldest a reachability, mounts, pods or job reading may be. At most `queue_reading_stale_after`; the `resync` of a source that supplies one of these must not exceed it, and a managed host's [power timeouts](power.md#the-power-section) must be above it. See [staleness](../concepts.md#demand-and-observation) |
| `interactive_ttl` | required | Required by the schema and unused: nothing issues an interactive lease. At most `1d` |
| `drain_timeout` | required | How long in-flight work runs once a wind-down starts. At most `1d`. See [Timeouts](../architecture/power.md#timeouts) |
| `lockout_timeout` | `drain_timeout` | As `drain_timeout`, after a lockout. At most `1d`. See [Timeouts](../architecture/power.md#timeouts) |

The `1d` ceiling is `MAX_LEASE_DURATION` (`crates/sched-model/src/config.rs`). A class's
`kill_timeout` and a managed host's power timeouts share it.

```yaml
policy:
  strategy: fifo-exhaustive
  min_dwell: 5m
  max_dwell: 2h
  idle_grace: 10m
  age_threshold: 30m
  queue_reading_stale_after: 10m
  host_reading_stale_after: 30s
  interactive_ttl: 15m
  drain_timeout: 10m
```
