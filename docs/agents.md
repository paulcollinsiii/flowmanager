# For agents

For an AI agent configuring, operating or answering questions about flowmanager. To change the
code, follow [CLAUDE.md](../CLAUDE.md). The code is the source of truth; these pages are
explanations of it.

## Where the truth lives

| Topic | Code | Explained in |
| --- | --- | --- |
| CLI flags and env | `crates/sched-service/src/main.rs` (`struct Args`) | [install.md](install.md) |
| Config keys and types | `crates/sched-model/src/{config,power_params,source_params,recovery_params}.rs` | [configuration/README.md](configuration/README.md) |
| Config validation | `crates/sched-model/src/validate.rs` | [configuration/README.md](configuration/README.md) |
| Ingress handlers | `crates/sched-service/src/{scheduler,object,leases,resume,recovery,power}.rs` (`#[handler]`; `observe` is private) | [operations.md](operations.md) |
| Metrics | `crates/sched-service/src/metrics.rs` | [operations.md](operations.md) |
| Invariants | `crates/sched-service/src/invariants.rs` (each check is an `out.push`) | [architecture.md](architecture.md) |
| Persisted documents and versions | `crates/sched-service/src/persist.rs` | [operations.md](operations.md) |
| Planner (which model owns the GPU) | `crates/sched-planner/src/fifo.rs` | [concepts.md](concepts.md), [architecture.md](architecture.md) |
| Power lifecycle, drains, exclusive hosts, status | `crates/sched-sequencer/src/{power,drain,exclusive,status,switch,adopt}.rs` | [architecture/power.md](architecture/power.md), [configuration/power.md](configuration/power.md) |
| Scheduled jobs | `crates/sched-sequencer/src/jobs.rs`, `crates/sched-service/src/jobs.rs` | [configuration/jobs.md](configuration/jobs.md) |
| What each source kind emits | `crates/sched-observe/src/{kube_watch,kube_mounts,kube_jobs,http_json,prometheus,tcp_probe}.rs`, `nudge.rs` (webhook nudges) | [configuration/sources.md](configuration/sources.md) |
| Toggle and webhook listeners | `crates/sched-service/src/{toggle,webhook}.rs` | [home-assistant.md](home-assistant.md) |
| KEDA scaler | `crates/sched-keda/src/lib.rs`, `crates/sched-service/src/keda_scaler.rs` | [overview.md](overview.md), [architecture.md](architecture.md) |
| Power actuators | `crates/sched-actuate/src/{power,wol,ssh}.rs` | [configuration/power.md](configuration/power.md) |

## Verifying a doc claim

1. Find the owning file in the table.
2. Grep it for the key, flag, metric or handler name.
3. Read the code around the match.

Code wins over docs, and over code comments, which can be stale. A complete example config is
`config/example.yaml`.

## Invariant numbering

Invariants 1-5 and 16-21 are in [architecture.md](architecture.md); 6-15 are in
[architecture/power.md](architecture/power.md). Every check is in
`crates/sched-service/src/invariants.rs`. Enumerate the `out.push` calls there rather than
trusting a count, in this file or any other.

## Pitfalls

- **`SourceKind::can_supply` over-accepts.** It (`crates/sched-model/src/config.rs`) allows more
  than the observers emit: `http_json` is accepted for reachability and `prometheus` for demand,
  but neither produces those readings. What is emitted:

  | Kind | Emits |
  | --- | --- |
  | `kubernetes_watch` | pods, reachability, mounts, jobs |
  | `http_json` | demand, switch |
  | `prometheus` | reachability |
  | `tcp_probe` | reachability |

  Trust the observers or [configuration/sources.md](configuration/sources.md), not `can_supply`.
- **Kinds `sql`, `session` and `webhook` have no observer.** A source of those kinds yields no
  demand. A `webhook` *listener* nudging `http_json` polls is a different thing; see
  [home-assistant.md](home-assistant.md).
- **The example's `interactive-llm` class uses an inert `session` source**; `config/example.yaml`
  marks it "Inert".
- **`$?` after a pipeline is the last stage's exit code.** Redirect to a file and read `$?` from
  the bare command.
- **Building needs `protoc`**, declared in `mise.toml` and used by `crates/sched-keda/build.rs`.
- **Config is reloaded every `--reload-interval`; a rejected config changes nothing.** After an
  edit, check the logs, `sched_config_reloads_total` (outcome `rejected` or `unreadable`) and
  `sched_config_generation`, both defined in `metrics.rs`.
