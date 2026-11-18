# Documentation

One line per page, grouped by reader. Start with [overview](overview.md) if unsure.

## Product owner

| Page | For |
| --- | --- |
| [overview.md](overview.md) | What flowmanager is, why it exists, and how it behaves |

## Operator

| Page | For |
| --- | --- |
| [requirements.md](requirements.md) | What your cluster and hosts need before you install |
| [install.md](install.md) | Build, deploy and check the service |
| [configuration/README.md](configuration/README.md) | The config file: layout, reloading, how the sections fit |
| [configuration/power.md](configuration/power.md) | Resources and power: hosts, wake, shutdown |
| [configuration/workloads.md](configuration/workloads.md) | Models and classes |
| [configuration/sources.md](configuration/sources.md) | Sources: what each kind reads and emits |
| [configuration/jobs.md](configuration/jobs.md) | Scheduled jobs |
| [home-assistant.md](home-assistant.md) | The toggle and webhook listeners |
| [operations.md](operations.md) | Handlers, metrics, alerts, state, blind spots |
| [../CHANGELOG.md](../CHANGELOG.md) | Upgrade and rollback notes |

## Developer

| Page | For |
| --- | --- |
| [concepts.md](concepts.md) | The one place each term is defined |
| [architecture.md](architecture.md) | Components, design and the invariant list |
| [architecture/power.md](architecture/power.md) | The host power model and its invariants |
| [../CLAUDE.md](../CLAUDE.md) | How to contribute (review process) |

## Agent

| Page | For |
| --- | --- |
| [agents.md](agents.md) | Where the truth lives in the code, and how to verify a doc claim |
