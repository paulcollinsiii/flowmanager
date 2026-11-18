# flowmanager

flowmanager decides what runs on a scarce, power-hungry machine in a homelab Kubernetes
cluster, and when that machine is on. It watches work queues and hosts, picks which model owns
the GPU, powers managed hosts on and off, and tells KEDA what may run through an external
scaler. It is written in Rust and keeps its decisions and leases in Restate.

## Documentation

| Read | For |
| --- | --- |
| [docs/overview.md](docs/overview.md) | What it is and how it behaves |
| [docs/requirements.md](docs/requirements.md) | What your cluster needs |
| [docs/install.md](docs/install.md) | Build, deploy, check |
| [docs/configuration/README.md](docs/configuration/README.md) | The config file |
| [docs/operations.md](docs/operations.md) | Handlers, metrics, alerts, state, blind spots |
| [docs/home-assistant.md](docs/home-assistant.md) | The toggle and webhook listeners |
| [docs/architecture.md](docs/architecture.md) | Components, invariants, design |
| [CHANGELOG.md](CHANGELOG.md) | Upgrade and rollback notes |

## Development

The toolchain (`rust`, `protoc` for the KEDA proto, and the `restate` CLI and server) is
declared in `mise.toml`: `mise install`. The workspace is `crates/*`; the service is
`crates/sched-service`, whose binary is `flowmanager`.

```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets
cargo fmt
docker build -t flowmanager .          # the release image; see docs/install.md
```

To run it against a local Restate server:

```sh
restate-server &
cargo run -p sched-service -- --config config/example.yaml --backstop 30s
restate deployments register http://localhost:9080
```

Working conventions for agents are in [CLAUDE.md](CLAUDE.md).
