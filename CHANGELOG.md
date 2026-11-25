# Changelog

What changed in ways that affect an upgrade or a rollback. Builds carry no release numbers
here, so entries are grouped by the persisted-state version or the topic they changed. Current
behaviour is in [docs/](docs/overview.md); this file is the only place for history.

## Persisted state

Each document is stored as `{"version": N, "data": ...}`. This build reads `scheduler_state`
4, `lease_book` 2, and `job_book`, `recovery_attempts` and `power_last_action` 1. Operating on
them is in [docs/operations.md](docs/operations.md#reading-and-changing-state).

**`scheduler_state`.** There is no migration. An older version is dropped at the first tick
(warning `state_version_discarded`) and the sequencer starts from observation:

- a host that is up is not flowmanager's, so a managed one is found running and adopted (also
  one a job woke before the upgrade, which then stays up until a person switches it off);
- a requested shutdown under way, or a stopped one, is forgotten, and the idle and `min_off`
  clocks start over;
- leases, lockouts and job holds are kept.

| Version | Change |
| --- | --- |
| 2 | Per resource, `shutdown`, `idle_since` and `off_since` replaced `drain`, `mounts_veto`, `host_idle_since` and `shutdown_confirmed_at` |
| 3 | The `reclaim` shutdown cause and wind-down flag became `lockout`; `locked_down` added |
| 4 | The per-resource `managed` flag became `ours` on the `off` and `on` power phases |

The map of switched classes' switch-offs (`classes`) was added without a version bump: an absent
map reads as none, so a build before it cannot read a state that has some. The `loading` and
`draining` mode phases written before queue-only switching are still read (as serving since the
same instant, and as idle).

The map of when each `allowed: with_work` class last had work (`work_seen`) was added the same
way, without a version bump: an absent map reads as no recent work. A build before it reads the
same version but cannot decode a state whose map is not empty (`cannot decode version 4: unknown
field`, not a version mismatch): its ticks report an error until the state is discarded with
`discard_state`. The same holds for `classes`.

**`lease_book`, version 2.** Every lease has an `origin`, `requested` or `adopted`. Version 1
marked an adopted lease only by the id prefix `adopted-host-`. A version 1 book is read with
those leases dropped, one warning each (`legacy_adopted_lease_dropped`); nothing is lost,
because a host that is up is adopted afresh at the first tick. A person's leases, lockouts and
job holds are kept. No operator step.

## Reclaim became lockout

The manual "keep away from this host" lease was `reclaim`. It is now `lockout`.

| Was | Now |
| --- | --- |
| Handlers `reclaim`, `unreclaim` | `lockout`, `clear_lockout` |
| `policy.reclaim_timeout` | `policy.lockout_timeout` |
| Lease kind `reclaim`, ids `reclaim-N` | Lease kind `lockout`, ids `lockout-N` |

The old key is refused at config load (an unknown `policy` key), so rename it before upgrading.
A stored `reclaim` lease does not parse, and the lease book is not versioned away: until it is
cleared every tick publishes nothing and the lease handlers answer `503`. **Before upgrading**,
list the leases and clear each reclaim with the old build's `unreclaim`. If the upgrade came
first, either roll back, clear it and roll forward, or discard `lease_book` (every lease is
lost) and take the leases you still want again.

## Rolling back

- **Roll the config back with the binary.** An older build rejects keys it does not know
  (`min_off`, the `jobs:` section), and the build before queue-only switching rejects a gate
  class (a requirement on an exclusive resource with no model) and a config with no pods source.
  A build before `allowed` rejects a config that sets it (`allowed: with_work`), so remove the key
  first. A build before filter lists rejects a `where` (or `list.where`) given as a list, and a
  filter with `is_null`, so rewrite those first.
- **Discard what the older build cannot read.** A build before `scheduler_state` 4 and
  `lease_book` 2 reads neither: its ticks report an error until both are discarded with
  `discard_state`, which drops every lease. Rolling forward again needs nothing.
- **A stored drain.** An older build cannot decode a `scheduler_state` that holds a requested
  drain (a graceful or immediate `off`): let it finish, or discard the state, first.
- **Scheduled jobs.** A build without them ignores `job_book` and never releases the
  `hold-job-*` leases already in the lease book, so a run in progress at the rollback keeps its
  NFS host held and powered until `release_lease` or a `lease_book` discard. Roll back between
  runs, or check `leases` after. A Job already created runs to its end under Kubernetes.
  Rolling forward finds a possibly stale `job_book`; discard it for a clean start.

## The legacy power app

The earlier flowmanager (a root crate that read servers from labelled ConfigMaps and gated
workloads itself) is removed; its last commit is `1de46ca`. Its replacement is the scheduler's
KEDA external scaler.

| Retired | Do this |
| --- | --- |
| The `autoscaling.keda.sh/paused-replicas` annotation on ScaledObjects | Remove it from git **and** from the live objects: while it is set KEDA holds the count whatever any scaler says |
| The `power.kaffi.internal/group` and `power.kaffi.internal/gpu-role` labels | Remove; nothing reads them |
| ScaledObject triggers of the old gating | Replace with the one `external` or `external-push` trigger ([docs/install.md](docs/install.md#8-keda)) |
| Server ConfigMaps labelled `power.kaffi.internal/server`, with `HOST`, `PORT`, `MAC`, `WOL_INTERFACE`, `SHUTDOWN_USER`, `HOST_KEY_FPR`; the keys `GROUPS`, `DRAIN`, `EXCLUSIVE_ROLES`, `BROADCAST` | Delete. Hosts are `resources[]` with a `power` section in the config |
| The `PowerObject` Restate handlers | Gone; the `Scheduler` handlers replace them |

## Configuration

- **A demand source that counts `failed` needs a recovery.** A config whose class has a `demand`
  source with a request or an `also` entry of `counts_as: failed` and no `recovery` loaded before
  and is now rejected, naming the class and the source. A reload that fails keeps the last good
  config, but a restart with such a config does not start: add the class's `recovery` (or drop
  the `failed` count) before upgrading.
- **A source kind `schedule` is gone.** Scheduled work is the `jobs:` section.
- **`reclaim_timeout`** is refused: see above.
- **`http_json` `where` with `equals`.** A number after `equals:` was once compared as text. An
  item whose field holds a non-null value of another JSON type now fails the whole source, so
  its demand reads as unknown instead of silently counting low.
- **Queue-only switching.** Model choice no longer waits for pod observations. A gate class is
  an exclusive resource with no model, and the `loading` and `draining` mode phases, and the
  metric labels with those names, no longer exist. Update alerts that name them.

## Metrics

| Was | Now |
| --- | --- |
| `sched_decisions_total` | `sched_tick_decisions_total` |
| `sched_resource_phase_age_seconds{phase="loading"}` or `"draining"` | Removed |
