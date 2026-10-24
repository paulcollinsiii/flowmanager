#!/usr/bin/env python3
"""Power the home servers and their workloads up and down, with KEDA as the gate.

Nothing is hand-listed here: workloads are KEDA ScaledObjects labelled in git,
and servers are ConfigMaps labelled in git, both discovered from the cluster.

  power.kaffi.internal/group: nfs | gpu | idle      on a ScaledObject
  power.kaffi.internal/gpu-role: llm | ocr | asr    on gpu ScaledObjects that
      use the GPU; a gpu ScaledObject without one (a client of both, such as
      paperless-gpt) runs whenever cuda-central is on
  power.kaffi.internal/server: <name>               on a ConfigMap whose data
      has GROUPS (groups it serves), DRAIN (nfs|gpu), HOST, PORT, BROADCAST,
      MAC, SHUTDOWN_USER

A group is paused by annotating its ScaledObjects with
autoscaling.keda.sh/paused-replicas=0 (KEDA scales the target to zero and holds
it) and unpaused by removing the annotation. A gate is held up the same way,
with paused-replicas=1: KEDA scales the target to one and stops scaling it.
Flux Kustomizations are never suspended. An annotation that Flux owns (it is in
git) is refused, not fought.

Switches (independent) and presets:

  basestar on|off            the nfs group, and the NFS server
  gpu off|auto|ocr|llm|asr   the gpu group, and cuda-central
  idle on|off                workloads paused only to save bladerunner's RAM

  low   basestar off, gpu off, idle off
  full  basestar on,  gpu auto, idle on
  nfs   basestar on,  gpu off
  ai    basestar off, gpu auto
  idle  idle on (nothing else changes)

gpu modes: auto unpauses every gpu ScaledObject (KEDA formulas arbitrate); ocr
pauses the other roles and holds the ocr ones up at one replica, demand or not;
llm and asr are the same for their own role; off pauses all of them. OCR, the
LLM and speech recognition cannot claim the single GPU together. A gpu
ScaledObject with no role is paused by off and unpaused by every other mode.
Holding is what serves a client with no queue for KEDA to read: Mealie's
ingredient parser gets Ollama from `set --gpu llm`, and Speakr's transcription
gets WhisperX from `set --gpu asr` (its summaries then need `--gpu llm` after).
In auto mode the asr ScaledObject is not held, and has no demand of its own, so
WhisperX stays at zero.

Going off: pause the groups, wait (bounded) until no non-terminal pod still
holds the server's resources, then ssh the forced-command shutdown key. Going
on: wake it, wait for its port, then unpause. Everything is verified before the
first mutation, partial state is reported on abort, and --dry-run prints the
mutations instead of running them. The wake logic is
kubernetes/infra/05-db/wake-server.py, loaded by path. There is no automatic
shutdown or wake; this is run by hand.

Usage:
  ops/scripts/power.py status
  ops/scripts/power.py low | full | nfs | ai | idle
  ops/scripts/power.py set [--basestar on|off] [--gpu MODE] [--idle on|off]
Environment: KUBECTL overrides the kubectl binary.
"""

from __future__ import annotations

import argparse
import importlib.util
import ipaddress
import json
import os
import pathlib
import re
import shutil
import socket
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass, field
from typing import Any

REPO = pathlib.Path(__file__).resolve().parents[2]
WAKE_SERVER = REPO / "kubernetes/infra/05-db/wake-server.py"
KUBECTL = os.environ.get("KUBECTL", "kubectl")

L_GROUP = "power.kaffi.internal/group"
L_ROLE = "power.kaffi.internal/gpu-role"
L_SERVER = "power.kaffi.internal/server"
PAUSE = "autoscaling.keda.sh/paused-replicas"
FIELD_MANAGER = "power.py"
FLUX_MANAGER = "kustomize-controller"
GPU_NODE_LABEL = "kaffi.internal/gpu=nvidia"
GPU_RESOURCE = "nvidia.com/gpu"

# The vocabulary: switch name -> group it controls.
SWITCH_GROUP = {"basestar": "nfs", "gpu": "gpu", "idle": "idle"}
SWITCH_VALUES = {
    "basestar": ("on", "off"),
    "gpu": ("off", "auto", "ocr", "llm", "asr"),
    "idle": ("on", "off"),
}
# gpu modes that name a role: hold that role's ScaledObjects up, pause the rest.
GPU_ROLE_MODES = ("ocr", "llm", "asr")
PRESETS = {
    "low": {"basestar": "off", "gpu": "off", "idle": "off"},
    "full": {"basestar": "on", "gpu": "auto", "idle": "on"},
    "nfs": {"basestar": "on", "gpu": "off"},
    "ai": {"basestar": "off", "gpu": "auto"},
    "idle": {"idle": "on"},
}


class PowerError(Exception):
    """A refusal or failure; the message is printed and the exit code is 1."""


@dataclass
class Server:
    name: str
    groups: list[str]
    drain: str
    host: str
    port: int
    broadcast: str
    mac: str
    user: str


@dataclass
class Gate:
    namespace: str
    name: str
    group: str
    role: str | None
    paused: str | None
    owner: str | None
    target_kind: str
    target_name: str

    @property
    def flux_owned(self) -> bool:
        return self.owner == FLUX_MANAGER

    @property
    def ref(self) -> str:
        return f"{self.namespace}/{self.name}"


@dataclass
class Ctx:
    dry_run: bool
    timeout: int
    ssh_key: pathlib.Path
    done: list[str] = field(default_factory=list)


def kubectl_json(*args: str) -> dict[str, Any]:
    out = subprocess.run([KUBECTL, *args, "-o", "json"], check=True, capture_output=True, text=True).stdout
    data: dict[str, Any] = json.loads(out)
    return data


def load_wake() -> Any:
    spec = importlib.util.spec_from_file_location("wake_server", WAKE_SERVER)
    if spec is None or spec.loader is None:
        raise PowerError(f"cannot load {WAKE_SERVER}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# --- discovery ---------------------------------------------------------------


def discover_servers() -> dict[str, Server]:
    items = kubectl_json("get", "configmap", "-A", "-l", L_SERVER)["items"]
    servers: dict[str, Server] = {}
    for cm in items:
        d = cm.get("data") or {}
        name = cm["metadata"]["labels"][L_SERVER]
        servers[name] = Server(
            name=name,
            groups=[g.strip() for g in d.get("GROUPS", "").split(",") if g.strip()],
            drain=d.get("DRAIN", ""),
            host=d.get("HOST", ""),
            port=int(d.get("PORT", "2049") or 2049),
            broadcast=d.get("BROADCAST", ""),
            mac=d.get("MAC", ""),
            user=d.get("SHUTDOWN_USER", ""),
        )
    return servers


def pause_owner(obj: dict[str, Any]) -> str | None:
    """Field manager that owns the pause annotation (Flux wins over others), if any."""
    owners = []
    for mf in obj["metadata"].get("managedFields") or []:
        annotations = (mf.get("fieldsV1") or {}).get("f:metadata", {}).get("f:annotations", {})
        if f"f:{PAUSE}" in annotations:
            owners.append(str(mf.get("manager")))
    if FLUX_MANAGER in owners:
        return FLUX_MANAGER
    return owners[0] if owners else None


def discover_gates() -> list[Gate]:
    items = kubectl_json("get", "scaledobject", "-A", "-l", L_GROUP)["items"]
    gates = []
    for so in items:
        meta = so["metadata"]
        ref = so["spec"]["scaleTargetRef"]
        gates.append(
            Gate(
                namespace=meta["namespace"],
                name=meta["name"],
                group=meta["labels"][L_GROUP],
                role=meta["labels"].get(L_ROLE),
                paused=(meta.get("annotations") or {}).get(PAUSE),
                owner=pause_owner(so),
                target_kind=ref.get("kind", "Deployment"),
                target_name=ref["name"],
            )
        )
    return sorted(gates, key=lambda g: g.ref)


def server_for_group(servers: dict[str, Server], group: str) -> Server | None:
    for s in servers.values():
        if group in s.groups:
            return s
    return None


# --- held resources ----------------------------------------------------------


def live_pods() -> list[dict[str, Any]]:
    pods = kubectl_json("get", "pods", "-A")["items"]
    return [p for p in pods if p.get("status", {}).get("phase") not in ("Succeeded", "Failed")]


def describe(pod: dict[str, Any]) -> str:
    meta = pod["metadata"]
    state = pod.get("status", {}).get("phase", "?")
    if meta.get("deletionTimestamp"):
        state += " Terminating"
    return f"{meta['namespace']}/{meta['name']} {state}"


def nfs_holders() -> list[str]:
    pvs = kubectl_json("get", "pv")["items"]
    claims = {
        f"{pv['spec']['claimRef']['namespace']}/{pv['spec']['claimRef']['name']}"
        for pv in pvs
        if pv["spec"].get("nfs") and pv["spec"].get("claimRef")
    }
    out = []
    for pod in live_pods():
        ns = pod["metadata"]["namespace"]
        for vol in pod["spec"].get("volumes") or []:
            pvc = (vol.get("persistentVolumeClaim") or {}).get("claimName")
            # An inline `nfs:` volume mounts a server directly with no PV; be
            # conservative and count any of them, whichever server it names.
            if (pvc and f"{ns}/{pvc}" in claims) or vol.get("nfs"):
                out.append(describe(pod))
                break
    return out


def gpu_holders() -> list[str]:
    nodes = {n["metadata"]["name"] for n in kubectl_json("get", "nodes", "-l", GPU_NODE_LABEL)["items"]}
    out = []
    for pod in live_pods():
        if any(o.get("kind") == "DaemonSet" for o in pod["metadata"].get("ownerReferences") or []):
            continue
        containers = (pod["spec"].get("containers") or []) + (pod["spec"].get("initContainers") or [])
        wants_gpu = any(
            GPU_RESOURCE
            in {**(c.get("resources") or {}).get("requests", {}), **(c.get("resources") or {}).get("limits", {})}
            for c in containers
        )
        if wants_gpu or pod["spec"].get("nodeName") in nodes:
            out.append(describe(pod))
    return out


def holders(server: Server) -> list[str]:
    if server.drain == "nfs":
        return nfs_holders()
    if server.drain == "gpu":
        return gpu_holders()
    raise PowerError(f"server {server.name}: unknown DRAIN {server.drain!r}")


# --- plan --------------------------------------------------------------------


@dataclass
class Plan:
    pause: list[Gate] = field(default_factory=list)
    hold: list[Gate] = field(default_factory=list)
    unpause: list[Gate] = field(default_factory=list)
    off: list[Server] = field(default_factory=list)
    on: list[Server] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def wanted_replicas(switch: str, value: str, gate: Gate) -> str | None:
    """The paused-replicas a gate should carry: "0" paused, "1" held up, None for KEDA to decide."""
    if switch == "gpu":
        if value == "off":
            return "0"
        if gate.role is None:
            # Not a GPU user itself (e.g. paperless-gpt): up whenever the GPU is.
            return None
        if value in GPU_ROLE_MODES:
            # The GPU is this role's alone, so hold it up for clients KEDA
            # cannot see (Mealie for llm, Speakr for asr) rather than wait on
            # queue demand.
            return "1" if gate.role == value else "0"
        return None
    return "0" if value == "off" else None


def is_up(wake: Any, server: Server) -> bool:
    return bool(wake.port_up(server.host, server.port))


def resolves(host: str) -> bool:
    if re.fullmatch(r"\d{1,3}(\.\d{1,3}){3}", host):
        return True
    try:
        socket.getaddrinfo(host, None, socket.AF_INET)
    except OSError:
        return False
    return True


def build_plan(desired: dict[str, str], servers: dict[str, Server], gates: list[Gate], ctx: Ctx, wake: Any) -> Plan:
    """Decide what to do and refuse, before any mutation, anything that cannot work."""
    plan = Plan()
    if shutil.which(KUBECTL) is None:
        raise PowerError(f"{KUBECTL} not found on PATH")
    if "off" in desired.values() and shutil.which("ssh") is None:
        raise PowerError("ssh not found on PATH")
    for switch, value in desired.items():
        group = SWITCH_GROUP[switch]
        members = [g for g in gates if g.group == group]
        server = server_for_group(servers, group)
        if not members and not (switch == "gpu" and value in GPU_ROLE_MODES):
            raise PowerError(
                f"no ScaledObjects labelled {L_GROUP}={group}: is the KEDA rollout deployed? (see docs/runbooks/server-renumber.md)"
            )
        if switch == "gpu" and value in GPU_ROLE_MODES and not any(g.role == value for g in members):
            plan.notes.append(
                f"gpu {value}: no {value}-role workload exists in the cluster yet; only pausing the others"
            )
        for gate in members:
            want = wanted_replicas(switch, value, gate)
            if want is not None:
                if gate.paused != want:
                    if gate.flux_owned:
                        raise PowerError(
                            f"{gate.ref}: {PAUSE}={gate.paused} is owned by Flux (it is in git) and would be reverted. "
                            "Remove the annotation from git (and push) to hand control to power.py."
                        )
                    (plan.pause if want == "0" else plan.hold).append(gate)
            elif gate.paused is not None:
                if gate.flux_owned:
                    raise PowerError(
                        f"{gate.ref}: {PAUSE}={gate.paused} is owned by Flux (it is in git) and would be reverted. "
                        "Remove the annotation from git (and push) to hand control to power.py."
                    )
                plan.unpause.append(gate)
        if server is None:
            if group != "idle":
                raise PowerError(f"no server definition (ConfigMap labelled {L_SERVER}) serves group {group}")
            continue
        going_off = value == "off"
        if going_off:
            plan.off.append(server)
            if not ctx.ssh_key.exists():
                raise PowerError(f"ssh shutdown key {ctx.ssh_key} not found (--ssh-key)")
            if not server.user or not server.host:
                raise PowerError(f"server {server.name}: HOST and SHUTDOWN_USER are required to shut it down")
        else:
            plan.on.append(server)
            if not server.host or not server.port:
                raise PowerError(f"server {server.name}: HOST and PORT are required")
            try:
                ipaddress.IPv4Address(server.broadcast)
            except ValueError:
                raise PowerError(
                    f"server {server.name}: BROADCAST {server.broadcast!r} is not a valid IPv4 address"
                ) from None
            if not is_up(wake, server) and not server.mac:
                raise PowerError(
                    f"server {server.name} is down and has no MAC in its ConfigMap (TODO); refusing to wake it"
                )
        if not resolves(server.host):
            raise PowerError(f"cannot resolve {server.host}; nothing changed. Is bind live?")
    return plan


# --- actions -----------------------------------------------------------------


def mutate(ctx: Ctx, description: str, *cmd: str) -> None:
    if ctx.dry_run:
        print("+ " + " ".join(cmd))
    else:
        subprocess.run(cmd, check=True)
    ctx.done.append(description)


def set_pause(ctx: Ctx, gate: Gate, replicas: str | None) -> None:
    """Annotate paused-replicas=<replicas>, or remove it (None) to hand the gate back to KEDA."""
    manager = f"--field-manager={FIELD_MANAGER}"
    target = ["-n", gate.namespace, "scaledobject", gate.name]
    if replicas is not None:
        cmd = [KUBECTL, "annotate", "--overwrite", manager, *target, f"{PAUSE}={replicas}"]
        done = f"paused {gate.ref}" if replicas == "0" else f"held {gate.ref} at {replicas}"
        mutate(ctx, done, *cmd)
    else:
        cmd = [KUBECTL, "annotate", manager, *target, f"{PAUSE}-"]
        mutate(ctx, f"unpaused {gate.ref}", *cmd)


def wait_drained(ctx: Ctx, server: Server) -> None:
    deadline = time.time() + ctx.timeout
    while True:
        held = holders(server)
        if not held:
            print(f"{server.name}: nothing holds it")
            return
        if ctx.dry_run:
            print(f"{server.name}: would wait for these to exit (they are running now):")
            print("\n".join(f"  {h}" for h in held))
            return
        if time.time() >= deadline:
            raise PowerError(
                f"timed out after {ctx.timeout}s; still holding {server.name}:\n"
                + "\n".join(f"  {h}" for h in held)
                + "\n(pods not owned by a gated ScaledObject must be stopped by hand.)"
            )
        print(f"{server.name}: waiting for {len(held)} pod(s) to exit...")
        time.sleep(5)


def shut_down(ctx: Ctx, wake: Any, server: Server) -> None:
    if not ctx.dry_run and not is_up(wake, server):
        print(f"{server.name}: {server.host}:{server.port} is not answering; already off, no ssh")
        return
    # IdentitiesOnly: without it ssh offers agent/config keys before -i, and an
    # unrestricted key wins, opening a shell instead of the forced poweroff.
    cmd = [
        "ssh",
        "-i",
        str(ctx.ssh_key),
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "ConnectTimeout=10",
        f"{server.user}@{server.host}",
    ]
    if ctx.dry_run:
        print("+ " + " ".join(cmd))
        return
    rc = subprocess.run(cmd, check=False).returncode
    if rc in (0, 255):
        # The forced command drops the connection as the host goes down.
        ctx.done.append(f"shutdown requested for {server.name} (ssh rc={rc})")
        down = False
        for _ in range(24):
            if not is_up(wake, server):
                down = True
                break
            time.sleep(5)
        if not down:
            raise PowerError(
                f"{server.name}: shutdown requested (ssh rc={rc}) but {server.host}:{server.port} is still "
                "answering after 120s; it did not go down."
            )
        print(f"{server.name}: shutdown requested (ssh rc={rc}); it is down.")
    else:
        raise PowerError(
            f"{server.name}: shutdown request FAILED (ssh rc={rc}); it may still be up. Pods are already drained."
        )


def wake_up(ctx: Ctx, wake: Any, server: Server) -> None:
    if is_up(wake, server):
        print(f"{server.name}: already up")
        return
    if ctx.dry_run:
        print(
            f"+ wake {server.name} (mac {server.mac}, broadcast {server.broadcast}), wait for {server.host}:{server.port}"
        )
        return
    wake.send_magic(server.mac, server.broadcast)
    ctx.done.append(f"sent magic packet to {server.name}")
    if not wake.wait_for_port(server.host, server.port, ctx.timeout * 4):
        raise PowerError(f"{server.name} did not answer on {server.host}:{server.port}; nothing was unpaused for it")


def apply(desired: dict[str, str], ctx: Ctx) -> int:
    wake = load_wake()
    servers = discover_servers()
    gates = discover_gates()
    plan = build_plan(desired, servers, gates, ctx, wake)
    print("plan: " + ", ".join(f"{k} {v}" for k, v in desired.items()))
    for note in plan.notes:
        print(f"note: {note}")
    try:
        for gate in plan.pause:
            set_pause(ctx, gate, "0")
        for server in plan.off:
            wait_drained(ctx, server)
            shut_down(ctx, wake, server)
        for server in plan.on:
            wake_up(ctx, wake, server)
        # Held gates come up only once their server answers, as unpaused ones do.
        for gate in plan.hold:
            set_pause(ctx, gate, "1")
        for gate in plan.unpause:
            set_pause(ctx, gate, None)
    except (PowerError, OSError, subprocess.CalledProcessError, KeyboardInterrupt) as exc:
        if ctx.done:
            where = "in a dry run: nothing was changed" if ctx.dry_run else "after these changes, which are NOT undone"
            print(f"ABORTED {where}:", file=sys.stderr)
            for step in ctx.done:
                print(f"  {step}", file=sys.stderr)
        if isinstance(exc, KeyboardInterrupt):
            print("interrupted", file=sys.stderr)
            return 130
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except Exception:  # noqa: BLE001
        # Deliberately broad: whatever went wrong, report what was already changed before exiting.
        # Unexpected (bad JSON, missing key, ...): still say what was changed.
        if ctx.done:
            where = "in a dry run: nothing was changed" if ctx.dry_run else "after these changes, which are NOT undone"
            print(f"ABORTED {where}:", file=sys.stderr)
            for step in ctx.done:
                print(f"  {step}", file=sys.stderr)
        traceback.print_exc()
        return 1
    return 0


# --- status ------------------------------------------------------------------


def target_replicas(gate: Gate) -> str:
    kind = gate.target_kind.lower()
    try:
        obj = kubectl_json("get", kind, "-n", gate.namespace, gate.target_name)
    except subprocess.CalledProcessError:
        return "?"
    return f"{obj.get('status', {}).get('readyReplicas', 0)}/{obj['spec'].get('replicas', '(unset)')} ready/desired"


def status() -> int:
    wake = load_wake()
    servers = discover_servers()
    gates = discover_gates()
    print("servers:")
    for s in sorted(servers.values(), key=lambda s: s.name):
        up = "up" if s.host and is_up(wake, s) else "down"
        mac = "set" if s.mac else "MISSING"
        print(f"  {s.name:14} {s.host}:{s.port} {up}  groups={','.join(s.groups)} drain={s.drain} mac={mac}")
        if up == "up":
            held = holders(s)
            print(f"  {'':14} held by {len(held)} pod(s)" + "".join(f"\n    {h}" for h in held))
    print("gates:")
    if not gates:
        print(f"  none: no ScaledObjects labelled {L_GROUP} (KEDA rollout not deployed?)")
    for g in gates:
        paused = "running"
        if g.paused is not None:
            who = "Flux" if g.flux_owned else (g.owner or "unknown")
            state = "paused" if g.paused == "0" else "held"
            paused = f"{state}={g.paused} ({who})"
        role = f" role={g.role}" if g.role else ""
        print(f"  {g.group:5}{role:11} {g.ref:42} {paused:18} {target_replicas(g)}")
    return 0


# --- cli ---------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[0], formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--dry-run", action="store_true", help="print mutations instead of running them")
    parser.add_argument(
        "--timeout", type=int, default=300, metavar="SECONDS", help="drain wait (wake wait is 4x) (default 300)"
    )
    parser.add_argument("--ssh-key", type=pathlib.Path, default=pathlib.Path("~/.ssh/shutdown_key").expanduser())
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status", help="read the state back from the cluster")
    for name, preset in PRESETS.items():
        sub.add_parser(name, help="preset: " + ", ".join(f"{k} {v}" for k, v in preset.items()))
    p_set = sub.add_parser("set", help="set individual switches")
    for switch, values in SWITCH_VALUES.items():
        p_set.add_argument(f"--{switch}", choices=values)
    args = parser.parse_args(argv)

    ctx = Ctx(dry_run=args.dry_run, timeout=args.timeout, ssh_key=args.ssh_key)
    try:
        if args.cmd == "status":
            return status()
        if args.cmd == "set":
            desired = {s: getattr(args, s) for s in SWITCH_VALUES if getattr(args, s)}
            if not desired:
                parser.error("set needs at least one of --basestar/--gpu/--idle")
        else:
            desired = dict(PRESETS[args.cmd])
        return apply(desired, ctx)
    except PowerError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as exc:
        print(f"error: {' '.join(map(str, exc.cmd))} failed: {exc.stderr or exc}", file=sys.stderr)
        return 1
    except OSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
