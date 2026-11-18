# Overview

flowmanager decides what runs on a scarce, power-hungry machine in a homelab Kubernetes
cluster, and when that machine is on. Terms are defined in [concepts.md](concepts.md).

## The problem

A typical homelab has one GPU node. Several services want it: document OCR, an LLM,
speech-to-text, photo machine learning. Each loads a large model, and the GPU has room for one
at a time. Without coordination:

- Services fight for the GPU. Pods stay pending, or models are loaded and dropped in turn.
- The GPU node stays on all day for work that arrives in bursts, because switching it off by
  hand is too much trouble and doing it automatically is not safe without checks.
- Other hosts the workloads depend on, such as an NFS server, cannot be switched off safely
  while a pod still mounts them.

An autoscaler such as KEDA (the Kubernetes event-driven autoscaler) scales each workload on its
own signal. It cannot arbitrate between workloads that share one GPU or one host, and it does
not know that one deployment must go down before another can come up.

## What flowmanager does

flowmanager watches the work waiting for each service (a document queue, a transcription
backlog) through [sources](concepts.md#demand-and-observation) such as HTTP APIs, Prometheus
(a metrics server) and the Kubernetes API. It decides which [model](concepts.md#workloads) the
GPU should hold next and publishes the [allowed set](concepts.md#choosing-and-serving). It is
KEDA's external scaler: KEDA asks it which Deployments may run, and scales each to 0 or 1.

- **Chooses by queue.** The [planner](concepts.md#choosing-and-serving) picks the model with
  the highest-priority ready work, except that a queue that has waited too long goes first.
  A loaded model keeps serving while it has work, within [dwell](concepts.md#choosing-and-serving)
  limits.
- **Protects work in progress.** Work gets a bounded time to finish before it is cut off.
  A [class](concepts.md#workloads) (one service's unit of demand) can ask never to be displaced
  by another model's work.
- **Optionally powers hosts.** For a host set to [managed](concepts.md#hosts-and-power) control,
  flowmanager wakes it with Wake-on-LAN (through the router's OPNsense API) and shuts it down
  over SSH when nothing needs it, after checking that no pod still uses it.
- **Runs [scheduled jobs](concepts.md#scheduled-jobs).** A cron schedule starts a Kubernetes
  Job (a backup, say). If the job needs a managed host that is off, flowmanager wakes it first;
  for a manual host it waits for a person to power it on.
- **Takes manual overrides.** A person can take a [lease](concepts.md#leases-and-manual-control)
  to keep a host on or pin a model, switch an app on or off from Home Assistant (a
  home-automation server), or tell flowmanager to stay away from a host.
- **Fails safe.** Missing or stale readings are handled conservatively; see
  [unknown demand](concepts.md#demand-and-observation).

Each decision is made by pure planning code from a snapshot of observations. State is kept in
Restate, a durable-execution server, so decisions, leases and half-finished transitions
survive a restart.

## What it does not do

- It does not schedule pods or enforce one model per GPU. Kubernetes does that.
- It does not decide from pod state, apart from the [two exceptions](concepts.md#demand-and-observation)
  the glossary names.
- It does not retry a failed power action.
- It does not power a host unless that host is managed and something asks for it.
- It does not own your workloads' queues. It reads them.

## Example: one scanned document

The example environment: `cuda-central` is the GPU node, `basestar-nfs` is an NFS server, and
`blade-runner` is the always-on node. The example configuration sets both powered hosts to
manual control.

1. A scan lands in paperless (a document archive) and carries the `ocr-pending` tag. The
   `paperless-ocr-pending` source queries paperless for that tag and reports one ready item
   for the `paperless-ocr` class.
2. The planner sees ready work for `paperless-ocr`, which needs the `mineru` model (an OCR
   model). If `basestar-nfs` is up, it chooses `mineru`, even while `cuda-central` is down. In
   that case the sequencer waits until a person starts the host.
3. Once the host is up and the previous model is idle, `mineru` enters the allowed set and the
   old model leaves in the same decision. KEDA scales `mineru` to 1, and Kubernetes starts its
   pod once the GPU is free. A busy old model would be drained first.
4. `mineru` serves the whole OCR queue.
5. Whatever tags the document after OCR is outside flowmanager; the `paperless-post-ocr`
   source reads the `post-ocr` tag. Once it is present the tagging step is ready, but `mineru`
   is idle and gives way only after the [idle grace](concepts.md#choosing-and-serving) period
   (10 minutes in the example configuration). Then the decision switches to `llm` (a language
   model). OCR runs before tagging because tagging is not ready until then, not because of
   priority.
6. `llm` serves the tagging queue. When both queues are empty and the idle grace period has
   passed, the model is withdrawn and its pod scales to 0.

Steps 1 to 6 need no power configuration. With managed power on `cuda-central` and
`may_change_power` on the class, step 2 would wake the host instead of waiting. After step 6,
nothing would hold it, and flowmanager would start a [shutdown](concepts.md#shutdown) and power
it off once the checks pass.
