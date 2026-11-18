# Overview

flowmanager decides what runs on a scarce, power-hungry machine in a homelab Kubernetes
cluster, and when that machine is on. Terms are defined in [concepts.md](concepts.md).

## The problem

A typical homelab has one GPU node. Several services want it: document OCR, an LLM,
speech-to-text, photo machine learning. Each loads a large model, and the GPU has room for one
at a time. Without coordination:

- Services fight for the GPU. Pods stay pending, or models are loaded and dropped in turn.
- The GPU node runs all day for work that arrives in bursts, because nobody dares to switch it
  off.
- Other hosts the workloads depend on, such as an NFS server, cannot be switched off safely
  while a pod still mounts them.

Kubernetes autoscaling does not help. It scales on load a pod already generates, and it does
not know that one deployment must go down before another can come up.

## What flowmanager does

flowmanager watches the work waiting for each service (a document queue, a transcription
backlog) through [sources](concepts.md#demand-and-observation), and decides which
[model](concepts.md#workloads) the GPU should hold next. It publishes the
[allowed set](concepts.md#choosing-and-serving). KEDA, the Kubernetes event-driven autoscaler,
asks flowmanager (its external scaler) which Deployments may run, and scales each to 0 or 1.

- **Chooses by queue.** The next model is the one with the highest-priority ready work.
  A loaded model keeps serving while it has work, within [dwell](concepts.md#choosing-and-serving)
  limits.
- **Protects work in progress.** Work gets a bounded time to finish before it is cut off.
  A class can ask never to be displaced by another model's work.
- **Optionally powers hosts.** For a host set to [managed](concepts.md#hosts-and-power) control,
  flowmanager wakes it with Wake-on-LAN and shuts it down over SSH when nothing needs it,
  after checking that no pod still uses it.
- **Runs [scheduled jobs](concepts.md#scheduled-jobs).** A cron schedule starts a Kubernetes
  Job (a backup, say). If the job needs a managed host that is off, flowmanager wakes it first;
  a manual host it waits for a person to power on.
- **Takes [manual overrides](concepts.md#leases-and-manual-control).** A person can keep a host
  on, pin a model, switch an app on or off from Home Assistant (a home-automation server), or
  tell flowmanager to stay away from a host.
- **Fails safe.** A reading that is missing or too old is
  [unknown](concepts.md#demand-and-observation), and unknown is never read as "nothing to do".

Each decision is made by pure planning code from a snapshot of observations. State is kept in
Restate, a durable-execution server, so decisions, leases and half-finished transitions
survive a restart.

## What it does not do

- It does not schedule pods or enforce one model per GPU. Kubernetes does that.
- It does not scale beyond 0 or 1 replicas.
- It does not decide from pod state, apart from two named exceptions.
- It does not retry a failed power action.
- It does not power a host unless that host is managed and something
  [asks for it](concepts.md#hosts-and-power).
- It does not own your workloads' queues. It reads them.

## Example: one scanned document

The example environment: `cuda-central` is the GPU node, `basestar-nfs` is the NFS server,
and `blade-runner` is the always-on node running flowmanager, Restate and KEDA.

1. A scan lands in paperless and carries the `ocr-pending` tag. The `paperless-ocr-pending`
   source queries paperless for that tag and reports one ready item for the `paperless-ocr`
   class.
2. The planner sees ready work for `paperless-ocr`, which needs the `mineru` model. If
   `cuda-central` and `basestar-nfs` are up, it chooses `mineru`. If a host is down, the work
   waits for a person to start it. Waking a host for queued work takes managed control on the
   host and `may_change_power` on the class; the example configuration sets neither.
3. In one decision `mineru` enters the allowed set and the model that held the GPU leaves.
   KEDA scales `mineru` to 1, and Kubernetes starts its pod once the GPU is free.
4. `mineru` serves the whole OCR queue.
5. Whatever tags the document after OCR is outside flowmanager; the `paperless-post-ocr`
   source reads the `post-ocr` tag. Once that tag is present the tagging step is ready, but
   `mineru` is idle and gives way only after the idle grace period (10 minutes in the example
   configuration). Then the decision switches to `llm`. OCR runs before tagging because
   tagging is not ready until then, not because of priority.
6. `llm` serves the tagging queue. When both queues are empty and the idle grace period has
   passed, the model is withdrawn and its pod scales to 0.
7. If flowmanager powered `cuda-central` on for this work, nothing now holds it, so it starts a
   [shutdown](concepts.md#shutdown) and powers the host off once the checks pass. A host that
   something else started is held for the person until they release it, and a manual host is
   never powered off by flowmanager.
