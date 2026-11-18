# Overview

flowmanager decides what runs on a scarce, power-hungry machine in a homelab Kubernetes
cluster, and when that machine is on. Terms in *italics* are defined in
[concepts.md](concepts.md).

## The problem

A typical homelab has one GPU node. Several services want it: document OCR, an LLM,
speech-to-text, photo machine learning. Each service loads a large model, and the GPU has room
for one at a time. Three things go wrong when nothing coordinates them:

- Services fight for the GPU. Pods crash, stay pending, or thrash model loads.
- The GPU node runs all day for work that arrives in bursts, because nobody dares to switch it
  off.
- Other hosts the workloads depend on, such as an NFS server, have the same problem: switching
  them off while a pod still mounts them breaks the pod.

Kubernetes autoscaling does not help. It scales on load a pod already generates, and it has no
idea that scaling one deployment up requires another to go down first.

## What flowmanager does

flowmanager watches the work waiting for each service (a document queue, a transcription
backlog) and decides which *model* the GPU should hold next. It publishes the result as the
*allowed set*: the models and workload *classes* that may run right now. A KEDA external scaler
serves that set, so each Deployment is scaled to 0 or 1 replicas by its own ScaledObject.

- **Chooses by queue, not by pod state.** The next model is the one with the highest-*priority*
  ready work. A loaded model keeps serving while it has work, up to a time limit, or until
  another queue has waited too long.
- **Protects work in progress.** A class marked non-preemptible is never interrupted.
  Others get a bounded time to finish before they are cut off.
- **Optionally powers hosts.** When a host is set to *managed* control, flowmanager wakes it
  with Wake-on-LAN and shuts it down over SSH once nothing needs it. Before a shutdown it checks
  that no pod still uses the host.
- **Runs scheduled jobs.** A cron schedule starts a Kubernetes Job (a nightly backup, say)
  and first makes sure the hosts it needs are up.
- **Takes manual overrides.** A person can keep a host on, pin a model, switch an app on or
  off from Home Assistant, or tell flowmanager to stay away from a host.
- **Fails safe.** Missing or stale readings count as *unknown*, and a power-off waits for a fresh
  reading that the host is unused.

Decisions are made by pure planning code from a snapshot of observations, and are stored in
Restate, so a restart changes nothing: the scaler answers "unavailable" until the first
decision, and KEDA then keeps the replica counts it has.

## What it does not do

- It does not schedule pods. Kubernetes enforces one model per GPU (`nvidia.com/gpu: 1`, one
  GPU exposed, a tainted node). On a switch the old model leaves and the new one enters the
  allowed set in the same decision, and the new pod waits until the GPU is free.
- It does not scale beyond 0 or 1 replicas.
- It does not read pod state to make decisions. The one exception is the check before a
  power-off.
- It does not retry a failed power action. It raises an alert and stops, and a person acts.
- It does not power a host unless that host is configured as managed and a class or a person
  asks for it.
- It does not replace your workloads' own queues. It reads them.

## Example: one scanned document

This follows the reference environment: `cuda-central` is the GPU node, `basestar` is the NFS
host, and `blade-runner` is the always-on node that runs flowmanager, Restate and KEDA.

1. A scan lands in paperless and gets the `ocr-pending` tag. The `paperless-ocr` source
   (an HTTP query against paperless) now reports one ready item.
2. The planner sees ready work for the `paperless-ocr` class, which needs the `mineru` model.
   If `cuda-central` and `basestar` are up, it chooses `mineru`.
   *Opt-in:* if the host is *manual*, as in the reference configuration, a person must have
   powered it on, and the work waits until it is up. If the host is *managed* and the class
   may change power, flowmanager wakes it first.
3. In one decision, `mineru` enters the allowed set and whatever model held the GPU leaves.
   KEDA scales `mineru` to 1. Kubernetes starts its pod once the GPU is free.
4. `mineru` serves its whole queue. It keeps the GPU for at least `min_dwell` and at most
   `max_dwell`, unless another queue's oldest item exceeds `age_threshold`.
5. When OCR finishes, the document carries the `post-ocr` tag, and the tagging step counts as
   ready. Tagging is a different class on the `llm` model, so the next decision switches to
   `llm`. The order of OCR before tagging comes from this readiness rule, not from priority.
6. Both queues are empty. After `idle_grace`, the model is withdrawn and its pod scales to 0.
7. *Opt-in:* if flowmanager powered `cuda-central` on for this work, nothing now holds it,
   so it starts a shutdown. It first withdraws the consumers, then waits for a fresh reading
   that no pod of the host's models remains, then powers the host off over SSH. A host that something
   else started is held for the person until they release it, and a manual host is never powered
   off by flowmanager.

Steps 1 to 6 need no power configuration. Steps 2 (waking) and 7 (shutting down) happen only
for a managed host.
