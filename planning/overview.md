# Control theory based thoughts
## Planning
What does it take to drive a queue to zero? There are multiple queues and different services that can drain a queue. Those queues have priorities and mutual exclusion rules with them (only one service can hold the GPU at a time)

## Feedback Controller
This is what's driving KEDA to scale pods and Power to power on servers

## Dyanmic System
The availability of the hardware itself and the pods being up and running

## Sensors
Multiple but thankfully minimal noise
* queue depth (many)
* pod state
* server state


# Services being controlled
## Power
* cuda-central (GPU)
* basestar (NFS)

## User facing
* Speakr
  * transcription model
  * llm model to summarize
* Paperless-*
  * OCR model
  * llm model for tagging / doc type / titling
* Backups
  * Is the only service for now that can instruct NFS to power on to complete
* Mealie*
  * Sortof - the parsing queue for this is really bad with llm