# Kubernetes Deployment Design: stocker-list

## Overview

`stocker-list` is a pure RabbitMQ producer that publishes stock lists to an
exchange. It has no HTTP server, no Kafka consumer, no S3 interaction, and no
Postgres dependency. The Dockerfile's `EXPOSE 50051` is misleading — this
binary does not listen on any port at all; the EXPOSE directive was added by
convention but has no functional effect in-cluster.

This design covers a plain-vanilla Kubernetes deployment of
`git.wheeli.ca/brian/stocker-list:latest` into the shared `stocker` namespace.

## Scope

- New directory `deploy/kubernetes/` in this repository holding plain-vanilla
  Kubernetes YAML (no Helm, no Kustomize, no CRDs).
- Manifests: `configmap.yaml`, `secret.yaml`, `deployment.yaml`. **No
  Service** — nothing talks to this pod.
- The `stocker` namespace and the `wheeli-registry` pull secret are
  cluster-level prerequisites (see the plan).
- Out of scope: RabbitMQ broker, topic/queue creation.

## Global conventions (identical across all 12 stocker repositories)

These conventions are the same in every stocker-* deployment document.
`<service>` means the service name of the repository the document belongs to
(e.g. `stocker-list`).

- **Namespace:** `stocker` (all resources).
- **Image:** `git.wheeli.ca/brian/<service>:latest` with
  `imagePullPolicy: Always` (because the tag is `:latest`).
- **Registry auth:** `kubernetes.io/dockerconfigjson` Secret named
  `wheeli-registry` in namespace `stocker`, referenced from the Pod spec via
  `imagePullSecrets`.
- **Labels:** `app.kubernetes.io/name=<service>` and
  `app.kubernetes.io/part-of=stocker` on the Deployment, the Service (if any),
  and the Pod template labels. The Deployment `selector.matchLabels` uses
  `app.kubernetes.io/name=<service>`.
- **Replicas:** `1`. No HPA, no anti-affinity.
- **Resources:** requests `cpu: 50m`, `memory: 128Mi`; limits `cpu: 500m`,
  `memory: 512Mi`. Exception: `stocker-evaluator` uses limit `memory: 1Gi`.
- **Security:** container-level `securityContext` with
  `runAsNonRoot: true` and `allowPrivilegeEscalation: false`.
- **Service objects:** none, except `stocker-store` (ClusterIP
  `stocker-store`, port `3500`, name `grpc`).
- **Manifest layout:** plain-vanilla YAML under `deploy/kubernetes/` — one
  file per resource kind. No Helm, no Kustomize, no CRDs.
- **Credentials:** committed Secret manifests use `REPLACE_ME` placeholders.
  Real values for local fill-in live in `/home/brian/workspace/credentials.md`;
  long-lived tokens must never be written into committed manifests or docs.
- **Infrastructure endpoints (use exactly these):**
  - Kafka: `192.168.1.71:9092,192.168.1.75:9092` — do NOT use the
    `kafka1.lan` / `kafka2.lan` / `kafka-*.internal` names that appear in
    in-repo sample files.
  - RabbitMQ: `amqp://stocker:REPLACE_ME@192.168.1.48:5672/` (user
    `stocker`, vhost `/`). `guest` is rejected by the broker from
    non-loopback sources.
  - Ollama: `http://192.168.1.71:11434`.
  - Garage/S3 and Postgres: per-service values in the Configuration table
    below.

## Architecture

- **Namespace:** `stocker`.
- **Kind set:** ConfigMap + Secret + Deployment only. **No Service** — this is a RabbitMQ producer; nothing connects inbound to it.
- **Registry auth:** `imagePullSecrets: [wheeli-registry]`.
- **Deployment strategy:** default rolling update (`maxSurge: 25%`,
  `maxUnavailable: 0`), acceptable with `replicas: 1`.
- **Image:** built by `Containerfile` (distroless binary `/stocker-list`).
  The image is pushed to the registry by `deploy/push.sh` — nothing is built inside the cluster.

## Resource requirements

- CPU: requests `50m`, limits `500m`.
- Memory: requests `128Mi`, limits `512Mi`.

## Configuration (Environment Variables)

Variable names exactly as read by `internal/config/config.go` (`os.Getenv`). Values marked `REPLACE_ME` go in the Secret; the rest go in the ConfigMap.

### Set in ConfigMap `stocker-list-config`

| Variable | Value | Notes |
|---|---|---|
| `RABBITMQ_EXCHANGE` | `stock.update` | Exchange to publish stock list messages to (code default). This is the exchange that downstream consumers listen on. |
| `RABBITMQ_EXCHANGE_TYPE` | `topic` | Exchange type (code default). |
| `RABBITMQ_ROUTING_KEY` | `stock.update.v1` | Routing key for published messages (code default). |
| `REFRESH_CHECK_INTERVAL` | `24h` | How often to refresh/check the stock list (code default, 24 hours). |

### Set in Secret `stocker-list-env`

| Variable | Placeholder value | Real value from |
|---|---|---|
| `RABBITMQ_URL` | `amqp://stocker:REPLACE_ME@192.168.1.48:5672/` | `/home/brian/workspace/credentials.md` → RabbitMQ (`stocker`). **Note:** the code default uses `guest:guest`, which is rejected by the broker from non-loopback sources. Always use the `stocker` user with a real password in-cluster. |

## Message flow

```
stocker-list  ── publish stock list message ──▶ RabbitMQ exchange stock.update
                                                            routing key stock.update.v1
                                                                                      ▼
                                                                          downstream consumers
```

## Connectivity & probes

- **Probes: none (deliberate).** `stocker-list` has no HTTP listener and no health subcommand. It is a producer loop; Kubernetes restart policy (`restartPolicy: Always`, the Deployment default) is the recovery mechanism. Do **not** invent an `httpGet` probe — that would CrashLoop the pod. The Dockerfile's `EXPOSE 50051` is misleading and has no functional effect.
- **Service exposure:** none (worker service).
- **Outbound connectivity needed from the pod:**
  - RabbitMQ broker `192.168.1.48:5672`.
- **No Kafka, no S3/Garage, no Postgres, no gRPC, no Ollama.**

## Dependencies

- RabbitMQ broker at `192.168.1.48:5672` (operator-provided).
- Registry `git.wheeli.ca` reachable from cluster nodes + `wheeli-registry` pull secret.

## Manifest layout (exact files & resource names)

| File (under `deploy/kubernetes/`) | Kind | Name | Namespace |
|---|---|---|---|
| `configmap.yaml` | `ConfigMap` | `stocker-list-config` | `stocker` |
| `secret.yaml` | `Secret` | `stocker-list-env` | `stocker` |
| `deployment.yaml` | `Deployment` | `stocker-list` | `stocker` |

The Deployment consumes both via `envFrom` (`configMapRef` + `secretRef`).

## Verification (what "healthy" looks like)

- Pod `Running`, `READY 1/1`.
- Startup log showing the resolved RabbitMQ exchange, exchange type, and routing key.
- Messages appear in the target exchange with the configured routing key (`stock.update.v1`).
