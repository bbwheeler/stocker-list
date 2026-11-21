# Kubernetes Deployment Design: stocker-list

## Status

This document **supersedes** the previous Deployment-based design. The old
design deployed `stocker-list` as a long-running Deployment whose in-process
refresher loop ticked every `REFRESH_CHECK_INTERVAL`. That loop is removed.

The current design follows the sibling repository `stocker-store` as the
**gold standard** for Kubernetes manifests and adapts it to a batch workload.
Where this service cannot match `stocker-store` exactly (no listener, no
Service, no probes), the deviation is called out below.

## Overview

`stocker-list` is a RabbitMQ producer with no inbound interface. It does not
listen on a port, serve HTTP or gRPC, or expose metrics. One process
invocation is one complete unit of work:

1. Fetch the full symbol universe from every configured provider (TSX and the
   US NASDAQ screener).
2. Publish one `stockstorev1.Stock{Symbol, Exchange}` protobuf message per
   symbol to the `stock.update` topic exchange with routing key
   `stock.update.v1`.
3. Close the AMQP connection (if one was opened) and exit `0`.

It is deployed as a **Kubernetes CronJob**, not a Deployment. The CronJob
`schedule` is the only refresh cadence. `REFRESH_CHECK_INTERVAL` is removed
from the code and is no longer a configuration option.

## Goals and non-goals

Goals:

- Run one complete refresh cycle per scheduled invocation and exit cleanly.
- Make the Kubernetes CronJob the sole scheduler.
- Match `stocker-store` conventions for namespace, naming, labels, `envFrom`,
  security context, and resources.
- Tolerate a single provider outage without failing the run.
- Fail visibly (non-zero exit, failed Job) when publishing fails.
- Bound a hung run so it cannot block indefinitely.

Non-goals:

- Provisioning the RabbitMQ broker, exchange, queues, or bindings.
- Helm, Kustomize, CRDs, or an operator.
- In-cluster metrics, alerting, or a monitoring stack.
- Changing provider endpoints or adding provider configuration.
- Maintaining a non-Kubernetes scheduler in this repository.

## Why a CronJob instead of a Deployment

| Concern | Old Deployment | New CronJob |
|---|---|---|
| Lifetime | Long-running process, foreground loop | One process per run, exits when done |
| Cadence | `REFRESH_CHECK_INTERVAL` env var (default `24h`) | `spec.schedule` in the manifest |
| Overlap | `replicas: 1`, rolling update | `concurrencyPolicy: Forbid` |
| Recovery | `restartPolicy: Always`, pod restarts forever | `backoffLimit`, then next scheduled run |
| Probes | None | None (still no listener) |
| Service | None | None |
| State between runs | Stateless | Stateless |

Running the old Deployment alongside the CronJob would publish the same
universe twice on independent schedules. The migration therefore deletes the
old Deployment (see the implementation plan).

## Architecture

### One run (happy path)

```
CronJob stocker-list  (schedule "0 6 * * *")
  └─ Job              (created by the CronJob controller)
      └─ Pod          (restartPolicy: Never)
          └─ /stocker-list
               ├─ GET  TMX company directory        (TSX)
               ├─ GET  NASDAQ screener              (NASDAQ, NYSE, AMEX)
               ├─ PUB  stock.update / stock.update.v1
               │       one Stock{Symbol, Exchange} per symbol
               └─ exit 0
```

The container command is just the image entrypoint (`/stocker-list`). There is
no shell loop, no sleep, and no in-process timer.

### Cluster topology

- **No Service.** `stocker-store` has a ClusterIP Service because it serves
  gRPC. `stocker-list` accepts no inbound connections, so a Service would be
  meaningless.
- **No Deployment.** A Deployment would restart the process after every
  successful run, defeating the one-shot contract and publishing continuously.
- **No probes.** `stocker-store` uses a `tcpSocket` readiness probe because it
  listens on `3500`. `stocker-list` listens on nothing, so a probe would fail;
  a liveness probe would restart the container and a readiness probe is
  meaningless for a Job. Job completion is the health signal.

### Idempotency and delivery

- Each run re-fetches the **entire** universe and republishes every symbol. The
  process keeps no state between runs.
- The publisher declares only the durable exchange. It never declares queues
  or bindings; consumers own those.
- `MessageId` is the stock symbol and the body is deterministic for a given
  symbol/exchange pair, so consumers should **upsert/dedupe by symbol**.
- Delivery is at-least-once. Publisher confirms are enabled and a failed
  publish is retried once by the producer; a retry can duplicate a message
  whose original confirm was lost.
- `concurrencyPolicy: Forbid` prevents two runs from publishing the same
  universe concurrently.

## Manifest layout

`deploy/kubernetes/` contains exactly **two** files, matching the
`stocker-store` gold standard:

| File | Documents | Kinds |
|---|---|---|
| `stocker-list.yaml` | Main manifest, multi-document YAML | `Namespace` → `ConfigMap` → `CronJob` |
| `secret.yaml` | Credential template, applied separately | `Secret` |

- The `Namespace` document is declared first in the main file. Every namespaced
  resource carries an explicit `namespace: stocker`.
- The `Secret` is a template with placeholder values. Real credentials never
  enter the repository.
- There is no `service.yaml`, no `deployment.yaml`, and no separate
  `configmap.yaml`.

## Conventions (aligned with the stocker-store gold standard)

- **Namespace:** `stocker`.
- **Names:** workload `stocker-list`, ConfigMap `stocker-list-config`, Secret
  `stocker-list-env`.
- **Labels:** one minimal label, `app: stocker-list`, on the ConfigMap, Secret,
  CronJob, Job template, and Pod template. The old
  `app.kubernetes.io/name` / `app.kubernetes.io/part-of` labels are dropped.
  A CronJob has no `selector`; the Job controller generates it.
- **Configuration injection:** `envFrom` with `configMapRef` for non-sensitive
  values and `secretRef` for sensitive values. No per-variable `valueFrom`, no
  pod-level `env:`.
- **ConfigMap:** non-sensitive values only.
- **Secret:** `type: Opaque`, plaintext `stringData`, ALL-CAPS placeholders.
- **Image:** `git.wheeli.ca/brian/stocker-list:latest` with
  `imagePullPolicy: Always`.
- **Registry auth:** `imagePullSecrets: [wheeli-registry]`. This is a
  deliberate deviation from the `stocker-store` manifest: `git.wheeli.ca` is a
  private registry and the existing deployment already depends on the
  `wheeli-registry` docker-config secret. Without it, image pulls fail unless
  every node is pre-authenticated to the registry.
- **Security:** container-level `securityContext` only; no pod-level
  `securityContext`, no service account.
- **Resources:** requests `cpu: 50m`, `memory: 128Mi`; limit `memory: 512Mi`
  only (no CPU limit).
- **Replicas/parallelism:** one Pod per Job (Job defaults `parallelism: 1`,
  `completions: 1`). No HPA, no anti-affinity.
- **Credentials:** all real credentials live outside the repository. The
  committed Secret contains placeholders only.

## CronJob specification

| Field | Value | Rationale |
|---|---|---|
| `apiVersion` / `kind` | `batch/v1` / `CronJob` | Stable batch API. |
| `metadata.name` | `stocker-list` | Workload name convention. |
| `spec.schedule` | `"0 6 * * *"` | Daily at 06:00. The schedule is interpreted in the controller manager's time zone (normally UTC); set `spec.timeZone` if the control plane is not UTC. Edit this field to change cadence; no code or env var is involved. |
| `spec.concurrencyPolicy` | `Forbid` | A second run must not overlap the first; overlapping runs would duplicate the full universe. A run that is still active when the next schedule fires is skipped. |
| `spec.successfulJobsHistoryLimit` | `3` | Keep three successful Jobs (and their logs) for troubleshooting; bound object growth. |
| `spec.failedJobsHistoryLimit` | `3` | Keep three failed Jobs for diagnosis; bound object growth. |
| `spec.jobTemplate.metadata.labels` | `app: stocker-list` | Makes `kubectl get jobs -l app=stocker-list` work. |
| `spec.jobTemplate.spec.activeDeadlineSeconds` | `600` | Hard 10-minute cap on a Job, covering all Pod attempts and retries. A hung provider or broker cannot block forever. |
| `spec.jobTemplate.spec.backoffLimit` | `1` | One retry after a failed attempt, then the Job is marked Failed. A publish outage gets one immediate second chance and is then visible. |
| `spec.jobTemplate.spec.template.spec.restartPolicy` | `Never` | Each retry gets a fresh Pod. The kubelet does not restart a failed container inside the same Pod, so failed-Pod logs are preserved and there is no fast restart loop. `backoffLimit` owns retry policy. |
| `spec.jobTemplate.spec.template.metadata.labels` | `app: stocker-list` | Minimal Pod label for `kubectl get pods -l app=stocker-list`. |
| `parallelism` / `completions` | defaults (`1` / `1`) | One Pod per Job. |
| `suspend` | not set (`false`) | Normal scheduling. |
| `startingDeadlineSeconds` | not set | A run missed while the controller was down may start late; acceptable for a daily batch. |

`activeDeadlineSeconds` is a **Job-level** field: it covers the whole Job,
including retries. If a run hangs, the deadline kills it and the Job fails; a
retry will not get a fresh deadline.

## Container

| Setting | Value |
|---|---|
| `image` | `git.wheeli.ca/brian/stocker-list:latest` |
| `imagePullPolicy` | `Always` |
| `command` / `args` | none; the image's `ENTRYPOINT ["/stocker-list"]` runs one cycle and exits |
| `ports` | none |
| `livenessProbe` / `readinessProbe` | none (deliberate; no listener) |
| `envFrom` | `configMapRef: stocker-list-config`, `secretRef: stocker-list-env` |
| `resources.requests` | `cpu: 50m`, `memory: 128Mi` |
| `resources.limits` | `memory: 512Mi` (no CPU limit) |
| `securityContext` | `allowPrivilegeEscalation: false`, `capabilities.drop: ["ALL"]`, `runAsNonRoot: true`, `seccompProfile.type: RuntimeDefault`, `runAsUser: 65532`, `runAsGroup: 65532` |

`runAsUser` / `runAsGroup` `65532` match the distroless `nonroot` user. The
image is distroless static: no shell, no package manager, no writable
filesystem is required.

## Configuration

Variable names are exactly as read by `internal/config/config.go`.

### ConfigMap `stocker-list-config`

| Variable | Value | Notes |
|---|---|---|
| `RABBITMQ_EXCHANGE` | `stock.update` | Durable topic exchange the publisher declares. |
| `RABBITMQ_EXCHANGE_TYPE` | `topic` | One of `direct`, `topic`, `fanout`, `headers`. |
| `RABBITMQ_ROUTING_KEY` | `stock.update.v1` | Routing key for every message. |

### Secret `stocker-list-env`

| Variable | Placeholder | Notes |
|---|---|---|
| `RABBITMQ_URL` | `amqp://stocker:RABBITMQ_PASSWORD@rabbitmq.messaging.svc.cluster.local:5672/` | Broker URI with the `stocker` user. |

The Secret template assumes the RabbitMQ **client Service** is named
`rabbitmq` in namespace `messaging`. Verify with:

```bash
kubectl -n messaging get svc
```

If the broker is external or the Service differs, substitute the correct host
and port. URL-encode any special characters in the password (`@`, `:`, `/`,
`?`, `#`).

### Cadence

`REFRESH_CHECK_INTERVAL` is **removed**. The CronJob `schedule` is the only
cadence control:

- Change `spec.schedule` in `deploy/kubernetes/stocker-list.yaml` and
  re-apply.
- No environment variable, ConfigMap key, or code change is involved.

### `RABBITMQ_URL` semantics

- Unset: the code default `amqp://guest:guest@192.168.1.48:5672/` is used.
- Explicitly empty (`RABBITMQ_URL=`): publishing is disabled and the producer
  is a no-op. The process still fetches symbols and exits `0` without
  publishing. **Do not use this in the cluster**; it produces successful Jobs
  that send nothing.
- Always set `RABBITMQ_URL` in the Secret when deploying to Kubernetes. The
  code default uses `guest:guest`, which stock RabbitMQ rejects from
  non-loopback sources; the Secret must supply the `stocker` user.

## Credentials

- The committed `deploy/kubernetes/secret.yaml` contains placeholders only
  (`RABBITMQ_PASSWORD`).
- Real credentials live outside the repository. For the current operator they
  are in `/home/brian/workspace/credentials.md`; do not copy them into a
  tracked file, a commit, or this document.
- Apply the filled-in Secret from a temporary copy, then delete the copy (see
  the implementation plan). Never commit the filled-in version.
- `wheeli-registry` is a namespace prerequisite in `stocker`. A docker-registry
  pull Secret is namespaced, so create it once in `stocker`, not per workload;
  its credentials also stay outside the repository.

## Dependencies

- RabbitMQ broker reachable from the cluster, a `stocker` user, and exchange
  `stock.update` (topic). The publisher declares the exchange; consumers own
  queues and bindings.
- Registry `git.wheeli.ca` reachable from the nodes, plus the `wheeli-registry`
  image-pull Secret in namespace `stocker`.
- Outbound HTTPS to the public provider endpoints (TMX company directory and
  the NASDAQ screener).
- The standard Kubernetes CronJob/Job controllers.
- No Postgres, S3/Garage, Kafka, gRPC listener, or Ollama dependency.

## Failure semantics

| Situation | Behavior | Process exit | Job result |
|---|---|---|---|
| One provider fails | Logs `provider failed to list symbols`, continues with the remaining providers, publishes their symbols | `0` | Complete (degraded) |
| All providers fail | Logs `refresh cycle completed with provider failures` with a `symbols` value of `0`, publishes nothing | `0` | Complete (degraded); alert when `symbols` is `0` |
| Publish or confirm fails | Producer reconnects and retries the publish once; a second failure is returned | `1` | Retried once by `backoffLimit`, then Failed |
| Initial broker connection fails | No internal retry on the first dial; the error is returned | `1` | Retried once by `backoffLimit`, then Failed |
| Run exceeds 10 minutes | `activeDeadlineSeconds` terminates the Pod | terminated | Failed; no further retry in that Job |
| `SIGTERM` / context cancellation | `RunOnce` returns the context error; an interrupted run is not reported as success | `1` | Failed/terminated |
| Duplicate publish on retry | At-least-once delivery; consumers upsert by symbol | n/a | Consumers must be idempotent |

A run where **every** provider fails exits `0` because per-provider failures
are explicitly tolerated. This is a known limitation: monitoring should alert
on the `symbols` attribute reaching `0` or `failed_providers` reaching `2` in
the Job's JSON logs. Making zero symbols fatal is a possible future change,
not part of this design.

## Code changes

### `internal/refresher`

- Replace the long-running `Run` (immediate tick + `time.Ticker` loop) with
  `RunOnce(ctx)`, which performs exactly one cycle and returns.
- Remove the `Config` dependency and the unused repository seam from
  `Refresher` and from `New`. Cadence is no longer the refresher's concern.
- Keep per-provider failure tolerance: a provider error is logged and the
  remaining providers are still used.
- Keep publish failures fatal: a publish error is returned to the caller.
- Surface the context error (directly or wrapped) for an already-cancelled or
  interrupted context so a `SIGTERM`/deadline does not look like success.

### `internal/config`

- Remove `RefreshCheckInterval`, its validation, the duration parsing helper,
  and the now-unused integer helper.
- Keep only the four `RABBITMQ_*` settings and their existing semantics.

### `cmd/server`

- Remove the goroutine, `sync.WaitGroup`, ticker, and shutdown coordination.
- Load config, build providers, build the producer, call `RunOnce`, close the
  producer, and return.
- Exit `0` on success; exit `1` on configuration or publish errors.
- Defer `producer.Close()` so every path, including failures, closes the AMQP
  connection. A close error after a successful run is logged as a warning and
  does not fail the Job; every publish was already confirmed.

### Tests

- Rewrite the refresher tests for one-shot semantics: direct `RunOnce` calls
  instead of goroutines plus sleeps.
- Add a test proving the provider is called exactly once per invocation.
- Add tests proving an already-cancelled context and a context cancelled
  during a provider call both return a context error.
- Add a test proving the all-providers-fail case still returns `nil` and
  publishes nothing.
- Keep coverage for provider tolerance, publish failure, and the HTML
  maintenance-page incident.
- Remove the config tests for `REFRESH_CHECK_INTERVAL`; add one that sets it
  to a value the old validation rejected (`0s`) and asserts `Load` succeeds,
  proving the variable is no longer read.

## Repository cleanup

| Item | Decision | Reason |
|---|---|---|
| `deploy/quadlet/stocker-list.container` | **Remove** | It is a long-running `Restart=always` systemd user unit. That contradicts one-shot semantics and would restart the process after every successful run. The CronJob is the deployment mechanism. |
| Makefile `quadlet-*` targets | **Remove** | `quadlet-install` references a nonexistent `stocker-list-rootless.container`; the targets are broken. |
| `podman-compose.yml` | **Replace with one local `stocker-list` service** | Remove vestigial `ports: 50051` and `depends_on: postgres`; the process listens on nothing and has no Postgres dependency. Set `restart: "no"` because the process exits after one cycle. Remove the unrelated legacy `tsx-evaluator` / `tsx-history` services, which reference images and dependencies that do not exist in this repository. |
| `Containerfile` `EXPOSE 50051` | **Remove** | The binary listens on no port. Keep the distroless static runtime. Add `USER 65532:65532` so the image's non-root default matches the Pod security context, and drop the unused `/root/go/bin` PATH entry. |
| `README.md`, `design.md`, `.env.example`, `.env.podman` | **Update** | Remove the refresh-loop/interval language and the quadlet instructions; describe one-shot execution and the CronJob. |

If a non-Kubernetes scheduler is needed later, use a systemd **timer** that
runs `podman run --rm` once per schedule; do not resurrect a long-running
container unit. That alternative is documented but not implemented.

## Deviations from the stocker-store gold standard

| Area | stocker-store | stocker-list | Justification |
|---|---|---|---|
| Workload kind | `Deployment` | `CronJob` | Batch producer versus long-running gRPC service. |
| Service | ClusterIP Service on `3500` | None | No listener, no inbound traffic. |
| Probes | `tcpSocket` readiness/liveness on `3500` | None | No port to probe; a probe would CrashLoop the pod. |
| `imagePullSecrets` | Not referenced | `wheeli-registry` | Private `git.wheeli.ca` registry requires auth. |
| Secret contents | Database and RabbitMQ credentials | RabbitMQ URL only | No database dependency. Same template style and placeholder convention. |
| Manifest file | `stocker-store.yaml` with a Service | `stocker-list.yaml` without a Service | Same Namespace → ConfigMap → workload ordering, minus the Service. |

Everything else (namespace, names, `app` label, `envFrom`, security context,
resources, image policy, no service account, no pod security context) matches.

## Migration from the superseded manifests

1. Apply the new `deploy/kubernetes/stocker-list.yaml`. This creates the
   `stocker` Namespace if needed, updates `stocker-list-config`, and creates
   the CronJob.
2. Immediately suspend the CronJob so it cannot start before the Secret
   exists or while the old Deployment is still publishing:
   `kubectl -n stocker patch cronjob stocker-list --type=merge -p '{"spec":{"suspend":true}}'`.
3. Verify the live ConfigMap no longer contains `REFRESH_CHECK_INTERVAL`. If
   it persists because the ConfigMap was not previously managed by
   `kubectl apply`, remove the key manually.
4. Delete the old Deployment:
   `kubectl -n stocker delete deployment stocker-list --ignore-not-found`.
   Otherwise the old long-running loop and the CronJob will both publish.
5. Apply the filled-in Secret from a temporary copy (the implementation plan
   shows a `mktemp`/`chmod 600` procedure).
6. Ensure the `wheeli-registry` pull Secret exists in namespace `stocker`.
7. Unsuspend the CronJob:
   `kubectl -n stocker patch cronjob stocker-list --type=merge -p '{"spec":{"suspend":false}}'`.
8. Run the smoke test in the implementation plan.

## Verification (what "healthy" looks like)

- `kubectl -n stocker get cronjob stocker-list` shows schedule `0 6 * * *`
  and `SUSPEND False`; `-o jsonpath='{.spec.concurrencyPolicy}'` prints
  `Forbid` (concurrency policy is not a default output column).
- A manual Job and a scheduled Job complete with `status.succeeded: 1`.
- The Job's JSON logs contain `refresh cycle starting`, `refresh cycle
  complete` with a non-zero `symbols` value, and `one-shot run complete;
  exiting`.
- No Deployment or Service named `stocker-list` exists.
- Messages appear on exchange `stock.update` with routing key
  `stock.update.v1` (bind a queue before the run, or observe the downstream
  consumer).
- A provider failure produces a `failed_providers` value of `1` but still
  exits `0` and publishes the healthy provider's symbols.
- An unreachable broker produces exit `1`, one retry, and a Failed Job.

## Out of scope

- RabbitMQ broker, exchange, queue, and binding management.
- Downstream consumers and their idempotency implementation.
- Registry provisioning and `wheeli-registry` credential rotation.
- Monitoring/alerting rules for a zero `symbols` count or failed Jobs.
- Migration of historical symbol data.
