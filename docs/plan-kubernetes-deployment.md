# Kubernetes Deployment Plan: stocker-list

Companion design doc: `docs/design-kubernetes-deployment.md` (source of truth
for variable names, values, and rationale). Implement the tasks **one at a
time, in order**; each task is self-contained.

## Prerequisites

1. A Kubernetes cluster whose nodes can reach:
   - registry `git.wheeli.ca` (image pull),
   - RabbitMQ `192.168.1.48:5672`.
2. Namespace exists:
   ```bash
   kubectl create namespace stocker
   ```
3. Registry pull secret exists in the namespace (create once per cluster, not
   per service). Real user/token: `/home/brian/workspace/credentials.md`
   → `git.wheeli.ca`:
   ```bash
   kubectl create secret docker-registry wheeli-registry \
     --docker-server=git.wheeli.ca \
     --docker-username=REPLACE_ME \
     --docker-password=REPLACE_ME \
     -n stocker
   ```
4. RabbitMQ exchange `stock.update` (topic) with routing key `stock.update.v1` exists (operator-provided).
5. The image `git.wheeli.ca/brian/stocker-list:latest` has been built and
   pushed (this repo's `deploy/push.sh` does that from a host with a Go
   toolchain and git credentials for `git.wheeli.ca`).

## Tasks

### Task 1: Create `deploy/kubernetes/`

Create the directory `deploy/kubernetes/` at the repository root. It will
hold exactly three files (created in Tasks 2–4):

| File | Kind | Name |
|---|---|---|
| `deploy/kubernetes/configmap.yaml` | `ConfigMap` | `stocker-list-config` |
| `deploy/kubernetes/secret.yaml` | `Secret` | `stocker-list-env` |
| `deploy/kubernetes/deployment.yaml` | `Deployment` | `stocker-list` |

No other files. No `service.yaml` (nothing connects to this pod). The Dockerfile's `EXPOSE 50051` is misleading — the binary does not listen on any port.

### Task 2: Write `deploy/kubernetes/configmap.yaml`

Create `deploy/kubernetes/configmap.yaml` with exactly this content:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: stocker-list-config
  namespace: stocker
  labels:
    app.kubernetes.io/name: stocker-list
    app.kubernetes.io/part-of: stocker
data:
  RABBITMQ_EXCHANGE: "stock.update"
  RABBITMQ_EXCHANGE_TYPE: "topic"
  RABBITMQ_ROUTING_KEY: "stock.update.v1"
  REFRESH_CHECK_INTERVAL: "24h"
```

Then validate it parses:

```bash
kubectl apply --dry-run=client -f deploy/kubernetes/configmap.yaml
```

### Task 3: Write `deploy/kubernetes/secret.yaml`

Create `deploy/kubernetes/secret.yaml` with exactly this content
(**`REPLACE_ME` stays in the committed file** — fill only in your local copy
before applying; RabbitMQ value from `/home/brian/workspace/credentials.md`):

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: stocker-list-env
  namespace: stocker
  labels:
    app.kubernetes.io/name: stocker-list
    app.kubernetes.io/part-of: stocker
type: Opaque
stringData:
  RABBITMQ_URL: "amqp://stocker:REPLACE_ME@192.168.1.48:5672/"
```

Then validate:

```bash
kubectl apply --dry-run=client -f deploy/kubernetes/secret.yaml
```

### Task 4: Write `deploy/kubernetes/deployment.yaml`

Create `deploy/kubernetes/deployment.yaml` with exactly this content:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: stocker-list
  namespace: stocker
  labels:
    app.kubernetes.io/name: stocker-list
    app.kubernetes.io/part-of: stocker
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: stocker-list
  template:
    metadata:
      labels:
        app.kubernetes.io/name: stocker-list
        app.kubernetes.io/part-of: stocker
    spec:
      imagePullSecrets:
        - name: wheeli-registry
      containers:
        - name: stocker-list
          image: git.wheeli.ca/brian/stocker-list:latest
          imagePullPolicy: Always
          securityContext:
            runAsNonRoot: true
            allowPrivilegeEscalation: false
          envFrom:
            - configMapRef:
                name: stocker-list-config
            - secretRef:
                name: stocker-list-env
          resources:
            requests:
              cpu: 50m
              memory: 128Mi
            limits:
              cpu: 500m
              memory: 512Mi
          # Deliberately NO liveness/readiness probe: stocker-list has no
          # HTTP endpoint and no healthcheck subcommand (see design doc).
```

Note: no `ports:` section — the process listens on nothing. Then validate:

```bash
kubectl apply --dry-run=client -f deploy/kubernetes/deployment.yaml
```

### Task 5: Apply and verify

Apply all three manifests:

```bash
kubectl apply -f deploy/kubernetes/
```

Wait for the rollout and inspect:

```bash
kubectl -n stocker rollout status deployment/stocker-list --timeout=120s
kubectl -n stocker get pods -l app.kubernetes.io/name=stocker-list
kubectl -n stocker describe pod -l app.kubernetes.io/name=stocker-list
kubectl -n stocker logs deployment/stocker-list --tail=100
```

Expected:

- Pod `Running`, `0` restarts, no `CrashLoopBackOff`.
- Startup log showing the resolved RabbitMQ exchange and routing key.
- If `RABBITMQ_URL` is missing/wrong (user must be `stocker`, not `guest`), check the Secret.

**One-shot smoke test:**

1. Bind a listener to exchange `stock.update` with routing key `stock.update.v1`.
2. Wait up to 24h (or temporarily set `REFRESH_CHECK_INTERVAL=10s` for testing).
3. PASS = message appears on the exchange. FAIL = no message within the interval (check pod logs).
