# Plan: deployment script fixes + README (providers & env vars)

## Goal

Make the deployment (image target, quadlet, env files, install scripts) consistent
around the single image `git.wheeli.ca/brian/stocker-list:latest`, and update `README.md`
to accurately document **both** stock providers (TSX + US/FMP) and **all** environment
variables the service consumes — wiring those new variables (Kafka config + FMP key) into
the Go code so that the documented values are real.

## Authoritative environment variables

This exact table is what `README.md` must mirror (step 11). Groupings: **Core**,
**Kafka**, **Provider**.

| Name | Type | Default | Required? | Description |
|------|------|---------|-----------|-------------|
| `GRPC_PORT` | int | `50051` | No | Legacy port value. Parsed and retained for backward compatibility; the running service does **not** open a gRPC server today. Must be `1`–`65535` or `Load()` returns an error. |
| `REFRESH_CHECK_INTERVAL` | Go duration string (e.g. `24h`, `12h`, `30m`) | `24h` | No | How often the background refresher pulls the symbol list and re-publishes to Kafka. Must parse and be `> 0`. |
| `KAFKA_BROKERS` | comma-separated `host:port` list (e.g. `kafka1:9093,kafka2:9093`) | empty | No | Kafka brokers to publish to. **Empty = Kafka disabled → the producer is a graceful no-op** (no broker is ever dialed). |
| `KAFKA_TOPIC` | string | `stock.update.v1` | No | Topic published to. Empty/absent falls back to `stock.update.v1`. |
| `KAFKA_SASL_USERNAME` | string | empty | No | SASL username. Empty (with empty password) = SASL disabled. |
| `KAFKA_SASL_PASSWORD` | string | empty | No | SASL password. |
| `KAFKA_SASL_MECHANISM` | string | `PLAIN` | No | SASL mechanism. One of `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`. (Accepted case-insensitively.) |
| `KAFKA_SSL_ENABLED` | bool | `true` | No | Enable TLS for Kafka connections. Parsed as `true`/`false`. |
| `FMP_API_KEY` | string | empty | No | Financial Modeling Prep API key. **Empty = US provider disabled (TSX-only).** When set, the US provider is added to the refresh loop. |

**Semantics that must hold in code and docs:**
- With `KAFKA_BROKERS` empty, the service still runs the refresh loop but the producer is
  a no-op; it does **not** dial any broker and does **not** crash.
- With `FMP_API_KEY` empty, only the TSX provider runs. With it set, both TSX and US run.

## Out of scope

- **No gRPC server is started and none is invented.** The service is a Kafka publisher
  driven by the refresh loop. `GRPC_PORT` is parsed for back-compat only. Do **not** add a
  gRPC listener, ports, or gRPC handlers in `main.go`.
- **No live Kafka broker is required for any test.** The zero-value `kafka.Producer`
  remains a no-op, and `internal/kafka` tests never dial a real broker.
- **No Postgres / `DB_*` environment variables.** The legacy database block is being
  removed from env templates and scripts.
- **Do not modify `internal/provider/fmp_test.go`** (it is misnamed; it tests the TSX
  `Client` and stays green unchanged).
- Do not change the `kafkastockv1` dependency or the `StockUpdate` message shape.

## Numbered Implementation Instructions

> Apply steps in order. After each Go step, `go build ./...` should pass (the repo is
> green at the start of step 1). Each step is self-contained and independently
> implementable by one developer assuming the steps before it are already applied.

---

### Step 1 — Extend `internal/config` with Kafka + Provider env vars

**Files to change:** `internal/config/config.go`

**Required end-state:**

1. Add the following fields to `type Config struct` (keep `GRPCPort` and
   `RefreshCheckInterval` — use named fields, do not reposition existing ones so that the
   existing struct literal `config.Config{RefreshCheckInterval: time.Hour}` in
   `refresher_test.go` still compiles):

   - `KafkaBrokers string`
   - `KafkaTopic string`
   - `KafkaSASLUsername string`
   - `KafkaSASLPassword string`
   - `KafkaSASLMechanism string`
   - `KafkaSSLEnabled bool`
   - `FMPAPIKey string`

2. Populate them in `Load()` using the existing `getEnv` helper plus a **new** helper
   `getEnvBool(key string, fallback bool) bool` (add it near the other helpers; parse with
   `strconv.ParseBool`, returning `fallback` on error/absence):

   ```go
   KafkaBrokers:       getEnv("KAFKA_BROKERS", ""),
   KafkaTopic:         getEnv("KAFKA_TOPIC", "stock.update.v1"),
   KafkaSASLUsername:  getEnv("KAFKA_SASL_USERNAME", ""),
   KafkaSASLPassword:  getEnv("KAFKA_SASL_PASSWORD", ""),
   KafkaSASLMechanism: getEnv("KAFKA_SASL_MECHANISM", "PLAIN"),
   KafkaSSLEnabled:    getEnvBool("KAFKA_SSL_ENABLED", true),
   FMPAPIKey:          getEnv("FMP_API_KEY", ""),
   ```

3. **Constraint:** NO new required field. Every existing `Load()` call in
   `config_test.go` (which only sets `GRPC_PORT` / `REFRESH_CHECK_INTERVAL`) must still
   return a successful `*Config` with the new fields at their defaults. Do **not** add a
   validation block that fails `Load()` when any new var is unset. The only errors `Load()`
   may still return are the two existing ones (out-of-range `GRPC_PORT`, non-positive
   `RefreshCheckInterval`).

**Verify:** `go build ./...` passes. The five existing tests in `config_test.go` still pass
(`go test ./internal/config/`).

---

### Step 2 — Make `internal/kafka` a real (but no-op-by-default) producer

**Files to change/create:** `internal/kafka/producer.go` (rework), `go.mod` / `go.sum`
(add dependency).

**External exports that MUST survive (do not rename/move/remove):**
- `const StockUpdateTopic = "stock.update.v1"` (value unchanged).
- `type Producer struct{ ... }` exposing the exported field `Topic string` (tests build
  `&kafka.Producer{Topic: kafka.StockUpdateTopic}`).
- method `func (p *Producer) PublishStockUpdate(ctx context.Context, msg *kafkastockv1.StockUpdate) error`.
- **The zero value of `Producer` must be a no-op**: `(&Producer{}).PublishStockUpdate(...)`
  returns `nil` and does **not** dial a broker.

**Required end-state:**

1. Define a small unexported client interface so the sender is swappable/testable and the
   zero value stays a no-op (nil):
   ```go
   type sender interface {
       SendMessage(*sarama.ProducerRecord) (int32, int64, error)
       Close() error
   }
   ```
   `Producer` becomes:
   ```go
   type Producer struct {
       Topic   string
       client  sender // nil = no-op (Kafka disabled / not configured)
   }
   ```
2. Add `func NewProducer(cfg *config.Config) (*Producer, error)`:
   - If `cfg.KafkaBrokers == ""` → return `(&Producer{Topic: topicOrDefault(cfg)}, nil)`
     with a **nil** client and **no dial**. `topicOrDefault` returns `cfg.KafkaTopic` if
     non-empty else `StockUpdateTopic`.
   - Else parse brokers: `brokers := strings.Split(cfg.KafkaBrokers, ",")` (trim spaces,
     drop empties). Build `conf := sarama.NewConfig()`:
     - SASL (only when a mechanism/username is effectively configured):
       `conf.Net.SASL.Enable = true` and set
       `conf.Net.SASL.Mechanism` from `cfg.KafkaSASLMechanism`
       (`"PLAIN"` → `sarama.SASLPlain`,
       `"SCRAM-SHA-256"` → `sarama.SASLSCRAMSHA256`,
       `"SCRAM-SHA-512"` → `sarama.SASLSCRAMSHA512`; compare case-insensitively, default to
       `PLAIN` if unrecognized),
       `conf.Net.SASL.User = cfg.KafkaSASLUsername`,
       `conf.Net.SASL.Password = cfg.KafkaSASLPassword`.
       Leave `SASL.Enable` false when both username and password are empty.
     - TLS: `conf.Net.TLS.Enable = cfg.KafkaSSLEnabled`.
     - `conf.Producer.Return.Errors = true`.
   - `sp, err := sarama.NewSyncProducer(brokers, conf)`. On error return
     `nil, fmt.Errorf("create kafka producer: %w", err)`. On success return
     `(&Producer{Topic: topicOrDefault(cfg), client: sp}, nil).
   - **Import cycle is fine:** `internal/kafka` may import `internal/config`; `internal/config`
     does not import `internal/kafka`.
3. Rework `PublishStockUpdate`:
   ```go
   func (p *Producer) PublishStockUpdate(ctx context.Context, msg *kafkastockv1.StockUpdate) error {
       if p.client == nil || msg == nil {
           return nil // no-op when Kafka is disabled
       }
       if p.Topic == "" {
           p.Topic = StockUpdateTopic
       }
       data, err := msg.Marshal()
       if err != nil {
           return fmt.Errorf("marshal stock update: %w", err)
       }
       rec := sarama.NewProducerRecord(p.Topic, "", data)
       rec.Key = &sarama.StringEncoder{Value: msg.Symbol}
       if _, _, err := p.client.SendMessage(rec); err != nil {
           return fmt.Errorf("publish to kafka topic %q: %w", p.Topic, err)
       }
       return nil
   }
   ```
4. Add `func (p *Producer) Close() error` → `return nil` if `p.client == nil` else
   `return p.client.Close()`.
5. Add the dependency and tidy (offline-capable — the module is in the Go module cache):
   - Run `go get github.com/IBM/sarama@v1.60.2` (or a newer `v1.*`) then `go mod tidy`.
   - **If the build hits trouble** with sarama, the acceptable fallback is
     `github.com/segmentio/kafka-go`; in that case implement `NewProducer`/`Publish`/`Close`
     against that library instead, but **keep the three external exports and the zero-value
     no-op behavior exactly as specified above.**
   - Preserve the existing `kafkastockv1 "stocker-store/proto/v1/kafka"` import.

**Verify:** `go build ./...` passes. Existing `refresher` tests still pass
(`go test ./internal/refresher/`) because `&kafka.Producer{}` and
`&kafka.Producer{Topic: kafka.StockUpdateTopic}` remain no-ops returning `nil`.

---

### Step 3 — Wire the US provider + real producer into `cmd/server/main.go`

**Files to change:** `cmd/server/main.go`

**Required end-state (in `run()`):**

1. Replace the placeholder producer:
    ```go
    // was: kafkaProducer := &kafka.Producer{}
    kafkaProducer, err := kafka.NewProducer(cfg)
    if err != nil {
        return fmt.Errorf("create kafka producer: %w", err)
    }
    ```
   - Treat `cfg.KafkaBrokers == ""` (the no-op path) as normal/successful. A non-nil error
     here means brokers were configured but unreachable — it is **fatal** in `run()` so the
     service does not silently drop data. (The documented "Kafka disabled" case is only the
     *empty-brokers* path, which returns no error.)
2. Build the providers slice — TSX always, US only when a key is set:
   ```go
   providers := []provider.Provider{provider.NewClient()} // TSX — no key, always on
   if cfg.FMPAPIKey != "" {
       providers = append(providers, provider.NewUSClient(cfg.FMPAPIKey)) // US/FMP
   }
   ```
3. Pass it through:
   ```go
   ref := refresher.New(cfg, nil, providers, log, kafkaProducer)
   ```
   (everything else — the goroutine, `wg`, `<-ctx.Done()`, `wg.Wait()`, signal handling —
   stays).
4. Graceful shutdown: after `wg.Wait()` and before `return nil`, call
   `if cerr := kafkaProducer.Close(); cerr != nil { log.Warn("close kafka producer", "error", cerr) }`.
5. Keep the package comment accurate: the service is a **Kafka publisher driven by a
   refresh loop** (not a gRPC server). Remove any wording that implies a gRPC server is
   started here (the existing line `// manages lifecycle ... publishing to Kafka` is fine).

**Verify:** `go build ./...` passes. `cmd/server` compiles. (Do **not** start the service.)

---

### Step 4 — Add `config` tests for the new env vars

**Files to change:** `internal/config/config_test.go` (append new `func`; do **not** modify
the five existing test functions).

**Required end-state** (use `t.Setenv` for any set var, exactly as the existing tests do):

- `TestLoad_KafkaDefaults` — no Kafka/Provider env vars set → assert:
  `cfg.KafkaBrokers == ""`, `cfg.KafkaTopic == "stock.update.v1"`,
  `cfg.KafkaSASLUsername == ""`, `cfg.KafkaSASLPassword == ""`,
  `cfg.KafkaSASLMechanism == "PLAIN"`, `cfg.KafkaSSLEnabled == true`,
  `cfg.FMPAPIKey == ""`. And `Load()` returns no error.
- `TestLoad_KafkaCustom` — set:
  `KAFKA_BROKERS="kafka1:9093,kafka2:9093"`, `KAFKA_TOPIC="stock.other"`,
  `KAFKA_SASL_USERNAME="svc"`, `KAFKA_SASL_PASSWORD="s3cr3t"`,
  `KAFKA_SASL_MECHANISM="SCRAM-SHA-512"`, `KAFKA_SSL_ENABLED="false"`,
  `FMP_API_KEY="fmpkey123"`. Assert each config field equals the corresponding value
  (e.g. `cfg.KafkaSSLEnabled == false`, `cfg.FMPAPIKey == "fmpkey123"`). `Load()` returns
  no error.
- `TestLoad_InvalidSSLBool` — set `KAFKA_SSL_ENABLED="not-a-bool"` → `Load()` returns no
  error and `cfg.KafkaSSLEnabled == true` (fallback).

**Verify:** `go test ./internal/config/` passes (7 tests total).

---

### Step 5 — Add `kafka` producer tests (no live broker)

**Files to create:** `internal/kafka/producer_test.go`

**Required end-state** — all tests must pass with **no** Kafka broker available:

- `TestProducer_ZeroValue_NoOp` —
  `p := &kafka.Producer{}`; call
  `p.PublishStockUpdate(context.Background(), &kafkastockv1.StockUpdate{Symbol: "AC", Exchange: "TSX"})`;
  expect `err == nil`. (Must not panic, must not dial.)
- `TestProducer_WithTopic_NoOp` —
   `p := &kafka.Producer{Topic: kafka.StockUpdateTopic}`; same call → `err == nil`.
   This mirrors how `refresher_test.go` uses it.
- `TestNewProducer_EmptyBrokers_NoDial` —
  `cfg := &config.Config{RefreshCheckInterval: time.Hour}` (leave `KafkaBrokers` empty);
  `p, err := kafka.NewProducer(cfg)` → `err == nil` and `p != nil`, and
  `p.PublishStockUpdate(ctx, msg) == nil` (proves no-op). Also assert `p.Topic == "stock.update.v1"`
  (default applied) when `cfg.KafkaTopic` is empty.
- **Optional (only if step 2 used the `sender` interface):** add a fake `sender` that records
  `SendMessage` calls, inject it into `Producer{client: fake, Topic: "t"}`, and assert that
  `PublishStockUpdate` marshals the message and forwards exactly one `*sarama.ProducerRecord`
  to the fake with key `"AC"`. If step 2 inlined `sarama.SyncProducer` directly, **skip** this
  case (it is not required).
- Do **not** write any test that calls `kafka.NewProducer(cfg)` with a non-empty
  `KafkaBrokers`, and do **not** attempt a real network connection.

**Verify:** `go test ./internal/kafka/` passes with no broker running.

---

### Step 6 — Make the US provider testable + add `provider/us_test.go`

**Files to change/create:** `internal/provider/us.go` (add a test constructor),
`internal/provider/us_test.go` (new).

**Required end-state:**

1. Add a test constructor to `us.go` (mirroring `NewClientForTest` in `tsx.go`), without
   changing `NewUSClient(apiKey)` or `ListSymbols` behavior:
   ```go
   func NewUSClientForTest(baseURL, apiKey string) *USClient {
       return &USClient{baseURL: baseURL, apiKey: apiKey, httpClient: &http.Client{Timeout: 15 * time.Second}}
   }
   ```
2. Create `internal/provider/us_test.go`. Use `net/http/httptest` servers (like the TSX
   tests do), pointing at `NewUSClientForTest(srv.URL, "testkey")`. FMP's `stock/list`
   endpoint returns a JSON **array** of `{symbol, name, currency, exchange}`:
   - `TestUS_Success` — server body:
     `[{"symbol":"SPY","name":"SPDR S&P 500 ETF Trust","currency":"USD"}]` (no `exchange`
     → defaults apply). Assert: `len == 1`, `SPY`, name matches, `Exchange == "US"`
     (default), `Currency == "USD"` (default).
   - `TestUS_ExchangeAndCurrencyOverrides` — body:
     `[{"symbol":"BRK.B","name":"Berkshire","currency":"CAD","exchange":"TSX"}]`. Assert
     `Exchange == "TSX"`, `Currency == "CAD"` (FMP values win over defaults).
   - `TestUS_EmptyResponse` — body `[]` → `len == 0`, no error.
   - `TestUS_HTTPError` — server returns `500` → `ListSymbols` returns a non-nil error.
   - `TestUS_InvalidJSON` — body `[invalid` → non-nil error.
3. Keep the `?apikey=testkey` request behavior as-is. (You may optionally assert the server
   saw `r.URL.Query().Get("apikey") == "testkey"`.)
4. **Do NOT modify `fmp_test.go`** and do NOT rename `USClient`, `NewUSClient`, or
   `ListSymbols`.

**Verify:** `go test ./internal/provider/` passes (TSX tests in `fmp_test.go` + new US
tests).

---

### Step 7 — Fix `deploy/push.sh`

**Files to change:** `deploy/push.sh`

**Required end-state** (keep `#!/usr/bin/env bash`, `set -euo pipefail`, and
`PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"`):

```bash
REGISTRY="git.wheeli.ca"
IMAGE_NAME="brian/stocker-list:latest"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="${REGISTRY}/${IMAGE_NAME}"   # = git.wheeli.ca/brian/stocker-list:latest

echo "==> Building image for $IMAGE"
podman build -t "$IMAGE" "$PROJECT_DIR"

echo "==> Pushing image to $IMAGE"
podman push "$IMAGE"

echo "Done. Image pushed to $IMAGE"
```

- The final image reference **must** be `git.wheeli.ca/brian/stocker-list:latest`.
- No other behavior changes (build then push; still built from `$PROJECT_DIR`).

**Verify:** `bash -n deploy/push.sh` (syntax check) passes. The only image ref in the file is
`git.wheeli.ca/brian/stocker-list:latest`.

---

### Step 8 — Reconcile `Makefile`

**Files to change:** `Makefile`

**Required end-state:**

1. `build` target → output binary named `stocker-list`:
   ```
   build: proto
   	go build -o bin/stocker-list ./cmd/server
   ```
2. `quadlet-install` → correct unit file name + correct env dir (drop `tsx-tracker`):
   ```
   quadlet-install:
   	mkdir -p ~/.config/containers/systemd
   	mkdir -p ~/.config/stocker-list
   	cp deploy/quadlet/stocker-list-rootless.container ~/.config/containers/systemd/stocker-list.container
   	cp -n .env.podman ~/.config/stocker-list/.env.podman 2>/dev/null || true
   	chmod 600 ~/.config/stocker-list/.env.podman
   	systemctl --user daemon-reload
   ```
3. `quadlet-build` → tag the same image as `deploy/push.sh`:
   ```
   quadlet-build:
   	podman build --no-cache -t git.wheeli.ca/brian/stocker-list:latest .
   ```
4. `quadlet-enable: quadlet-build` → start `stocker-list.service`:
   ```
   quadlet-enable: quadlet-build
   	systemctl --user daemon-reload
   	systemctl --user start stocker-list.service
   ```
5. Keep `.PHONY`, `proto`, `proto-protoc`, `tidy`, `run`, `podman-*` targets. `run` may stay
   `go run ./cmd/server`.

**Constraints after edit:** the literal strings `tsx-tracker` and `localhost/tsx-tracker` must
no longer appear anywhere in the Makefile. The only image tag referenced is
`git.wheeli.ca/brian/stocker-list:latest`.

**Verify:** `make -n build` and `make -n quadlet-install` (dry run) show the corrected
commands. `make -n build` output path is `bin/stocker-list`.

---

### Step 9 — Reconcile `deploy/install.sh`

**Files to change:** `deploy/install.sh`

**Required end-state:**

1. Copy the **real** quadlet unit file (there is no `stocker-list.build`; the image is built
   and pushed via `deploy/push.sh`). Replace the two `cp` lines with a single correct copy to
   the system quadlet dir, renaming to the unit `stocker-list`:
   ```bash
   cp "$SCRIPT_DIR"/quadlet/stocker-list-rootless.container "$QUADLET_DIR"/stocker-list.container
   ```
   (i.e. `cp deploy/quadlet/stocker-list-rootless.container /etc/containers/systemd/stocker-list.container`).
   Remove the now-obsolete `stocker-list.build` reference entirely.
2. Keep `ENV_DIR="/etc/stocker-list"`, the `.env.podman` copy, and `chmod 600`.
3. Rewrite the "Next steps" block to remove all `DB_*` / Postgres and `tsx-tracker` language
   and reference the real unit + image. It should instruct, in this order:
   - Build & push the image to `git.wheeli.ca/brian/stocker-list:latest`
     (`(cd deploy && ./push.sh)`, or `podman build -t git.wheeli.ca/brian/stocker-list:latest .`
     then `podman push git.wheeli.ca/brian/stocker-list:latest`).
   - Fill in `KAFKA_*` vars and `FMP_API_KEY` in the env file (`$ENV_DIR/.env.podman`)
     **as needed** — note that with `KAFKA_BROKERS` empty Kafka is disabled, and with
     `FMP_API_KEY` empty the US provider is disabled (TSX-only).
   - `systemctl daemon-reload`; then `systemctl enable --now stocker-list.service`.
   - `systemctl status stocker-list`; `podman logs stocker-list`.
4. Leave the source-copy block (copied into `INSTALL_DIR`) and the root check as-is unless
   they reference broken paths.

**Constraints after edit:** the strings `stocker-list.build`, `tsx-tracker`, `DB_USER`, and
`DB_PASSWORD` must not appear. The env-file references in the "Next steps" text must be
`KAFKA_BROKERS`, `FMP_API_KEY` (not `DB_*`).

**Verify:** `bash -n deploy/install.sh` passes.

---

### Step 10 — Replace env templates (drop Postgres, add Kafka + FMP)

**Files to change:** `.env.example`, `.env.podman`

Both files must end up with the **same variable set** (the authoritative table above), with
only placeholder/comment differences. Remove the entire `DB_*` block and any
"Fill in DB_USER and DB_PASSWORD" wording.

**Required end-state (shared variables):**

```bash
# --- Core ---
GRPC_PORT=50051
# Go duration; drives the refresh loop (default: 24h).
REFRESH_CHECK_INTERVAL=24h

# --- Kafka ---
# Comma-separated host:port list, e.g. kafka1:9093,kafka2:9093.
# Empty = Kafka DISABLED (producer is a graceful no-op; no broker is dialed).
KAFKA_BROKERS=
KAFKA_TOPIC=stock.update.v1
KAFKA_SASL_USERNAME=
KAFKA_SASL_PASSWORD=
# PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512
KAFKA_SASL_MECHANISM=PLAIN
# true | false
KAFKA_SSL_ENABLED=true

# --- Provider (US / FMP) ---
# Empty = US provider DISABLED (TSX-only). Set to enable the US (FMP) provider.
FMP_API_KEY=
```

- In `.env.podman` you may use the placeholder style the file already used (e.g.
  `KAFKA_BROKERS=__FILL_IN__` or blank) — but the variable **names** and the
  `GRPC_PORT`/`REFRESH_CHECK_INTERVAL`/`KAFKA_TOPIC`/`KAFKA_SASL_MECHANISM`/
  `KAFKA_SSL_ENABLED` defaults must match the table. Keep `.env.podman` tracked as a
  template even though it is gitignored (it is already in the repo).
- The `.env.example` header note should stay (a copy-of instructions line), but must no longer
  mention Postgres.

**Constraints after edit:** neither file contains `DB_HOST`, `DB_PORT`, `DB_USER`,
`DB_PASSWORD`, `DB_NAME`, or `DB_SSLMODE`. Both contain all nine variable names from the
authoritative table.

**Verify:** `grep -E 'DB_HOST|DB_USER|DB_PASSWORD' .env.example .env.podman` returns nothing.

---

### Step 11 — Rewrite `README.md`

**Files to change:** `README.md`

**Required end-state** (remove **all** `TODO` markers and the `tstocker-list` / `tsx-tracker`
typos):

1. **Intro:** describe it accurately — a Go service that pulls stock symbols from public
   providers and publishes them as Kafka (`StockUpdate`) messages on a refresh timer. State
   plainly that it does **not** run a gRPC server (`GRPC_PORT` is retained for
   back-compatibility only).
2. **Architecture:** a short list (roughly what's there now) —
   `cmd/server` (wiring, refresh loop, **not** a server), `internal/config`,
   `internal/provider`, `internal/refresher`, `internal/kafka`.
3. **Kafka integration:** keep the `stocker-store` / `kafkastockv1.StockUpdate{Symbol,
   Exchange, Scores}` explanation. Add: publishing is disabled (graceful no-op) when
   `KAFKA_BROKERS` is empty; otherwise messages are sent to `KAFKA_TOPIC` (default
   `stock.update.v1`) with optional SASL/TLS per the env vars.
4. **Providers — list BOTH:**
   - **TSX** — official TMX company directory
     (`https://www.tsx.com/json/company-directory/search/tsx/*`), no API key, always active.
   - **US (Financial Modeling Prep)** —
     `https://financialmodelingprep.com/api/v3/stock/list`, requires `FMP_API_KEY`; when
     `FMP_API_KEY` is empty the US provider is disabled and the service tracks **TSX only**.
5. **Environment variables:** reproduce the **"Authoritative environment variables"** table
   from the top of this plan verbatim, grouped into the three groups (Core / Kafka /
   Provider). Add the two key semantic notes (empty `KAFKA_BROKERS` ⇒ Kafka off; empty
   `FMP_API_KEY` ⇒ US off).
6. **Running it** (no TODOs, all commands valid):
   - **Local build/run:** `make build` (produces `bin/stocker-list`); `make run` /
     `go run ./cmd/server` (with the env vars set above). Note Kafka/US are opt-in via
     their env vars.
   - **Podman:** `make podman-up` / `make podman-down`.
   - **Quadlet (rootless):**
     ```
     make quadlet-install
     $EDITOR ~/.config/stocker-list/.env.podman        # set KAFKA_* / FMP_API_KEY
     (cd deploy && ./push.sh)                          # build + push git.wheeli.ca/brian/stocker-list:latest
     systemctl --user daemon-reload
     systemctl --user start stocker-list.service
     systemctl --user status stocker-list
     podman logs stocker-list
     systemctl --user stop stocker-list
     ```
   - State the deployment facts once, clearly: **image** `git.wheeli.ca/brian/stocker-list:latest`,
     **service name** `stocker-list`, **env file** `~/.config/stocker-list/.env.podman`.

**Constraints after edit:** `grep -n TODO README.md` returns nothing; `grpc server` /
`gRPC server` is not described as *running*; the literal strings `tsx-tracker`,
`tstocker-list`, `DB_USER`, `DB_PASSWORD` do not appear; both provider names (TSX and
FMP/US) and all nine env var names appear.

---

### Step 12 — Final verification

**Files to change:** none.

**Run (in this order):**

1. `go mod tidy`
2. `go build ./...`
3. `go vet ./...`
4. `go test ./...`

All four must pass. `go test ./...` must run with **no** Kafka broker and **no** network
mocks beyond `httptest` servers (no US/TSX live endpoints, no Postgres).

**Checklist — deployment artifacts now say:**
- `deploy/push.sh`: single image ref `git.wheeli.ca/brian/stocker-list:latest`; no
  `containers.wheeli.ca`. ✓
- `Makefile`: binary `bin/stocker-list`; unit `deploy/quadlet/stocker-list-rootless.container`
  → `stocker-list.container`; env dir `~/.config/stocker-list`; image tag
  `git.wheeli.ca/brian/stocker-list:latest`; no `tsx-tracker`. ✓
- `deploy/quadlet/stocker-list-rootless.container`: already has
  `Image=git.wheeli.ca/brian/stocker-list:latest` and
  `EnvironmentFile=%h/.config/stocker-list/.env.podman` — unchanged, and consistent with the
  rest. ✓
- `deploy/install.sh`: copies `deploy/quadlet/stocker-list-rootless.container`; references
  service `stocker-list`; no `stocker-list.build`, no `tsx-tracker`, no `DB_*`. ✓
- `.env.example` / `.env.podman`: the nine env vars from the table; no `DB_*`. ✓

**Checklist — `README.md` contains:**
- Both providers (TSX + US/FMP) described. ✓
- The full environment variable table (Core / Kafka / Provider). ✓
- A working "Running it" section with **no** `TODO`s, using service name `stocker-list` and
  env dir `~/.config/stocker-list`. ✓

**Checklist — invariants preserved (tests prove these):**
- `kafka.Producer` zero value is a no-op; `kafka.StockUpdateTopic`,
  `Producer.Topic`, and `Producer.PublishStockUpdate` unchanged in signature/behavior. ✓
- All five original `config_test.go` tests still pass (no new required field). ✓
- `refresher_test.go` and `fmp_test.go` still pass unmodified. ✓
