# Kafka Producer Rewrite (sarama → segmentio/kafka-go) Implementation Plan

**Goal:** Rewrite the Kafka producer in `internal/kafka/producer.go` to use
`github.com/segmentio/kafka-go` v0.4.51 (the same library our sibling service
stocker-store uses) instead of `github.com/IBM/sarama`, and remove all SASL/TLS
support (brokers are plaintext-only), while keeping the public Producer API
source-compatible and every existing test green.

**Architecture:** The producer keeps its `sender` test-seam: an unexported
interface that `*kafka.Writer` satisfies, stored in `Producer.client`. A `nil`
client keeps the zero-value / no-op producer working exactly as before. The
topic is set **per-message** on `kafka.Message.Topic` (not on `WriterConfig.Topic`)
because kafka-go treats those two as mutually exclusive; this also keeps the
fake-sender tests asserting on the record's topic. `sarama.StringEncoder`/`ByteEncoder`
become plain `[]byte` fields on `kafka.Message`. Config drops its four
SASL/SSL fields and their env vars.

**Tech Stack:** Go 1.25, `github.com/segmentio/kafka-go v0.4.51` (module cache
already populated), protobuf via `google.golang.org/protobuf`, `kafkastockv1.Stock`
from `git.wheeli.ca/brian/stocker-store/proto/v1`.

**Spec:** This document. It is self-contained — the current state of every file
is described in "Current state" below, so an executor with no other context can
follow the steps.

## Global Constraints

- **No SASL, no TLS.** Brokers are plaintext-only. Do not plan, add, or reference
  any SASL or TLS code, config field, dialer option, or env var. The only Kafka
  env vars that may remain are `KAFKA_BROKERS` and `KAFKA_TOPIC`.
- **Public API is frozen.** Used by `cmd/server/main.go` and
  `internal/refresher/refresher.go`; must stay source-compatible:
  - const `kafka.StockUpdateTopic = "stock.update.v1"`
  - `kafka.NewProducer(cfg *config.Config) (*Producer, error)`
  - `(*Producer).PublishStockUpdate(ctx context.Context, msg *kafkastockv1.Stock) error`
  - `(*Producer).Close() error`
  - exported field `Producer.Topic string`
- **No-op contract preserved.** When `KAFKA_BROKERS` is empty (or `client` is
  nil), the producer dials nothing and every publish is a no-op returning `nil`.
- **kafka-go version pinned to v0.4.51** as a **direct** dependency of
  `github.com/anomalyco/stocker-list` (mirror stocker-store's style). Do not
  bump a different minor.
- **Test seam stays injectable.** The `sender` interface must remain unexported,
  satisfied by `*kafka.Writer`, and settable via the package-internal `client`
  field so `fakeSender`-style tests and the zero-value Producer keep working.
- **Each step leaves the module buildable and testable.** Steps are executed
  strictly sequentially, one developer each. Never leave a step with a broken
  `go build ./...`.
- Go toolchain: `go 1.25`. No new transitive direct dependencies beyond what
  `go mod tidy` resolves for kafka-go.

---

## Current state (verified, git HEAD `7f51aa8`)

**`internal/kafka/producer.go`** (142 lines):
- `import "github.com/IBM/sarama"`.
- unexported `sender` interface: `SendMessage(*sarama.ProducerMessage) (int32, int64, error)` + `Close() error`.
- `Producer struct { Topic string; client sender }`.
- `NewProducer(cfg)` reads `cfg.KafkaBrokers`, `cfg.KafkaSSLEnabled`,
  `cfg.KafkaSASLUsername/Password/Mechanism`; builds `sarama.NewSyncProducer`.
- helpers `topicOrDefault`, `parseBrokers`, `saslMechanism` (to delete).
- `PublishStockUpdate` builds `sarama.ProducerMessage{Topic, Key: StringEncoder(symbol), Value: ByteEncoder(data)}`.
- `Close` is a no-op when `client == nil`.

**`internal/kafka/producer_test.go`** (116 lines):
- `TestProducer_ZeroValue_NoOp`, `TestProducer_WithTopic_NoOp`,
  `TestNewProducer_EmptyBrokers_NoDial` (asserts topic `stock.update.v1`),
  `TestProducer_Close_NoClient` — depend on the nil-client no-op design and the
  `client` field.
- `fakeSender implements sender` (`sent []*sarama.ProducerMessage`, `SendMessage`, `Close`).
- `TestPublish_WithFakeSender` — injects `fake`, asserts topic `t`, key type
  `sarama.StringEncoder` == `"AC"`, proto-unmarshals value from `sarama.ByteEncoder`.

**`internal/config/config.go`** (81 lines): `Config` fields `KafkaBrokers`,
`KafkaTopic`, `KafkaSASLUsername`, `KafkaSASLPassword`, `KafkaSASLMechanism`,
`KafkaSSLEnabled`. `Load()` reads `KAFKA_BROKERS` (default `""`),
`KAFKA_TOPIC` (default `stock.update.v1`), `KAFKA_SASL_USERNAME`,
`KAFKA_SASL_PASSWORD`, `KAFKA_SASL_MECHANISM` (default `PLAIN`), and
`KAFKA_SSL_ENABLED` (default `true`, via `getEnvBool`). `getEnvBool` is used
**only** for `KAFKA_SSL_ENABLED` — it becomes dead code once that field is removed.

**`internal/config/config_test.go`** (124 lines):
`TestLoad_KafkaDefaults` (asserts all Kafka fields incl. SASL/SSL) and
`TestLoad_KafkaCustom` (sets `KAFKA_SASL_*`/`KAFKA_SSL_ENABLED` and asserts)
must drop the SASL/SSL assertions (keep `KafkaBrokers`/`KafkaTopic`).
`TestLoad_InvalidSSLBool` must be deleted (it targets `KAFKA_SSL_ENABLED`).

**`cmd/server/main.go`**: `kafka.NewProducer(cfg)` (line 42),
`kafkaProducer.Close()` (line 68), passes `*kafka.Producer` to `refresher.New`
(line 50). **No changes needed** — API stays compatible.

**`internal/refresher/refresher.go`**: holds `producer *kafka.Producer`,
calls `r.producer.PublishStockUpdate(ctx, msg)` (line 88). **No changes needed.**

**`internal/refresher/refresher_test.go`**: constructs the refresher with
`&kafka.Producer{}` (line 52) and `&kafka.Producer{Topic: kafka.StockUpdateTopic}`
(line 79); producer referenced in a comment (line 72). **No changes needed** —
the zero-value / Topic-only no-op Producer must keep compiling and passing.

**`README.md`**: SASL/TLS prose at lines ~38–41 and env-var table rows at lines
76–79 (`KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`, `KAFKA_SASL_MECHANISM`,
`KAFKA_SSL_ENABLED`). Also a stale `StockUpdate` message-type reference (lines
22, 29 vs. actual `kafkastockv1.Stock`) — optional touch-up. The env file
instructions at line ~127 say "set `KAFKA_*` … as needed" (still accurate for
the two remaining vars).

**`design.md`**: generic, no SASL/SSL specifics. No changes required.

**`deploy/quadlet/stocker-list.container`**, `deploy/push.sh`,
`Containerfile`, `Makefile`, `podman-compose.yml`: **no** KAFKA/SASL/SSL
references that need changing (the quadlet `.container` uses a generic
`EnvironmentFile=`; `podman-compose.yml` only has unrelated `DB_SSLMODE`).

**`go.mod`**: direct `github.com/IBM/sarama v1.60.2`; indirect deps pulled by
sarama: `davecgh/go-spew`, `eapache/go-resiliency`, `hashicorp/go-uuid`,
`jcmturner/aescts|dnsutils|gofork|gokrb5|rpc`, `klauspost/compress`,
`pierrec/lz4/v4`, `rcrowley/go-metrics`, `golang.org/x/crypto`, `x/net`, `x/sys`.
**`go.sum`**: 94 lines incl. sarama and testify-only entries.

**kafka-go v0.4.51 API facts** (verified in module cache):
- `kafka.NewWriter(kafka.WriterConfig{Brokers []string, ...}) *Writer` — **panics**
  if `Brokers` is empty (`WriterConfig.Validate` returns an error, `NewWriter` panics).
- `(*Writer).WriteMessages(ctx context.Context, msgs ...kafka.Message) error` —
  default sync writer blocks until written; dialing happens inside `WriteMessages`,
  not at construction (acceptable replacement for sarama's sync-producer semantics).
- `(*Writer).Close() error`.
- `kafka.Message{Topic string, Key []byte, Value []byte, ...}`.
- **Gotcha:** `WriterConfig.Topic` and `kafka.Message.Topic` are mutually
  exclusive — `chooseTopic` (writer.go:907) errors if both are set. → set the topic
  **only** on each `kafka.Message`, leave `WriterConfig.Topic` empty.

**stocker-store reference**: `internal/kafka/client.go` uses
`kafka.NewReader(kafka.ReaderConfig{...})` and `kafka.Message{Value: raw}` with
kafka-go imported as a direct dependency. Style target: direct kafka-go dependency.

---

## Design (the shapes this plan implements)

**New `sender` seam** (`internal/kafka/producer.go`):
```go
type sender interface {
    WriteMessages(ctx context.Context, msgs ...kafka.Message) error
    Close() error
}
```
`*kafka.Writer` satisfies it. The `Producer` struct is unchanged in shape:
```go
type Producer struct {
    Topic  string // exported; empty defaults to StockUpdateTopic
    client sender // nil → Kafka disabled, no-op
}
```

**`NewProducer`**: return the no-op producer when `cfg.KafkaBrokers == ""` or
parse yields zero brokers (`return nil, error`, so we do not let `NewWriter`
panic). Otherwise `kafka.NewWriter(kafka.WriterConfig{Brokers: brokers})`
(**Topic left empty**). No SASL/TLS dialer fields.

**`PublishStockUpdate`**: no-op on nil client / nil msg; proto-marshals;
builds `kafka.Message{Topic: p.Topic, Key: []byte(msg.Symbol), Value: data}`;
calls `client.WriteMessages(ctx, msg)`; wraps error as before.

**`Close`**: no-op when client nil; else `client.Close()`.

**Fake sender for tests** (`internal/kafka/producer_test.go`):
```go
type fakeSender struct {
    sent []kafka.Message
    err  error
}
func (f *fakeSender) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
    f.sent = append(f.sent, msgs...)
    return f.err
}
func (f *fakeSender) Close() error { return nil }
```
`TestPublish_WithFakeSender` asserts `len(sent)==1`, `sent[0].Topic=="t"`,
`string(sent[0].Key)=="AC"`, and proto-unmarshals `sent[0].Value` to a
`kafkastockv1.Stock` with `Symbol=="AC"`, `Exchange=="TSX"`.

**Config removal** (`internal/config/config.go`, `config_test.go`):
delete `KafkaSASLUsername`, `KafkaSASLPassword`, `KafkaSASLMechanism`,
`KafkaSSLEnabled` fields, their `Load()` env reads, and the now-unused
`getEnvBool` helper. Keep `KafkaBrokers`/`KafkaTopic` (+ their `Load()` reads
and their test assertions).

**go.mod**: add direct `github.com/segmentio/kafka-go v0.4.51`; remove
`github.com/IBM/sarama`; `go mod tidy` drops the sarama-only transitive deps.

---

## Implementation plan (sequential, one developer per step)

> Each step: **Files** → **Do** → **Verify** → **Done when**. Run steps in order.
> No step leaves the module unbuildable. Do not commit beyond what a step
> explicitly asks; do not push.

### Step 1 — Add the kafka-go dependency (additive, non-breaking)

**Files:** `go.mod`, `go.sum` (tooling-only changes; do **not** edit source).

**Do:**
- Add kafka-go as a **direct** dependency at the pinned version:
  `go get github.com/segmentio/kafka-go@v0.4.51`
- This is additive. `producer.go` still imports sarama and still builds; nothing
  else references kafka-go yet. (sarama and its transitive deps stay in `go.mod`
  for now — they are pruned in Step 5 by `go mod tidy`.)

**Verify:**
- `go build ./...`

**Done when:** `go.mod` lists `github.com/segmentio/kafka-go v0.4.51` as a
direct `require`; `go build ./...` succeeds; `go get` exited 0 (dependency found
in the module cache, no network download needed).

---

### Step 2 — Rewrite the producer onto kafka-go

**Files:**
- Modify: `internal/kafka/producer.go` (full rewrite of the sarama parts)
- Modify: `internal/kafka/producer_test.go` (new `fakeSender`, rewrite `TestPublish_WithFakeSender`)

**Do:**
1. **`producer.go`:**
   - Replace `import "github.com/IBM/sarama"` with `import "github.com/segmentio/kafka-go"`.
   - Replace the `sender` interface with the kafka-go shape
     (`WriteMessages(ctx, ...kafka.Message) error` + `Close() error`).
   - Rewrite `NewProducer(cfg)`:
     - keep `import "github.com/anomalyco/stocker-list/internal/config"`, `fmt`, `strings`, `google.golang.org/protobuf/proto`, `kafkastockv1`.
     - `if cfg.KafkaBrokers == "" { return &Producer{Topic: topicOrDefault(cfg)}, nil }` (no-op).
     - `brokers := parseBrokers(cfg.KafkaBrokers)`; `if len(brokers)==0 { return nil, fmt.Errorf("create kafka producer: no brokers configured") }` (guard so we never let `NewWriter` panic).
     - `w := kafka.NewWriter(kafka.WriterConfig{ Brokers: brokers })` — **leave `Topic` empty** (topic is set per-message to avoid the `chooseTopic` conflict). No SASL/TLS fields.
     - `return &Producer{Topic: topicOrDefault(cfg), client: w}, nil`.
   - Delete the `saslMechanism` helper and all SASL/TLS references.
   - Keep helpers `topicOrDefault` and `parseBrokers` unchanged.
   - Rewrite `PublishStockUpdate(ctx, msg)`:
     - no-op if `p.client == nil || msg == nil`.
     - `if p.Topic == "" { p.Topic = StockUpdateTopic }`.
     - `data, err := proto.Marshal(msg)`; wrap error.
     - `msgOut := kafka.Message{ Topic: p.Topic, Key: []byte(msg.Symbol), Value: data }`.
     - `if err := p.client.WriteMessages(ctx, msgOut); err != nil { return fmt.Errorf("publish to kafka topic %q: %w", p.Topic, err) }`; return nil.
   - `Close()`: unchanged in behavior (`nil` check → `client.Close()`).
   - Update the package/field doc comments to refer to `kafka-go` instead of sarama.
2. **`producer_test.go`:**
   - Replace `import "github.com/IBM/sarama"` with `import "github.com/segmentio/kafka-go"`.
   - Replace `fakeSender` with the kafka-go shape above (`sent []kafka.Message`,
     `WriteMessages(ctx, msgs...)`, `Close`).
   - Rewrite `TestPublish_WithFakeSender` to assert on `kafka.Message` fields:
     `len(fake.sent)==1`; `sent[0].Topic=="t"`; `string(sent[0].Key)=="AC"`;
     proto-unmarshal `sent[0].Value` into `kafkastockv1.Stock` and assert
     `Symbol=="AC"`, `Exchange=="TSX"`.
   - Leave `TestProducer_ZeroValue_NoOp`, `TestProducer_WithTopic_NoOp`,
     `TestNewProducer_EmptyBrokers_NoDial`, `TestProducer_Close_NoClient` **as
     is** — they exercise only the nil-client no-op path and the `client` field,
     which are unchanged.

**Verify:**
- `go build ./...`
- `go vet ./internal/kafka/...`
- `go test ./internal/kafka/... -count=1`

**Done when:** no file in the module imports `github.com/IBM/sarama`
(`rg -n "IBM/sarama" --glob '*.go'` → no hits); `producer.go` has no SASL/TLS
references, no `saslMechanism`, and sets the topic on `kafka.Message` (not on
`WriterConfig`); **all** tests in `./internal/kafka/...` pass including the
rewritten `TestPublish_WithFakeSender`. (`go.mod` still lists sarama as an unused
direct dep — expected; pruned in Step 5.)

---

### Step 3 — Remove SASL/SSL from config

**Files:**
- Modify: `internal/config/config.go` (delete 4 fields, 4 env reads, `getEnvBool`)
- Modify: `internal/config/config_test.go` (drop SASL/SSL assertions; delete `TestLoad_InvalidSSLBool`)

**Do:**
1. **`config.go`:**
   - Delete `KafkaSASLUsername`, `KafkaSASLPassword`, `KafkaSASLMechanism`,
     `KafkaSSLEnabled` fields from `Config`.
   - Delete their four lines from the `Load()` struct literal
     (`KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`, `KAFKA_SASL_MECHANISM`,
     `KAFKA_SSL_ENABLED`).
   - Delete the `getEnvBool` helper (its only caller was `KAFKA_SSL_ENABLED`);
     re-check imports (`strconv` is still used by `getEnvInt`/`getEnvDuration`,
     so it stays).
   - Keep `KafkaBrokers` and `KafkaTopic` and their `Load()` reads exactly as-is.
2. **`config_test.go`:**
   - `TestLoad_KafkaDefaults`: remove the blocks asserting
     `KafkaSASLUsername`, `KafkaSASLPassword`, `KafkaSASLMechanism`,
     `KafkaSSLEnabled`. Keep `KafkaBrokers == ""`, `KafkaTopic == "stock.update.v1"`,
     `FMPAPIKey == ""`.
   - `TestLoad_KafkaCustom`: remove the four `KAFKA_SASL_*`/`KAFKA_SSL_ENABLED`
     env entries and their assertions. Keep `KAFKA_BROKERS`, `KAFKA_TOPIC`,
     `FMP_API_KEY` entries/assertions.
   - Delete the whole `TestLoad_InvalidSSLBool` function.

**Verify:**
- `go build ./...`
- `go vet ./internal/config/...`
- `go test ./internal/config/... -count=1`

**Done when:** `rg -n "KafkaSASL|KafkaSSL|KAFKA_SASL|KAFKA_SSL|getEnvBool" --glob '*.go'`
→ no hits; `internal/config` compiles and **all** its tests pass; the module
still builds; no reference to a removed field anywhere.

---

### Step 4 — Update documentation for the plaintext-only Kafka config

**Files:**
- Modify: `README.md` (Kafka prose lines ~36–41; env table rows lines 76–79)

**Do:**
1. In the "Kafka integration" section, reword the bullet at lines ~38–41 so it
   states: messages go to `KAFKA_TOPIC` (default `stock.update.v1`) with the
   stock symbol as the message key over a **plaintext** connection; brokers are
   plaintext-only and **SASL/TLS are not supported**. Remove the SASL/TLS
   phrasing and the `KAFKA_SASL_*` / `KAFKA_SSL_ENABLED` mentions.
2. In the "Environment variables → Kafka" table, **delete** the four rows for
   `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`, `KAFKA_SASL_MECHANISM`, and
   `KAFKA_SSL_ENABLED`. Keep the `KAFKA_BROKERS` and `KAFKA_TOPIC` rows.
   (Optionally add a short note to the `KAFKA_BROKERS` row: "plaintext only.")
3. (Optional, low-risk) Fix the stale message-type reference at lines ~22 and 29:
   the wire type is `kafkastockv1.Stock{Symbol, Exchange}` (used by the code and
   stocker-store), not `StockUpdate{...Scores}`. Only change if it is clearly
   stale; do not change the import path if uncertain.

**Verify:**
- `rg -n "KAFKA_SASL|KAFKA_SSL|SASL|TLS" README.md` → only any deliberate
  "SASL/TLS are not supported" note remains, no live env-var rows/instructions.
- `go build ./...` (sanity — no code touched this step, must still build)

**Done when:** `README.md` documents only `KAFKA_BROKERS` and `KAFKA_TOPIC` as
Kafka env vars and explicitly says connections are plaintext with SASL/TLS
unsupported; no `KAFKA_SASL_*`/`KAFKA_SSL_ENABLED` env rows remain; module still
builds.

---

### Step 5 — `go mod tidy` and full verification

**Files:** `go.mod`, `go.sum` (tooling-only).

**Do:**
- `go mod tidy` — this removes the now-unused `github.com/IBM/sarama` and the
  sarama-only transitive deps (`eapache/go-resiliency`, `hashicorp/go-uuid`,
  `jcmturner/*`, `rcrowley/go-metrics`, `golang.org/x/crypto`, and any
  testify-only entries), and records the kafka-go requirements
  (`klauspost/compress`, `pierrec/lz4/v4`, `golang.org/x/net`, plus kafka-go's
  SCRAM/text deps if referenced by the go directive).
- Format and vet the whole tree: `gofmt -l ./...` and `go vet ./...`.
- Run the full test suite.

**Verify:**
- `go mod verify`
- `go build ./... && go test ./...`
- `go vet ./...`
- `gofmt -l ./...` (expect empty output)
- `rg -n "IBM/sarama" ./ --glob '*.go'` → no hits; `rg -n "sarama" go.mod go.sum` → no hits
- `rg -n "KAFKA_SASL|KAFKA_SSL" .` → no hits anywhere in the repo (no .md, .container, .yml, Makefile, or .go file references them)

**Done when:** `go.mod` has `github.com/segmentio/kafka-go v0.4.51` as a direct
require and **no** `github.com/IBM/sarama` (direct or indirect); `go.sum` has no
sarama entries; `go build ./... && go test ./...` all green; `go vet ./...`
clean; `gofmt -l ./...` empty; `go mod verify` OK; a repo-wide search shows zero
remaining `sarama` / `KAFKA_SASL` / `KAFKA_SSL` references in code or docs.

---

## Docs / env-var sweep (what was searched and what changes)

Requested to be searched & updated (SASL/SSL env vars, sarama) — results:

| Location | KAFKA_SASL / KAFKA_SSL / sarama | Action |
|---|---|---|
| `internal/kafka/producer.go` | sarama (import, types, config) | Step 2 (rewrite) |
| `internal/kafka/producer_test.go` | sarama (fakeSender, encoders) | Step 2 (rewrite) |
| `internal/config/config.go` | `KAFKA_SASL_*`, `KAFKA_SSL_ENABLED`, `getEnvBool` | Step 3 (remove) |
| `internal/config/config_test.go` | `KAFKA_SASL_*`/`KAFKA_SSL_ENABLED` asserts, `TestLoad_InvalidSSLBool` | Step 3 (remove) |
| `README.md` (prose ~38–41, table 76–79) | `KAFKA_SASL_*`, `KAFKA_SSL_ENABLED`, SASL/TLS text | Step 4 (reword + delete rows) |
| `go.mod` / `go.sum` | `github.com/IBM/sarama` + transitive | Step 1 (add kafka-go), Step 5 (`go mod tidy`) |
| `cmd/server/main.go` | none (only calls the stable API) | none |
| `internal/refresher/refresher.go` | none (comment only) | none |
| `internal/refresher/refresher_test.go` | none (uses `kafka.Producer{}` no-op) | none |
| `design.md` | none | none |
| `deploy/quadlet/stocker-list.container` | none (generic `EnvironmentFile=`) | none |
| `deploy/push.sh` | none | none |
| `Containerfile` | none (has `go mod tidy`) | none |
| `Makefile` | none | none |
| `podman-compose.yml` | `DB_SSLMODE` only (unrelated DB) | none |

## Rollout / compatibility notes

- **Breaking config change:** `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`,
  `KAFKA_SASL_MECHANISM`, `KAFKA_SSL_ENABLED` are removed. Operators who had them
  set in `~/.config/stocker-list/.env.podman` should delete those lines (they are
  now ignored/harmless but should be cleaned up). Only `KAFKA_BROKERS` and
  `KAFKA_TOPIC` remain.
- **Delivery semantics:** sarama sync-producer → kafka-go synchronous writer.
  `NewProducer` creates the writer eagerly but does not dial; dialing and writes
  happen inside `WriteMessages` (per-call), which is an acceptable equivalent of
  the previous sync-producer behavior and keeps the no-op path truly dial-free.
- **Topic handling:** the topic is always carried on `kafka.Message.Topic`
  (never on `WriterConfig.Topic`) to avoid kafka-go's mutual-exclusion check.
  The exported `Producer.Topic` field and `StockUpdateTopic` const are preserved.
- **No network in tests:** every kafka test uses the nil-client or `fakeSender`
  path; `TestNewProducer_EmptyBrokers_NoDial` and the refresher tests never
  construct a real writer, so no broker is dialed in CI.
