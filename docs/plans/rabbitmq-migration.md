# RabbitMQ Migration Plan

**Status:** Ready for implementation
**Branch:** `feat/rabbitmq-replace-kafka`
**Scope:** This repository only (`stocker-list`)
**Goal:** Replace the legacy Kafka publisher with a configurable RabbitMQ
(AMQP 0-9-1) publisher without changing the refresh-loop behavior: one protobuf
`Stock` message per discovered symbol, symbol as the message identifier, publish
failures fatal to the refresh cycle, provider failures non-fatal.

> **Zero-reference rule and this document.** The finished working tree must
> contain no occurrence of the legacy broker's name, including package names,
> env vars, comments, docs, and module files. This plan necessarily names the
> legacy broker and its paths so an implementer can find and remove them.
> Therefore Step 5 deletes this plan file as its final action. If the team wants
> to retain the plan for history, archive it outside the repository; the
> acceptance grep in this repository has no exceptions.

---

## 1. Goal and scope

### In scope

- Replace `internal/kafka` with a new `internal/rabbitmq` package.
- Replace `KAFKA_BROKERS` and `KAFKA_TOPIC` with RabbitMQ configuration.
- Keep publishing one protobuf `Stock` message per discovered symbol on every
  refresh cycle.
- Keep the existing failure semantics:
  - A provider failure is logged and the cycle continues with healthy providers.
  - A publish failure is returned by the refresher and terminates the cycle.
- Rename the protobuf import alias from `kafkastockv1` to `stockstorev1`
  everywhere. The generated Go package is already named `stockstorev1`.
- Remove every legacy dependency, env var, comment, document, deployment
  reference, and module entry from this repository.
- Document the RabbitMQ topology that a consumer must bind to.

### Out of scope

- Migrating or modifying the sibling `stocker-store` consumer. That repository
  is a separate change. This plan documents the exchange, exchange type,
  routing key, and message shape the consumer must use.
- Declaring queues or bindings from the publisher. Consumers own their queues.
- Adding batching, asynchronous confirms, dead-letter queues, retry/backoff
  policy, or a background reconnect loop.
- Custom TLS CA/mTLS configuration. `amqps://` works with the system trust
  store; custom TLS settings are a follow-up if required.
- Changing provider endpoints, refresh scheduling, or the `provider.Provider`
  interface.

---

## 2. Design

### 2.1 Architecture

```
cmd/server
  ├─ config.Load()                         env-var configuration
  ├─ rabbitmq.NewProducer(cfg, log)        no network I/O at construction
  └─ refresher.New(..., producer)
        └─ Run/tick
              ├─ provider.ListSymbols()    TSX + US, provider failures non-fatal
              └─ producer.PublishStockUpdate(ctx, *stockstorev1.Stock)
                    └─ AMQP connection + channel
                          ├─ declare exchange (durable)
                          ├─ publish persistent protobuf message
                          └─ wait for publisher confirm
```

The producer is lazy: `NewProducer` validates configuration but does not dial.
The first `PublishStockUpdate` call establishes the connection, opens a channel,
declares the exchange, and enables publisher confirms. A single mutex serializes
connection lifecycle and publishes, so there is at most one in-flight publish.

### 2.2 Configuration

All values are environment variables. `Config` gains these fields:

| Field | Env var | Type | Default | Required | Description |
|---|---|---|---|---|---|
| `RefreshCheckInterval` | `REFRESH_CHECK_INTERVAL` | Go duration string | `24h` | No | Unchanged. Must parse and be `> 0`. |
| `RabbitMQURL` | `RABBITMQ_URL` | AMQP URI string | `amqp://guest:guest@192.168.1.48:5672/` | No | Broker URI. Supports `amqp://` and `amqps://`. **Unset** uses the default; **explicitly empty** (`RABBITMQ_URL=`) disables publishing. Credentials are embedded in the URI; never log the URI. |
| `RabbitMQExchange` | `RABBITMQ_EXCHANGE` | string | `stock.update` | No | Exchange the publisher declares and publishes to. Empty falls back to the default. |
| `RabbitMQExchangeType` | `RABBITMQ_EXCHANGE_TYPE` | enum string | `topic` | No | One of `direct`, `topic`, `fanout`, `headers`. Trimmed and lowercased; any other value is a startup error. |
| `RabbitMQRoutingKey` | `RABBITMQ_ROUTING_KEY` | string | `stock.update.v1` | No | Routing key used for every message. Empty falls back to the default. |

**Explicit-empty URL semantics.** The existing `getEnv` helper treats an empty
value as “unset” and returns the fallback. That is wrong for `RABBITMQ_URL`
because an operator must be able to disable publishing with `RABBITMQ_URL=`.
Add `func getEnvPreserveEmpty(key, fallback string) string` that uses
`os.LookupEnv` and returns the value even when it is empty; only an absent
variable returns the fallback. Use `getEnv` for the exchange, exchange type, and
routing key.

**Validation.** `config.Load` validates the exchange type after trimming and
lowercasing. An invalid value returns an error such as
`RABBITMQ_EXCHANGE_TYPE must be one of direct, topic, fanout, headers, got "bogus"`.
Validation happens even when `RABBITMQ_URL` is empty so a bad config never sits
dormant.

**Deliberately not configurable.** Exchange durability and publisher confirms
are fixed invariants, not operator knobs:

- Exchange: `durable=true`, `autoDelete=false`, `internal=false`.
- Publisher confirms: enabled with `Channel.Confirm(false)`.
- Message delivery mode: persistent.
- `mandatory` and `immediate`: `false`.

Exposing these invites producer/consumer topology mismatches. If a deployment
needs a different topology, it should use a different exchange name and a
separate reviewed change.

### 2.3 Topology

The publisher declares only the exchange. It never declares queues or bindings.

- Exchange name: `RABBITMQ_EXCHANGE` (default `stock.update`).
- Exchange type: `RABBITMQ_EXCHANGE_TYPE` (default `topic`).
- Routing key: `RABBITMQ_ROUTING_KEY` (default `stock.update.v1`).

Consumers must declare their own durable queue and bind it to the exchange. For
the default topic exchange, an exact binding is:

```
rabbitmqadmin declare queue name=stock-updates durable=true
rabbitmqadmin declare binding source=stock.update destination=stock-updates routing_key=stock.update.v1
```

A consumer that wants future `stock.update.*` versions can bind
`stock.update.#`. With a `direct` exchange the binding key must match exactly;
with `fanout` the routing key is ignored; with `headers` the consumer binds on
message headers instead (for example, binding arguments `x-match=all`,
`symbol=AC`, and `exchange=TSX`). The publisher always sets `symbol` and
`exchange` headers so a headers exchange can route on them.

**Important:** if no queue is bound, the exchange accepts and discards the
message. Publisher confirms acknowledge that the exchange received the message,
not that a consumer received it. Consumers must be deployed with their bindings
before the publisher is switched over.

### 2.4 Message mapping

Each discovered symbol produces exactly one AMQP message:

| AMQP field | Value | Rationale |
|---|---|---|
| Exchange | `RabbitMQExchange` | Configurable topology. |
| Routing key | `RabbitMQRoutingKey` | Replaces the old topic name; default `stock.update.v1`. |
| Body | `proto.Marshal(*stockstorev1.Stock)` | Same protobuf payload as before. |
| `ContentType` | `application/x-protobuf` | Explicit, consumer-friendly schema marker. |
| `ContentEncoding` | empty | Protobuf is binary; no transfer encoding. |
| `DeliveryMode` | `amqp.Persistent` (2) | Survives broker restart when the consumer queue is durable. |
| `MessageId` | `msg.Symbol` | Preserves the old symbol-as-key semantics. The exchange header disambiguates identical tickers on different exchanges. |
| `Type` | `stock.update.v1` (constant `StockUpdateMessageType`) | Identifies the schema/version independently of the routing key. |
| `Timestamp` | `time.Now().UTC()` | Publish time for observability and consumer ordering checks. |
| `Headers` | `symbol`: `msg.Symbol`, `exchange`: `msg.Exchange` | Human- and headers-exchange-friendly metadata. |
| `Priority` | `0` | No priority semantics. |
| `CorrelationId`, `ReplyTo`, `Expiration` | unset | Request/reply and TTL are not part of this flow. |
| `mandatory`, `immediate` | `false` | No unroutable-message return handling; consumers own bindings. |

### 2.5 Publish, reconnect, and close semantics

**No-op contract.** When `RABBITMQ_URL` is empty, `PublishStockUpdate` returns
`nil` without dialing or marshalling, and `Close` returns `nil`. A zero-value
`Producer` behaves the same way. A nil `msg` is also a no-op, preserving the old
producer's behavior.

**Publish sequence.**

1. If disabled or `msg == nil`, return `nil`.
2. Marshal the message. On error, return `marshal stock update: %w`.
3. If `ctx.Err() != nil`, return the context error before any network I/O.
4. Lock the producer mutex.
5. If the producer has been closed, return `rabbitmq producer is closed`.
6. Check `ctx.Err()` again so a context canceled while waiting for the mutex does
   not trigger a dial.
7. Ensure a live connection and channel:
   - Reuse the current channel when `ch != nil && !ch.IsClosed()`.
   - Otherwise close any stale channel/connection, dial the configured URI,
     open a channel, declare the exchange, and enable confirms.
   - Wrap failures as `connect to rabbitmq: %w`, `open rabbitmq channel: %w`,
     `declare rabbitmq exchange %q: %w`, or `enable publisher confirms: %w`.
8. Publish with
   `PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, false, false, publishing)`.
   This API is preferred over `PublishWithContext` plus a separate
   `NotifyPublish` loop because it returns the confirmation for this specific
   publish, avoiding delivery-tag bookkeeping and races.
9. If the returned confirmation is nil, return
   `publisher confirms not enabled` instead of dereferencing it.
10. Wait for the confirm with `DeferredConfirmation.WaitContext(ctx)`.
    - A wait error or `acked == false` is a publish failure.
11. On success, unlock and return `nil`.

**Reconnect-and-retry-once.** A failure in step 8, step 9, or step 10 triggers
one retry:

- If `ctx.Err() != nil`, do not retry; return the context error.
- Otherwise close and clear the current channel/connection, log a warning with
  the symbol, exchange, and first error, then call the same connect-and-publish
  path once more.
- If the retry succeeds, return `nil`.
- If the retry fails, return `publish to rabbitmq exchange %q: %w` wrapping the
  second error. The first error is already logged; it is not joined into the
  returned error to keep the error chain focused on the final failure.
- If reconnecting itself fails, return
  `reconnect to rabbitmq after publish failure: %w`.

The initial connection attempt is not retried. If the broker is unreachable on
the first publish, the dial error is returned without a retry; the refresher
treats it as a fatal cycle error. This matches the old lazy-producer behavior
and avoids doubling the dial timeout. A process supervisor restarts the service.

**Close.** `Close` is idempotent and safe for a disabled or zero-value producer:

- Lock the mutex.
- If already closed, return `nil`.
- Mark closed.
- Close the channel, then the connection. Collect non-nil errors with
  `errors.Join` and return them.
- After `Close`, `PublishStockUpdate` on an enabled producer returns
  `rabbitmq producer is closed`. A disabled producer remains a no-op.

**Logging.** Log an info line when a connection is established, including the
exchange, exchange type, and routing key, but never the URL or credentials. Log
a warning when the retry path runs. Log close errors at warning level in
`cmd/server/main.go`. Pass a non-nil `*slog.Logger` from `main`; `NewProducer`
uses a discard logger when given `nil` so tests stay quiet.

---

## 3. File-by-file migration map

| Path | Action | Responsibility |
|---|---|---|
| `cmd/server/main.go` | Modify | Import `internal/rabbitmq`; construct with `rabbitmq.NewProducer(cfg, log)`; wrap the error as `create rabbitmq producer`; close it on shutdown and log `close rabbitmq producer`; update the package comment. |
| `internal/config/config.go` | Modify | Remove `KafkaBrokers` and `KafkaTopic`; add the four `RabbitMQ*` fields; add defaults, `getEnvPreserveEmpty`, and exchange-type validation. |
| `internal/config/config_test.go` | Modify | Remove legacy config tests; add RabbitMQ default/override/explicit-empty/invalid-type tests; add an env-unset helper for hermetic defaults. |
| `internal/kafka/producer.go` | Delete | Replaced by `internal/rabbitmq/producer.go`. |
| `internal/kafka/producer_test.go` | Delete | Replaced by `internal/rabbitmq/producer_test.go`. |
| `internal/rabbitmq/producer.go` | Create | Producer, test seams, AMQP adapters, publish/confirm/retry/close logic. |
| `internal/rabbitmq/producer_test.go` | Create | Network-free unit tests with fake connection/channel/confirmation. |
| `internal/rabbitmq/integration_test.go` | Create | Gated live-broker round-trip test using `RABBITMQ_TEST_URL`. |
| `internal/refresher/refresher.go` | Modify | Rename the protobuf alias to `stockstorev1`; update the `Producer` interface signature; replace legacy comments with RabbitMQ/publish wording; rename “Kafka-only mode” to “publish-only mode”. |
| `internal/refresher/refresher_test.go` | Modify | Rename the alias; use `&rabbitmq.Producer{}` for the no-op case; update comments; change the fake error to `rabbitmq unavailable`. |
| `internal/provider/provider.go` | Modify | Change the comment “consumed directly by the Kafka producer” to “consumed directly by the RabbitMQ producer”. |
| `README.md` | Modify | Rewrite the title/description, architecture, broker integration, env table, key semantics, and run instructions for RabbitMQ. Include the consumer binding example. |
| `design.md` | Modify | Replace the Kafka section with a RabbitMQ section; keep the protobuf message description. |
| `.env.example` | Modify | Replace the Kafka block with `RABBITMQ_URL`, `RABBITMQ_EXCHANGE`, `RABBITMQ_EXCHANGE_TYPE`, and `RABBITMQ_ROUTING_KEY`; document explicit-empty disabling. |
| `.env.podman` | Modify | Same RabbitMQ block, suitable for the quadlet deployment. |
| `deploy/quadlet/stocker-list.container` | Modify | Remove `After=kafka-broker-1.container`. Keep `After=network-online.target`; the broker is remote and the producer reconnects lazily. If a local broker is ever added, use `After=rabbitmq.service` (or the container unit) and `Wants=`. |
| `docs/kafka-go-producer-rewrite.md` | Delete | Obsolete historical rewrite plan. |
| `docs/plans/symbol-discovery-html-response.md` | Modify | Scrub all 16 legacy references; replace with RabbitMQ/publish wording and current identifiers; add a one-line note that the document was updated after the broker migration and that the incident analysis is unchanged. |
| `docs/plans/rabbitmq-migration.md` | Delete in Step 5 | This plan names the legacy broker throughout; delete it after implementation to satisfy the zero-reference rule. |
| `go.mod`, `go.sum` | Modify | Add `github.com/rabbitmq/amqp091-go v1.16.0`; `go mod tidy` removes `github.com/segmentio/kafka-go` and now-unused indirects. |
| `podman-compose.yml` | Verify | No legacy references today; update only if a reference is found. |
| `Makefile` | Verify | No legacy references today; update only if a reference is found. |
| `Containerfile` | Verify | No legacy references today; update only if a reference is found. |
| `deploy/push.sh` | Verify | No legacy references today; update only if a reference is found. |
| `.gitignore` | Verify | No legacy references today; no change expected. |

---

## 4. Implementation steps

Each step is sequential and leaves the module buildable and testable. Later
steps may depend on earlier ones; no step depends on a later step.

### Step 1: Add RabbitMQ configuration (keep legacy fields temporarily)

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`

**Do:**

1. Add the four fields from the configuration table to `Config`:
   `RabbitMQURL`, `RabbitMQExchange`, `RabbitMQExchangeType`,
   `RabbitMQRoutingKey`.
2. Add defaults in `Load`:
   - `RabbitMQURL`: `getEnvPreserveEmpty("RABBITMQ_URL", "amqp://guest:guest@192.168.1.48:5672/")`
   - `RabbitMQExchange`: `getEnv("RABBITMQ_EXCHANGE", "stock.update")`
   - `RabbitMQExchangeType`: `getEnv("RABBITMQ_EXCHANGE_TYPE", "topic")`
   - `RabbitMQRoutingKey`: `getEnv("RABBITMQ_ROUTING_KEY", "stock.update.v1")`
3. Add `getEnvPreserveEmpty` as described in §2.2. Keep the existing `getEnv`
   for the other variables.
4. Normalize the exchange type with `strings.TrimSpace` and `strings.ToLower`,
   then validate it against `direct`, `topic`, `fanout`, and `headers`. Return a
   descriptive error for any other value. Store the normalized value.
5. Keep `KafkaBrokers`, `KafkaTopic`, and their env reads untouched so the
   current code still compiles. They are removed in Step 3.
6. Update `internal/config/config_test.go`:
   - Leave the two legacy config tests untouched in this step; they are removed
     in Step 3.
   - Add a hermetic helper that records and unsets `RABBITMQ_*` variables with
     `t.Cleanup`, because `t.Setenv(key, "")` is not the same as “unset” for the
     URL field.
   - Add tests for: all RabbitMQ defaults when unset; all overrides; explicit
     empty `RABBITMQ_URL` producing `""`; absent URL producing the default;
     invalid exchange type returning an error; trimming/lowercasing a valid
     exchange type.
   - Keep the existing refresh-interval tests unchanged.

**Done when:**

- `go test ./internal/config -count=1` passes.
- `go build ./...` passes.
- The new tests prove that unset and explicitly empty `RABBITMQ_URL` are
  different, and that an invalid exchange type is rejected.
- No runtime code has been switched to RabbitMQ yet.

### Step 2: Implement `internal/rabbitmq` with unit and integration tests

**Files:**
- Create: `internal/rabbitmq/producer.go`
- Create: `internal/rabbitmq/producer_test.go`
- Create: `internal/rabbitmq/integration_test.go`
- Modify: `go.mod`, `go.sum` (via `go get`/`go mod tidy`)

**Do:**

1. Add `github.com/rabbitmq/amqp091-go v1.16.0` as a direct dependency.
2. Implement `Producer` with these exported members:
   - `const StockUpdateMessageType = "stock.update.v1"`
   - `func NewProducer(cfg *config.Config, log *slog.Logger) (*Producer, error)`
   - `func (p *Producer) PublishStockUpdate(ctx context.Context, msg *stockstorev1.Stock) error`
   - `func (p *Producer) Close() error`
3. `NewProducer` behavior:
   - If `cfg.RabbitMQURL == ""`, return a no-op producer with the exchange,
     type, and routing key defaults applied; do not dial.
   - Otherwise validate the URI with `amqp.ParseURI` (no network I/O) and
     return `parse RABBITMQ_URL: %w` on failure.
   - Validate/normalize the exchange type again so direct construction in tests
     is safe.
   - Apply defaults for empty exchange/type/routing key.
   - Use a discard `slog.Logger` when `log == nil`.
   - Install the default dial function that calls `amqp.Dial` and wraps the
     result in the connection adapter.
4. Define unexported test seams:
   - `connection` interface: `Channel() (channel, error)` and `Close() error`.
   - `channel` interface: `ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error`,
     `Confirm(noWait bool) error`,
     `PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (confirmation, error)`,
     `IsClosed() bool`, and `Close() error`.
   - `confirmation` interface: `WaitContext(ctx context.Context) (bool, error)`.
   - `dial func(string) (connection, error)` field on `Producer`.
   - Adapters that wrap `*amqp.Connection` and `*amqp.Channel` so the concrete
     library types satisfy these interfaces. The channel adapter returns the
     concrete `*amqp.DeferredConfirmation` as a `confirmation`.
5. Implement publish, reconnect-and-retry-once, and close exactly as specified
   in §2.5. Use `sync.Mutex`, `errors.Join`, `time.Now().UTC()`, and
   `amqp.Persistent`.
6. Unit tests (network-free, package `rabbitmq`):
   - Disabled producer and zero-value producer: publish returns `nil`; `Close`
     returns `nil`; dial is never called.
   - Invalid URL and invalid exchange type are rejected by `NewProducer`.
   - Exchange declaration uses the configured name/type with durable true.
   - Confirms are enabled once per connection.
   - Published message has the expected exchange, routing key, body (decode with
     `proto.Unmarshal`), content type, persistent delivery mode, message ID,
     type, timestamp, and symbol/exchange headers.
   - Publish error triggers exactly one reconnect and one republish; a successful
     retry returns `nil`.
   - Two consecutive failures return an error wrapping the second failure and
     do not retry a third time.
   - A non-acked confirm is treated as a failure and triggers the retry.
   - A canceled context returns the context error without a retry and without a
     dial.
   - A closed channel causes a reconnect before the next publish.
   - `Close` is idempotent; closing twice returns `nil` and closes resources
     once.
   - Publishing after `Close` on an enabled producer returns
     `rabbitmq producer is closed`.
7. Integration test (`internal/rabbitmq/integration_test.go`, package
   `rabbitmq_test`):
   - Skip immediately when `RABBITMQ_TEST_URL` is unset.
   - Dial the broker with `amqp.Dial`, open a channel, and declare a unique
     durable topic exchange named with a timestamp/PID suffix.
   - Declare an exclusive, auto-delete queue and bind it to that exchange with a
     unique routing key.
   - Construct the producer through `NewProducer` with the same URL/exchange/type
     and routing key, publish `Stock{Symbol: "AC", Exchange: "TSX"}`, and consume
     the message with a timeout.
   - Assert protobuf round-trip and the metadata from §2.4.
   - Clean up the exchange, queue, channel, connection, and producer with
     `defer`.
8. Run `go mod tidy`. At this step `internal/kafka` still imports the legacy
   library, so it remains in `go.mod`; the new dependency must be added without
   breaking the existing package.

**Done when:**

- `go test ./internal/rabbitmq -count=1` passes with the integration test
  skipped (no `RABBITMQ_TEST_URL`).
- `go test ./... -count=1`, `go build ./...`, and `go vet ./...` pass.
- Unit tests do not open a network connection.
- If a local broker is available, the integration test passes with
  `RABBITMQ_TEST_URL` set.
- `go.mod` lists `github.com/rabbitmq/amqp091-go v1.16.0` directly.

### Step 3: Cut over runtime and remove the legacy package/config

**Files:**
- Modify: `cmd/server/main.go`
- Modify: `internal/refresher/refresher.go`
- Modify: `internal/refresher/refresher_test.go`
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`
- Modify: `internal/provider/provider.go`
- Delete: `internal/kafka/producer.go`
- Delete: `internal/kafka/producer_test.go`
- Modify: `go.mod`, `go.sum` (via `go mod tidy`)

**Do:**

1. In `cmd/server/main.go`:
   - Replace the `internal/kafka` import with `internal/rabbitmq`.
   - Construct `producer, err := rabbitmq.NewProducer(cfg, log)`.
   - Wrap errors as `create rabbitmq producer: %w`.
   - Pass the producer to `refresher.New`.
   - On shutdown, log `close rabbitmq producer` if `producer.Close()` fails.
   - Update the package comment to say “publishing to RabbitMQ”.
2. In `internal/refresher/refresher.go`:
   - Import `stockstorev1 "git.wheeli.ca/brian/stocker-store/proto/v1"`.
   - Change the `Producer` interface to
     `PublishStockUpdate(ctx context.Context, msg *stockstorev1.Stock) error`.
   - Use `stockstorev1.Stock` when constructing messages.
   - Replace comments that mention the legacy broker with RabbitMQ/publish
     wording.
   - Rename “Kafka-only mode” to “publish-only mode” and explain that the
     refresher publishes upstream instead of persisting locally.
3. In `internal/refresher/refresher_test.go`:
   - Rename the protobuf alias to `stockstorev1`.
   - Replace `&kafka.Producer{}` with `&rabbitmq.Producer{}` in the no-op test.
   - Update the fake producer signature and comments.
   - Change the fake publish error from `kafka unavailable` to
     `rabbitmq unavailable` and update the error-string assertions.
   - Remove the `internal/kafka` import.
4. In `internal/config/config.go`:
   - Remove `KafkaBrokers`, `KafkaTopic`, and their env reads.
   - Keep the RabbitMQ fields and validation from Step 1.
5. In `internal/config/config_test.go`:
   - Delete the two legacy config tests.
   - Keep the RabbitMQ tests and refresh-interval tests.
6. In `internal/provider/provider.go`, update the comment to reference the
   RabbitMQ producer.
7. Delete the `internal/kafka` directory.
8. Run `go mod tidy`. It must remove `github.com/segmentio/kafka-go` and its
   now-unused indirect dependencies from `go.mod`, and remove their entries from
   `go.sum`. The pinned `stocker-store` module still lists the legacy library in
   its own `go.mod`, but the imported protobuf package does not use it, so this
   module does not need it.

**Done when:**

- `go build ./...`, `go test ./... -count=1`, and `go vet ./...` pass.
- `internal/kafka` no longer exists.
- `grep -rn "KAFKA_\|segmentio/kafka-go\|internal/kafka" --include='*.go' .`
  returns nothing.
- `go.mod` and `go.sum` contain no `segmentio/kafka-go` or other legacy broker
  dependency entries.
- Documentation still contains legacy references; Step 4 handles them.

### Step 4: Documentation and deployment cleanup

**Files:**
- Modify: `README.md`
- Modify: `design.md`
- Modify: `.env.example`
- Modify: `.env.podman`
- Modify: `deploy/quadlet/stocker-list.container`
- Delete: `docs/kafka-go-producer-rewrite.md`
- Modify: `docs/plans/symbol-discovery-html-response.md`
- Verify: `podman-compose.yml`, `Makefile`, `Containerfile`, `deploy/push.sh`,
  `.gitignore`

**Do:**

1. `README.md`:
   - Rewrite the title/description to say RabbitMQ instead of the legacy broker.
   - Update the architecture block to list `internal/rabbitmq` and remove
     `internal/kafka`.
   - Replace the “Kafka integration” section with “RabbitMQ integration”:
     protobuf message type `stockstorev1.Stock`, import path
     `git.wheeli.ca/brian/stocker-store/proto/v1`, exchange/routing-key
     semantics, persistent delivery, publisher confirms, and lazy connection.
   - Add a short “Topology” subsection with the consumer binding example from
     §2.3.
   - Replace the env table with the RabbitMQ table from §2.2 and keep the core
     `REFRESH_CHECK_INTERVAL` row.
   - Update “Key semantics” to explain unset vs explicitly empty
     `RABBITMQ_URL`.
   - Update all provider text that says symbols are re-published to the legacy
     broker; say “published to RabbitMQ”.
   - Update local/Podman/Quadlet instructions: set `RABBITMQ_*` in the env file,
     not `KAFKA_*`.
2. `design.md`: replace the legacy broker section with a RabbitMQ section that
   names the exchange, routing key, protobuf message, and persistent/confirm
   behavior. Keep the existing product description and message-shape note.
3. `.env.example` and `.env.podman`: replace the legacy block with:
   - `RABBITMQ_URL=amqp://guest:guest@192.168.1.48:5672/` (comment: empty
     disables publishing; use credentials for the target broker)
   - `RABBITMQ_EXCHANGE=stock.update`
   - `RABBITMQ_EXCHANGE_TYPE=topic`
   - `RABBITMQ_ROUTING_KEY=stock.update.v1`
   - Keep the core block. Leave the unrelated `GRPC_PORT` line in
     `.env.example` untouched; it is outside this migration.
4. `deploy/quadlet/stocker-list.container`: remove the `After=` line that names
   the local legacy broker container. Keep `After=network-online.target`.
   Rationale: the broker is remote and the producer connects lazily; there is no
   local unit to order against. Document in the unit or README that a future
   local RabbitMQ container would need `After=rabbitmq.service` (or the
   container unit) and `Wants=`.
5. Delete `docs/kafka-go-producer-rewrite.md`.
6. Scrub `docs/plans/symbol-discovery-html-response.md`:
   - Replace all legacy broker references with “publish”, “publisher”, or
     “RabbitMQ” as appropriate.
   - Update the affected code snippets from `kafkastockv1`/`kafka.Producer` to
     `stockstorev1`/`rabbitmq.Producer`, or describe the snippet as current
     RabbitMQ code.
   - Update the env-var mentions to `RABBITMQ_*`.
   - Update the document-inventory rows that describe `design.md`, `README.md`,
     and the deleted rewrite plan. Remove the deleted plan row or replace it
     with a note that it was removed.
   - Keep the incident analysis, evidence, and provider behavior intact.
   - Add a one-line note at the top: “Updated after the RabbitMQ migration; the
     incident analysis and provider behavior are unchanged.”
7. Run
   `grep -ri kafka . --exclude-dir=.git --exclude=rabbitmq-migration.md` and fix
   every hit outside this plan file. Check `podman-compose.yml`, `Makefile`,
   `Containerfile`, `deploy/push.sh`, and `.gitignore` even though they are
   expected to be clean.
8. Verify Markdown internal references: every relative link in `README.md` and
   the incident plan resolves to an existing file.

**Done when:**

- `grep -ri kafka . --exclude-dir=.git --exclude=rabbitmq-migration.md`
  returns nothing.
- `go test ./... -count=1` still passes.
- `README.md` documents all four RabbitMQ env vars with correct defaults and
  the explicit-empty behavior.
- The consumer binding example is present.
- No broken relative Markdown links remain.

### Step 5: Final verification and remove this plan

**Files:**
- Delete: `docs/plans/rabbitmq-migration.md`

**Do:**

1. Run the full verification suite from §7.
2. Run the gated integration test if a broker is available:
   `RABBITMQ_TEST_URL=amqp://guest:guest@localhost:5672/ go test ./internal/rabbitmq -run Integration -v -count=1`.
3. Run `grep -ri kafka . --exclude-dir=.git` and confirm it returns no output.
   If it still finds this plan, delete the plan and re-run.
4. Delete `docs/plans/rabbitmq-migration.md` as the final action. This plan
   names the legacy broker and its paths throughout, so it cannot remain in a
   zero-reference repository. If the team wants to keep it, move it to an
   external archive before deleting it from the repository.
5. Commit the implementation and push the branch.

**Done when:**

- `gofmt -l .` prints nothing.
- `go build ./...`, `go test ./... -count=1`, and `go vet ./...` pass.
- `go mod tidy` produces no diff.
- `grep -ri kafka . --exclude-dir=.git` returns no output.
- This plan file is gone.
- The branch is committed and pushed; `git status --short` is clean.

---

## 5. Test plan

### 5.1 Unit tests

All unit tests run without a broker. The producer tests inject a fake `dial`
function and fake connection/channel/confirmation implementations.

| Package / test area | What it proves |
|---|---|
| `internal/config` defaults | Unset `RABBITMQ_*` yields the documented defaults, including the remote URL. |
| `internal/config` overrides | Every RabbitMQ env var is honored. |
| `internal/config` explicit empty URL | `RABBITMQ_URL=` yields `""` (disabled), distinct from an absent variable. |
| `internal/config` invalid exchange type | `RABBITMQ_EXCHANGE_TYPE=bogus` returns an error naming the variable and valid values. |
| `internal/config` normalization | Whitespace/case around a valid exchange type is normalized. |
| `internal/rabbitmq` no-op | Disabled and zero-value producers publish and close without dialing. |
| `internal/rabbitmq` construction | Invalid URL and invalid exchange type are rejected before any network I/O. |
| `internal/rabbitmq` topology | Exchange name/type and durable flag are correct; confirms are enabled. |
| `internal/rabbitmq` message mapping | Body round-trips through protobuf; content type, delivery mode, message ID, type, timestamp, and headers match §2.4. |
| `internal/rabbitmq` publish failure | One reconnect and one republish; success returns `nil`. |
| `internal/rabbitmq` retry exhaustion | Two failures return the second error; no third attempt. |
| `internal/rabbitmq` nack | A non-acked confirm triggers the retry path. |
| `internal/rabbitmq` context cancellation | A canceled context returns the context error without dialing or retrying. |
| `internal/rabbitmq` closed channel | A closed channel is replaced before the next publish. |
| `internal/rabbitmq` close | `Close` is idempotent; double close returns `nil`; publish after close fails. |
| `internal/refresher` | Provider failures remain non-fatal; publish failures remain fatal; the fake error string says `rabbitmq unavailable`. |

### 5.2 Integration test (gated)

`internal/rabbitmq/integration_test.go` runs only when `RABBITMQ_TEST_URL` is
set. It must:

1. Dial the broker directly with `amqp.Dial`.
2. Declare a unique durable topic exchange and a unique exclusive/auto-delete
   queue bound to it.
3. Construct the producer through the public `NewProducer`.
4. Publish one `Stock{Symbol: "AC", Exchange: "TSX"}` message.
5. Consume it within a timeout and assert the protobuf body and metadata.
6. Clean up the exchange, queue, channel, connection, and producer.

Run it against a local broker:

```
podman run -d --rm --name rabbitmq-test -p 5672:5672 rabbitmq:4
RABBITMQ_TEST_URL=amqp://guest:guest@localhost:5672/ go test ./internal/rabbitmq -run Integration -v -count=1
```

The test must skip cleanly, not fail, when `RABBITMQ_TEST_URL` is unset.

### 5.3 Manual verification

Use a broker reachable from the service. The discovered broker is RabbitMQ 4.4.0
at `192.168.1.48:5672`; the default URL points there. Remote `guest` login is
commonly refused, so use the credentials supplied by the broker owner or a local
container.

1. Start a local broker if needed:
   `podman run -d --rm --name rabbitmq-test -p 5672:5672 rabbitmq:4`.
2. Create a queue and binding before the refresh publishes, because the
   publisher does not declare queues:
   `rabbitmqadmin declare queue name=stock-updates durable=true` and
   `rabbitmqadmin declare binding source=stock.update destination=stock-updates routing_key=stock.update.v1`.
3. Run with a short refresh interval and the target URL:
   `REFRESH_CHECK_INTERVAL=1m RABBITMQ_URL=amqp://guest:guest@localhost:5672/ go run ./cmd/server`.
4. Confirm logs contain `refresh cycle starting`, a connection line with the
   exchange/type/routing key, and `refresh cycle complete`.
5. Confirm messages appear in `stock-updates` with protobuf bodies and the
   metadata from §2.4.
6. Stop the broker and run again. Confirm the first publish fails after the
   dial attempt, the refresher returns an error, and the process exits. This is
   the existing fatal-publish behavior.
7. Run with `RABBITMQ_URL=` and confirm the process completes refresh cycles
   without dialing or publishing.

For the remote broker at `192.168.1.48`, use port `5672` (plaintext; `5671` was
closed during discovery). The management API on `15672` may be locked. If
`guest` is refused, set `RABBITMQ_URL` with the correct user, password, and
vhost; URL-encode special characters.

---

## 6. Risks, decisions, and open questions

### Decisions

- **Package shape:** `internal/rabbitmq` replaces `internal/kafka`; the exported
  API is `NewProducer`, `PublishStockUpdate`, and `Close`. The refresher keeps
  its narrow `Producer` interface.
- **Lazy connection:** `NewProducer` validates but does not dial. Startup is not
  brittle when the broker is down, matching the old lazy behavior.
- **One connection/channel + mutex:** simple ordering and lifecycle management.
  Per-message synchronous confirms are slower than batching but preserve the
  old “publish failure is fatal” semantics.
- **Retry once:** a publish/confirm failure tears down the connection, reconnects,
  and republishes once. Initial dial failures are not retried.
- **No queue declaration:** consumers own queues and bindings. The publisher
  declares only the exchange.
- **Fixed reliability settings:** durable exchange, publisher confirms, and
  persistent messages are invariants, not env vars.
- **Invalid exchange type is fatal:** no silent fallback to `topic`.
- **Explicit empty URL disables:** requires `getEnvPreserveEmpty`, because the
  existing `getEnv` cannot distinguish empty from unset.
- **This plan is deleted in Step 5:** it is the only intentionally retained
  legacy reference until the final cleanup.

### Risks

- **Remote `guest` restriction:** the default URL uses `guest:guest`, but
  RabbitMQ normally refuses remote `guest` logins. The default is for
  defaults/testing per the requirement; production must override it with a
  dedicated user and vhost. Do not log the URL.
- **Consumer migration is out of scope:** if the `stocker-store` consumer still
  reads from the legacy broker, messages will not reach it. Coordinate the
  cutover and the consumer's queue/binding deployment.
- **Unbound exchange drops messages:** confirms acknowledge exchange receipt,
  not consumer delivery. A missing binding is silent data loss from the
  consumer's perspective.
- **Exchange mismatch:** if an exchange with the configured name already exists
  with a different type or durability, `ExchangeDeclare` fails with a
  precondition error and publishing fails. Operators must pre-create matching
  topology or use a new exchange name.
- **Permissions:** the broker user needs configure permission on the exchange
  for `ExchangeDeclare`. A read/write-only user will fail at first publish. If
  the deployment cannot grant configure permission, pre-create the exchange and
  add a passive-declare mode in a follow-up.
- **Publish failure exits the process:** the refresher treats a publish error as
  fatal, so a sustained broker outage causes a restart loop under systemd
  (`Restart=always`). This is intentional parity with the old behavior. A
  future change could make publish failures retryable/non-fatal.
- **Synchronous confirms:** a large symbol list incurs one network round trip
  per message. At a 24-hour cadence this is acceptable; batching is a follow-up
  if needed.
- **Dial is not context-aware:** `amqp.Dial` uses the library's default 30-second
  connection/handshake timeout and does not accept a context. A hung broker can
  delay the first publish by up to that timeout before the fatal cycle error is
  returned.
- **TLS scope:** `amqps://` uses the system trust store. Custom CA, client
  certificates, and hostname overrides are not configurable yet.
- **MessageId collision:** `MessageId` is the symbol only, matching the old key.
  Identical tickers on different exchanges share a MessageId; the `exchange`
  header disambiguates. If consumers require globally unique IDs, use
  `symbol:exchange` in a future change.
- **Plan deletion:** retaining this plan in the repository would break the
  zero-reference acceptance check. Archive it externally if it must be kept.

### Open questions

- Which consumer queue name and binding key will `stocker-store` use? The
  default documented here is `stock-updates` bound with `stock.update.v1`; the
  consumer owner must confirm.
- What are the production RabbitMQ URL, vhost, user, and permissions? The
  default remote URL is a development/test convenience.
- Is TLS required in production? If so, is the system trust store sufficient,
  or is a custom CA needed?
- Should the exchange be pre-created by operations (with configure permission
  withheld from the publisher), or should the publisher continue to declare it?
- Is a future consumer expected to bind `stock.update.#` for forward
  compatibility, or exact `stock.update.v1`?
- Should publish failures eventually become retryable/non-fatal so the service
  survives a broker outage without a restart loop?

---

## 7. Final verification checklist

Run from the repository root after Step 5, with this plan deleted:

- [ ] `gofmt -l .` prints nothing.
- [ ] `go build ./...` succeeds.
- [ ] `go test ./... -count=1` passes.
- [ ] `go vet ./...` passes.
- [ ] `go test -race ./... -count=1` passes (recommended; catches producer mutex
      mistakes).
- [ ] `go mod tidy` produces no diff.
- [ ] `go.mod` and `go.sum` contain no `segmentio/kafka-go` or other legacy
      broker dependency entries.
- [ ] Stale `bin/` artifacts are removed or rebuilt, so the grep cannot scan an
      old binary that still contains legacy strings.
- [ ] `grep -ri kafka . --exclude-dir=.git` returns no output.
- [ ] `grep -rn "KAFKA_\|segmentio/kafka-go\|internal/kafka" . --exclude-dir=.git`
      returns no output.
- [ ] `internal/kafka/` does not exist.
- [ ] `docs/kafka-go-producer-rewrite.md` does not exist.
- [ ] `docs/plans/rabbitmq-migration.md` does not exist (deleted in Step 5).
- [ ] `README.md` documents `RABBITMQ_URL`, `RABBITMQ_EXCHANGE`,
      `RABBITMQ_EXCHANGE_TYPE`, and `RABBITMQ_ROUTING_KEY` with the defaults in
      §2.2.
- [ ] The consumer binding example is present in `README.md`.
- [ ] `deploy/quadlet/stocker-list.container` has no legacy broker `After=` line.
- [ ] The gated integration test was run against a broker, or explicitly skipped
      because no broker/credentials were available.
- [ ] `git status --short` is clean after the implementation commit and push.
