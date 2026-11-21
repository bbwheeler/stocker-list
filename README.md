# stocker-list

A Go service that pulls stock symbols from public providers and publishes them as RabbitMQ
`stockstorev1.Stock` protobuf messages. It performs exactly one full refresh cycle per
invocation and exits; scheduling is external (a Kubernetes CronJob).

## Architecture

```
cmd/server         wires everything together and runs exactly one cycle
internal/config    env-var configuration (RabbitMQ only)
internal/provider  stock symbol provider clients (TSX, US)
internal/refresher one-shot cycle: fetch all symbols, publish, return
internal/rabbitmq  RabbitMQ producer (no-op when publishing is disabled)
```

## RabbitMQ integration

The service publishes `Stock` messages using protobuf serialization from
[stocker-store](https://git.wheeli.ca/brian/stocker-store).

- **Message type:** `stockstorev1.Stock{Symbol, Exchange}`
- **Package:** `stockstorev1`
- **Import path:** `git.wheeli.ca/brian/stocker-store/proto/v1`

```go
import stockstorev1 "git.wheeli.ca/brian/stocker-store/proto/v1"

msg := &stockstorev1.Stock{
    Symbol:   "AAPL",
    Exchange: "NASDAQ",
}
```

- **Exchange/routing key:** the producer declares the durable exchange named by
  `RABBITMQ_EXCHANGE` (default `stock.update`) of type `RABBITMQ_EXCHANGE_TYPE` (default
  `topic`) and publishes every message with the routing key `RABBITMQ_ROUTING_KEY`
  (default `stock.update.v1`). The stock symbol rides as the `MessageId` (plus
  `symbol`/`exchange` headers); the exchange is the only thing the publisher declares —
  consumers own their queues, bindings, and routing on their side.
- **Persistent delivery:** every message is published with persistent delivery mode, so it
  survives a broker restart when the consumer's queue is durable.
- **Publisher confirms:** confirms are enabled and every publish waits for the broker's
  confirmation. A failed or non-acked publish tears down the connection, reconnects, and
  republishes once; a second failure is fatal to the run. An unreachable broker on
  the very first publish is not retried.
- **Lazy connection:** `NewProducer` validates configuration but does not dial. The
  connection, channel, exchange declaration, and confirms happen at the first publish.
- Publishing is **disabled** (a graceful no-op; no broker is ever dialed) when
  `RABBITMQ_URL` is explicitly empty.

### Topology

The publisher declares only the exchange; it never declares queues or bindings. Consumers
must declare their own durable queue and bind it to the exchange. For the default topic
exchange, an exact binding is:

```
rabbitmqadmin declare queue name=stock-updates durable=true
rabbitmqadmin declare binding source=stock.update destination=stock-updates routing_key=stock.update.v1
```

A consumer wanting future versions can bind `stock.update.#`; on a `direct` exchange the
binding key must match exactly; on a `fanout` exchange the routing key is ignored; on a
`headers` exchange the consumer binds on the `symbol`/`exchange` headers the publisher sets.
Note that if no queue is bound, the exchange accepts and discards the message — publisher
confirms acknowledge exchange receipt, not consumer delivery.

## Providers

### TSX (Toronto Stock Exchange)

The service uses the **official TMX company directory** — a free, public JSON API provided
by the Toronto Stock Exchange itself. No API key is required; the TSX provider is always
active.

The endpoint `https://www.tsx.com/json/company-directory/search/tsx/*` returns all
TSX-listed companies. The service holds no symbol list of its own between cycles: each
cycle the provider's full current TSX list is fetched fresh and re-published to RabbitMQ, so
what is published always reflects the provider's live listings.

### US (NASDAQ screener)

The US provider uses the **public NASDAQ screener API** — `api.nasdaq.com/api/screener/stocks`
(the endpoint that powers the nasdaq.com web app). It returns US common stocks (NYSE,
NASDAQ, NYSE American) as paginated JSON and **requires no API key**. The US provider is
always active, alongside TSX.

Note: the API does not expose a per-row exchange or currency field, so the provider runs the
screener once per listing exchange and tags each symbol with that exchange (`NASDAQ`, `NYSE`,
or `AMEX`); `Currency` is `"USD"` for this all-US universe.

### Response validation and graceful degradation

Both providers validate the HTTP response before decoding it as JSON. A non-200 status, or a
non-JSON `Content-Type` when one is present (for example, an HTML maintenance or error page
served with HTTP 200), is reported as a provider failure instead of being passed to the JSON
decoder. The error contains the URL, the HTTP status, the `Content-Type`, and a body preview
of up to 512 bytes (whitespace-collapsed), so the real cause is visible in the logs. Requests
send a browser `User-Agent` and `Accept: application/json`, read at most 20 MiB per response,
and fail with `stopped after 5 redirects` when a redirect chain reaches the limit.

A provider failure does not abort the run:

- The failure is logged at error level as `provider failed to list symbols`, with the provider
  type and the full error.
- The cycle continues with the remaining providers, and the symbols they return are still
  published to RabbitMQ.
- The cycle then logs `refresh cycle completed with provider failures` with the number of
  symbols collected (`symbols`) and the number of failed providers (`failed_providers`).
- The next scheduled run retries every provider, so a temporarily unavailable provider (for
  example, a TMX maintenance page) recovers automatically without a restart.

Provider failures are never fatal: the run still completes and publishes the healthy providers'
symbols. The process exits `0` after publishing the healthy provider's symbols. The only run
failure it treats as an error is a publish failure. Provider base URLs are compile-time constants
(`defaultBaseURL` in `internal/provider/tsx.go` and `nasdaqBaseURL` in `internal/provider/us.go`);
they cannot be changed with an environment variable or config file.

See [`docs/plans/symbol-discovery-html-response.md`](docs/plans/symbol-discovery-html-response.md)
for the incident analysis behind this behavior.

## Environment variables

### Cadence

There is no refresh-interval environment variable. `stocker-list` performs
one cycle per invocation. In Kubernetes, the CronJob `schedule` in
`deploy/kubernetes/stocker-list.yaml` controls cadence; change that field
and re-apply the manifest.

### RabbitMQ

| Name | Type | Default | Required? | Description |
|------|------|---------|-----------|-------------|
| `RABBITMQ_URL` | AMQP URI string (`amqp://`/`amqps://`) | `amqp://guest:guest@192.168.1.48:5672/` | No | Broker URI. Credentials are embedded in the URI and are never logged. **Unset = default; explicitly empty (`RABBITMQ_URL=`) = publishing disabled (graceful no-op).** |
| `RABBITMQ_EXCHANGE` | string | `stock.update` | No | Exchange the publisher declares and publishes to. Empty falls back to the default. |
| `RABBITMQ_EXCHANGE_TYPE` | enum string | `topic` | No | One of `direct`, `topic`, `fanout`, `headers`. Trimmed and lowercased; any other value is a startup error. |
| `RABBITMQ_ROUTING_KEY` | string | `stock.update.v1` | No | Routing key used for every message. Empty falls back to the default. |

**Key semantics:**

- `RABBITMQ_URL` unset ⇒ the default broker URL `amqp://guest:guest@192.168.1.48:5672/` is used.
- `RABBITMQ_URL=` (explicitly empty) ⇒ publishing disabled: the process still performs one
  cycle, but the producer is a no-op (no broker is dialed, nothing is published).
- The default URL uses the stock `guest:guest` account. Stock RabbitMQ only accepts `guest`
  from the broker's own loopback interface, so connecting to `192.168.1.48` with `guest` will
  be refused unless the broker owner has relaxed that restriction; supply a dedicated user
  (and vhost, if any) via `RABBITMQ_URL` for any non-local broker.
- Both providers (TSX and US) are active by default; neither requires an API key.

## Running it

### Local (build & run)

```
make build     # produces bin/stocker-list
make run       # performs one cycle and exits; or: go run ./cmd/server
```

Set the environment variables above in the process environment (see `.env.example`). With
`RABBITMQ_URL` explicitly empty publishing is a no-op; both providers (TSX and US) run
unconditionally. The US provider paginates through all three listing exchanges and tags each
symbol with its exchange (`NASDAQ`, `NYSE`, or `AMEX`), matching
`internal/provider/us.go`.

### Podman (podman-compose)

```
make podman-up   # starts one local container that runs one cycle and stops
make podman-down # stops the container
```

The container uses `.env.podman` for its environment (copy values from `.env.example` if needed).
It runs exactly one full cycle and then exits (`restart: "no"`); scheduling is external.


### Kubernetes

Deployment manifests live in `deploy/kubernetes/`. `stocker-list.yaml`
contains the Namespace, ConfigMap, and CronJob; `secret.yaml` is a template
for the RabbitMQ credentials. See
`docs/design-kubernetes-deployment.md` and
`docs/plan-kubernetes-deployment.md`.
