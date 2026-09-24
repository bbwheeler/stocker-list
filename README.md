# stocker-list

A Go service that pulls stock symbols from public providers and publishes them as Kafka
`StockUpdate` protobuf messages on a refresh timer. It is a Kafka publisher driven by a
background refresh loop

## Architecture

```
cmd/server         wires everything together and runs the refresh loop (not a server)
internal/config    env-var configuration (core, Kafka, provider)
internal/provider  stock symbol provider clients (TSX, US)
internal/refresher background loop retrieving symbols each interval
internal/kafka     Kafka producer (no-op when Kafka is disabled)
```

## Kafka integration

The service publishes `StockUpdate` messages using protobuf serialization from
[stocker-store](https://git.wheeli.ca/brian/stocker-store).

- **Message type:** `kafkastockv1.StockUpdate{Symbol, Exchange, Scores}`
- **Package:** `kafkastockv1`
- **Import path:** `stocker-store/proto/v1/kafka`

```go
import kafkastockv1 "stocker-store/proto/v1/kafka"

msg := &kafkastockv1.StockUpdate{
    Symbol:   "AAPL",
    Exchange: "NASDAQ",
    Scores:   map[string]float64{"momentum": 0.7},
}
```

- Publishing is **disabled** (a graceful no-op; no broker is ever dialed) when
  `KAFKA_BROKERS` is empty.
- Otherwise, messages are sent to `KAFKA_TOPIC` (default `stock.update.v1`) with the stock
  symbol as the message key, using optional SASL (enabled only when **both**
  `KAFKA_SASL_USERNAME` and `KAFKA_SASL_PASSWORD` are non-empty; `PLAIN`, `SCRAM-SHA-256`,
  or `SCRAM-SHA-512`) and optional TLS per `KAFKA_SSL_ENABLED`.

## Providers

### TSX (Toronto Stock Exchange)

The service uses the **official TMX company directory** — a free, public JSON API provided
by the Toronto Stock Exchange itself. No API key is required; the TSX provider is always
active.

The endpoint `https://www.tsx.com/json/company-directory/search/tsx/*` returns all
TSX-listed companies. The service holds no symbol list of its own between cycles: each
cycle the provider's full current TSX list is fetched fresh and re-published to Kafka, so
what is published always reflects the provider's live listings.

### US (Financial Modeling Prep)

The US provider uses
`https://financialmodelingprep.com/api/v3/stock/list` and **requires** `FMP_API_KEY`.
When `FMP_API_KEY` is empty, the US provider is disabled and the service tracks TSX only.

## Environment variables

### Core

| Name | Type | Default | Required? | Description |
|------|------|---------|-----------|-------------|
| `REFRESH_CHECK_INTERVAL` | Go duration string (e.g. `24h`, `12h`, `30m`) | `24h` | No | How often the background refresher pulls the symbol list and re-publishes to Kafka. Must parse and be `> 0`. |

### Kafka

| Name | Type | Default | Required? | Description |
|------|------|---------|-----------|-------------|
| `KAFKA_BROKERS` | comma-separated `host:port` list (e.g. `kafka1:9093,kafka2:9093`) | empty | No | Kafka brokers to publish to. **Empty = Kafka disabled → the producer is a graceful no-op** (no broker is ever dialed). |
| `KAFKA_TOPIC` | string | `stock.update.v1` | No | Topic published to. Empty/absent falls back to `stock.update.v1`. |
| `KAFKA_SASL_USERNAME` | string | empty | No | SASL username. SASL is enabled **only when both this and `KAFKA_SASL_PASSWORD` are non-empty**; otherwise SASL is disabled. |
| `KAFKA_SASL_PASSWORD` | string | empty | No | SASL password. Required (together with a non-empty username) to enable SASL. |
| `KAFKA_SASL_MECHANISM` | string | `PLAIN` | No | SASL mechanism. One of `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`. (Accepted case-insensitively.) |
| `KAFKA_SSL_ENABLED` | bool | `true` | No | Enable TLS for Kafka connections. Parsed as `true`/`false`. |

### Provider

| Name | Type | Default | Required? | Description |
|------|------|---------|-----------|-------------|
| `FMP_API_KEY` | string | empty | No | Financial Modeling Prep API key. **Empty = US provider disabled (TSX-only).** When set, the US provider is added to the refresh loop. |

**Key semantics:**

- `KAFKA_BROKERS` empty ⇒ Kafka disabled: the service still runs the refresh loop, but the
  producer is a no-op (no broker is dialed, nothing is published).
- `FMP_API_KEY` empty ⇒ US provider disabled: only the TSX provider runs. With it set, both
  TSX and US run.

## Running it

### Local (build & run)

```
make build     # produces bin/stocker-list
make run       # or: go run ./cmd/server
```

Set the environment variables above in the process environment (see `.env.example`). Kafka
and the US provider are opt-in via their env vars: with `KAFKA_BROKERS` empty publishing is
a no-op, and with `FMP_API_KEY` empty only the TSX provider runs.

### Podman (podman-compose)

```
make podman-up
make podman-down
```

### Quadlet (rootless, systemd user service)

Deployment facts: **image** `git.wheeli.ca/brian/stocker-list:latest`, **service name**
`stocker-list`, **env file** `~/.config/stocker-list/.env.podman`.

1. Clone the repo and install the quadlet unit (copies the unit file to
   `~/.config/containers/systemd/` and the env template to
   `~/.config/stocker-list/.env.podman`):

   ```
   make quadlet-install
   ```

2. Fill in the environment file (set `KAFKA_*` and `FMP_API_KEY` as needed):

   ```
   $EDITOR ~/.config/stocker-list/.env.podman
   ```

3. Build and push the image:

   ```
   (cd deploy && ./push.sh)    # builds + pushes git.wheeli.ca/brian/stocker-list:latest
   ```

4. Start the service:

   ```
   systemctl --user daemon-reload
   systemctl --user start stocker-list.service
   ```

   The `WantedBy=default.target` in the `.container` file ensures the service starts
   automatically on boot.

5. Check status and logs:

   ```
   systemctl --user status stocker-list
   podman logs stocker-list
   ```

Stopping:

```
systemctl --user stop stocker-list
```

Updating: build and push a new image (step 3) and restart the service
(`systemctl --user restart stocker-list`); the unit has `AutoUpdate=registry` and will pick
up the new image.
