# stocker-list

A Go service that pulls stock symbols from public providers and publishes them as Kafka
`Stock` protobuf messages on a refresh timer. It is a Kafka publisher driven by a
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

The service publishes `Stock` messages using protobuf serialization from
[stocker-store](https://git.wheeli.ca/brian/stocker-store).

- **Message type:** `kafkastockv1.Stock{Symbol, Exchange}`
- **Package:** `kafkastockv1`
- **Import path:** `stocker-store/proto/v1/kafka`

```go
import kafkastockv1 "stocker-store/proto/v1/kafka"

msg := &kafkastockv1.Stock{
    Symbol:   "AAPL",
    Exchange: "NASDAQ",
}
```

- Publishing is **disabled** (a graceful no-op; no broker is ever dialed) when
  `KAFKA_BROKERS` is empty.
- Otherwise, messages are sent to `KAFKA_TOPIC` (default `stock.update.v1`) with the stock
  symbol as the message key, over a **plaintext** connection. Brokers must be reachable
  plaintext: **SASL and TLS are not supported**.

## Providers

### TSX (Toronto Stock Exchange)

The service uses the **official TMX company directory** — a free, public JSON API provided
by the Toronto Stock Exchange itself. No API key is required; the TSX provider is always
active.

The endpoint `https://www.tsx.com/json/company-directory/search/tsx/*` returns all
TSX-listed companies. The service holds no symbol list of its own between cycles: each
cycle the provider's full current TSX list is fetched fresh and re-published to Kafka, so
what is published always reflects the provider's live listings.

### US (NASDAQ screener)

The US provider uses the **public NASDAQ screener API** — `api.nasdaq.com/api/screener/stocks`
(the endpoint that powers the nasdaq.com web app). It returns the full set of US common
stocks (NYSE, NASDAQ, NYSE American) as a single JSON payload and **requires no API key**.
The US provider is always active, alongside TSX.

Note: the API does not expose a per-row exchange or currency field, so the provider maps
every US symbol to `Exchange: "US"` and `Currency: "USD"` (safe for this all-US, USD universe).

## Environment variables

### Core

| Name | Type | Default | Required? | Description |
|------|------|---------|-----------|-------------|
| `REFRESH_CHECK_INTERVAL` | Go duration string (e.g. `24h`, `12h`, `30m`) | `24h` | No | How often the background refresher pulls the symbol list and re-publishes to Kafka. Must parse and be `> 0`. |

### Kafka

| Name | Type | Default | Required? | Description |
|------|------|---------|-----------|-------------|
| `KAFKA_BROKERS` | comma-separated `host:port` list (e.g. `kafka1:9093,kafka2:9093`) | empty | No | Kafka brokers to publish to (**plaintext only**). **Empty = Kafka disabled → the producer is a graceful no-op** (no broker is ever dialed). |
| `KAFKA_TOPIC` | string | `stock.update.v1` | No | Topic published to. Empty/absent falls back to `stock.update.v1`. |

**Key semantics:**

- `KAFKA_BROKERS` empty ⇒ Kafka disabled: the service still runs the refresh loop, but the
  producer is a no-op (no broker is dialed, nothing is published).
- Both providers (TSX and US) are active by default; neither requires an API key.

## Running it

### Local (build & run)

```
make build     # produces bin/stocker-list
make run       # or: go run ./cmd/server
```

Set the environment variables above in the process environment (see `.env.example`). With
`KAFKA_BROKERS` empty publishing is a no-op; both providers (TSX and US) run unconditionally.

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

2. Fill in the environment file (set `KAFKA_*` as needed):

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
