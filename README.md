# stocker-list

A Go service that pulls stock information from the internet and sends it out on a kafka topic.

## Architecture

```
cmd/server/main.go        wires everything together, starts the gRPC server
internal/config           env-var configuration (Kafka config, refresh cadence)
internal/provider         Company directory clients
internal/refresher        background loop retrieving symbols
```

### Where the data goes - Kafka Integration

The service publishes StockUpdate messages to a Kafka topic using protobuf serialization from [stocker-store](https://git.wheeli.ca/brian/stocker-store).

- **Module:** `stocker-store` (resolved to `git.wheeli.ca/brian/stocker-store` via a `replace` directive in `go.mod`)
- **Package:** `kafkastockv1`
- **Import path:** `stocker-store/proto/v1/kafka` (the external Go module dependency listed in `go.mod`)
- **Message type:** `kafkastockv1.StockUpdate{Symbol, Exchange, Scores}`

```go
import kafkastockv1 "stocker-store/proto/v1/kafka"

msg := &kafkastockv1.StockUpdate{
    Symbol:   "AAPL",
    Exchange: "NASDAQ",
    Scores: map[string]float64{"momentum": 0.7},
}
```

### Where the data comes from
#### TSX

The service uses the **official TMX company directory** — a free, public
JSON API provided by the Toronto Stock Exchange itself. No API key is
required.

The endpoint `https://www.tsx.com/json/company-directory/search/tsx/*`
returns all TSX-listed companies in a single request. The service queries
this once per sync cycle to build the complete symbol list.

Every cycle the full TSX symbol list is synced — new listings are added
and delisted symbols are removed.

#### US Companies

https://financialmodelingprep.com/api/v3/stock/list

## Running it

### 3a. Run with Podman
```
make podman-up
```

### 3b. Deploy with Podman Quadlet (rootless, Debian)

Quadlet lets you manage Podman containers as systemd user services — the
container starts on boot without root.

**1. Clone the repo and install:**
```
git clone https://github.com/youruser/stocker-list.git ~/stocker-list
cd ~/stocker-list
make quadlet-install
```

This copies the Quadlet unit file to `~/.config/containers/systemd/`,
installs the env file to `~/.config/stocker-list/.env.podman`, and runs
`systemctl --user daemon-reload`.

**2. Edit the environment file** with your credentials:
```
$EDITOR ~/.config/tsx-tracker/.env.podman
```

**3. Deploy the latest image to the container registry**

TODO

**4. Start the service:**
```
systemctl --user start stocker-list.service
```

The `WantedBy=default.target` in the `.container` file ensures the
service starts automatically on boot.

**5. Check status and logs:**
```
systemctl --user status stocker-list
podman logs stocker-list
```

**Stopping:**
```
systemctl --user stop tstocker-list
```

**Updating:** Deploy the latest image to the container registry

TODO


