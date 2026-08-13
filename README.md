# stocker-list

A Go service that pulls stock information from the internet and adds it to the stocker-store. On a timer, it adds new
listings and removing delisted companies.

## Architecture

```
cmd/server/main.go        wires everything together, starts the gRPC server
internal/config           env-var configuration (DB, refresh cadence)
internal/provider         Company directory clients
internal/refresher        background loop syncing symbols + pruning delisted
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

TODO

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

**3. Build the container image:**
```
make quadlet-build
```

Or equivalently:
```
podman build -t localhost/stocker-list:latest .
```

**4. Start the service:**
```
systemctl --user start tsx-tracker.service
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

**Updating:** pull new code, rebuild, and restart:
```
cd ~/tsx-tracker
git pull
make quadlet-build
systemctl --user restart stocker-list
```

### 3d. Run locally
```
# start a local Postgres, then:
export $(cat .env | xargs)
make run
```

The service listens on `:50051` (configurable via `GRPC_PORT`) and
registers gRPC reflection, so you can explore/call it with `grpcurl`
without needing the `.proto` file locally:

```
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext localhost:50051 tsx.v1.CompanyService/ListCompanies
grpcurl -plaintext -d '{"symbol": "SHOP.TO"}' localhost:50051 tsx.v1.CompanyService/GetCompany
```

## gRPC API

```protobuf
service CompanyService {
  rpc ListCompanies(ListCompaniesRequest) returns (ListCompaniesResponse);
  rpc GetCompany(GetCompanyRequest) returns (GetCompanyResponse);
}
```

- `ListCompanies` — paginated (keyset pagination via `page_token`, default
  page size 50, max 500).
- `GetCompany` — fetch one company by `symbol` (case-insensitive). Returns
  a `NOT_FOUND` gRPC status if the symbol isn't tracked.

See `proto/tsx/v1/tsx.proto` for full message definitions, including the
`Company` message (symbol, name, exchange, currency).

## Notes / next steps for production use

- This ships a minimal `Migrate()` that runs the schema SQL idempotently
  on startup; for a real production system, use a proper migration tool
  (golang-migrate, atlas, etc.) once the schema evolves.
- Add TLS/auth to the gRPC server before exposing it outside a trusted
  network — it currently runs in plaintext for simplicity.
- Consider adding the standard gRPC health-checking protocol
  (`grpc_health_v1`) for orchestrator liveness/readiness probes.
- `go.mod` lists direct dependencies; run `make tidy` (`go mod tidy`) after
  generating the proto code to resolve exact versions and populate
  `go.sum` for your environment.
