# Design: stocker-list

---

## Proto Changes (`proto/tsx/v1/tsx.proto`)

The existing `Stock` message already contains fields compatible with stocker-store's schema (ID, ticker, name, price, currency, market). No changes are needed to this message.

### New service definition

Add a `StockerList` RPC alongside the existing `GetQuotes` RPC:

```protobuf
service SteckerList {
  rpc GetStocks(stream Stock) returns (Empty {}
}
```

- **Direction**: client-streaming — each retrieved stock is sent as it arrives (no need to accumulate in memory).
- **Response** → `Empty` — caller does not need per-stock acknowledgment details; the stream's completion confirms success. Add a gRPC status code for errors when needed.

### Required imports

Ensure the proto uses the same package and import structure as stocker-store so both services share the generated `stockerstore/tsx/v1/tsx.pb.go` types (or an equivalent shared module).

---

## Remove Local Database Model (`internal/model/stock`)

Delete:
- **`internal/model/stock.go`** — never used by current code; remove.

(This file is dead code, but include its deletion to keep the repo clean.)

---

## Remove Database Layer

### Source files → delete
- **`internal/database/`** (if it exists) — database connection, pool initialization
- All files under **`queries/`** — SQLC-generated query methods and interfaces

### `Makefile` → remove
```make
sqlc-gen    # remove this target entirely
schema      # or update if used elsewhere
```

### `go.mod` → remove dependencies (run `go mod tidy` after)
- `github.com/jackc/pgx/v5`
- `github.com/sqlc-dev/sqlc/packe*/sqlcl`
- `github.com/kyleconroy/sqlc-go`
- Any `-with-go-modules` transitive SQLC deps

### Go imports → remove from remaining files
Any `.go` file that still references the deleted database package must have:
- `import ( "stocker-list/internal/database" ... )` → remove
- `database.NewPool(...`, `database.QueryContext`, etc. → replace as noted below

---

## Create gRPC Client Layer (`internal/grpcstore/`)

Create two Go files in **`internal/grpcstore/`**:

### `grpcstore.go`

```go
package grpcstore

type StockerClient interface {
    GetStocks(ctx context.Context, opts ...grpc.DialOption) error
}

type Config struct {
    Address string // e.g. "localhost:50051" or env-override
}
```

### `store.go`

Implements the streaming call to stocker-store:

1. Dial into `Config.Address` (respecting TLS, timeouts, etc.)
2. Call `StockerList.GetStocks(ctx)` on the stub
3. For each stock from the provider's iteration → send over the client stream
4. Close and check the server's final status/error before returning

Add **reconnection logic** using a retry middleware (e.g.; `grpc-go/retryinterceptor`, `golang.org/x/net/http2`) for resilience against transient network failures in this service-to-service call.

---

## Wire into the Provider Layer (`internal/provider/`)

Modify whichever provider handles the stock retrieval:

1. Accept an `*grpcstore.StockerClient` via dependency injection (not global state)
2. Stream each stock directly to gRPC as soon as it is received from whatever data source feeds it — no accumulation into a slice or map first
3. Ensure proper context cancellation and cleanup

Example shape:
```go
func (p *Provider) Fetch(ctx context.Context, client grpcstore.StockerClient) error {
    stream, err := client.GetStocks(ctx)
    if err != nil {
        return err
    }  
    for _, s := range p.dataFeed(ctx) {
        stockMsg := transformToProto(s)  // convert retrieved row → protobuf Stock message
        if err := stream.Send(stockMsg); err != nil {
            return err
        }
    }
    return stream.CloseSend()
}
```

---

## Update Application Configuration (`deploy/` Helm chart values)

Remove or comment out the database section in your deployment config and add a `grpcstore.address` field pointing to stocker-store's service name:

```yaml
grpcstore:
  address: "stocker-store:50051"  # Kubernetes service DNS
```

---

## Update `cmd/server/main.go` (or equivalent entry point)

1. Accept `grpcstore.Config` from the application config (via CLI flag, env variable, or config file — whichever pattern this project follows).
2. Instantiate a `grpcstore.StockerClient` using a gRPC connection with appropriate dial options.  
3. Pass the client to providers that now need it for streaming.
4. Remove database pool creation and injection code.

---

## Summary Checklist

| Item | Action | Location |
|------|--------|----------|
| Remove dead model | delete file | `internal/model/stock.go` |
| Remove DB layer | delete directory | `internal/database/` |
| Remove SQLC queries | delete directory | `queries/` |
| Update Makefile | remove sqlc-gen | `Makefile` |
| Update go.mod | remove DB deps, run tidy | `go.mod`, `go.sum` |
| Add proto service | add GetStocks RPC | `proto/tsx/v1/tsx.proto` |
| Regenerate protos | `buf generate` | run via Makefile or manually |
| Add gRPC client layer | create new package | `internal/grpcstore/` |
| Update provider | inject & stream to grpcstore | `internal/provider/` |
| Wire config | add grpcstore address field | deploy configs, CLI flags |
| Wire main | create connection, pass to providers | `cmd/server/main.go` |

After all changes are applied, run `go mod tidy`, verify the build passes, and confirm the service starts.
