# Integration Plan: stocker-store Kafka Protobuf Messages into stocker-list

## Goal

Consume the `StockUpdate` message type (`kafkastockv1.StockUpdate`) from the protobuf Kafka messages defined in `stocker-store` and use them as the payload format for the stocker-list Kafka producer.

---

## Recommendation: Hybrid Approach — Copy to `internal/proto/` Now, Migrate External Later

**Why:** The proto's `go_package` uses a non-domain module path (`stocker-store/proto/v1/kafka;kafkastockv1`). The referenced Go module root is simply `stocker-store`. Since `github.com/wheeli-ca/stocker-store` does not exist yet, there is **no public vanity import path** to resolve this as a standard remote dependency right now. Using it as an external module dependency at all — even with a Git replace directive — creates fragility because:

1. The module name and the `go_package` import path have no domain prefix (`stocker-store/...`).
2. Any change to the stocker-store repo's public visibility would break an existing replace directive.
3. The generated code is fully self-contained (only depends on `google.golang.org/protobuf`, which stocker-list already has), making it trivial to copy.

**The hybrid approach:**

- **Now:** Copy `stock_message.pb.go` + `stock_message.proto` into a local `internal/proto/kafka/v1/kafkastockv1/` directory (package name is already `kafkastockv1`). This works immediately for both private and future public states.
- **When/if** the repo gets published with a valid vanity import path (`github.com/wheeli-ca/stocker-store/proto/v1/kafka;kafkastockv1`), update to use it as an external module dependency with zero code changes — swap just the import statement.

---

## Numbered Implementation Instructions

### 1. Create local proto directory structure

Create `internal/proto/kafka/v1/kafkastockv1/` inside stocker-list:

```bash
mkdir -p internal/proto/kafka/v1/kafkastockv1
```

### 2. Copy the protobuf files from stocker-store

Copy **two** files into the directory:

- `stock_message.proto` — the source definition (for documentation, future regeneration, and schema evolution tracking)
- `stock_message.pb.go` — the generated code (already self-contained, no changes needed)

```bash
cp ../stocker-store/proto/v1/kafka/stock_message.proto \
   internal/proto/kafka/v1/kafkastockv1/

cp ../stocker-store/proto/v1/kafka/stock_message.pb.go \
   internal/proto/kafka/v1/kafkastockv1/
```

**Resulting package:** The Go file declares `package kafkastockv1`. No changes to the file are necessary — no import path modifications, no package renaming.

### 3. Update go.mod with a buf dependency (optional but recommended)

The proto files in local `internal/proto/` do **not** need a new module dependency since they live inside stocker-list's own module scope. However, add the buf modules that may be used for future proto code regeneration:

```bash
go get -d github.com/bufbuild/buf/cmd/buf@latest
```

This is optional — you can also regenerate with `buf generate` if your existing `buf.yaml` + `buf.gen.yaml` in the repo already point to the right modules.

Actually, no new module dependency is needed at all. The existing `go.mod` already has:

```
google.golang.org/protobuf v1.34.2   // used by stock_message.pb.go
google.golang.org/grpc    v1.67.0    // available for gRPC services later
```

**No go.mod changes required.**

### 4. Expose the package with a simple re-export (optional)

Add `internal/internalkafka/types.go` that imports and re-exports `kafkastockv1`:

```go
package internalkafka

import kafkastockv1 "github.com/anomalyco/stocker-list/internal/proto/kafka/v1/kafkastockv1"

// StockUpdate is the protobuf message published to Kafka topics.
type StockUpdate = kafkastockv1.StockUpdate
```

Or skip this entirely and import directly from the full path in consumer code:

```go
import kafkastockv1 "github.com/anomalyco/stocker-list/internal/proto/kafka/v1/kafkastockv1"

msg := &kafkastockv1.StockUpdate{
    Symbol:   "AAPL",
    Exchange: "NASDAQ",
    Scores: map[string]float64{"momentum": 0.7},
}
```

**Recommendation:** Use direct import — no re-export layer needed for a single package. Keep it simple.

### 5. Wire the type into the Kafka producer

Locate or create the producer that publishes `StockUpdate` messages:

```go
// internal/kafka/producer.go (or wherever the producer lives)

package kafka

import (
    kafkastockv1 "github.com/anomalyco/stocker-list/internal/proto/kafka/v1/kafkastockv1"
    // ... other imports
)

// SendMessage publishes one StockUpdate record to the configured Kafka topic.
func (p *Producer) SendMessage(ctx context.Context, msg *kafkastockv1.StockUpdate) error {
    payload, err := proto.Marshal(msg)
    if err != nil {
        return fmt.Errorf("marshal StockUpdate: %w", err)
    }

    _, err = p.topicProduce(ctx, stockUpdateTopic, payload)
    return err
}
```

### 6. Wire the type into the Kafka consumer

Wherever the service consumes `StockUpdate` messages:

```go
func handleStockUpdate(payload []byte) (*kafkastockv1.StockUpdate, error) {
    msg := &kafkastockv1.StockUpdate{}
    if err := proto.Unmarshal(payload, msg); err != nil {
        return nil, fmt.Errorf("unmarshal StockUpdate: %w", err)
    }

    // Use the message fields:
    symbol := msg.Symbol
    exchange := msg.Exchange
    scores := msg.Scores  // map[string]float64 or nil

    _ = symbol + exchange
    for k, v := range scores {
        // ...
    }

    return msg, nil
}
```

### 7. Update the README to document the local proto module

Add a section to stocker-list's `README.md` documenting where the shared protobuf types live:

```markdown
### Shared Protobuf Messages

The `StockUpdate` message type lives at
`internal/proto/kafka/v1/kafkastockv1/`, copied from [stocker-store](https://git.wheeli.ca/brian/stocker-store).
It uses the package name `kafkastockv1` (go_package suffix).

Import:

    import kafkastockv1 "github.com/anomalyco/stocker-list/internal/proto/kafka/v1/kafkastockv1"

When stocker-store is published publicly, this will migrate to an external
module dependency (no code changes beyond the import path).
```

### 8. Verify everything compiles

```bash
# Ensure imports resolve
go build ./...

# Run existing tests plus proto-related tests
go test ./...

# Tidy dependencies (should be no-op since no new deps added)
go mod tidy
```

### 9. Commit the changes

```bash
git add internal/proto/kafka/v1/kafkastockv1/
git commit -m "Add local copy of stocker-store Kafka protobuf types (kafkastockv1)

Copied from git.wheeli.ca/brian/stocker-store:
  - proto/v1/kafka/stock_message.proto  (schema definition)
  - proto/v1/kafka/stock_message.pb.go  (generated code)

Package: kafkastockv1
Import path: github.com/anomalyco/stocker-list/internal/proto/kafka/v1/kafkastockv1

This local copy works during the private repo phase and will migrate
to an external module dependency when stocker-store publishes publicly."
```

---

## Migration Path to External Module (Future)

When `github.com/wheeli-ca/stocker-store` exists with a valid vanity import path:

### Step M1: Replace local copy with go.mod dependency

```bash
go get github.com/wheeli-ca/stocker-store/proto/v1/kafka@latest
# Note: go_package says "stocker-store/proto/v1/kafka;kafkastockv1"
# The import path becomes: kafkastockv1 "github.com/wheeli-ca/stocker-store/proto/v1/kafka"
```

### Step M2: Update all import statements

Replace every instance of:

```go
import kafkastockv1 "github.com/anomalyco/stocker-list/internal/proto/kafka/v1/kafkastockv1"
```

With:

```go
import kafkastockv1 "github.com/wheeli-ca/stocker-store/proto/v1/kafka"
```

(The go_package `kafkastockv1` suffix becomes the package name.)

### Step M3: Remove local copy

```bash
git rm -r internal/proto/kafka/v1/kafkastockv1/
go mod tidy
rm -rf internal/proto  # if no other protos remain
```

**Result:** All code works identically, only the import path changes. The `kafkastockv1` package name and the `StockUpdate` type are binary-compatible in both scenarios.

---

## Summary of Approach

| Aspect | Decision |
|--------|----------|
| **Current integration** | Direct copy to `internal/proto/kafka/v1/kafkastockv1/`, no module dependency needed |
| **Package name** | `kafkastockv1` — matches the go_package suffix, unchanged from upstream |
| **go.mod changes** | None required (no external dependency needed for local `internal/` code) |
| **Future migration** | Replace with `import kafkastockv1 "github.com/wheeli-ca/stocker-store/proto/v1/kafka"` once published |
| **Compatibility** | The `kafkastockv1` package name is the same in both scenarios — zero breaking changes |
| **Risk** | Minimal. The pb.go file only depends on `google.golang.org/protobuf` (already a dep). No import path or package renaming required. |
