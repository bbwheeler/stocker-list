# Kubernetes CronJob Implementation Plan: stocker-list

**Status:** This plan **replaces** the previous Deployment plan (old Tasks
1–5). The old Tasks 1–4 produced `configmap.yaml`, `secret.yaml`, and
`deployment.yaml` for a long-running Deployment. Those files are superseded by
this plan.

**Companion design:** `docs/design-kubernetes-deployment.md` is the source of
truth for the target architecture, manifest layout, and rationale. Implement
the tasks **one at a time, in order**.

**Task status:** every task below is new work relative to the current branch.
Tasks 1–4 are the one-shot code refactor; Task 5 updates prose and env
templates; Tasks 6–7 replace the superseded manifest work; Tasks 8–10 are
removals and cleanups; Tasks 11–14 are verification and rollout.

## Global constraints

- **Do not change provider endpoints** (`internal/provider/tsx.go`,
  `internal/provider/us.go`) or the producer's exchange/confirm behavior.
- **No new third-party dependencies.** Standard library only.
- **All real credentials live outside the repository.** The committed Secret
  contains placeholders only. Never commit a filled-in Secret, a password, or a
  private registry token. For the current operator, real values are in
  `/home/brian/workspace/credentials.md`; do not copy them into any tracked
  file.
- **One process invocation = one complete cycle.** No ticker, no sleep loop, no
  in-process scheduler.
- Match the `stocker-store` gold standard: single main manifest, separate
  Secret template, Namespace → ConfigMap → workload ordering, minimal
  `app: stocker-list` label, `envFrom` only, exact security context and
  resources.
- Every task must state what "done" looks like. Tasks 1–4 intentionally leave
  the repository in an intermediate state (the module may not build until Task
  4 completes); each task names the package-level command that must pass.

## Prerequisites (provided outside this repository)

1. A Kubernetes cluster with a `stocker` namespace (the manifest can create
   it, but the operator needs permission to create Namespaces). If the
   operator cannot create Namespaces, create it first:
   ```bash
   kubectl create namespace stocker
   ```
2. Registry credentials for `git.wheeli.ca` in namespace `stocker`, if the
   `wheeli-registry` Secret does not already exist. A docker-registry pull
   Secret is namespaced: create it once in `stocker`, not per workload. Real
   values stay outside the repo:
   ```bash
   kubectl create secret docker-registry wheeli-registry \
     --docker-server=git.wheeli.ca \
     --docker-username=REPLACE_ME \
     --docker-password=REPLACE_ME \
     -n stocker
   ```
   Passing the token with `--docker-password` records it in your shell
   history; use a file-based `.dockerconfigjson` Secret if history is shared.
3. A RabbitMQ broker reachable from the cluster, a `stocker` user, and a
   client Service. The Secret template assumes the Service is `rabbitmq` in
   namespace `messaging`; verify with `kubectl -n messaging get svc`.
4. Exchange `stock.update` (topic) with routing key `stock.update.v1`. The
   publisher declares the exchange; consumers own queues and bindings.
5. The image `git.wheeli.ca/brian/stocker-list:latest` built and pushed:
   ```bash
   (cd deploy && ./push.sh)
   ```
6. `kubectl` is **optional** for the static validation tasks. It is not
   installed in every development environment; validation steps are
   best-effort and must not block the code/manifest work.

---

## Task 1 [NEW]: Rewrite the refresher tests for one-shot semantics

This is the first half of a TDD pair. Write the tests against the new API
before implementing it; the package is expected **not to compile** until Task
2.

**File:** `internal/refresher/refresher_test.go` — replace the entire file
with the following. Tests use `slog.NewTextHandler`, so log attributes match
as `key=value`; the production handler in `cmd/server` is JSON
(`"key":value`).

```go
package refresher_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	stockstorev1 "git.wheeli.ca/brian/stocker-store/proto/v1"
	"github.com/anomalyco/stocker-list/internal/provider"
	"github.com/anomalyco/stocker-list/internal/refresher"
)

func testLog(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(os.Stderr, nil))
}

// symbolsHandler returns an httptest server emulating the TMX company
// directory, responding to a single GET on the base URL.
func symbolsHandler(t *testing.T, symbols []string) *httptest.Server {
	t.Helper()
	entries := make([]map[string]string, 0, len(symbols))
	for _, s := range symbols {
		entries = append(entries, map[string]string{"symbol": s, "name": "Test Corp"})
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"results": entries})
	}))
}

// TestRunOnce_PublishesSymbols verifies one invocation publishes every symbol
// returned by the provider.
func TestRunOnce_PublishesSymbols(t *testing.T) {
	srv := symbolsHandler(t, []string{"AC", "RY", "TD"})
	defer srv.Close()

	p := provider.NewClientForTest(srv.URL)
	producer := &fakeProducer{}
	ref := refresher.New([]provider.Provider{p}, testLog(t), producer)

	if err := ref.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error: %v", err)
	}

	got := producer.published()
	want := []string{"AC", "RY", "TD"}
	if len(got) != len(want) {
		t.Fatalf("published %d symbols, want %d: %v", len(got), len(want), got)
	}
	for i, symbol := range want {
		if got[i].GetSymbol() != symbol {
			t.Errorf("published[%d].Symbol = %q, want %q", i, got[i].GetSymbol(), symbol)
		}
	}
}

// TestRunOnce_RunsExactlyOneCycle proves the process no longer loops: the
// provider is called once and RunOnce returns.
func TestRunOnce_RunsExactlyOneCycle(t *testing.T) {
	healthy := &countingProvider{}
	ref := refresher.New([]provider.Provider{healthy}, testLog(t), &fakeProducer{})

	if err := ref.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error: %v", err)
	}
	if healthy.calls != 1 {
		t.Errorf("provider called %d times, want exactly 1", healthy.calls)
	}
}

// TestRunOnce_CancelledContextIsAnError proves an already-cancelled run does
// not report success (important for SIGTERM and Job deadlines).
func TestRunOnce_CancelledContextIsAnError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	p := &countingProvider{}
	ref := refresher.New([]provider.Provider{p}, testLog(t), &fakeProducer{})
	err := ref.RunOnce(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunOnce() = %v, want context.Canceled", err)
	}
	if p.calls != 0 {
		t.Errorf("provider called %d times after pre-cancelled context, want 0", p.calls)
	}
}

// TestRunOnce_CancelledDuringRunIsAnError proves cancellation detected after
// the provider phase is still surfaced, even when no symbols were published.
func TestRunOnce_CancelledDuringRunIsAnError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := &cancellingProvider{cancel: cancel}
	ref := refresher.New([]provider.Provider{p}, testLog(t), &fakeProducer{})

	err := ref.RunOnce(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunOnce() = %v, want context.Canceled", err)
	}
}

// TestRunOnce_AllProvidersFailStillSucceeds documents the tolerated degraded
// mode: the process exits 0, publishes nothing, and logs the counts.
func TestRunOnce_AllProvidersFailStillSucceeds(t *testing.T) {
	var logs bytes.Buffer
	producer := &fakeProducer{}
	providers := []provider.Provider{
		errorProvider{err: errors.New("tsx down")},
		errorProvider{err: errors.New("us down")},
	}
	ref := refresher.New(providers, slog.New(slog.NewTextHandler(&logs, nil)), producer)

	if err := ref.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error: %v; all-provider failure is tolerated", err)
	}
	if msgs := producer.published(); len(msgs) != 0 {
		t.Errorf("published %d messages, want 0", len(msgs))
	}
	for _, want := range []string{
		"refresh cycle completed with provider failures",
		"symbols=0",
		"failed_providers=2",
	} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("logs do not contain %q:\n%s", want, logs.String())
		}
	}
}

type errorProvider struct {
	err error
}

func (p errorProvider) ListSymbols(context.Context) ([]provider.Company, error) {
	return nil, p.err
}

type countingProvider struct {
	calls int
}

func (p *countingProvider) ListSymbols(context.Context) ([]provider.Company, error) {
	p.calls++
	return []provider.Company{{Symbol: "AC", Exchange: "TSX"}}, nil
}

// cancellingProvider cancels the run's context during ListSymbols to exercise
// the post-provider cancellation check.
type cancellingProvider struct {
	cancel context.CancelFunc
}

func (p *cancellingProvider) ListSymbols(context.Context) ([]provider.Company, error) {
	p.cancel()
	return nil, context.Canceled
}

// fakeProducer records published messages so tests can assert what the
// refresher sent without a RabbitMQ broker, and can fail publishes on demand.
type fakeProducer struct {
	mu   sync.Mutex
	msgs []*stockstorev1.Stock
	err  error
}

func (f *fakeProducer) PublishStockUpdate(_ context.Context, msg *stockstorev1.Stock) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.msgs = append(f.msgs, msg)
	return f.err
}

// published returns the messages published so far.
func (f *fakeProducer) published() []*stockstorev1.Stock {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*stockstorev1.Stock(nil), f.msgs...)
}

// TestRunOnce_ProviderFailureDoesNotAbortCycle verifies one failing provider
// does not stop the remaining providers or the publish step.
func TestRunOnce_ProviderFailureDoesNotAbortCycle(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	healthy := &countingProvider{}
	producer := &fakeProducer{}
	providers := []provider.Provider{
		errorProvider{err: errors.New(`unexpected Content-Type "text/html"`)},
		healthy,
	}
	ref := refresher.New(providers, log, producer)

	if err := ref.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error: %v; provider failures must not abort the cycle", err)
	}

	if healthy.calls != 1 {
		t.Errorf("healthy provider called %d times, want 1", healthy.calls)
	}
	if msgs := producer.published(); len(msgs) == 0 || msgs[0].GetSymbol() != "AC" {
		t.Errorf("the healthy provider's symbols were not published; got %v", msgs)
	}
	if !strings.Contains(logs.String(), "provider failed to list symbols") {
		t.Errorf("logs do not mention the provider failure: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "refresh cycle completed with provider failures") {
		t.Errorf("logs do not mention the degraded cycle: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "failed_providers=1") {
		t.Errorf("logs do not report failed_providers=1: %s", logs.String())
	}
}

// TestRunOnce_PublishFailureSurfacesError verifies a RabbitMQ publish failure
// is still treated as fatal: RunOnce returns the wrapped error.
func TestRunOnce_PublishFailureSurfacesError(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	healthy := &countingProvider{}
	producer := &fakeProducer{err: errors.New("rabbitmq unavailable")}
	ref := refresher.New([]provider.Provider{healthy}, log, producer)

	err := ref.RunOnce(context.Background())
	if err == nil {
		t.Fatal("RunOnce succeeded although a publish failed; want an error")
	}
	for _, want := range []string{
		"refresh cycle failed",
		"failed to publish update for symbol AC",
		"rabbitmq unavailable",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

// TestRunOnce_HTMLProviderFailurePublishesHealthySymbols reproduces the
// incident end to end with the real provider clients: the TSX provider
// receives an HTML maintenance page served with HTTP 200 while the US
// provider returns JSON. The cycle must log a diagnosable TSX failure, publish
// the US symbols, and return nil.
func TestRunOnce_HTMLProviderFailurePublishesHealthySymbols(t *testing.T) {
	tsx := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "<!DOCTYPE html><html><body>Our websites are currently unavailable.</body></html>")
	}))
	defer tsx.Close()

	us := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{"table": map[string]any{
				"rows": []map[string]string{{"symbol": "ACME", "name": "Acme Corp"}},
			}},
		})
	}))
	defer us.Close()

	var logs bytes.Buffer
	producer := &fakeProducer{}
	providers := []provider.Provider{
		provider.NewClientForTest(tsx.URL),
		provider.NewUSClientForTest(us.URL),
	}
	ref := refresher.New(providers, slog.New(slog.NewTextHandler(&logs, nil)), producer)

	if err := ref.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error: %v", err)
	}

	published := false
	for _, m := range producer.published() {
		if m.GetSymbol() == "ACME" && m.GetExchange() == "NASDAQ" {
			published = true
		}
	}
	if !published {
		t.Errorf("US symbols were not published despite the healthy provider; got %v", producer.published())
	}

	logStr := logs.String()
	for _, want := range []string{
		"provider failed to list symbols",
		"*provider.Client",
		"unexpected Content-Type",
		"text/html",
		"refresh cycle completed with provider failures",
		"failed_providers=1",
	} {
		if !strings.Contains(logStr, want) {
			t.Errorf("logs do not contain %q:\n%s", want, logStr)
		}
	}
	for _, unwanted := range []string{"invalid character '<'", "refresh cycle failed"} {
		if strings.Contains(logStr, unwanted) {
			t.Errorf("logs contain %q:\n%s", unwanted, logStr)
		}
	}
}
```

**Verify (expected failure):**

```bash
go test ./internal/refresher
```

Expected: a compile error because `refresher.New` still takes five arguments
and `RunOnce` does not exist. Do not "fix" the tests by reverting to `Run`.

---

## Task 2 [NEW]: Implement one-shot execution in the refresher

**File:** `internal/refresher/refresher.go` — replace the entire file with:

```go
// Package refresher runs exactly one refresh cycle: it pulls the latest
// symbols from the configured providers and publishes a StockUpdate message to
// RabbitMQ for every tracked symbol. Scheduling is external (the Kubernetes
// CronJob).
package refresher

import (
	"context"
	"fmt"
	"log/slog"

	stockstorev1 "git.wheeli.ca/brian/stocker-store/proto/v1"
	"github.com/anomalyco/stocker-list/internal/provider"
)

// Producer delivers StockUpdate messages. It is the subset of
// *rabbitmq.Producer the refresher needs; declaring it as an interface lets
// tests observe publishes and publish failures without a RabbitMQ broker.
type Producer interface {
	PublishStockUpdate(ctx context.Context, msg *stockstorev1.Stock) error
}

type Refresher struct {
	providers []provider.Provider
	log       *slog.Logger
	producer  Producer
}

func New(providers []provider.Provider, log *slog.Logger, producer Producer) *Refresher {
	return &Refresher{providers: providers, log: log, producer: producer}
}

// RunOnce performs exactly one full refresh cycle and returns when it
// completes. Provider failures are logged and tolerated; a publish failure is
// returned. A cancelled context is also returned as an error so an interrupted
// run does not report success.
func (r *Refresher) RunOnce(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.log.Info("refresh cycle starting")

	companies, failed, err := r.discoverAllSymbols(ctx)
	if err != nil {
		return fmt.Errorf("refresh cycle failed: %w", err)
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if failed > 0 {
		r.log.Warn("refresh cycle completed with provider failures",
			"symbols", len(companies), "failed_providers", failed)
	} else {
		r.log.Info("refresh cycle complete", "symbols", len(companies))
	}
	return nil
}

// discoverAllSymbols pulls the current symbol list from all providers and
// publishes a StockUpdate message to RabbitMQ for every symbol discovered.
// A failure from one provider is logged and does not prevent the remaining
// providers from being used; the cycle still succeeds.
func (r *Refresher) discoverAllSymbols(ctx context.Context) ([]provider.Company, int, error) {
	var allCompanies []provider.Company
	var failed int

	for _, p := range r.providers {
		companies, err := p.ListSymbols(ctx)
		if err != nil {
			failed++
			r.log.Error("provider failed to list symbols",
				"provider", fmt.Sprintf("%T", p), "error", err)
			continue
		}
		allCompanies = append(allCompanies, companies...)
	}

	if r.producer != nil {
		for _, c := range allCompanies {
			msg := &stockstorev1.Stock{
				Symbol:   c.Symbol,
				Exchange: c.Exchange,
			}
			if err := r.producer.PublishStockUpdate(ctx, msg); err != nil {
				return nil, failed, fmt.Errorf("failed to publish update for symbol %s on exchange %s: %w", c.Symbol, c.Exchange, err)
			}
		}
	}

	return allCompanies, failed, nil
}
```

**Verify:**

```bash
gofmt -w internal/refresher/refresher.go
go test ./internal/refresher -count=1
```

Expected: all refresher tests pass.

---

## Task 3 [NEW]: Remove `REFRESH_CHECK_INTERVAL` from configuration

**File:** `internal/config/config.go` — replace the entire file with:

```go
// Package config centralizes all runtime configuration, loaded from
// environment variables so the RabbitMQ target is fully configurable without
// code changes.
package config

import (
	"fmt"
	"os"
	"strings"
)

type Config struct {
	// RabbitMQ
	RabbitMQURL          string
	RabbitMQExchange     string
	RabbitMQExchangeType string
	RabbitMQRoutingKey   string
}

func Load() (*Config, error) {
	cfg := &Config{
		RabbitMQURL:          getEnvPreserveEmpty("RABBITMQ_URL", "amqp://guest:guest@192.168.1.48:5672/"),
		RabbitMQExchange:     getEnv("RABBITMQ_EXCHANGE", "stock.update"),
		RabbitMQExchangeType: getEnv("RABBITMQ_EXCHANGE_TYPE", "topic"),
		RabbitMQRoutingKey:   getEnv("RABBITMQ_ROUTING_KEY", "stock.update.v1"),
	}

	exchangeType := strings.ToLower(strings.TrimSpace(cfg.RabbitMQExchangeType))
	switch exchangeType {
	case "direct", "topic", "fanout", "headers":
		cfg.RabbitMQExchangeType = exchangeType
	default:
		return nil, fmt.Errorf("RABBITMQ_EXCHANGE_TYPE must be one of direct, topic, fanout, headers, got %q", exchangeType)
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

// getEnvPreserveEmpty returns the variable's value even when it is empty.
// Only an absent variable yields the fallback. This lets RABBITMQ_URL= disable
// publishing while an unset RABBITMQ_URL selects the default broker.
func getEnvPreserveEmpty(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}
```

`getEnvInt` and `getEnvDuration` are removed along with the `strconv` and
`time` imports.

**File:** `internal/config/config_test.go`

1. Delete the three interval tests: `TestLoad_Defaults`,
   `TestLoad_CustomValues`, and `TestLoad_InvalidDuration`.
2. Delete the now-unused `time` import.
3. Add this test after the imports. It sets the variable to a duration the old
   `<= 0` validation rejected, and unsets the `RABBITMQ_*` variables so the
   test does not depend on the developer's shell:

```go
// TestLoad_IgnoresRefreshCheckInterval proves the removed variable has no
// effect: cadence is owned by the Kubernetes CronJob schedule. Before the
// removal, 0s failed the RefreshCheckInterval > 0 validation.
func TestLoad_IgnoresRefreshCheckInterval(t *testing.T) {
	unsetEnv(t, rabbitMQEnvKeys...)
	t.Setenv("REFRESH_CHECK_INTERVAL", "0s")
	if _, err := Load(); err != nil {
		t.Fatalf("Load() error: %v; REFRESH_CHECK_INTERVAL is no longer read", err)
	}
}
```

Leave all `RABBITMQ_*` tests unchanged.

**Verify:**

```bash
gofmt -w internal/config/config.go internal/config/config_test.go
go test ./internal/config -count=1
```

Expected: config tests pass.

---

## Task 4 [NEW]: Make `cmd/server` a one-shot process

**File:** `cmd/server/main.go` — replace the entire file with:

```go
// Command stocker-list runs exactly one refresh cycle: it fetches the full
// stock symbol universe from every provider, publishes each symbol to
// RabbitMQ, and exits. Kubernetes CronJob scheduling replaces the old
// in-process refresher loop.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/anomalyco/stocker-list/internal/config"
	"github.com/anomalyco/stocker-list/internal/provider"
	"github.com/anomalyco/stocker-list/internal/rabbitmq"
	"github.com/anomalyco/stocker-list/internal/refresher"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := run(log); err != nil {
		log.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// SIGTERM (Pod termination / activeDeadlineSeconds) cancels in-flight
	// provider requests and publishes so the process exits promptly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	providers := []provider.Provider{
		provider.NewClient(),   // TSX — no key, always on
		provider.NewUSClient(), // US — NASDAQ screener, no key, always on
	}

	producer, err := rabbitmq.NewProducer(cfg, log)
	if err != nil {
		return fmt.Errorf("create rabbitmq producer: %w", err)
	}
	defer func() {
		if cerr := producer.Close(); cerr != nil {
			log.Warn("close rabbitmq producer", "error", cerr)
		}
	}()

	ref := refresher.New(providers, log, producer)
	if err := ref.RunOnce(ctx); err != nil {
		return fmt.Errorf("run refresh cycle: %w", err)
	}

	log.Info("one-shot run complete; exiting")
	return nil
}
```

There is no goroutine, no `sync.WaitGroup`, no ticker, and no shutdown
coordination. A successful run returns `nil` and the process exits `0`; a
config or publish error exits `1`. A close error is logged as a warning and
does not change the exit code, because all publishes were already confirmed.

**Verify:**

```bash
gofmt -w cmd/server/main.go
go build ./...
```

Expected: the whole module builds.

---

## Task 5 [NEW]: Update repository prose and env templates

### `design.md`

Replace the first paragraph with:

```markdown
# stocker-list

A Go service that pulls stock symbols from the internet and publishes each one
to a RabbitMQ exchange as a `stockstorev1.Stock` protobuf message. It performs
exactly one full refresh cycle per process invocation and exits; scheduling is
external (a Kubernetes CronJob).
```

Also change "before failing the refresh cycle" to "before failing the run".

### `.env.example` and `.env.podman`

Replace `.env.example` with exactly:

```bash
# Copy to .env and fill in, or export these as real environment variables.
#
# stocker-list performs one full refresh cycle per process invocation and
# exits. Scheduling is external: the Kubernetes CronJob `schedule` field (see
# deploy/kubernetes/stocker-list.yaml) or your own timer.

# --- RabbitMQ ---
# AMQP URI. Unset uses the default amqp://guest:guest@192.168.1.48:5672/.
# Explicitly empty (RABBITMQ_URL=) = publishing DISABLED (producer is a graceful
# no-op; no broker is dialed). Stock RabbitMQ refuses the guest account from
# non-loopback addresses, so a remote broker needs a dedicated user here.
RABBITMQ_URL=amqp://guest:guest@192.168.1.48:5672/
# Exchange the publisher declares and publishes to (default below).
RABBITMQ_EXCHANGE=stock.update
# One of direct, topic, fanout, headers; anything else is a startup error.
RABBITMQ_EXCHANGE_TYPE=topic
# Routing key used for every message (default below).
RABBITMQ_ROUTING_KEY=stock.update.v1
```

Replace `.env.podman` with the same content, changing the first two lines to:

```bash
# Environment variables for the local Podman deployment (stocker-list).
#
```

Remove `GRPC_PORT` and `REFRESH_CHECK_INTERVAL` from both files.

### `README.md`

Make these edits:

1. **Intro and architecture:** replace "publishes ... on a refresh timer" and
   "driven by a background refresh loop" with one-shot language. Update the
   architecture block:
   ```
   cmd/server         wires everything together and runs exactly one cycle
   internal/config    env-var configuration (RabbitMQ only)
   internal/provider  stock symbol provider clients (TSX, US)
   internal/refresher one-shot cycle: fetch all symbols, publish, return
   internal/rabbitmq  RabbitMQ producer (no-op when publishing is disabled)
   ```
2. **RabbitMQ section:** change "fatal to the refresh cycle" to "fatal to the
   run".
3. **Provider degradation section:** change "A provider failure does not stop
   the service" to "A provider failure does not abort the run", "The next
   refresh interval retries every provider" to "The next scheduled run retries
   every provider", "the refresher keeps running" to "the run still completes
   and publishes the healthy providers' symbols", and "the only refresh
   failure it treats as an error" to "the only run failure it treats as an
   error". The process exits `0` after publishing the healthy provider's
   symbols.
4. **Environment variables:** delete the entire `### Core` subsection and its
   `REFRESH_CHECK_INTERVAL` table. Add a short `### Cadence` note:
   ```markdown
   ### Cadence

   There is no refresh-interval environment variable. `stocker-list` performs
   one cycle per invocation. In Kubernetes, the CronJob `schedule` in
   `deploy/kubernetes/stocker-list.yaml` controls cadence; change that field
   and re-apply the manifest.
   ```
5. **Key semantics:** change "the service still runs the refresh loop" to
   "the process still performs one cycle". Remove the `REFRESH_CHECK_INTERVAL`
   row and any remaining references.
6. **Running it / Local:** state that `make run` performs one cycle and exits.
   Also correct the US provider description: the screener responses are
   paginated, and symbols are tagged with the listing exchange (`NASDAQ`,
   `NYSE`, or `AMEX`), not `Exchange: "US"`. Both match
   `internal/provider/us.go`.
7. **Podman:** state that `make podman-up` starts one local container that runs
   one cycle and stops (`restart: "no"`), and that `.env.podman` supplies its
   environment (copy values from `.env.example` if needed).
8. **Quadlet:** delete the entire "Quadlet (rootless, systemd user service)"
   section and all `make quadlet-*` instructions.
9. **Deployment:** add a short section:
   ```markdown
   ### Kubernetes

   Deployment manifests live in `deploy/kubernetes/`. `stocker-list.yaml`
   contains the Namespace, ConfigMap, and CronJob; `secret.yaml` is a template
   for the RabbitMQ credentials. See
   `docs/design-kubernetes-deployment.md` and
   `docs/plan-kubernetes-deployment.md`.
   ```

**Verify:**

```bash
grep -rn "REFRESH_CHECK_INTERVAL" README.md design.md .env.example .env.podman
```

Expected: no output.

---

## Task 6 [REPLACE]: Consolidate the Kubernetes manifests

The old manifest files are superseded. Delete them and create a single main
manifest.

```bash
git rm deploy/kubernetes/configmap.yaml deploy/kubernetes/deployment.yaml
```

Create `deploy/kubernetes/stocker-list.yaml` with exactly this content:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: stocker
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: stocker-list-config
  namespace: stocker
  labels:
    app: stocker-list
data:
  RABBITMQ_EXCHANGE: stock.update
  RABBITMQ_EXCHANGE_TYPE: topic
  RABBITMQ_ROUTING_KEY: stock.update.v1
---
apiVersion: batch/v1
kind: CronJob
metadata:
  name: stocker-list
  namespace: stocker
  labels:
    app: stocker-list
spec:
  schedule: "0 6 * * *"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 3
  jobTemplate:
    metadata:
      labels:
        app: stocker-list
    spec:
      activeDeadlineSeconds: 600
      backoffLimit: 1
      template:
        metadata:
          labels:
            app: stocker-list
        spec:
          restartPolicy: Never
          imagePullSecrets:
            - name: wheeli-registry
          containers:
            - name: stocker-list
              image: git.wheeli.ca/brian/stocker-list:latest
              imagePullPolicy: Always
              envFrom:
                - configMapRef:
                    name: stocker-list-config
                - secretRef:
                    name: stocker-list-env
              resources:
                requests:
                  cpu: 50m
                  memory: 128Mi
                limits:
                  memory: 512Mi
              securityContext:
                allowPrivilegeEscalation: false
                capabilities:
                  drop: ["ALL"]
                runAsNonRoot: true
                seccompProfile:
                  type: RuntimeDefault
                runAsUser: 65532
                runAsGroup: 65532
```

This file must contain exactly three documents in this order: `Namespace`,
`ConfigMap`, `CronJob`. Do not add a Service, Deployment, probes, ports, a
service account, pod-level `securityContext`, or `app.kubernetes.io/*` labels.
The `imagePullSecrets` entry is deliberate (private `git.wheeli.ca` registry).

**Verify:**

```bash
find deploy/kubernetes -maxdepth 1 -type f | sort
```

Expected: `deploy/kubernetes/secret.yaml` and
`deploy/kubernetes/stocker-list.yaml` (the Secret is rewritten in Task 7).

---

## Task 7 [REPLACE]: Rewrite the Secret as a template

Replace `deploy/kubernetes/secret.yaml` with exactly:

```yaml
# Fill in the real passwords, apply it, and don't commit the filled-in version.
#
# RABBITMQ_URL assumes the RabbitMQ client Service is named "rabbitmq" in
# namespace "messaging". Verify with:
#   kubectl -n messaging get svc
apiVersion: v1
kind: Secret
metadata:
  name: stocker-list-env
  namespace: stocker
  labels:
    app: stocker-list
type: Opaque
stringData:
  RABBITMQ_URL: "amqp://stocker:RABBITMQ_PASSWORD@rabbitmq.messaging.svc.cluster.local:5672/"
```

Notes:

- `RABBITMQ_PASSWORD` is an ALL-CAPS placeholder. The committed file must not
  contain a real password.
- If the broker Service or namespace differs, edit the host; if the password
  contains `@`, `:`, `/`, `?`, or `#`, URL-encode it.
- Apply the real value from a temporary copy, never from the tracked file.
  Use `mktemp` and a `0600` mode so the credential lands in a private,
  unpredictable path:
  ```bash
  tmp="$(mktemp)"
  chmod 600 "$tmp"
  cp deploy/kubernetes/secret.yaml "$tmp"
  $EDITOR "$tmp"   # replace RABBITMQ_PASSWORD
  kubectl apply -f "$tmp"
  rm -f "$tmp"
  ```
- `git status` must show `deploy/kubernetes/secret.yaml` unchanged except for
  the template above.

**Verify:**

```bash
grep -n "RABBITMQ_PASSWORD" deploy/kubernetes/secret.yaml
git diff -- deploy/kubernetes/secret.yaml
```

Expected: the placeholder is present; no real secret value appears.

---

## Task 8 [REMOVE]: Delete the quadlet and fix the Makefile

The quadlet is a long-running `Restart=always` unit that no longer matches
one-shot behavior, and `quadlet-install` references a nonexistent file.
Remove it:

```bash
git rm -r deploy/quadlet
```

Replace `Makefile` with exactly (recipes use **tabs**, not spaces):

```make
.PHONY: tidy run build podman-up podman-down podman-build

tidy:
	go mod tidy

build:
	mkdir -p bin
	go build -o bin/stocker-list ./cmd/server

run:
	go run ./cmd/server

podman-build:
	podman-compose -f podman-compose.yml build

podman-up:
	podman-compose -f podman-compose.yml up -d

podman-down:
	podman-compose -f podman-compose.yml down
```

This removes the broken `quadlet-install`, `quadlet-build`, and
`quadlet-enable` targets, removes the nonexistent `proto` / `proto-protoc`
targets from `.PHONY`, and fixes the `build` / `run` recipes, which currently
use spaces and fail with `missing separator`. The `build` target also creates
the gitignored `bin/` directory before writing `bin/stocker-list`.

**Verify:**

```bash
make -n build run
make build
test -x bin/stocker-list && echo "build OK"
```

Expected: the dry run prints without a `missing separator` error, the build
succeeds, and `bin/stocker-list` exists.

---

## Task 9 [REPLACE]: Simplify `podman-compose.yml`

This repository defines only `stocker-list`. The other services reference
images and inter-service dependencies that do not exist here, and the
`stocker-list` service has vestigial `ports` and a Postgres dependency.
Replace the entire file with:

```yaml
# Local development only. In Kubernetes, stocker-list runs as a CronJob (see
# deploy/kubernetes/). One container invocation performs one full cycle and
# exits; restart is disabled because scheduling is external.
services:
  stocker-list:
    build:
      context: .
      dockerfile: Containerfile
    env_file: .env.podman
    restart: "no"
```

Keep `restart` quoted (`"no"`), otherwise YAML parses it as a boolean.

**Verify:**

```bash
grep -n "50051\|depends_on\|postgres" podman-compose.yml || echo "no vestigial entries (expected)"
grep -n 'restart: "no"' podman-compose.yml
```

Expected: no `50051`, `depends_on`, or `postgres` match; exactly one
`restart: "no"` match. The comment above it also contains the word "restart",
which is why the two checks are separate.

---

## Task 10 [NEW]: Clean up the Containerfile

Replace `Containerfile` with:

```dockerfile
# Build stage
FROM docker.io/golang:1.25-bookworm AS build
WORKDIR /src

COPY . .

RUN go mod tidy
RUN CGO_ENABLED=0 go build -o /out/stocker-list ./cmd/server

# Runtime stage
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/stocker-list /stocker-list
USER 65532:65532
ENTRYPOINT ["/stocker-list"]
```

Changes:

- Remove `EXPOSE 50051`; the binary listens on no port.
- Remove the unused `ENV PATH` line; no tools are installed into
  `/root/go/bin`.
- Add `USER 65532:65532` so the image's non-root default matches the Pod
  `securityContext`.

**Verify:**

```bash
grep -n "EXPOSE" Containerfile || echo "no EXPOSE (expected)"
grep -n "USER 65532:65532" Containerfile
```

---

## Task 11 [VERIFY]: Build, vet, and test the Go module

Run from the repository root:

```bash
gofmt -l cmd internal
go build ./...
go vet ./...
go test ./... -count=1
```

Expected:

- `gofmt -l` prints nothing.
- `go build`, `go vet`, and `go test` exit `0`.
- `TestProducerIntegration` is skipped unless `RABBITMQ_TEST_URL` is set;
  that is expected and not a failure.

If any test fails, fix the implementation before continuing; do not weaken the
tests.

---

## Task 12 [VERIFY]: Validate the manifests (best effort)

`kubectl` may not be installed. Run the structural check first; it requires
only Python 3 and PyYAML (available in the current environment; if PyYAML is
missing, skip to the static greps and inspect the files manually):

```bash
python3 - <<'PY'
import sys
import yaml

# file -> [(kind, name, namespaced), ...]
expected = {
    "deploy/kubernetes/stocker-list.yaml": [
        ("Namespace", "stocker", False),
        ("ConfigMap", "stocker-list-config", True),
        ("CronJob", "stocker-list", True),
    ],
    "deploy/kubernetes/secret.yaml": [
        ("Secret", "stocker-list-env", True),
    ],
}
ok = True
for path, resources in expected.items():
    with open(path) as f:
        docs = [d for d in yaml.safe_load_all(f) if d]
    got = [(d.get("kind"), d.get("metadata", {}).get("name")) for d in docs]
    want = [(kind, name) for kind, name, _ in resources]
    print(f"{path}: {got}")
    if got != want:
        ok = False
        print(f"  ERROR: want {want}", file=sys.stderr)
    for doc, (kind, name, namespaced) in zip(docs, resources):
        ns = doc.get("metadata", {}).get("namespace")
        if namespaced and ns != "stocker":
            ok = False
            print(f"  ERROR: {kind}/{name} namespace={ns!r}, want 'stocker'", file=sys.stderr)
        if not namespaced and ns is not None:
            ok = False
            print(f"  ERROR: {kind}/{name} must not set namespace", file=sys.stderr)
sys.exit(0 if ok else 1)
PY
```

Expected output:

```
deploy/kubernetes/stocker-list.yaml: [('Namespace', 'stocker'), ('ConfigMap', 'stocker-list-config'), ('CronJob', 'stocker-list')]
deploy/kubernetes/secret.yaml: [('Secret', 'stocker-list-env')]
```

Additional static checks:

```bash
find deploy/kubernetes -maxdepth 1 -type f | sort
grep -rn "app.kubernetes.io" deploy/kubernetes && echo "ERROR: old labels remain" || true
grep -rn "REFRESH_CHECK_INTERVAL" cmd internal deploy .env.example .env.podman podman-compose.yml Makefile README.md design.md \
  | grep -v "^internal/config/config_test.go:" || true
grep -n 'schedule: "0 6 \* \* \*"' deploy/kubernetes/stocker-list.yaml
grep -n "concurrencyPolicy: Forbid" deploy/kubernetes/stocker-list.yaml
grep -n "restartPolicy: Never" deploy/kubernetes/stocker-list.yaml
grep -n "activeDeadlineSeconds: 600" deploy/kubernetes/stocker-list.yaml
grep -n "backoffLimit: 1" deploy/kubernetes/stocker-list.yaml
```

Expected: exactly two files; no old labels; no `REFRESH_CHECK_INTERVAL` in
production code, manifests, or env files; all five CronJob fields found. The
grep deliberately excludes `internal/config/config_test.go`, the one place
where the variable name remains: `TestLoad_IgnoresRefreshCheckInterval` uses it
to prove the variable is ignored. The design and plan docs intentionally
mention the removal and are excluded from the grep.

If `kubectl` is available, run client-side dry runs:

```bash
if command -v kubectl >/dev/null 2>&1; then
  kubectl apply --dry-run=client -f deploy/kubernetes/stocker-list.yaml
  kubectl apply --dry-run=client -f deploy/kubernetes/secret.yaml
  kubectl create --dry-run=client -f deploy/kubernetes/stocker-list.yaml -o name
else
  echo "kubectl not installed; skipping API dry-run (best effort)"
fi
```

Expected (when kubectl is present): each command exits `0` and reports the
resources it would create. Do not apply the Secret with its placeholder.

---

## Task 13 [MIGRATION]: Remove the old Deployment from the cluster

The old Deployment is no longer in the manifests, but `kubectl apply` does not
delete resources that disappear from a file. If it is left running, the old
24-hour loop and the CronJob will both publish.

1. Apply the new main manifest (updates the ConfigMap, creates the CronJob):
   ```bash
   kubectl apply -f deploy/kubernetes/stocker-list.yaml
   ```
2. Immediately suspend the CronJob so it cannot start before the Secret
   exists or while the old Deployment is still publishing:
   ```bash
   kubectl -n stocker patch cronjob stocker-list --type=merge \
     -p '{"spec":{"suspend":true}}'
   ```
3. Confirm the old ConfigMap key is gone:
   ```bash
   kubectl -n stocker get configmap stocker-list-config \
     -o jsonpath='{.data.REFRESH_CHECK_INTERVAL}{"\n"}'
   ```
   Expected: an empty line. If the key persists (the ConfigMap was not
   previously managed by `kubectl apply`), remove it:
   ```bash
   kubectl -n stocker patch configmap stocker-list-config --type=json \
     -p='[{"op":"remove","path":"/data/REFRESH_CHECK_INTERVAL"}]'
   ```
4. Delete the old Deployment:
   ```bash
   kubectl -n stocker delete deployment stocker-list --ignore-not-found
   ```
5. Verify only the CronJob remains:
   ```bash
   kubectl -n stocker get deploy,svc,cronjob | grep -w stocker-list || true
   ```
   Expected: the CronJob line only; no Deployment or Service.
6. Ensure the `wheeli-registry` pull Secret exists in namespace `stocker`
   (see Prerequisites).
7. Apply the filled-in Secret from a temporary copy (see Task 7).
8. Unsuspend the CronJob now that the Secret exists and the old publisher is
   gone:
   ```bash
   kubectl -n stocker patch cronjob stocker-list --type=merge \
     -p '{"spec":{"suspend":false}}'
   ```

---

## Task 14 [NEW]: Operational smoke test

Preconditions: image pushed, Secret applied, broker reachable, and the
`stock.update` exchange available.

### 14a. Manual Job

Trigger a Job from the CronJob, wait for completion, and inspect the result.
Do not trigger it while a scheduled Job is active: `concurrencyPolicy: Forbid`
applies to Jobs created by the CronJob controller, not to a manually created
Job. For the same reason, a manual Job is not covered by the CronJob's history
limits, so delete it in 14e.

```bash
name="stocker-list-smoke-$(date +%s)"
kubectl -n stocker create job --from=cronjob/stocker-list "$name"
kubectl -n stocker wait --for=condition=complete "job/$name" --timeout=600s
kubectl -n stocker get "job/$name" -o jsonpath='{.status.succeeded}{"\n"}'
kubectl -n stocker logs "job/$name" --tail=200
```

Expected:

- `status.succeeded` prints `1`.
- The JSON logs contain `refresh cycle starting`, `refresh cycle complete`
  with a non-zero `symbols` value, and `one-shot run complete; exiting`.
- No `"level":"ERROR"` entries for a healthy run. If one provider is down, the
  logs contain `provider failed to list symbols` and `"failed_providers":1`,
  but the Job still succeeds.

### 14b. Messages reach the exchange

Publisher confirms guarantee exchange receipt, not consumer delivery. Bind a
temporary queue **before** triggering a run, then drain it. The commands below
use `rabbitmqadmin`; if it is not available, use the broker's management UI or
the equivalent `kubectl`/client tooling:

```bash
# Broker host/user/password come from outside the repo.
rabbitmqadmin -H <broker-host> -u stocker -p '<password>' \
  declare queue name=stocker-list-smoke durable=false auto_delete=true
rabbitmqadmin -H <broker-host> -u stocker -p '<password>' \
  declare binding source=stock.update destination=stocker-list-smoke routing_key=stock.update.v1

# Trigger another run (repeat 14a), then:
rabbitmqadmin -H <broker-host> -u stocker -p '<password>' \
  get queue=stocker-list-smoke count=5 ackmode=ack_requeue_false
```

Expected: at least one message body (protobuf) with `stock.update.v1` type and
a symbol message ID. If a downstream consumer is running, its logs are an
equivalent check.

### 14c. Schedule and policy

```bash
kubectl -n stocker get cronjob stocker-list
kubectl -n stocker get cronjob stocker-list -o jsonpath='{.spec.schedule}{"\n"}'
kubectl -n stocker get cronjob stocker-list -o jsonpath='{.spec.concurrencyPolicy}{"\n"}'
```

Expected: the default output shows `SUSPEND False`; the schedule jsonpath
prints `0 6 * * *` and the concurrency-policy jsonpath prints `Forbid`
(concurrency policy is not a default column).

### 14d. Optional short-schedule test

To observe the CronJob controller without waiting until 06:00:

```bash
kubectl -n stocker patch cronjob stocker-list --type=merge \
  -p '{"spec":{"schedule":"*/2 * * * *"}}'
# Wait for a scheduled Job, verify it, then restore:
kubectl -n stocker get jobs -l app=stocker-list
kubectl -n stocker patch cronjob stocker-list --type=merge \
  -p '{"spec":{"schedule":"0 6 * * *"}}'
```

Always restore the daily schedule.

### 14e. Cleanup

Run this in the same shell as 14a (where `$name` was set), or substitute the
Job name explicitly:

```bash
kubectl -n stocker delete job "$name"
rabbitmqadmin -H <broker-host> -u stocker -p '<password>' \
  delete queue name=stocker-list-smoke
```

Confirm the final cluster state:

```bash
kubectl -n stocker get cronjob,job,pods -l app=stocker-list
```

Expected: the CronJob exists; only history-limited Jobs and no long-running
Pod remain.

---

## Completion checklist

- [ ] `internal/refresher` runs exactly one cycle and returns; no ticker.
- [ ] `REFRESH_CHECK_INTERVAL` is gone from production code, manifests, env
      files, and README. The only remaining reference is the config test that
      proves the variable is ignored.
- [ ] `cmd/server` exits `0` on success and `1` on publish/config failure.
- [ ] `deploy/kubernetes/` has exactly `stocker-list.yaml` and `secret.yaml`.
- [ ] `stocker-list.yaml` has Namespace → ConfigMap → CronJob, no Service or
      Deployment, `app: stocker-list` labels, `envFrom`, exact security
      context, and resources.
- [ ] `secret.yaml` is an unfilled template with `RABBITMQ_PASSWORD`.
- [ ] Quadlet directory and Makefile targets are removed; compose is a
      one-shot local service.
- [ ] `Containerfile` has no `EXPOSE` and runs as UID/GID `65532`.
- [ ] `gofmt`, `go build`, `go vet`, and `go test ./...` pass.
- [ ] A manual Job completes with `succeeded: 1` and messages arrive on the
      exchange.
- [ ] The old Deployment is deleted from the cluster.
