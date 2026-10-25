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
	"time"

	kafkastockv1 "git.wheeli.ca/brian/stocker-store/proto/v1"
	"github.com/anomalyco/stocker-list/internal/config"
	"github.com/anomalyco/stocker-list/internal/kafka"
	"github.com/anomalyco/stocker-list/internal/provider"
	"github.com/anomalyco/stocker-list/internal/refresher"
)

func testConfig() *config.Config {
	// A long interval keeps Run's ticker from firing again during the test,
	// so we only ever observe the initial immediate tick.
	return &config.Config{RefreshCheckInterval: time.Hour}
}

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

// TestRun_ExitsOnCancel verifies the refresher runs an immediate cycle and
// exits promptly once its context is cancelled.
func TestRun_ExitsOnCancel(t *testing.T) {
	srv := symbolsHandler(t, []string{"AC", "RY"})
	defer srv.Close()

	p := provider.NewClientForTest(srv.URL)
	log := testLog(t)
	ref := refresher.New(testConfig(), nil, []provider.Provider{p}, log, &kafka.Producer{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		ref.Run(ctx)
		close(done)
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit after context cancellation")
	}
}

// TestRun_PublishesSymbols verifies the refresher drives every provider and
// pushes each discovered symbol through the Kafka producer without error.
func TestRun_PublishesSymbols(t *testing.T) {
	srv := symbolsHandler(t, []string{"AC", "RY", "TD"})
	defer srv.Close()

	p := provider.NewClientForTest(srv.URL)
	log := testLog(t)
	producer := &kafka.Producer{Topic: kafka.StockUpdateTopic}
	ref := refresher.New(testConfig(), nil, []provider.Provider{p}, log, producer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		ref.Run(ctx)
		close(done)
	}()

	// Allow the immediate tick (which pulls symbols and calls the no-op
	// producer for each) to complete, then shut down cleanly.
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit after context cancellation")
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

// fakeProducer records published messages so tests can assert what the
// refresher sent without a Kafka broker, and can fail publishes on demand.
// The mutex lets tests poll publishes while Run is still executing.
type fakeProducer struct {
	mu   sync.Mutex
	msgs []*kafkastockv1.Stock
	err  error
}

func (f *fakeProducer) PublishStockUpdate(_ context.Context, msg *kafkastockv1.Stock) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.msgs = append(f.msgs, msg)
	return f.err
}

// published returns the messages published so far.
func (f *fakeProducer) published() []*kafkastockv1.Stock {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*kafkastockv1.Stock(nil), f.msgs...)
}

func TestRun_ProviderFailureDoesNotStopLoop(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	healthy := &countingProvider{}
	producer := &fakeProducer{}
	providers := []provider.Provider{
		errorProvider{err: errors.New(`unexpected Content-Type "text/html"`)},
		healthy,
	}
	ref := refresher.New(testConfig(), nil, providers, log, producer)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		ref.Run(ctx)
		close(done)
	}()

	// Give the immediate tick time to run. The failing provider must not make
	// Run exit.
	time.Sleep(200 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("Run exited after one provider failed; want it to keep running")
	default:
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit after context cancellation")
	}

	if healthy.calls == 0 {
		t.Error("healthy provider was not called")
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

// TestRun_PublishFailureSurfacesError verifies a Kafka publish failure is
// still treated as fatal: Run returns the wrapped error instead of logging
// and continuing.
func TestRun_PublishFailureSurfacesError(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	healthy := &countingProvider{}
	producer := &fakeProducer{err: errors.New("kafka unavailable")}
	ref := refresher.New(testConfig(), nil, []provider.Provider{healthy}, log, producer)

	err := ref.Run(context.Background())
	if err == nil {
		t.Fatal("Run succeeded although a publish failed; want an error")
	}
	for _, want := range []string{
		"symbol discovery failed",
		"failed to publish update for symbol AC",
		"kafka unavailable",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

// TestRun_HTMLProviderFailurePublishesHealthySymbols reproduces the incident
// end to end with the real provider clients: the TSX provider receives an
// HTML maintenance page served with HTTP 200 while the US provider returns
// JSON. The cycle must log a diagnosable TSX failure, publish the US
// symbols, and keep running.
func TestRun_HTMLProviderFailurePublishesHealthySymbols(t *testing.T) {
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
	ref := refresher.New(testConfig(), nil, providers, slog.New(slog.NewTextHandler(&logs, nil)), producer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		ref.Run(ctx)
		close(done)
	}()

	// Wait for the immediate tick to publish the US symbols. Cancelling
	// earlier could cancel the in-flight US fetch and mask the assertion;
	// Run exiting on its own is the original bug and must fail the test.
	deadline := time.Now().Add(5 * time.Second)
	for len(producer.published()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("US symbols were not published within 5s")
		}
		select {
		case <-done:
			t.Fatal("Run exited after an HTML response; want it to keep running")
		default:
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit after context cancellation")
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
	for _, unwanted := range []string{"invalid character '<'", "symbol discovery failed"} {
		if strings.Contains(logStr, unwanted) {
			t.Errorf("logs contain %q:\n%s", unwanted, logStr)
		}
	}
}
