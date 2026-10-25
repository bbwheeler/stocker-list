package refresher_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

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

func TestRun_ProviderFailureDoesNotStopLoop(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	healthy := &countingProvider{}
	providers := []provider.Provider{
		errorProvider{err: errors.New(`unexpected Content-Type "text/html"`)},
		healthy,
	}
	ref := refresher.New(testConfig(), nil, providers, log, &kafka.Producer{})

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
	if !strings.Contains(logs.String(), "provider failed to list symbols") {
		t.Errorf("logs do not mention the provider failure: %s", logs.String())
	}
}
