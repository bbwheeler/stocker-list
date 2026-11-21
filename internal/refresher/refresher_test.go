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
