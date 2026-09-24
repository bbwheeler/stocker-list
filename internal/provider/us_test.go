package provider

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestUS_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("apikey"); got != "testkey" {
			t.Errorf("server saw apikey %q, want %q", got, "testkey")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"symbol":"SPY","name":"SPDR S&P 500 ETF Trust","currency":"USD"}]`))
	}))
	defer srv.Close()

	c := NewUSClientForTest(srv.URL, "testkey")
	symbols, err := c.ListSymbols(context.Background())
	if err != nil {
		t.Fatalf("ListSymbols: %v", err)
	}
	if len(symbols) != 1 {
		t.Fatalf("got %d symbols, want 1", len(symbols))
	}
	if symbols[0].Symbol != "SPY" {
		t.Errorf("symbols[0].Symbol = %q, want %q", symbols[0].Symbol, "SPY")
	}
	if symbols[0].Name != "SPDR S&P 500 ETF Trust" {
		t.Errorf("symbols[0].Name = %q, want %q", symbols[0].Name, "SPDR S&P 500 ETF Trust")
	}
	if symbols[0].Exchange != "US" {
		t.Errorf("symbols[0].Exchange = %q, want %q", symbols[0].Exchange, "US")
	}
	if symbols[0].Currency != "USD" {
		t.Errorf("symbols[0].Currency = %q, want %q", symbols[0].Currency, "USD")
	}
}

func TestUS_ExchangeAndCurrencyOverrides(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"symbol":"BRK.B","name":"Berkshire","currency":"CAD","exchange":"TSX"}]`))
	}))
	defer srv.Close()

	c := NewUSClientForTest(srv.URL, "testkey")
	symbols, err := c.ListSymbols(context.Background())
	if err != nil {
		t.Fatalf("ListSymbols: %v", err)
	}
	if len(symbols) != 1 {
		t.Fatalf("got %d symbols, want 1", len(symbols))
	}
	if symbols[0].Exchange != "TSX" {
		t.Errorf("symbols[0].Exchange = %q, want %q", symbols[0].Exchange, "TSX")
	}
	if symbols[0].Currency != "CAD" {
		t.Errorf("symbols[0].Currency = %q, want %q", symbols[0].Currency, "CAD")
	}
}

func TestUS_EmptyResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	c := NewUSClientForTest(srv.URL, "testkey")
	symbols, err := c.ListSymbols(context.Background())
	if err != nil {
		t.Fatalf("ListSymbols: %v", err)
	}
	if len(symbols) != 0 {
		t.Errorf("got %d symbols, want 0", len(symbols))
	}
}

func TestUS_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewUSClientForTest(srv.URL, "testkey")
	_, err := c.ListSymbols(context.Background())
	if err == nil {
		t.Fatal("expected error for HTTP 500")
	}
}

func TestUS_InvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("[invalid"))
	}))
	defer srv.Close()

	c := NewUSClientForTest(srv.URL, "testkey")
	_, err := c.ListSymbols(context.Background())
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}
