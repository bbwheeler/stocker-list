package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestListSymbols_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(tsxResponse{
			Results: []tsxEntry{
				{Symbol: "AC", Name: "Air Canada"},
				{Symbol: "ATD", Name: "Alimentation Couche-Tard Inc."},
			},
		})
	}))
	defer srv.Close()

	c := &Client{baseURL: srv.URL, httpClient: srv.Client()}
	symbols, err := c.ListSymbols(context.Background())
	if err != nil {
		t.Fatalf("ListSymbols: %v", err)
	}
	if len(symbols) != 2 {
		t.Fatalf("got %d symbols, want 2", len(symbols))
	}
	if symbols[0].Symbol != "AC" {
		t.Errorf("symbols[0].Symbol = %q, want %q", symbols[0].Symbol, "AC")
	}
	if symbols[0].Name != "Air Canada" {
		t.Errorf("symbols[0].Name = %q, want %q", symbols[0].Name, "Air Canada")
	}
	if symbols[0].Exchange != "TSX" {
		t.Errorf("symbols[0].Exchange = %q, want %q", symbols[0].Exchange, "TSX")
	}
	if symbols[0].Currency != "CAD" {
		t.Errorf("symbols[0].Currency = %q, want %q", symbols[0].Currency, "CAD")
	}
}

func TestListSymbols_EmptyResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"results":[]}`))
	}))
	defer srv.Close()

	c := &Client{baseURL: srv.URL, httpClient: srv.Client()}
	symbols, err := c.ListSymbols(context.Background())
	if err != nil {
		t.Fatalf("ListSymbols: %v", err)
	}
	if len(symbols) != 0 {
		t.Errorf("got %d symbols, want 0", len(symbols))
	}
}

func TestListSymbols_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := &Client{baseURL: srv.URL, httpClient: srv.Client()}
	_, err := c.ListSymbols(context.Background())
	if err == nil {
		t.Fatal("expected error for HTTP 500")
	}
}

func TestListSymbols_InvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("[invalid"))
	}))
	defer srv.Close()

	c := &Client{baseURL: srv.URL, httpClient: srv.Client()}
	_, err := c.ListSymbols(context.Background())
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestListSymbols_ContextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(tsxResponse{Results: []tsxEntry{}})
	}))
	defer srv.Close()

	c := &Client{baseURL: srv.URL, httpClient: srv.Client()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.ListSymbols(ctx)
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestListSymbols_SkipsEmptySymbols(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(tsxResponse{
			Results: []tsxEntry{
				{Symbol: "GOOD", Name: "Good Corp"},
				{Symbol: "", Name: "Empty Symbol"},
				{Symbol: "ALSO_GOOD", Name: "Also Good Corp"},
			},
		})
	}))
	defer srv.Close()

	c := &Client{baseURL: srv.URL, httpClient: srv.Client()}
	symbols, err := c.ListSymbols(context.Background())
	if err != nil {
		t.Fatalf("ListSymbols: %v", err)
	}
	if len(symbols) != 2 {
		t.Errorf("got %d symbols, want 2 (empty filtered)", len(symbols))
	}
}
