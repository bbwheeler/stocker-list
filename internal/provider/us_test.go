package provider

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeExchangeServer builds an httptest server that mimics the NASDAQ
// screener API: it inspects the `exchange` query parameter and returns
// page-sized rows of the fake symbols mapped to that exchange.
func fakeExchangeServer(t *testing.T, pages map[string][]nasdaqRow) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("User-Agent"); got == "" {
			t.Errorf("server saw no User-Agent header; expected a browser-like UA")
		}
		q := r.URL.Query()
		ex := q.Get("exchange")
		off, _ := parseOffset(q.Get("offset"))
		rows, ok := pages[ex]
		if !ok {
			// Unknown exchange: return empty rows (mimic API returning 0 records).
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"data":{"table":{"rows":[]}}}`)
			return
		}
		// Emit at most `pageSize` rows starting at offset.
		start := off
		if start > len(rows) {
			start = len(rows)
		}
		end := start + pageSize
		if end > len(rows) {
			end = len(rows)
		}
		page := rows[start:end]
		if len(page) == 0 {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"data":{"table":{"rows":[]}}}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"data":{"table":{"rows":[` + strings.Join(rowJSONs(page), ",") + `]}}}`))
	}))
}

func rowJSONs(rows []nasdaqRow) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, fmt.Sprintf(`{"symbol":%q,"name":%q}`, r.Symbol, r.Name))
	}
	return out
}

func parseOffset(s string) (int, error) {
	if s == "" {
		return 0, nil
	}
	var n int
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return 0, err
	}
	return n, nil
}

func TestUS_OneRowPerExchange(t *testing.T) {
	srv := fakeExchangeServer(t, map[string][]nasdaqRow{
		"NASDAQ": {{Symbol: "NVDA", Name: "NVIDIA Corporation"}},
		"NYSE":   {{Symbol: "AAPL", Name: "Apple Inc."}},
		"AMEX":   {{Symbol: "BRK.A", Name: "Berkshire A"}},
	})
	defer srv.Close()

	c := NewUSClientForTest(srv.URL)
	symbols, err := c.ListSymbols(context.Background())
	if err != nil {
		t.Fatalf("ListSymbols: %v", err)
	}
	if len(symbols) != 3 {
		t.Fatalf("got %d symbols, want 3", len(symbols))
	}
	byExchange := map[string]string{}
	for _, s := range symbols {
		byExchange[s.Exchange] = s.Symbol
		if s.Currency != "USD" {
			t.Errorf("%s: currency = %q, want USD", s.Symbol, s.Currency)
		}
	}
	if got := byExchange["NASDAQ"]; got != "NVDA" {
		t.Errorf("NASDAQ symbol = %q, want NVDA", got)
	}
	if got := byExchange["NYSE"]; got != "AAPL" {
		t.Errorf("NYSE symbol = %q, want AAPL", got)
	}
	if got := byExchange["AMEX"]; got != "BRK.A" {
		t.Errorf("AMEX symbol = %q, want BRK.A", got)
	}
}

func TestUS_Pagination(t *testing.T) {
	// Build a NASDAQ list just over one page so we exercise pagination.
	rows := make([]nasdaqRow, pageSize+1)
	for i := range rows {
		rows[i] = nasdaqRow{Symbol: fmt.Sprintf("S%03d", i), Name: fmt.Sprintf("Company %d", i)}
	}
	srv := fakeExchangeServer(t, map[string][]nasdaqRow{
		"NASDAQ": rows,
		"NYSE":   {},
		"AMEX":   {},
	})
	defer srv.Close()

	c := NewUSClientForTest(srv.URL)
	symbols, err := c.ListSymbols(context.Background())
	if err != nil {
		t.Fatalf("ListSymbols: %v", err)
	}
	if len(symbols) != len(rows) {
		t.Fatalf("got %d symbols, want %d", len(symbols), len(rows))
	}
	for _, s := range symbols {
		if s.Exchange != "NASDAQ" {
			t.Errorf("symbol %s exchange = %q, want NASDAQ", s.Symbol, s.Exchange)
		}
	}
}

func TestUS_TrimWhitespace(t *testing.T) {
	srv := fakeExchangeServer(t, map[string][]nasdaqRow{
		"NYSE": {{Symbol: "  NVDA  ", Name: "  NVIDIA Corp  "}},
	})
	defer srv.Close()

	c := NewUSClientForTest(srv.URL)
	symbols, err := c.ListSymbols(context.Background())
	if err != nil {
		t.Fatalf("ListSymbols: %v", err)
	}
	if len(symbols) != 1 {
		t.Fatalf("got %d symbols, want 1", len(symbols))
	}
	if symbols[0].Symbol != "NVDA" {
		t.Errorf("Symbol = %q, want NVDA (trimmed)", symbols[0].Symbol)
	}
	if symbols[0].Name != "NVIDIA Corp" {
		t.Errorf("Name = %q, want %q (trimmed)", symbols[0].Name, "NVIDIA Corp")
	}
}

func TestUS_DeDuplicateAcrossExchanges(t *testing.T) {
	srv := fakeExchangeServer(t, map[string][]nasdaqRow{
		"NASDAQ": {
			{Symbol: "NVDA", Name: "NVIDIA (NASDAQ)"},
			{Symbol: "AAPL", Name: "Apple (NASDAQ)"},
		},
		"NYSE": {
			{Symbol: "NVDA", Name: "NVIDIA (NYSE)"}, // dup
			{Symbol: "MSFT", Name: "Microsoft (NYSE)"},
		},
	})
	defer srv.Close()

	c := NewUSClientForTest(srv.URL)
	symbols, err := c.ListSymbols(context.Background())
	if err != nil {
		t.Fatalf("ListSymbols: %v", err)
	}
	if len(symbols) != 3 {
		t.Fatalf("got %d symbols, want 3 (one NVDA dup removed)", len(symbols))
	}
	bySymbol := map[string]string{}
	for _, s := range symbols {
		if _, dup := bySymbol[s.Symbol]; dup {
			t.Errorf("duplicate symbol %s present", s.Symbol)
		}
		bySymbol[s.Symbol] = s.Exchange
	}
	if got := bySymbol["NVDA"]; got != "NASDAQ" {
		t.Errorf("NVDA exchange = %q, want NASDAQ (first seen wins)", got)
	}
}

func TestUS_EmptyResponse(t *testing.T) {
	srv := fakeExchangeServer(t, map[string][]nasdaqRow{
		"NASDAQ": {},
		"NYSE":   {},
		"AMEX":   {},
	})
	defer srv.Close()

	c := NewUSClientForTest(srv.URL)
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

	c := NewUSClientForTest(srv.URL)
	_, err := c.ListSymbols(context.Background())
	if err == nil {
		t.Fatal("expected error for HTTP 500")
	}
}

func TestUS_InvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[invalid`))
	}))
	defer srv.Close()

	c := NewUSClientForTest(srv.URL)
	_, err := c.ListSymbols(context.Background())
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}
