// Package provider fetches TSX and US ticker symbols. The US client uses
// the public NASDAQ screener API (api.nasdaq.com), which powers the
// nasdaq.com web app. No API key is required.
package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// nasdaqBaseURL is the NASDAQ screener endpoint. Note: unlike most APIs,
// the server caps a single page at ~250 rows regardless of the requested
// limit, so callers must paginate with ?offset=.
const nasdaqBaseURL = "https://api.nasdaq.com/api/screener/stocks"

const (
	// pageSize is the maximum page size the API will honor.
	pageSize = 250
	// maxPagesPerExchange is a hard ceiling on pages per exchange so a
	// misbehaving endpoint can never loop forever.
	maxPagesPerExchange = 200
)

// defaultExchanges are the US listing exchanges the screener exposes.
// Each is fetched separately because the API does not return an exchange
// field per row — the exchange a symbol belongs to is derived from which
// `exchange=` filter produced it.
var defaultExchanges = []string{"NASDAQ", "NYSE", "AMEX"}

// userAgent is required by api.nasdaq.com; requests without a browser-like
// User-Agent header are rejected.
const userAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

// maxBodySize caps how much of a single response body we will read.
const maxBodySize = 20 << 20

type USClient struct {
	baseURL    string
	exchanges  []string
	httpClient *http.Client
}

// nasdaqResponse mirrors the JSON shape of a single screener page.
type nasdaqResponse struct {
	Data struct {
		Table nasdaqTable `json:"table"`
	} `json:"data"`
}

type nasdaqTable struct {
	Rows []nasdaqRow `json:"rows"`
}

type nasdaqRow struct {
	Symbol string `json:"symbol"`
	Name   string `json:"name"`
}

func NewUSClient() *USClient {
	return &USClient{
		baseURL:    nasdaqBaseURL,
		exchanges:  defaultExchanges,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// NewUSClientForTest returns a USClient pointing at a custom base URL so
// tests can stand up an httptest server that mimics the NASDAQ endpoint.
func NewUSClientForTest(baseURL string) *USClient {
	return &USClient{
		baseURL:    baseURL,
		exchanges:  defaultExchanges,
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}
}

// ListSymbols returns every US common stock symbol from the NASDAQ
// screener, tagged with the exchange it is listed on (NASDAQ, NYSE, or
// AMEX/NYSE American). Currency is USD for this universe. Symbols are
// de-duplicated across exchanges (the first occurrence wins).
func (c *USClient) ListSymbols(ctx context.Context) ([]Company, error) {
	out := make([]Company, 0, 10000)
	seen := make(map[string]struct{})

	for _, ex := range c.exchanges {
		rows, err := c.fetchExchange(ctx, ex)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			sym := strings.TrimSpace(r.Symbol)
			if sym == "" {
				continue
			}
			if _, dup := seen[sym]; dup {
				continue
			}
			seen[sym] = struct{}{}
			out = append(out, Company{
				Symbol:   sym,
				Name:     strings.TrimSpace(r.Name),
				Exchange: ex,
				Currency: "USD",
			})
		}
	}
	return out, nil
}

// fetchExchange walks every page for a single exchange and returns the
// concatenated rows.
func (c *USClient) fetchExchange(ctx context.Context, exchange string) ([]nasdaqRow, error) {
	var all []nasdaqRow
	for offset, page := 0, 0; page < maxPagesPerExchange; page++ {
		url := fmt.Sprintf("%s?limit=%d&offset=%d&exchange=%s",
			c.baseURL, pageSize, offset, exchange)

		rows, err := c.fetchPage(ctx, url)
		if err != nil {
			return nil, err
		}
		all = append(all, rows...)
		if len(rows) < pageSize {
			break
		}
		offset += pageSize
	}
	return all, nil
}

// fetchPage performs a single screener GET and decodes the page of rows.
func (c *USClient) fetchPage(ctx context.Context, url string) ([]nasdaqRow, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch US symbols: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("NASDAQ returned HTTP %d", resp.StatusCode)
	}

	var pag nasdaqResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxBodySize)).Decode(&pag); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return pag.Data.Table.Rows, nil
}
