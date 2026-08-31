// Package provider fetches TSX ticker symbols from the official TMX
// company directory JSON API. No API key is required — this is a
// public endpoint provided by the Toronto Stock Exchange.
package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const defaultBaseURL = "https://www.tsx.com/json/company-directory/search/tsx/*"

type tsxResponse struct {
	Results []tsxEntry `json:"results"`
}

type tsxEntry struct {
	Symbol string `json:"symbol"`
	Name   string `json:"name"`
}

type Client struct {
	baseURL    string
	httpClient *http.Client
}

func NewClient() *Client {
	return &Client{
		baseURL:    defaultBaseURL,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// NewClientForTest creates a client pointing at a custom base URL.
// Used in tests to mock the TEX API.
func NewClientForTest(baseURL string) *Client {
	return &Client{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}
}

// ListSymbols returns every stock symbol listed on the Toronto Stock
// Exchange (TSX) by querying the official TMX company directory.
func (c *Client) ListSymbols(ctx context.Context) ([]Company, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch TSX symbols: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("TMX returned HTTP %d", resp.StatusCode)
	}

	const maxBodySize = 20 << 20
	var tsxResp tsxResponse
	dec := json.NewDecoder(io.LimitReader(resp.Body, maxBodySize))
	if err := dec.Decode(&tsxResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	out := make([]Company, 0, len(tsxResp.Results))
	for _, e := range tsxResp.Results {
		if e.Symbol == "" {
			continue
		}
		out = append(out, Company{
			Symbol:   e.Symbol,
			Name:     e.Name,
			Exchange: "TSX",
			Currency: "CAD",
		})
	}
	return out, nil
}
