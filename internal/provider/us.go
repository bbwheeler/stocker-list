package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/anomalyco/stocker-list/internal/db"
)

const maxBodySize = 20 << 20

type USClient struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

type fmpStock struct {
	Symbol   string `json:"symbol"`
	Name     string `json:"name"`
	Currency string `json:"currency"`
	Exchange string `json:"exchange"`
}

func NewUSClient(apiKey string) *USClient {
	return &USClient{
		baseURL: "https://financialmodelingprep.com/api/v3/stock/list",
		apiKey:  apiKey,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
	// Note: I'll use a placeholder for the base URL to make it easy to override.
}

func (c *USClient) ListSymbols(ctx context.Context) ([]db.Company, error) {
	url := fmt.Sprintf("%s?apikey=%s", c.baseURL, c.apiKey)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch US symbols: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("FMP returned HTTP %d", resp.StatusCode)
	}

	var stocks []fmpStock
	if err := json.NewDecoder(resp.Body).Decode(&stocks); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	out := make([]db.Company, 0, len(stocks))
	for _, s := range stocks {
		// FMP's stock list might not have all fields for every symbol in the free tier.
		// We'll use what we have.
		exchange := "US"
		if s.Exchange != "" {
			exchange = s.Exchange
		}
		currency := "USD"
		if s.Currency != "" {
			currency = s.Currency
		}

		out = append(out, db.Company{
			Symbol:   s.Symbol,
			Name:     s.Name,
			Exchange: exchange,
			Currency: currency,
		})
	}
	return out, nil
}
