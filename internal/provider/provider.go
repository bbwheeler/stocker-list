package provider

import (
	"context"
)

// Company is the canonical record of a tracked stock symbol. It is
// provider-local so the provider layer has no dependency on persistence
// internals and can be consumed directly by the Kafka producer.
type Company struct {
	Symbol   string
	Name     string
	Exchange string
	Currency string
}

// Provider defines the interface for fetching stock symbols from different exchanges.
type Provider interface {
	ListSymbols(ctx context.Context) ([]Company, error)
}
