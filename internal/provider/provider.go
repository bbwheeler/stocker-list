package provider

import (
	"context"

	"github.com/anomalyco/stocker-list/internal/db"
)

// Provider defines the interface for fetching stock symbols from different exchanges.
type Provider interface {
	ListSymbols(ctx context.Context) ([]db.Company, error)
}
