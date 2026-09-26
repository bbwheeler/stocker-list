// Package refresher runs a background loop that keeps the tracked stock
// symbol list up to date. Each cycle it pulls the latest symbols from the
// configured providers and publishes a StockUpdate message to Kafka for
// every tracked symbol.
package refresher

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	kafkastockv1 "git.wheeli.ca/brian/stocker-store/proto/v1"
	"github.com/anomalyco/stocker-list/internal/config"
	"github.com/anomalyco/stocker-list/internal/kafka"
	"github.com/anomalyco/stocker-list/internal/provider"
)

type Refresher struct {
	cfg *config.Config
	// repo is the (optional) persistence sink. It is nil in Kafka-only mode,
	// where discovered symbols are published upstream instead of stored locally.
	repo      any
	providers []provider.Provider
	log       *slog.Logger
	producer  *kafka.Producer
}

func New(cfg *config.Config, repo any, providers []provider.Provider, log *slog.Logger, producer *kafka.Producer) *Refresher {
	return &Refresher{cfg: cfg, repo: repo, providers: providers, log: log, producer: producer}
}

// Run blocks, performing an immediate sync and then repeating every
// cfg.RefreshCheckInterval, until ctx is cancelled.
func (r *Refresher) Run(ctx context.Context) {
	r.tick(ctx)

	ticker := time.NewTicker(r.cfg.RefreshCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.tick(ctx)
		}
	}
}

func (r *Refresher) tick(ctx context.Context) {
	r.log.Info("refresh cycle starting")

	companies, err := r.discoverAllSymbols(ctx)
	if err != nil {
		r.log.Error("discovering symbols failed", "error", err)
		return
	}

	r.log.Info("refresh cycle complete", "symbols", len(companies))
}

// discoverAllSymbols pulls the current symbol list from all providers and
// publishes a StockUpdate message to Kafka for every symbol discovered.
func (r *Refresher) discoverAllSymbols(ctx context.Context) ([]provider.Company, error) {
	var allCompanies []provider.Company

	for _, p := range r.providers {
		companies, err := p.ListSymbols(ctx)
		if err != nil {
			return nil, fmt.Errorf("provider failed to list symbols: %w", err)
		}
		allCompanies = append(allCompanies, companies...)
	}

	if r.producer != nil {
		for _, c := range allCompanies {
			msg := &kafkastockv1.Stock{
				Symbol:   c.Symbol,
				Exchange: c.Exchange,
			}
			if err := r.producer.PublishStockUpdate(ctx, msg); err != nil {
				return nil, fmt.Errorf("failed to publish update for symbol %s on exchange %s: %w", c.Symbol, c.Exchange, err)
			}
		}
	}

	return allCompanies, nil
}
