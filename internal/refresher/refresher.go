// Package refresher runs a background loop that keeps the tracked stock
// symbol list up to date. Every cycle it syncs the full list (adding
// new symbols, removing delisted ones).
package refresher

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/anomalyco/stocker-list/internal/config"
	"github.com/anomalyco/stocker-list/internal/db"
	"github.com/anomalyco/stocker-list/internal/kafka"
	"github.com/anomalyco/stocker-list/internal/provider"
	kafkastockv1 "github.com/anomalyco/stocker-list/internal/proto/kafka/v1/kafkastockv1"
)

type Refresher struct {
	cfg       *config.Config
	repo      *db.Repository
	providers []provider.Provider
	log       *slog.Logger
	producer  *kafka.Producer
}

func New(cfg *config.Config, repo *db.Repository, providers []provider.Provider, log *slog.Logger, producer *kafka.Producer) *Refresher {
	return &Refresher{cfg: cfg, repo: repo, providers: providers, log: log, producer: producer}
}

// Run blocks, performing an immediate sync and then repeating every
// cfg.RefreshCheckInterval, until ctx is upon cancelled.
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

	allSymbols, err := r.discoverAllSymbols(ctx)
	if err != nil {
		r.log.Error("discovering symbols failed", "error", err)
	} else {
		if err := r.pruneDelisted(ctx, allSymbols); err != nil {
			r.log.Error("pruning delisted symbols failed", "error", err)
		}
	}

	r.log.Info("refresh cycle complete")
}

// discoverAllSymbols pulls the current symbol list from all providers and inserts stub
// rows for any symbol we don't already track.
func (r *Refresher) discoverAllSymbols(ctx context.Context) ([]string, error) {
	var allCompanies []db.Company

	for _, p := range r.providers {
		companies, err := p.ListSymbols(ctx)
		if err != nil {
			r.log.Error("provider failed to list symbols", "error", err)
			continue
		}
		allCompanies = append(allCompanies, companies...)
	}

	if err := r.repo.InsertSymbolStubs(ctx, allCompanies); err != nil {
		return nil, err
	}

	if r.producer != nil {
		stocks := make([]*kafkastockv1.StockUpdate, len(allCompanies))
		for i, c := range allCompanies {
			stocks[i] = &kafkastockv1.StockUpdate{
				Symbol:   c.Symbol,
				Exchange: c.Exchange,
				Scores:   map[string]float64{"discovered": 1.0},
			}
			if err := r.producer.PublishStockUpdate(ctx, stocks[i]); err != nil {
				r.log.Warn("failed to publish stock update", "symbol", c.Symbol, "error", err)
			}
		}
		_ = stocks // use in future scoring work
	}

	symbols := make([]string, len(allCompanies))
	for i, c := range allCompanies {
		symbols[i] = c.Symbol
	}
	return symbols, nil
}

// pruneDelisted removes any companies from the database whose symbols
// are not in the current tracked symbol list returned by the providers.
func (r *Refresher) pruneDelisted(ctx context.Context, currentSymbols []string) error {
	current := make(map[string]struct{}, len(currentSymbols))
	for _, s := range currentSymbols {
		current[strings.ToUpper(s)] = struct{}{}
	}

	tracked, err := r.repo.AllSymbols(ctx)
	if err != nil {
		return err
	}

	var toDelete []string
	for _, s := range tracked {
		if _, ok := current[strings.ToUpper(s)]; !ok {
			toDelete = append(toDelete, s)
		}
	}

	if len(toDelete) == 0 {
		return nil
	}

	deleted, err := r.repo.DeleteBySymbols(ctx, toDelete)
	if err != nil {
		return err
	}
	r.log.Info("pruned delisted symbols", "count", deleted)
	return nil
}
