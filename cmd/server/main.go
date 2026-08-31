// Command stocker-list manages lifecycle for pulling stock data and publishing to Kafka.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/anomalyco/stocker-list/internal/config"
	"github.com/anomalyco/stocker-list/internal/kafka"
	"github.com/anomalyco/stocker-list/internal/provider"
	"github.com/anomalyco/stocker-list/internal/refresher"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := run(log); err != nil {
		log.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tsxClient := provider.NewClient()

	// Background loop that keeps the TSX symbol list fresh. Runs an
	// immediate sync on startup, then on cfg.RefreshCheckInterval.
	var wg sync.WaitGroup
	
	kafkaProducer := &kafka.Producer{}
	ref := refresher.New(cfg, nil, []provider.Provider{tsxClient}, log, kafkaProducer)
	wg.Add(1)
	go func() {
		defer wg.Done()
		ref.Run(ctx)
	}()

	<-ctx.Done()
	log.Info("shutting down")

	wg.Wait()
	
	return nil
}
