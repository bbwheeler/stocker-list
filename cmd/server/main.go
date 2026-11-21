// Command stocker-list runs exactly one refresh cycle: it fetches the full
// stock symbol universe from every provider, publishes each symbol to
// RabbitMQ, and exits. Kubernetes CronJob scheduling replaces the old
// in-process refresher loop.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/anomalyco/stocker-list/internal/config"
	"github.com/anomalyco/stocker-list/internal/provider"
	"github.com/anomalyco/stocker-list/internal/rabbitmq"
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

	// SIGTERM (Pod termination / activeDeadlineSeconds) cancels in-flight
	// provider requests and publishes so the process exits promptly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	providers := []provider.Provider{
		provider.NewClient(),   // TSX — no key, always on
		provider.NewUSClient(), // US — NASDAQ screener, no key, always on
	}

	producer, err := rabbitmq.NewProducer(cfg, log)
	if err != nil {
		return fmt.Errorf("create rabbitmq producer: %w", err)
	}
	defer func() {
		if cerr := producer.Close(); cerr != nil {
			log.Warn("close rabbitmq producer", "error", cerr)
		}
	}()

	ref := refresher.New(providers, log, producer)
	if err := ref.RunOnce(ctx); err != nil {
		return fmt.Errorf("run refresh cycle: %w", err)
	}

	log.Info("one-shot run complete; exiting")
	return nil
}
