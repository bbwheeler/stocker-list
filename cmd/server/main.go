// Command stocker-list runs a service that keeps an up-to-date record of
// TSX-listed companies in PostgreSQL, and exposes them over gRPC. It also
// publishes discovered stock updates to Kafka via the refresher.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/anomalyco/stocker-list/internal/config"
	"github.com/anomalyco/stocker-list/internal/db"
	"github.com/anomalyco/stocker-list/internal/grpcserver"
	"github.com/anomalyco/stocker-list/internal/kafka"
	"github.com/anomalyco/stocker-list/internal/provider"
	"github.com/anomalyco/stocker-list/internal/refresher"

	tsxv1 "github.com/anomalyco/stocker-list/gen/tsx/v1"
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

	repo, err := db.NewRepository(ctx, cfg.PostgresDSN())
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer repo.Close()

	if err := repo.Migrate(ctx, db.InitSchemaSQL()); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}
	log.Info("database ready")

	providers := []provider.Provider{provider.NewClient()}
	producer := &kafka.Producer{}

	// Background loop that keeps the TSX symbol list fresh. Runs an
	// immediate sync on startup, then on cfg.RefreshCheckInterval.
	var wg sync.WaitGroup
	ref := refresher.New(cfg, repo, providers, log, producer)
	wg.Add(1)
	go func() {
		defer wg.Done()
		ref.Run(ctx)
	}()

	// gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		return fmt.Errorf("listen on port %d: %w", cfg.GRPCPort, err)
	}

	grpcSrv := grpc.NewServer()
	tsxv1.RegisterCompanyServiceServer(grpcSrv, grpcserver.New(repo, log))
	reflection.Register(grpcSrv)

	go func() {
		<-ctx.Done()
		log.Info("shutting down gRPC server")
		grpcSrv.GracefulStop()
	}()

	log.Info("gRPC server listening", "port", cfg.GRPCPort)
	if err := grpcSrv.Serve(lis); err != nil {
		return fmt.Errorf("grpc serve: %w", err)
	}

	// Wait for the refresher goroutine to finish its current tick.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Info("refresher stopped cleanly")
	case <-time.After(30 * time.Second):
		log.Warn("refresher did not stop within timeout, continuing")
	}

	return nil
}
