// Package config centralizes all runtime configuration, loaded from
// environment variables so the service (database, refresh cadence, etc.)
// is fully configurable without code changes.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	// Refresh behaviour
	RefreshCheckInterval time.Duration

	// Kafka
	KafkaBrokers string
	KafkaTopic   string
}

func Load() (*Config, error) {
	cfg := &Config{
		RefreshCheckInterval: getEnvDuration("REFRESH_CHECK_INTERVAL", 24*time.Hour),

		KafkaBrokers: getEnv("KAFKA_BROKERS", ""),
		KafkaTopic:   getEnv("KAFKA_TOPIC", "stock.update.v1"),
	}

	if cfg.RefreshCheckInterval <= 0 {
		return nil, fmt.Errorf("REFRESH_CHECK_INTERVAL must be > 0, got %s", cfg.RefreshCheckInterval)
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
