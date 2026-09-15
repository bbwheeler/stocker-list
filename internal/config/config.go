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
	// gRPC
	GRPCPort int

	// Refresh behaviour
	RefreshCheckInterval time.Duration

	// Kafka
	KafkaBrokers       string
	KafkaTopic         string
	KafkaSASLUsername  string
	KafkaSASLPassword  string
	KafkaSASLMechanism string
	KafkaSSLEnabled    bool

	// Provider (US / FMP)
	FMPAPIKey string
}

func Load() (*Config, error) {
	cfg := &Config{
		GRPCPort: getEnvInt("GRPC_PORT", 50051),

		RefreshCheckInterval: getEnvDuration("REFRESH_CHECK_INTERVAL", 24*time.Hour),

		KafkaBrokers:       getEnv("KAFKA_BROKERS", ""),
		KafkaTopic:         getEnv("KAFKA_TOPIC", "stock.update.v1"),
		KafkaSASLUsername:  getEnv("KAFKA_SASL_USERNAME", ""),
		KafkaSASLPassword:  getEnv("KAFKA_SASL_PASSWORD", ""),
		KafkaSASLMechanism: getEnv("KAFKA_SASL_MECHANISM", "PLAIN"),
		KafkaSSLEnabled:    getEnvBool("KAFKA_SSL_ENABLED", true),
		FMPAPIKey:          getEnv("FMP_API_KEY", ""),
	}

	if cfg.GRPCPort < 1 || cfg.GRPCPort > 65535 {
		return nil, fmt.Errorf("GRPC_PORT must be 1-65535, got %d", cfg.GRPCPort)
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

func getEnvBool(key string, fallback bool) bool {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return fallback
}
