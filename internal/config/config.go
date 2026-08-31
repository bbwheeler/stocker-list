// Package config centralizes all runtime configuration, loaded from
// environment variables so the service (database, refresh cadence, etc.)
// is fully configurable without code changes.
package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"
)

type Config struct {
	// gRPC
	GRPCPort int

	// Database (PostgreSQL)
	DBHost     string
	DBPort     int
	DBUser     string
	DBPassword string
	DBName     string
	DBSSLMode  string

	// Refresh behaviour
	RefreshCheckInterval time.Duration
}

func Load() (*Config, error) {
	cfg := &Config{
		GRPCPort:   getEnvInt("GRPC_PORT", 50051),
		DBHost:     getEnv("DB_HOST", "localhost"),
		DBPort:     getEnvInt("DB_PORT", 5432),
		DBUser:     getEnv("DB_USER", "postgres"),
		DBPassword: getEnv("DB_PASSWORD", "postgres"),
		DBName:     getEnv("DB_NAME", "tsx_tracker"),
		DBSSLMode:  getEnv("DB_SSLMODE", "disable"),

		RefreshCheckInterval: getEnvDuration("REFRESH_CHECK_INTERVAL", 24*time.Hour),
	}

	if cfg.GRPCPort < 1 || cfg.GRPCPort > 65535 {
		return nil, fmt.Errorf("GRPC_PORT must be 1-65535, got %d", cfg.GRPCPort)
	}
	if cfg.DBPort < 1 || cfg.DBPort > 65535 {
		return nil, fmt.Errorf("DB_PORT must be 1-65535, got %d", cfg.DBPort)
	}
	if cfg.RefreshCheckInterval <= 0 {
		return nil, fmt.Errorf("REFRESH_CHECK_INTERVAL must be > 0, got %s", cfg.RefreshCheckInterval)
	}

	return cfg, nil
}

func (c *Config) PostgresDSN() string {
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(c.DBUser, c.DBPassword),
		Host:   fmt.Sprintf("%s:%d", c.DBHost, c.DBPort),
		Path:   c.DBName,
	}
	q := u.Query()
	q.Set("sslmode", c.DBSSLMode)
	u.RawQuery = q.Encode()
	return u.String()
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
