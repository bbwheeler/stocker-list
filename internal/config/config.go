// Package config centralizes all runtime configuration, loaded from
// environment variables so the RabbitMQ target is fully configurable without
// code changes.
package config

import (
	"fmt"
	"os"
	"strings"
)

type Config struct {
	// RabbitMQ
	RabbitMQURL          string
	RabbitMQExchange     string
	RabbitMQExchangeType string
	RabbitMQRoutingKey   string
}

func Load() (*Config, error) {
	cfg := &Config{
		RabbitMQURL:          getEnvPreserveEmpty("RABBITMQ_URL", "amqp://guest:guest@192.168.1.48:5672/"),
		RabbitMQExchange:     getEnv("RABBITMQ_EXCHANGE", "stock.update"),
		RabbitMQExchangeType: getEnv("RABBITMQ_EXCHANGE_TYPE", "topic"),
		RabbitMQRoutingKey:   getEnv("RABBITMQ_ROUTING_KEY", "stock.update.v1"),
	}

	exchangeType := strings.ToLower(strings.TrimSpace(cfg.RabbitMQExchangeType))
	switch exchangeType {
	case "direct", "topic", "fanout", "headers":
		cfg.RabbitMQExchangeType = exchangeType
	default:
		return nil, fmt.Errorf("RABBITMQ_EXCHANGE_TYPE must be one of direct, topic, fanout, headers, got %q", exchangeType)
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

// getEnvPreserveEmpty returns the variable's value even when it is empty.
// Only an absent variable yields the fallback. This lets RABBITMQ_URL= disable
// publishing while an unset RABBITMQ_URL selects the default broker.
func getEnvPreserveEmpty(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}
