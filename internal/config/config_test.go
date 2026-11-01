package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestLoad_Defaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.RefreshCheckInterval != 24*time.Hour {
		t.Errorf("RefreshCheckInterval = %s, want %s", cfg.RefreshCheckInterval, 24*time.Hour)
	}
}

func TestLoad_CustomValues(t *testing.T) {
	env := map[string]string{
		"REFRESH_CHECK_INTERVAL": "12h",
	}
	for k, v := range env {
		t.Setenv(k, v)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.RefreshCheckInterval != 12*time.Hour {
		t.Errorf("RefreshCheckInterval = %s, want %s", cfg.RefreshCheckInterval, 12*time.Hour)
	}
}

func TestLoad_InvalidDuration(t *testing.T) {
	t.Setenv("REFRESH_CHECK_INTERVAL", "not-a-duration")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.RefreshCheckInterval != 24*time.Hour {
		t.Errorf("RefreshCheckInterval = %s, want 24h (fallback)", cfg.RefreshCheckInterval)
	}
}

func TestLoad_KafkaDefaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.KafkaBrokers != "" {
		t.Errorf("KafkaBrokers = %q, want empty", cfg.KafkaBrokers)
	}
	if cfg.KafkaTopic != "stock.update.v1" {
		t.Errorf("KafkaTopic = %q, want %q", cfg.KafkaTopic, "stock.update.v1")
	}
}

func TestLoad_KafkaCustom(t *testing.T) {
	env := map[string]string{
		"KAFKA_BROKERS": "kafka1:9093,kafka2:9093",
		"KAFKA_TOPIC":   "stock.other",
	}
	for k, v := range env {
		t.Setenv(k, v)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.KafkaBrokers != "kafka1:9093,kafka2:9093" {
		t.Errorf("KafkaBrokers = %q, want %q", cfg.KafkaBrokers, "kafka1:9093,kafka2:9093")
	}
	if cfg.KafkaTopic != "stock.other" {
		t.Errorf("KafkaTopic = %q, want %q", cfg.KafkaTopic, "stock.other")
	}
}

var rabbitMQEnvKeys = []string{
	"RABBITMQ_URL",
	"RABBITMQ_EXCHANGE",
	"RABBITMQ_EXCHANGE_TYPE",
	"RABBITMQ_ROUTING_KEY",
}

// unsetEnv unsets each key for the duration of the test and restores the
// previous state afterwards. Unlike t.Setenv, it can express "absent", which
// matters for RABBITMQ_URL: absent means default, empty means disabled.
func unsetEnv(t *testing.T, keys ...string) {
	t.Helper()
	for _, key := range keys {
		old, ok := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}
		t.Cleanup(func() {
			if ok {
				if err := os.Setenv(key, old); err != nil {
					t.Errorf("restore %s: %v", key, err)
				}
				return
			}
			if err := os.Unsetenv(key); err != nil {
				t.Errorf("unset %s: %v", key, err)
			}
		})
	}
}

func TestLoad_RabbitMQDefaults(t *testing.T) {
	unsetEnv(t, rabbitMQEnvKeys...)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.RabbitMQURL != "amqp://guest:guest@192.168.1.48:5672/" {
		t.Errorf("RabbitMQURL = %q, want %q", cfg.RabbitMQURL, "amqp://guest:guest@192.168.1.48:5672/")
	}
	if cfg.RabbitMQExchange != "stock.update" {
		t.Errorf("RabbitMQExchange = %q, want %q", cfg.RabbitMQExchange, "stock.update")
	}
	if cfg.RabbitMQExchangeType != "topic" {
		t.Errorf("RabbitMQExchangeType = %q, want %q", cfg.RabbitMQExchangeType, "topic")
	}
	if cfg.RabbitMQRoutingKey != "stock.update.v1" {
		t.Errorf("RabbitMQRoutingKey = %q, want %q", cfg.RabbitMQRoutingKey, "stock.update.v1")
	}
}

func TestLoad_RabbitMQOverrides(t *testing.T) {
	env := map[string]string{
		"RABBITMQ_URL":           "amqp://user:pass@rabbit.local:5672/vhost",
		"RABBITMQ_EXCHANGE":      "stocks",
		"RABBITMQ_EXCHANGE_TYPE": "fanout",
		"RABBITMQ_ROUTING_KEY":   "stocks.v2",
	}
	for k, v := range env {
		t.Setenv(k, v)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.RabbitMQURL != env["RABBITMQ_URL"] {
		t.Errorf("RabbitMQURL = %q, want %q", cfg.RabbitMQURL, env["RABBITMQ_URL"])
	}
	if cfg.RabbitMQExchange != env["RABBITMQ_EXCHANGE"] {
		t.Errorf("RabbitMQExchange = %q, want %q", cfg.RabbitMQExchange, env["RABBITMQ_EXCHANGE"])
	}
	if cfg.RabbitMQExchangeType != env["RABBITMQ_EXCHANGE_TYPE"] {
		t.Errorf("RabbitMQExchangeType = %q, want %q", cfg.RabbitMQExchangeType, env["RABBITMQ_EXCHANGE_TYPE"])
	}
	if cfg.RabbitMQRoutingKey != env["RABBITMQ_ROUTING_KEY"] {
		t.Errorf("RabbitMQRoutingKey = %q, want %q", cfg.RabbitMQRoutingKey, env["RABBITMQ_ROUTING_KEY"])
	}
}

func TestLoad_RabbitMQURLSemantics(t *testing.T) {
	tests := []struct {
		name string
		url  *string
		want string
	}{
		{
			name: "absent uses default",
			url:  nil,
			want: "amqp://guest:guest@192.168.1.48:5672/",
		},
		{
			name: "explicitly empty disables publishing",
			url:  ptr(""),
			want: "",
		},
		{
			name: "custom value is preserved",
			url:  ptr("amqp://guest:guest@localhost:5672/"),
			want: "amqp://guest:guest@localhost:5672/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unsetEnv(t, rabbitMQEnvKeys...)
			if tt.url != nil {
				t.Setenv("RABBITMQ_URL", *tt.url)
			}

			cfg, err := Load()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.RabbitMQURL != tt.want {
				t.Errorf("RabbitMQURL = %q, want %q", cfg.RabbitMQURL, tt.want)
			}
		})
	}
}

func ptr[T any](v T) *T {
	return &v
}

func TestLoad_RabbitMQInvalidExchangeType(t *testing.T) {
	tests := []struct {
		name string
		url  string
		kind string
	}{
		{name: "enabled", url: "amqp://guest:guest@localhost:5672/", kind: "bogus"},
		{name: "disabled", url: "", kind: "bogus"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unsetEnv(t, rabbitMQEnvKeys...)
			t.Setenv("RABBITMQ_URL", tt.url)
			t.Setenv("RABBITMQ_EXCHANGE_TYPE", tt.kind)

			cfg, err := Load()
			if err == nil {
				t.Fatalf("Load() = %+v, want error for exchange type %q", cfg, tt.kind)
			}
			if !strings.Contains(err.Error(), "RABBITMQ_EXCHANGE_TYPE must be one of direct, topic, fanout, headers") {
				t.Errorf("error = %q, want it to name the variable and valid values", err)
			}
			if !strings.Contains(err.Error(), `"bogus"`) {
				t.Errorf("error = %q, want it to include the invalid value", err)
			}
		})
	}
}

func TestLoad_RabbitMQExchangeTypeNormalization(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "  TOPIC  ", want: "topic"},
		{in: "Direct", want: "direct"},
		{in: "FANOUT", want: "fanout"},
		{in: " headers ", want: "headers"},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			unsetEnv(t, rabbitMQEnvKeys...)
			t.Setenv("RABBITMQ_EXCHANGE_TYPE", tt.in)

			cfg, err := Load()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.RabbitMQExchangeType != tt.want {
				t.Errorf("RabbitMQExchangeType = %q, want %q", cfg.RabbitMQExchangeType, tt.want)
			}
		})
	}
}
