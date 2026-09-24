package config

import (
	"testing"
	"time"
)

func TestLoad_Defaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.GRPCPort != 50051 {
		t.Errorf("GRPCPort = %d, want 50051", cfg.GRPCPort)
	}
	if cfg.RefreshCheckInterval != 24*time.Hour {
		t.Errorf("RefreshCheckInterval = %s, want %s", cfg.RefreshCheckInterval, 24*time.Hour)
	}
}

func TestLoad_CustomValues(t *testing.T) {
	env := map[string]string{
		"GRPC_PORT":              "9999",
		"REFRESH_CHECK_INTERVAL": "12h",
	}
	for k, v := range env {
		t.Setenv(k, v)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.GRPCPort != 9999 {
		t.Errorf("GRPCPort = %d, want 9999", cfg.GRPCPort)
	}
	if cfg.RefreshCheckInterval != 12*time.Hour {
		t.Errorf("RefreshCheckInterval = %s, want %s", cfg.RefreshCheckInterval, 12*time.Hour)
	}
}

func TestLoad_InvalidInt(t *testing.T) {
	t.Setenv("GRPC_PORT", "not-a-number")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.GRPCPort != 50051 {
		t.Errorf("GRPCPort = %d, want 50051 (fallback)", cfg.GRPCPort)
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

func TestLoad_InvalidGRPCPort(t *testing.T) {
	t.Setenv("GRPC_PORT", "99999")
	_, err := Load()
	if err == nil {
		t.Fatal("expected error for out-of-range GRPC_PORT")
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
	if cfg.KafkaSASLUsername != "" {
		t.Errorf("KafkaSASLUsername = %q, want empty", cfg.KafkaSASLUsername)
	}
	if cfg.KafkaSASLPassword != "" {
		t.Errorf("KafkaSASLPassword = %q, want empty", cfg.KafkaSASLPassword)
	}
	if cfg.KafkaSASLMechanism != "PLAIN" {
		t.Errorf("KafkaSASLMechanism = %q, want %q", cfg.KafkaSASLMechanism, "PLAIN")
	}
	if cfg.KafkaSSLEnabled != true {
		t.Errorf("KafkaSSLEnabled = %v, want true", cfg.KafkaSSLEnabled)
	}
	if cfg.FMPAPIKey != "" {
		t.Errorf("FMPAPIKey = %q, want empty", cfg.FMPAPIKey)
	}
}

func TestLoad_KafkaCustom(t *testing.T) {
	env := map[string]string{
		"KAFKA_BROKERS":        "kafka1:9093,kafka2:9093",
		"KAFKA_TOPIC":          "stock.other",
		"KAFKA_SASL_USERNAME":  "svc",
		"KAFKA_SASL_PASSWORD":  "s3cr3t",
		"KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
		"KAFKA_SSL_ENABLED":    "false",
		"FMP_API_KEY":          "fmpkey123",
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
	if cfg.KafkaSASLUsername != "svc" {
		t.Errorf("KafkaSASLUsername = %q, want %q", cfg.KafkaSASLUsername, "svc")
	}
	if cfg.KafkaSASLPassword != "s3cr3t" {
		t.Errorf("KafkaSASLPassword = %q, want %q", cfg.KafkaSASLPassword, "s3cr3t")
	}
	if cfg.KafkaSASLMechanism != "SCRAM-SHA-512" {
		t.Errorf("KafkaSASLMechanism = %q, want %q", cfg.KafkaSASLMechanism, "SCRAM-SHA-512")
	}
	if cfg.KafkaSSLEnabled != false {
		t.Errorf("KafkaSSLEnabled = %v, want false", cfg.KafkaSSLEnabled)
	}
	if cfg.FMPAPIKey != "fmpkey123" {
		t.Errorf("FMPAPIKey = %q, want %q", cfg.FMPAPIKey, "fmpkey123")
	}
}

func TestLoad_InvalidSSLBool(t *testing.T) {
	t.Setenv("KAFKA_SSL_ENABLED", "not-a-bool")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.KafkaSSLEnabled != true {
		t.Errorf("KafkaSSLEnabled = %v, want true (fallback)", cfg.KafkaSSLEnabled)
	}
}
