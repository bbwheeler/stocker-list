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
