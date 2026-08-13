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
