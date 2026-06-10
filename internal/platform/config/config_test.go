package config

import (
	"testing"

	"github.com/turgut1907/notification.system/internal/domain"
)

func TestLoad_DefaultsAreValid(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("defaults should be valid: %v", err)
	}
	if len(cfg.Worker.Priorities) != 3 {
		t.Fatalf("expected 3 default priorities, got %d", len(cfg.Worker.Priorities))
	}
}

func TestLoad_WorkerPrioritiesParsed(t *testing.T) {
	t.Setenv("WORKER_PRIORITIES", "high, low , bogus")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []domain.Priority{domain.PriorityHigh, domain.PriorityLow}
	if len(cfg.Worker.Priorities) != len(want) {
		t.Fatalf("expected %v, got %v", want, cfg.Worker.Priorities)
	}
	for i, p := range want {
		if cfg.Worker.Priorities[i] != p {
			t.Fatalf("at %d expected %s got %s", i, p, cfg.Worker.Priorities[i])
		}
	}
}

func TestLoad_LeaseInvariantEnforced(t *testing.T) {
	// Lease shorter than the provider timeout must be rejected to prevent the
	// reaper from reclaiming in-flight deliveries and double-sending.
	t.Setenv("WORKER_LEASE_TTL", "5s")
	t.Setenv("PROVIDER_TIMEOUT", "10s")
	if _, err := Load(); err == nil {
		t.Fatal("expected error when lease <= provider timeout")
	}
}

func TestReplicaOrPrimary(t *testing.T) {
	cfg := Config{Postgres: PostgresConfig{DSN: "primary", ReplicaDSN: "replica"}}
	if cfg.Postgres.ReplicaOrPrimary() != "replica" {
		t.Fatal("expected replica")
	}
	cfg.Postgres.ReplicaDSN = ""
	if cfg.Postgres.ReplicaOrPrimary() != "primary" {
		t.Fatal("expected primary fallback")
	}
}

func TestLoad_InvalidRateLimiterRejected(t *testing.T) {
	t.Setenv("RATE_LIMITER", "memcached")
	if _, err := Load(); err == nil {
		t.Fatal("expected error for invalid RATE_LIMITER")
	}
}

func TestLoad_JWTSecretFromEnv(t *testing.T) {
	t.Setenv("JWT_SECRET", "01234567890123456789012345678901")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Auth.JWTSecret != "01234567890123456789012345678901" {
		t.Fatalf("got %q", cfg.Auth.JWTSecret)
	}
}
