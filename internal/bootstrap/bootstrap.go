// Package bootstrap centralizes construction of shared dependencies (logging,
// metrics, database, Redis, provider, rate limiter, retry policy) so each binary's
// composition root stays small and consistent.
package bootstrap

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	goredis "github.com/redis/go-redis/v9"
	"github.com/sony/gobreaker/v2"

	"github.com/turgut1907/notification.system/internal/adapters/postgres"
	"github.com/turgut1907/notification.system/internal/adapters/provider"
	redisadapter "github.com/turgut1907/notification.system/internal/adapters/redis"
	"github.com/turgut1907/notification.system/internal/delivery"
	"github.com/turgut1907/notification.system/internal/domain"
	"github.com/turgut1907/notification.system/internal/platform/config"
	"github.com/turgut1907/notification.system/internal/platform/logging"
	"github.com/turgut1907/notification.system/internal/platform/metrics"
)

// Metrics builds a dedicated registry and the metric set registered against it.
func Metrics() (*metrics.Metrics, *prometheus.Registry) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector())
	reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	return metrics.New(reg), reg
}

// Logger builds the application logger.
func Logger(cfg config.Config) *slog.Logger {
	return logging.New(cfg.LogLevel)
}

// Postgres connects the Store (primary + optional reader pool).
func Postgres(ctx context.Context, cfg config.Config) (*postgres.Store, error) {
	return postgres.New(ctx, postgres.Config{
		PrimaryDSN: cfg.Postgres.DSN,
		ReaderDSN:  cfg.Postgres.ReplicaOrPrimary(),
		MaxConns:   cfg.Postgres.MaxConns,
	})
}

// Redis connects a verified Redis client.
func Redis(ctx context.Context, cfg config.Config) (*goredis.Client, error) {
	return redisadapter.NewClient(ctx, cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB)
}

// Queue builds the Redis Streams queue using the configured consumer group.
func Queue(client *goredis.Client, cfg config.Config) *redisadapter.Queue {
	return redisadapter.NewQueue(client, cfg.Worker.GroupName)
}

// QueueWithDLQ builds a queue that publishes poison pills to the DLQ stream.
func QueueWithDLQ(client *goredis.Client, cfg config.Config, m *metrics.Metrics) *redisadapter.Queue {
	dlq := redisadapter.NewDLQ(client)
	return redisadapter.NewQueue(client, cfg.Worker.GroupName).WithDLQ(dlq, m)
}

// Limiter is the per-channel rate limiting port.
type Limiter interface {
	Allow(ctx context.Context, channel domain.Channel) (bool, time.Duration, error)
}

// RateLimiter selects the configured limiter implementation (fleet-wide Redis GCRA
// or an in-process fallback).
func RateLimiter(cfg config.Config, client *goredis.Client) Limiter {
	if cfg.RateLimiter.Kind == "inproc" {
		return redisadapter.NewInProcessRateLimiter(cfg.RateLimiter.PerSecond, cfg.RateLimiter.Burst)
	}
	return redisadapter.NewRedisRateLimiter(client, cfg.RateLimiter.PerSecond, cfg.RateLimiter.Burst, cfg.RateLimiter.FailOpen)
}

// RetryPolicy builds the delivery retry policy from config.
func RetryPolicy(cfg config.Config) delivery.Policy {
	return delivery.NewPolicy(cfg.Retry.MaxAttempts, cfg.Retry.Backoff, cfg.Retry.JitterRatio, nil)
}

// Provider builds the webhook client wrapped in a per-channel circuit breaker that
// reports state transitions to the circuit-state gauge.
func Provider(cfg config.Config, m *metrics.Metrics, log *slog.Logger) *provider.Breaker {
	client := provider.NewWebhookClient(cfg.Provider.WebhookURL, cfg.Provider.Timeout)
	onChange := func(channel domain.Channel, state gobreaker.State) {
		m.SetCircuitState(channel, circuitStateValue(state))
		log.Warn("circuit breaker state change",
			slog.String("channel", string(channel)),
			slog.String("state", state.String()))
	}
	return provider.NewBreaker(client, provider.BreakerConfig{
		FailureRateThreshold: cfg.Breaker.FailureRateThreshold,
		MinRequests:          cfg.Breaker.MinRequests,
		Window:               cfg.Breaker.Window,
		OpenTimeout:          cfg.Breaker.OpenTimeout,
		HalfOpenMax:          cfg.Breaker.HalfOpenMax,
	}, onChange)
}

func circuitStateValue(state gobreaker.State) float64 {
	switch state {
	case gobreaker.StateHalfOpen:
		return 1
	case gobreaker.StateOpen:
		return 2
	default:
		return 0
	}
}

// Readiness holds dependency health checks for background binaries.
type Readiness map[string]func(context.Context) error

// MetricsServer builds an HTTP server exposing /metrics, /healthz, and optional
// /readyz for a background binary (worker/scheduler). Address is METRICS_ADDR.
func MetricsServer(reg *prometheus.Registry, ready Readiness) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	if len(ready) > 0 {
		mux.HandleFunc("GET /readyz", handleReady(ready))
	}
	addr := os.Getenv("METRICS_ADDR")
	if addr == "" {
		addr = ":9090"
	}
	return &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
}

func handleReady(checks Readiness) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		results := make(map[string]string, len(checks))
		ok := true
		for name, check := range checks {
			if err := check(r.Context()); err != nil {
				results[name] = "down: " + err.Error()
				ok = false
			} else {
				results[name] = "ok"
			}
		}
		status := http.StatusOK
		if !ok {
			status = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"ready":` + boolJSON(ok) + `,"checks":` + marshalChecks(results) + `}`))
	}
}

func boolJSON(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

func marshalChecks(m map[string]string) string {
	b, _ := json.Marshal(m)
	return string(b)
}

// RedisHealth adapts a Redis client to the readiness HealthChecker port.
type RedisHealth struct{ Client *goredis.Client }

// Ping verifies Redis connectivity.
func (h RedisHealth) Ping(ctx context.Context) error {
	return h.Client.Ping(ctx).Err()
}
