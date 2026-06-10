// Package config loads and validates all runtime configuration from the environment.
// Defaults are chosen so the system runs locally with docker-compose out of the box.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/turgut1907/notification.system/internal/domain"
)

// Config is the fully-resolved application configuration shared by all binaries.
type Config struct {
	LogLevel string

	HTTPAddr        string
	ShutdownTimeout time.Duration

	Postgres PostgresConfig
	Redis    RedisConfig

	RateLimiter RateLimiterConfig
	Breaker     BreakerConfig
	Retry       RetryConfig
	Provider    ProviderConfig
	Worker      WorkerConfig
	Scheduler   SchedulerConfig
	Auth        AuthConfig
	Tracing     TracingConfig
}

// TracingConfig holds OpenTelemetry settings.
type TracingConfig struct {
	Enabled     bool
	Endpoint    string
	ServiceName string
}

// AuthConfig holds API authentication settings.
type AuthConfig struct {
	JWTSecret string
}

// PostgresConfig holds database connection settings.
type PostgresConfig struct {
	DSN        string
	ReplicaDSN string // optional; falls back to DSN when empty
	MaxConns   int32
}

// RedisConfig holds Redis connection settings.
type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

// RateLimiterConfig configures per-channel rate limiting.
type RateLimiterConfig struct {
	Kind         string // "redis" or "inproc"
	PerSecond    int
	Burst        int
	FailOpen     bool
	WaitInterval time.Duration
}

// BreakerConfig configures the per-channel circuit breaker.
type BreakerConfig struct {
	FailureRateThreshold float64
	MinRequests          uint32
	Window               time.Duration
	OpenTimeout          time.Duration
	HalfOpenMax          uint32
}

// RetryConfig configures the delivery retry policy.
type RetryConfig struct {
	MaxAttempts int
	Backoff     []time.Duration
	JitterRatio float64
}

// ProviderConfig configures the external provider adapter.
type ProviderConfig struct {
	WebhookURL string
	Timeout    time.Duration
}

// WorkerConfig configures the worker process.
type WorkerConfig struct {
	Priorities  []domain.Priority
	Concurrency int
	GroupName   string
	Consumer    string
	BatchSize   int
	LeaseTTL    time.Duration
	BlockTime   time.Duration
}

// SchedulerConfig configures the scheduler/outbox process.
type SchedulerConfig struct {
	PollInterval    time.Duration
	OutboxInterval  time.Duration
	OutboxBatchSize int
	PromoteBatch    int
	ReaperInterval  time.Duration
	ReconcileAfter  time.Duration
}

// Load reads configuration from the environment and validates invariants.
func Load() (Config, error) {
	cfg := Config{
		LogLevel:        getString("LOG_LEVEL", "info"),
		HTTPAddr:        getString("HTTP_ADDR", ":8080"),
		ShutdownTimeout: getDuration("SHUTDOWN_TIMEOUT", 15*time.Second),
		Postgres: PostgresConfig{
			DSN:        getString("POSTGRES_DSN", "postgres://notif:notif@localhost:5432/notifications?sslmode=disable"),
			ReplicaDSN: getString("POSTGRES_REPLICA_DSN", ""),
			MaxConns:   int32(getInt("POSTGRES_MAX_CONNS", 10)),
		},
		Redis: RedisConfig{
			Addr:     getString("REDIS_ADDR", "localhost:6379"),
			Password: getString("REDIS_PASSWORD", ""),
			DB:       getInt("REDIS_DB", 0),
		},
		RateLimiter: RateLimiterConfig{
			Kind:         getString("RATE_LIMITER", "redis"),
			PerSecond:    getInt("RATE_LIMIT_PER_SECOND", 100),
			Burst:        getInt("RATE_LIMIT_BURST", 100),
			FailOpen:     getBool("RATE_LIMIT_FAIL_OPEN", true),
			WaitInterval: getDuration("RATE_LIMIT_WAIT_INTERVAL", 50*time.Millisecond),
		},
		Breaker: BreakerConfig{
			FailureRateThreshold: getFloat("CB_FAILURE_RATE_THRESHOLD", 0.5),
			MinRequests:          uint32(getInt("CB_MIN_REQUESTS", 20)),
			Window:               getDuration("CB_WINDOW", 60*time.Second),
			OpenTimeout:          getDuration("CB_OPEN_TIMEOUT", 30*time.Second),
			HalfOpenMax:          uint32(getInt("CB_HALF_OPEN_MAX", 5)),
		},
		Retry: RetryConfig{
			MaxAttempts: getInt("MAX_ATTEMPTS", 5),
			Backoff:     getDurationList("RETRY_BACKOFF", []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute, 30 * time.Minute, 60 * time.Minute}),
			JitterRatio: getFloat("RETRY_JITTER_RATIO", 0.2),
		},
		Provider: ProviderConfig{
			WebhookURL: getString("PROVIDER_WEBHOOK_URL", ""),
			Timeout:    getDuration("PROVIDER_TIMEOUT", 10*time.Second),
		},
		Worker: WorkerConfig{
			Priorities:  getPriorities("WORKER_PRIORITIES", domain.AllPriorities()),
			Concurrency: getInt("WORKER_CONCURRENCY", 8),
			GroupName:   getString("WORKER_GROUP", "notification-workers"),
			Consumer:    getString("WORKER_CONSUMER", hostnameOrDefault()),
			BatchSize:   getInt("WORKER_BATCH_SIZE", 16),
			LeaseTTL:    getDuration("WORKER_LEASE_TTL", 2*time.Minute),
			BlockTime:   getDuration("WORKER_BLOCK_TIME", 5*time.Second),
		},
		Scheduler: SchedulerConfig{
			PollInterval:    getDuration("SCHEDULER_POLL_INTERVAL", time.Second),
			OutboxInterval:  getDuration("OUTBOX_POLL_INTERVAL", 200*time.Millisecond),
			OutboxBatchSize: getInt("OUTBOX_BATCH_SIZE", 256),
			PromoteBatch:    getInt("SCHEDULER_PROMOTE_BATCH", 500),
			ReaperInterval:  getDuration("SCHEDULER_REAPER_INTERVAL", 30*time.Second),
			ReconcileAfter:  getDuration("SCHEDULER_RECONCILE_AFTER", time.Minute),
		},
		Auth: AuthConfig{
			JWTSecret: getString("JWT_SECRET", ""),
		},
		Tracing: TracingConfig{
			Enabled:     getBool("OTEL_ENABLED", false),
			Endpoint:    getString("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318"),
			ServiceName: getString("OTEL_SERVICE_NAME", ""),
		},
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// validate enforces cross-field invariants that must hold for correctness.
func (c Config) validate() error {
	if len(c.Worker.Priorities) == 0 {
		return fmt.Errorf("config: WORKER_PRIORITIES must list at least one priority")
	}
	// Critical invariant: the lease must outlive a provider call, otherwise the
	// stuck-PROCESSING reaper could reclaim an in-flight delivery and double-send.
	if c.Worker.LeaseTTL <= c.Provider.Timeout {
		return fmt.Errorf("config: WORKER_LEASE_TTL (%s) must be greater than PROVIDER_TIMEOUT (%s)", c.Worker.LeaseTTL, c.Provider.Timeout)
	}
	if c.Retry.MaxAttempts < 1 {
		return fmt.Errorf("config: MAX_ATTEMPTS must be >= 1")
	}
	if len(c.Retry.Backoff) == 0 {
		return fmt.Errorf("config: RETRY_BACKOFF must have at least one entry")
	}
	if c.RateLimiter.Kind != "redis" && c.RateLimiter.Kind != "inproc" {
		return fmt.Errorf("config: RATE_LIMITER must be 'redis' or 'inproc'")
	}
	return nil
}

// ReplicaOrPrimary returns the replica DSN if configured, else the primary DSN.
func (p PostgresConfig) ReplicaOrPrimary() string {
	if p.ReplicaDSN != "" {
		return p.ReplicaDSN
	}
	return p.DSN
}

func hostnameOrDefault() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "worker-1"
}

func getString(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getFloat(key string, def float64) float64 {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func getBool(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func getDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func getDurationList(key string, def []time.Duration) []time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	parts := strings.Split(v, ",")
	out := make([]time.Duration, 0, len(parts))
	for _, p := range parts {
		d, err := time.ParseDuration(strings.TrimSpace(p))
		if err != nil {
			return def
		}
		out = append(out, d)
	}
	if len(out) == 0 {
		return def
	}
	return out
}

func getPriorities(key string, def []domain.Priority) []domain.Priority {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	parts := strings.Split(v, ",")
	out := make([]domain.Priority, 0, len(parts))
	for _, p := range parts {
		pr := domain.Priority(strings.TrimSpace(strings.ToLower(p)))
		if pr.Valid() {
			out = append(out, pr)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}
