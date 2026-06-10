// Package worker consumes delivery messages from Redis streams and runs the send
// pipeline: claim -> rate limit -> circuit breaker -> provider -> persist outcome.
// It depends only on ports so it can be tested without real infrastructure.
package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/turgut1907/notification.system/internal/delivery"
	"github.com/turgut1907/notification.system/internal/domain"
	"github.com/turgut1907/notification.system/internal/platform/clock"
)

// Queue is the streaming port the worker consumes from.
type Queue interface {
	EnsureGroup(ctx context.Context, stream string) error
	Read(ctx context.Context, consumer string, streams []string, count int, block time.Duration) ([]domain.StreamMessage, error)
	Ack(ctx context.Context, stream, id string) error
}

// Repository is the persistence port for claiming and finalizing deliveries.
type Repository interface {
	GetRequestByID(ctx context.Context, id uuid.UUID) (domain.Request, error)
	ClaimDelivery(ctx context.Context, id uuid.UUID, createdAt time.Time, leaseUntil time.Time) (domain.Delivery, bool, error)
	MarkSent(ctx context.Context, id uuid.UUID, createdAt time.Time, requestID uuid.UUID, providerMessageID string) error
	MarkRetrying(ctx context.Context, id uuid.UUID, createdAt time.Time, attemptCount int, nextRetryAt time.Time, lastErr string) error
	MarkFailed(ctx context.Context, id uuid.UUID, createdAt time.Time, requestID uuid.UUID, attemptCount int, lastErr string) error
}

// Provider is the external send port (already wrapped by the circuit breaker).
type Provider interface {
	Send(ctx context.Context, req domain.ProviderRequest) (domain.ProviderResult, error)
}

// RateLimiter is the per-channel limiting port.
type RateLimiter interface {
	Allow(ctx context.Context, channel domain.Channel) (bool, time.Duration, error)
}

// Metrics is the narrow metrics port the worker reports to.
type Metrics interface {
	IncSent(c domain.Channel, p domain.Priority)
	IncFailed(c domain.Channel, p domain.Priority)
	IncRetry(c domain.Channel, p domain.Priority)
	ObserveProviderLatency(c domain.Channel, seconds float64)
	ObserveProcessing(c domain.Channel, p domain.Priority, seconds float64)
	ObserveE2ELatency(c domain.Channel, p domain.Priority, seconds float64)
	IncDLQ(reason string)
}

// DLQPublisher publishes terminal failures to the dead-letter stream.
type DLQPublisher interface {
	Publish(ctx context.Context, msg domain.DLQMessage) error
}

// Config tunes the worker.
type Config struct {
	Priorities   []domain.Priority
	Channels     []domain.Channel
	Concurrency  int
	Consumer     string
	BatchSize    int
	LeaseTTL     time.Duration
	BlockTime    time.Duration
	RateWaitMax  time.Duration
	RateWaitStep time.Duration
}

// Worker orchestrates consumption and processing.
type Worker struct {
	cfg      Config
	queue    Queue
	repo     Repository
	provider Provider
	limiter  RateLimiter
	policy   delivery.Policy
	clock    clock.Clock
	metrics  Metrics
	dlq      DLQPublisher
	log      *slog.Logger
	streams  []string
}

// New constructs a Worker and computes the set of streams to consume (priorities x
// channels), ordered so higher priorities are read first.
func New(cfg Config, q Queue, repo Repository, p Provider, rl RateLimiter, policy delivery.Policy, clk clock.Clock, m Metrics, log *slog.Logger) *Worker {
	return newWorker(cfg, q, repo, p, rl, policy, clk, m, nil, log)
}

// NewWithDLQ is like New but also publishes terminal failures to a DLQ stream.
func NewWithDLQ(cfg Config, q Queue, repo Repository, p Provider, rl RateLimiter, policy delivery.Policy, clk clock.Clock, m Metrics, dlq DLQPublisher, log *slog.Logger) *Worker {
	return newWorker(cfg, q, repo, p, rl, policy, clk, m, dlq, log)
}

func newWorker(cfg Config, q Queue, repo Repository, p Provider, rl RateLimiter, policy delivery.Policy, clk clock.Clock, m Metrics, dlq DLQPublisher, log *slog.Logger) *Worker {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1
	}
	if cfg.RateWaitStep <= 0 {
		cfg.RateWaitStep = 50 * time.Millisecond
	}
	if cfg.RateWaitMax <= 0 {
		cfg.RateWaitMax = 30 * time.Second
	}

	streams := make([]string, 0, len(cfg.Priorities)*len(cfg.Channels))
	for _, pr := range cfg.Priorities {
		for _, ch := range cfg.Channels {
			streams = append(streams, domain.Stream(pr, ch))
		}
	}

	return &Worker{
		cfg: cfg, queue: q, repo: repo, provider: p, limiter: rl,
		policy: policy, clock: clk, metrics: m, dlq: dlq, log: log, streams: streams,
	}
}

// Run consumes until ctx is cancelled, then drains in-flight work and returns.
func (w *Worker) Run(ctx context.Context) error {
	for _, stream := range w.streams {
		if err := w.queue.EnsureGroup(ctx, stream); err != nil {
			return err
		}
	}
	w.log.Info("worker started",
		slog.Any("streams", w.streams),
		slog.Int("concurrency", w.cfg.Concurrency))

	jobs := make(chan domain.StreamMessage, w.cfg.Concurrency)
	var wg sync.WaitGroup
	for i := 0; i < w.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range jobs {
				w.process(context.WithoutCancel(ctx), msg)
			}
		}()
	}

	err := w.readLoop(ctx, jobs)
	close(jobs)
	wg.Wait()
	w.log.Info("worker stopped")
	return err
}

// readLoop blocks on the queue and feeds messages to the worker pool.
func (w *Worker) readLoop(ctx context.Context, jobs chan<- domain.StreamMessage) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		msgs, err := w.queue.Read(ctx, w.cfg.Consumer, w.streams, w.cfg.BatchSize, w.cfg.BlockTime)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.log.Error("queue read failed", slog.String("error", err.Error()))
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
			}
			continue
		}
		for _, msg := range msgs {
			select {
			case <-ctx.Done():
				return nil
			case jobs <- msg:
			}
		}
	}
}

// logger returns a logger enriched with delivery context.
func (w *Worker) logger(msg domain.StreamMessage) *slog.Logger {
	return w.log.With(
		slog.String("delivery_id", msg.Delivery.DeliveryID.String()),
		slog.String("request_id", msg.Delivery.RequestID.String()),
		slog.String("channel", string(msg.Delivery.Channel)),
		slog.String("priority", string(msg.Delivery.Priority)),
	)
}
