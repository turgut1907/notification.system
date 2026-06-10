// Package scheduler runs the background jobs that keep delivery processing healthy:
// the outbox relay, due/retry promotion, the stuck-lease reaper, reconciliation,
// partition/retention maintenance, and metric refresh. Each job is independent and
// individually tunable.
package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/turgut1907/notification.system/internal/domain"
)

// DB is the persistence port the scheduler drives.
type DB interface {
	PromoteDue(ctx context.Context, limit int) (int, error)
	SweepRetries(ctx context.Context, limit int) (int, error)
	ReapStuck(ctx context.Context) (int, error)
	ReconcilePending(ctx context.Context, olderThan time.Duration, limit int) (int, error)
	FetchUnpublishedOutbox(ctx context.Context, limit int) ([]domain.OutboxEntry, error)
	MarkOutboxPublished(ctx context.Context, ids []uuid.UUID) error
	CountUnpublishedOutbox(ctx context.Context) (int64, error)
	OldestDueLagSeconds(ctx context.Context) (float64, error)
	CreateDeliveryPartition(ctx context.Context, day time.Time) error
	ArchiveAndDropPartition(ctx context.Context, day time.Time) error
	PurgePublishedOutbox(ctx context.Context, before time.Time) (int64, error)
}

// Queue is the publish/inspect port.
type Queue interface {
	Publish(ctx context.Context, stream string, msg domain.DeliveryMessage) error
	Depth(ctx context.Context, stream string) (int64, error)
	Pending(ctx context.Context, stream string) (int64, error)
}

// Metrics is the metrics port the scheduler reports to.
type Metrics interface {
	SetQueueDepth(stream string, n float64)
	SetQueuePending(stream string, n float64)
	SetOutboxUnpublished(n float64)
	SetSchedulerLag(seconds float64)
	AddPromoted(n float64)
	AddReclaimed(n float64)
	AddReconciled(n float64)
}

// Config tunes the scheduler jobs.
type Config struct {
	PollInterval    time.Duration
	OutboxInterval  time.Duration
	OutboxBatchSize int
	PromoteBatch    int
	ReaperInterval  time.Duration
	ReconcileAfter  time.Duration
	RetentionDays   int
	PreCreateDays   int
	OutboxRetention time.Duration
}

// Scheduler bundles the dependencies for all background jobs.
type Scheduler struct {
	cfg     Config
	db      DB
	queue   Queue
	metrics Metrics
	log     *slog.Logger
}

// New constructs a Scheduler, applying sensible defaults.
func New(cfg Config, db DB, queue Queue, m Metrics, log *slog.Logger) *Scheduler {
	if cfg.RetentionDays <= 0 {
		cfg.RetentionDays = 30
	}
	if cfg.PreCreateDays <= 0 {
		cfg.PreCreateDays = 3
	}
	if cfg.OutboxRetention <= 0 {
		cfg.OutboxRetention = 24 * time.Hour
	}
	return &Scheduler{cfg: cfg, db: db, queue: queue, metrics: m, log: log}
}

// RunOutboxRelay publishes unpublished outbox rows to Redis and marks them published.
// It is exported for integration tests that drive the relay synchronously.
func (s *Scheduler) RunOutboxRelay(ctx context.Context) {
	s.relayOutbox(ctx)
}

// Run starts every background job and blocks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) error {
	// Ensure today's and upcoming partitions exist before any work is enqueued.
	s.maintainPartitions(ctx)

	var wg sync.WaitGroup
	jobs := []struct {
		name     string
		interval time.Duration
		fn       func(context.Context)
	}{
		{"outbox-relay", s.cfg.OutboxInterval, s.relayOutbox},
		{"promote-due", s.cfg.PollInterval, s.promoteDue},
		{"retry-sweep", s.cfg.PollInterval, s.sweepRetries},
		{"reaper", s.cfg.ReaperInterval, s.reapStuck},
		{"reconcile", s.cfg.ReaperInterval, s.reconcile},
		{"metrics", 5 * time.Second, s.refreshMetrics},
		{"maintenance", time.Hour, s.maintainPartitions},
	}

	for _, job := range jobs {
		wg.Add(1)
		go func(name string, interval time.Duration, fn func(context.Context)) {
			defer wg.Done()
			s.runLoop(ctx, name, interval, fn)
		}(job.name, job.interval, job.fn)
	}

	s.log.Info("scheduler started")
	<-ctx.Done()
	wg.Wait()
	s.log.Info("scheduler stopped")
	return nil
}

// allStreams returns every priority/channel stream name for metric scraping.
func allStreams() []string {
	var out []string
	for _, p := range domain.AllPriorities() {
		for _, c := range domain.AllChannels() {
			out = append(out, domain.Stream(p, c))
		}
	}
	return out
}

// runLoop invokes fn on a fixed interval until ctx is cancelled.
func (s *Scheduler) runLoop(ctx context.Context, name string, interval time.Duration, fn func(context.Context)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fn(ctx)
		}
	}
}
