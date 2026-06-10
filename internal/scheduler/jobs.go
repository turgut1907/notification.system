package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	"github.com/turgut1907/notification.system/internal/platform/tracing"
)

// relayOutbox publishes unpublished outbox rows to their streams, then marks them
// published. SKIP LOCKED in the fetch lets multiple relays run concurrently.
func (s *Scheduler) relayOutbox(ctx context.Context) {
	ctx, span := tracing.StartSpan(ctx, "scheduler.outbox_relay")
	defer span.End()

	for {
		entries, err := s.db.FetchUnpublishedOutbox(ctx, s.cfg.OutboxBatchSize)
		if err != nil {
			s.log.Error("fetch outbox failed", slog.String("error", err.Error()))
			return
		}
		if len(entries) == 0 {
			return
		}
		span.SetAttributes(attribute.Int("batch_size", len(entries)))

		published := make([]uuid.UUID, 0, len(entries))
		for _, e := range entries {
			if err := s.queue.Publish(ctx, e.Stream, e.Message); err != nil {
				s.log.Error("publish failed",
					slog.String("stream", e.Stream),
					slog.String("error", err.Error()))
				continue // leave unpublished; a later tick retries (publish is idempotent downstream)
			}
			published = append(published, e.ID)
		}

		if err := s.db.MarkOutboxPublished(ctx, published); err != nil {
			s.log.Error("mark published failed", slog.String("error", err.Error()))
			return
		}

		// If we drained a full batch there may be more; loop again immediately.
		if len(entries) < s.cfg.OutboxBatchSize {
			return
		}
	}
}

// promoteDue moves due SCHEDULED deliveries to PENDING (drain style).
func (s *Scheduler) promoteDue(ctx context.Context) {
	for {
		n, err := s.db.PromoteDue(ctx, s.cfg.PromoteBatch)
		if err != nil {
			s.log.Error("promote due failed", slog.String("error", err.Error()))
			return
		}
		if n > 0 {
			s.metrics.AddPromoted(float64(n))
			s.log.Debug("promoted scheduled deliveries", slog.Int("count", n))
		}
		if n < s.cfg.PromoteBatch {
			return
		}
	}
}

// sweepRetries re-enqueues due RETRYING deliveries (drain style).
func (s *Scheduler) sweepRetries(ctx context.Context) {
	for {
		n, err := s.db.SweepRetries(ctx, s.cfg.PromoteBatch)
		if err != nil {
			s.log.Error("retry sweep failed", slog.String("error", err.Error()))
			return
		}
		if n > 0 {
			s.log.Debug("swept retries", slog.Int("count", n))
		}
		if n < s.cfg.PromoteBatch {
			return
		}
	}
}

// reapStuck reclaims deliveries whose processing lease expired (crashed workers).
func (s *Scheduler) reapStuck(ctx context.Context) {
	n, err := s.db.ReapStuck(ctx)
	if err != nil {
		s.log.Error("reaper failed", slog.String("error", err.Error()))
		return
	}
	if n > 0 {
		s.metrics.AddReclaimed(float64(n))
		s.log.Warn("reclaimed stuck deliveries", slog.Int("count", n))
	}
}

// reconcile re-enqueues orphaned PENDING deliveries that lost their outbox row.
func (s *Scheduler) reconcile(ctx context.Context) {
	n, err := s.db.ReconcilePending(ctx, s.cfg.ReconcileAfter, s.cfg.PromoteBatch)
	if err != nil {
		s.log.Error("reconcile failed", slog.String("error", err.Error()))
		return
	}
	if n > 0 {
		s.metrics.AddReconciled(float64(n))
		s.log.Warn("reconciled orphaned deliveries", slog.Int("count", n))
	}
}

// maintainPartitions pre-creates upcoming daily partitions and archives+drops aged
// ones, then purges old published outbox rows.
func (s *Scheduler) maintainPartitions(ctx context.Context) {
	today := time.Now().UTC().Truncate(24 * time.Hour)

	for i := 0; i <= s.cfg.PreCreateDays; i++ {
		if err := s.db.CreateDeliveryPartition(ctx, today.AddDate(0, 0, i)); err != nil {
			s.log.Error("create partition failed", slog.String("error", err.Error()))
		}
	}

	// Drop the partition that just passed the retention window.
	dropDay := today.AddDate(0, 0, -(s.cfg.RetentionDays + 1))
	if err := s.db.ArchiveAndDropPartition(ctx, dropDay); err != nil {
		s.log.Error("archive/drop partition failed", slog.String("error", err.Error()))
	}

	if deleted, err := s.db.PurgePublishedOutbox(ctx, time.Now().UTC().Add(-s.cfg.OutboxRetention)); err != nil {
		s.log.Error("purge outbox failed", slog.String("error", err.Error()))
	} else if deleted > 0 {
		s.log.Info("purged published outbox rows", slog.Int64("count", deleted))
	}
}

// refreshMetrics updates gauges (outbox backlog and per-stream depth).
func (s *Scheduler) refreshMetrics(ctx context.Context) {
	if n, err := s.db.CountUnpublishedOutbox(ctx); err == nil {
		s.metrics.SetOutboxUnpublished(float64(n))
	}
	if lag, err := s.db.OldestDueLagSeconds(ctx); err == nil {
		s.metrics.SetSchedulerLag(lag)
	}
	for _, stream := range allStreams() {
		if depth, err := s.queue.Depth(ctx, stream); err == nil {
			s.metrics.SetQueueDepth(stream, float64(depth))
		}
		if pending, err := s.queue.Pending(ctx, stream); err == nil {
			s.metrics.SetQueuePending(stream, float64(pending))
		}
	}
}
