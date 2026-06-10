package scheduler

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/turgut1907/notification.system/internal/domain"
)

type mockDB struct {
	outbox       []domain.OutboxEntry
	promoted     int
	swept        int
	reaped       int
	reconciled   int
	unpublished  int64
	publishIDs   []uuid.UUID
}

func (m *mockDB) PromoteDue(context.Context, int) (int, error)         { return m.promoted, nil }
func (m *mockDB) SweepRetries(context.Context, int) (int, error)       { return m.swept, nil }
func (m *mockDB) ReapStuck(context.Context) (int, error)             { return m.reaped, nil }
func (m *mockDB) ReconcilePending(context.Context, time.Duration, int) (int, error) {
	return m.reconciled, nil
}
func (m *mockDB) FetchUnpublishedOutbox(_ context.Context, limit int) ([]domain.OutboxEntry, error) {
	if len(m.outbox) > limit {
		return m.outbox[:limit], nil
	}
	return m.outbox, nil
}
func (m *mockDB) MarkOutboxPublished(_ context.Context, ids []uuid.UUID) error {
	m.publishIDs = append(m.publishIDs, ids...)
	return nil
}
func (m *mockDB) CountUnpublishedOutbox(context.Context) (int64, error) { return m.unpublished, nil }
func (m *mockDB) OldestDueLagSeconds(context.Context) (float64, error) { return 0, nil }
func (m *mockDB) CreateDeliveryPartition(context.Context, time.Time) error { return nil }
func (m *mockDB) ArchiveAndDropPartition(context.Context, time.Time) error { return nil }
func (m *mockDB) PurgePublishedOutbox(context.Context, time.Time) (int64, error) {
	return 0, nil
}

type mockQueue struct {
	published []string
	depth     int64
}

func (m *mockQueue) Publish(_ context.Context, stream string, _ domain.DeliveryMessage) error {
	m.published = append(m.published, stream)
	return nil
}
func (m *mockQueue) Depth(context.Context, string) (int64, error)      { return m.depth, nil }
func (m *mockQueue) Pending(context.Context, string) (int64, error)    { return 0, nil }

type mockSchedMetrics struct {
	promoted float64
}

func (m *mockSchedMetrics) SetQueueDepth(string, float64)   {}
func (m *mockSchedMetrics) SetQueuePending(string, float64) {}
func (m *mockSchedMetrics) SetOutboxUnpublished(float64)    {}
func (m *mockSchedMetrics) SetSchedulerLag(float64)         {}
func (m *mockSchedMetrics) AddPromoted(n float64)           { m.promoted += n }
func (m *mockSchedMetrics) AddReclaimed(float64)            {}
func (m *mockSchedMetrics) AddReconciled(float64)           {}

func testScheduler(db *mockDB, q *mockQueue, m *mockSchedMetrics) *Scheduler {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(Config{OutboxBatchSize: 10, PromoteBatch: 10}, db, q, m, log)
}

func TestRelayOutbox(t *testing.T) {
	id := uuid.New()
	db := &mockDB{outbox: []domain.OutboxEntry{{
		ID: id, Stream: "notifications.high.sms",
		Message: domain.DeliveryMessage{Channel: domain.ChannelSMS, Priority: domain.PriorityHigh},
	}}}
	q := &mockQueue{}
	s := testScheduler(db, q, &mockSchedMetrics{})
	s.relayOutbox(context.Background())
	if len(q.published) != 1 || len(db.publishIDs) != 1 {
		t.Fatalf("published=%d marked=%d", len(q.published), len(db.publishIDs))
	}
}

func TestPromoteDue(t *testing.T) {
	db := &mockDB{promoted: 3}
	m := &mockSchedMetrics{}
	s := testScheduler(db, &mockQueue{}, m)
	s.promoteDue(context.Background())
	if m.promoted != 3 {
		t.Fatalf("promoted %v", m.promoted)
	}
}

func TestReapStuck(t *testing.T) {
	db := &mockDB{reaped: 2}
	m := &mockSchedMetrics{}
	s := testScheduler(db, &mockQueue{}, m)
	s.reapStuck(context.Background())
}

func TestAllStreams(t *testing.T) {
	streams := allStreams()
	if len(streams) != 9 {
		t.Fatalf("want 9 streams, got %d", len(streams))
	}
}
