package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/turgut1907/notification.system/internal/delivery"
	"github.com/turgut1907/notification.system/internal/domain"
	"github.com/turgut1907/notification.system/internal/platform/clock"
)

type mockQueue struct {
	acked []string
}

func (m *mockQueue) EnsureGroup(context.Context, string) error { return nil }
func (m *mockQueue) Read(context.Context, string, []string, int, time.Duration) ([]domain.StreamMessage, error) {
	return nil, nil
}
func (m *mockQueue) Ack(_ context.Context, _, id string) error {
	m.acked = append(m.acked, id)
	return nil
}

type mockRepo struct {
	del       domain.Delivery
	req       domain.Request
	claimOK   bool
	markSent  bool
	markRetry bool
	markFail  bool
}

func (m *mockRepo) GetRequestByID(context.Context, uuid.UUID) (domain.Request, error) {
	return m.req, nil
}
func (m *mockRepo) ClaimDelivery(context.Context, uuid.UUID, time.Time, time.Time) (domain.Delivery, bool, error) {
	return m.del, m.claimOK, nil
}
func (m *mockRepo) MarkSent(context.Context, uuid.UUID, time.Time, uuid.UUID, string) error {
	m.markSent = true
	return nil
}
func (m *mockRepo) MarkRetrying(context.Context, uuid.UUID, time.Time, int, time.Time, string) error {
	m.markRetry = true
	return nil
}
func (m *mockRepo) MarkFailed(context.Context, uuid.UUID, time.Time, uuid.UUID, int, string) error {
	m.markFail = true
	return nil
}

type mockProvider struct {
	res domain.ProviderResult
	err error
}

func (m *mockProvider) Send(context.Context, domain.ProviderRequest) (domain.ProviderResult, error) {
	return m.res, m.err
}

type mockLimiter struct {
	allowed bool
}

func (m *mockLimiter) Allow(context.Context, domain.Channel) (bool, time.Duration, error) {
	return m.allowed, 0, nil
}

type mockMetrics struct{}

func (mockMetrics) IncSent(domain.Channel, domain.Priority)                   {}
func (mockMetrics) IncFailed(domain.Channel, domain.Priority)                 {}
func (mockMetrics) IncRetry(domain.Channel, domain.Priority)                  {}
func (mockMetrics) ObserveProviderLatency(domain.Channel, float64)            {}
func (mockMetrics) ObserveProcessing(domain.Channel, domain.Priority, float64) {}
func (mockMetrics) ObserveE2ELatency(domain.Channel, domain.Priority, float64) {}
func (mockMetrics) IncDLQ(string)                                              {}

type captureMetrics struct {
	e2eLatency float64
}

func (m *captureMetrics) IncSent(domain.Channel, domain.Priority)                   {}
func (m *captureMetrics) IncFailed(domain.Channel, domain.Priority)                 {}
func (m *captureMetrics) IncRetry(domain.Channel, domain.Priority)                  {}
func (m *captureMetrics) ObserveProviderLatency(domain.Channel, float64)            {}
func (m *captureMetrics) ObserveProcessing(domain.Channel, domain.Priority, float64) {}
func (m *captureMetrics) ObserveE2ELatency(_ domain.Channel, _ domain.Priority, seconds float64) {
	m.e2eLatency = seconds
}
func (m *captureMetrics) IncDLQ(string) {}

func testWorker(t *testing.T, q Queue, repo *mockRepo, p *mockProvider, lim *mockLimiter) *Worker {
	t.Helper()
	clk := clock.NewFake(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	policy := delivery.NewPolicy(3, []time.Duration{time.Minute}, 0, nil)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(Config{
		Priorities: []domain.Priority{domain.PriorityHigh},
		Channels:   []domain.Channel{domain.ChannelSMS},
		Consumer:   "test", LeaseTTL: time.Minute, RateWaitStep: time.Millisecond, RateWaitMax: time.Second,
	}, q, repo, p, lim, policy, clk, mockMetrics{}, log)
}

func TestProcess_Success(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	delID := uuid.New()
	reqID := uuid.New()
	q := &mockQueue{}
	repo := &mockRepo{
		claimOK: true,
		del:     domain.Delivery{ID: delID, RequestID: reqID, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, CreatedAt: now},
		req:     domain.Request{ID: reqID, Recipient: "a@b.com", RenderedContent: "hi"},
	}
	p := &mockProvider{res: domain.ProviderResult{MessageID: "m1"}}
	w := testWorker(t, q, repo, p, &mockLimiter{allowed: true})

	msg := domain.StreamMessage{
		Stream: "notifications.high.sms", ID: "1-0",
		Delivery: domain.DeliveryMessage{DeliveryID: delID, DeliveryCreatedAt: now, RequestID: reqID, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh},
	}
	w.process(context.Background(), msg)

	if !repo.markSent || len(q.acked) != 1 {
		t.Fatalf("sent=%v acks=%d", repo.markSent, len(q.acked))
	}
}

func TestProcess_SuccessRecordsE2ELatency(t *testing.T) {
	created := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	now := created.Add(3 * time.Second)
	delID := uuid.New()
	reqID := uuid.New()
	m := &captureMetrics{}
	clk := clock.NewFake(now)
	policy := delivery.NewPolicy(3, []time.Duration{time.Minute}, 0, nil)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	w := New(Config{
		Priorities: []domain.Priority{domain.PriorityHigh},
		Channels:   []domain.Channel{domain.ChannelSMS},
		Consumer:   "test", LeaseTTL: time.Minute, RateWaitStep: time.Millisecond, RateWaitMax: time.Second,
	}, &mockQueue{}, &mockRepo{
		claimOK: true,
		del:     domain.Delivery{ID: delID, RequestID: reqID, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, CreatedAt: created},
		req:     domain.Request{ID: reqID, Recipient: "a@b.com", RenderedContent: "hi"},
	}, &mockProvider{res: domain.ProviderResult{MessageID: "m1"}}, &mockLimiter{allowed: true},
		policy, clk, m, log)

	w.process(context.Background(), domain.StreamMessage{
		Stream: "notifications.high.sms", ID: "1-0",
		Delivery: domain.DeliveryMessage{DeliveryID: delID, DeliveryCreatedAt: created, RequestID: reqID, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh},
	})

	if m.e2eLatency != 3 {
		t.Fatalf("e2e latency = %v, want 3", m.e2eLatency)
	}
}

func TestProcess_ClaimLost(t *testing.T) {
	q := &mockQueue{}
	repo := &mockRepo{claimOK: false}
	w := testWorker(t, q, repo, &mockProvider{}, &mockLimiter{allowed: true})
	w.process(context.Background(), domain.StreamMessage{ID: "1-0", Delivery: domain.DeliveryMessage{Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}})
	if len(q.acked) != 1 || repo.markSent {
		t.Fatal("should ack and skip")
	}
}

func TestProcess_RateLimitedRequeue(t *testing.T) {
	now := time.Now().UTC()
	delID := uuid.New()
	q := &mockQueue{}
	repo := &mockRepo{claimOK: true, del: domain.Delivery{ID: delID, CreatedAt: now}}
	w := testWorker(t, q, repo, &mockProvider{}, &mockLimiter{allowed: false})
	w.cfg.RateWaitMax = 0
	w.process(context.Background(), domain.StreamMessage{
		ID: "1-0", Delivery: domain.DeliveryMessage{DeliveryID: delID, DeliveryCreatedAt: now, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh},
	})
	if !repo.markRetry {
		t.Fatal("expected requeue without attempt")
	}
}

func TestProcess_BreakerOpenRequeue(t *testing.T) {
	now := time.Now().UTC()
	delID := uuid.New()
	repo := &mockRepo{claimOK: true, del: domain.Delivery{ID: delID, RequestID: uuid.New(), CreatedAt: now}, req: domain.Request{RenderedContent: "x"}}
	p := &mockProvider{err: &domain.ProviderError{BreakerOpen: true, Err: errors.New("open")}}
	w := testWorker(t, &mockQueue{}, repo, p, &mockLimiter{allowed: true})
	w.process(context.Background(), domain.StreamMessage{
		ID: "1-0", Delivery: domain.DeliveryMessage{DeliveryID: delID, DeliveryCreatedAt: now, RequestID: repo.req.ID, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh},
	})
	if !repo.markRetry || repo.markFail {
		t.Fatal("breaker should requeue without fail")
	}
}

func TestProcess_PermanentFailure(t *testing.T) {
	now := time.Now().UTC()
	delID := uuid.New()
	reqID := uuid.New()
	repo := &mockRepo{
		claimOK: true,
		del:     domain.Delivery{ID: delID, RequestID: reqID, CreatedAt: now},
		req:     domain.Request{ID: reqID, RenderedContent: "x"},
	}
	p := &mockProvider{err: &domain.ProviderError{Permanent: true, Err: errors.New("bad")}}
	w := testWorker(t, &mockQueue{}, repo, p, &mockLimiter{allowed: true})
	w.process(context.Background(), domain.StreamMessage{
		ID: "1-0", Delivery: domain.DeliveryMessage{DeliveryID: delID, DeliveryCreatedAt: now, RequestID: reqID, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh},
	})
	if !repo.markFail {
		t.Fatal("expected permanent fail")
	}
}

func TestReadLoopProcessesMessage(t *testing.T) {
	now := time.Now().UTC()
	delID := uuid.New()
	msg := domain.StreamMessage{
		Stream: "notifications.high.sms", ID: "1-0",
		Delivery: domain.DeliveryMessage{
			DeliveryID: delID, DeliveryCreatedAt: now,
			RequestID: uuid.New(), Channel: domain.ChannelSMS, Priority: domain.PriorityHigh,
		},
	}
	q := &readQueue{msgs: [][]domain.StreamMessage{{msg}}}
	repo := &mockRepo{
		claimOK: true,
		del:     domain.Delivery{ID: delID, RequestID: uuid.New(), Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, CreatedAt: now},
		req:     domain.Request{RenderedContent: "x", Recipient: "a@b.com"},
	}
	w := testWorker(t, q, repo, &mockProvider{res: domain.ProviderResult{MessageID: "m"}}, &mockLimiter{allowed: true})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_ = w.Run(ctx)
	if !repo.markSent {
		t.Fatal("expected message processed")
	}
}

type readQueue struct {
	mockQueue
	msgs  [][]domain.StreamMessage
	calls int
}

func (q *readQueue) Read(ctx context.Context, _ string, _ []string, _ int, _ time.Duration) ([]domain.StreamMessage, error) {
	if q.calls >= len(q.msgs) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	m := q.msgs[q.calls]
	q.calls++
	return m, nil
}

func TestWorkerNewDefaults(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	w := New(Config{Priorities: []domain.Priority{domain.PriorityHigh}, Channels: []domain.Channel{domain.ChannelSMS}},
		&mockQueue{}, &mockRepo{}, &mockProvider{}, &mockLimiter{allowed: true},
		delivery.NewPolicy(3, []time.Duration{time.Minute}, 0, nil), clock.NewFake(time.Now()), mockMetrics{}, log)
	if w.cfg.Concurrency != 1 || w.cfg.BatchSize != 1 {
		t.Fatalf("defaults not applied")
	}
}
