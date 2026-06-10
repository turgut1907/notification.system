package scheduler

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestNewDefaults(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := New(Config{}, &mockDB{}, &mockQueue{}, &mockSchedMetrics{}, log)
	if s.cfg.RetentionDays != 30 || s.cfg.PreCreateDays != 3 {
		t.Fatalf("defaults: %+v", s.cfg)
	}
}

func TestRunLoopCancels(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := New(Config{PollInterval: 5 * time.Millisecond}, &mockDB{}, &mockQueue{}, &mockSchedMetrics{}, log)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.runLoop(ctx, "test", time.Millisecond, func(context.Context) {})
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runLoop did not stop")
	}
}

func TestRefreshMetrics(t *testing.T) {
	db := &mockDB{unpublished: 5}
	q := &mockQueue{depth: 2}
	s := testScheduler(db, q, &mockSchedMetrics{})
	s.refreshMetrics(context.Background())
}

func TestMaintainPartitions(t *testing.T) {
	s := testScheduler(&mockDB{}, &mockQueue{}, &mockSchedMetrics{})
	s.maintainPartitions(context.Background())
}

func TestRunStartsAndStops(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := New(Config{
		PollInterval:     10 * time.Millisecond,
		OutboxInterval:   10 * time.Millisecond,
		ReaperInterval:   10 * time.Millisecond,
		PromoteBatch:     10,
		OutboxBatchSize:  10,
	}, &mockDB{}, &mockQueue{}, &mockSchedMetrics{}, log)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestSweepRetriesAndReconcile(t *testing.T) {
	db := &mockDB{swept: 1, reconciled: 2}
	s := testScheduler(db, &mockQueue{}, &mockSchedMetrics{})
	s.sweepRetries(context.Background())
	s.reconcile(context.Background())
}
