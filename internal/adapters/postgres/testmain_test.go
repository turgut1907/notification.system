package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"

	"github.com/turgut1907/notification.system/internal/domain"
)

var intStore *Store

func TestMain(m *testing.M) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://notif:notif@localhost:5432/notifications?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := New(ctx, Config{PrimaryDSN: dsn, MaxConns: 8})
	if err != nil {
		fmt.Fprintf(os.Stderr, "postgres integration tests skipped: %v\n", err)
		os.Exit(0)
	}

	if err := runMigrations(dsn); err != nil {
		fmt.Fprintf(os.Stderr, "postgres migrations failed: %v\n", err)
		store.Close()
		os.Exit(1)
	}

	intStore = store
	code := m.Run()
	store.Close()
	os.Exit(code)
}

func runMigrations(dsn string) error {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return fmt.Errorf("runtime.Caller failed")
	}
	dir := filepath.Join(filepath.Dir(file), "..", "..", "..", "migrations")
	m, err := migrate.New("file://"+dir, dsn)
	if err != nil {
		return err
	}
	defer m.Close()
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return err
	}
	return nil
}

func resetDB(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	lock, err := intStore.HoldIntegrationTestLock(ctx)
	if err != nil {
		t.Fatalf("integration lock: %v", err)
	}
	t.Cleanup(func() { releaseIntegrationLock(ctx, lock) })
	if err := intStore.ResetIntegrationDB(ctx); err != nil {
		t.Fatalf("reset db: %v", err)
	}
}

func sampleRequestDelivery(now time.Time) (domain.Request, domain.Delivery, *domain.OutboxRecord) {
	reqID := uuid.New()
	delID := uuid.New()
	req := domain.Request{
		ID:              reqID,
		UserID:          "test-user",
		Recipient:       "+15551234567",
		Channel:         domain.ChannelSMS,
		Priority:        domain.PriorityHigh,
		RenderedContent: "hello",
		Status:          domain.RequestPending,
		CreatedAt:       now,
	}
	del := domain.Delivery{
		ID:        delID,
		RequestID: reqID,
		Channel:   domain.ChannelSMS,
		Priority:  domain.PriorityHigh,
		Status:    domain.DeliveryPending,
		CreatedAt: now,
		UpdatedAt: now,
	}
	outbox := &domain.OutboxRecord{
		ID:                uuid.New(),
		DeliveryID:        delID,
		DeliveryCreatedAt: now,
		Stream:            domain.Stream(domain.PriorityHigh, domain.ChannelSMS),
		Message: domain.DeliveryMessage{
			DeliveryID:        delID,
			DeliveryCreatedAt: now,
			RequestID:         reqID,
			Channel:           domain.ChannelSMS,
			Priority:          domain.PriorityHigh,
		},
	}
	return req, del, outbox
}
