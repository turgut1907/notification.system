package e2e

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
	goredis "github.com/redis/go-redis/v9"

	"github.com/turgut1907/notification.system/internal/adapters/postgres"
)

var (
	envStore  *postgres.Store
	envRedis  *goredis.Client
	skipE2E   bool
	skipReason string
)

func TestMain(m *testing.M) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://notif:notif@localhost:5432/notifications?sslmode=disable"
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	store, err := postgres.New(ctx, postgres.Config{PrimaryDSN: dsn, MaxConns: 8})
	if err != nil {
		skipE2E = true
		skipReason = fmt.Sprintf("postgres unavailable: %v", err)
		os.Exit(m.Run())
	}
	if err := runMigrations(dsn); err != nil {
		store.Close()
		fmt.Fprintf(os.Stderr, "e2e migrations failed: %v\n", err)
		os.Exit(1)
	}

	client := goredis.NewClient(&goredis.Options{Addr: redisAddr})
	if err := client.Ping(ctx).Err(); err != nil {
		store.Close()
		skipE2E = true
		skipReason = fmt.Sprintf("redis unavailable: %v", err)
		os.Exit(m.Run())
	}

	envStore = store
	envRedis = client
	code := m.Run()
	store.Close()
	_ = client.Close()
	os.Exit(code)
}

func runMigrations(dsn string) error {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return fmt.Errorf("runtime.Caller failed")
	}
	dir := filepath.Join(filepath.Dir(file), "..", "..", "migrations")
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

func requireInfra(t *testing.T) {
	t.Helper()
	if skipE2E {
		t.Skip(skipReason)
	}
}
