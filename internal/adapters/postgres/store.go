// Package postgres implements the persistence ports using PostgreSQL via pgx.
// A single Store concrete type satisfies the focused repository interfaces declared
// by the service/orchestrator packages.
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store holds connection pools. The primary handles writes and claims; the reader
// pool (which may point at a replica) serves read-only list/status queries.
type Store struct {
	primary *pgxpool.Pool
	reader  *pgxpool.Pool
}

// Config configures the Store connection pools.
type Config struct {
	PrimaryDSN string
	ReaderDSN  string
	MaxConns   int32
}

// New connects the primary and reader pools and verifies connectivity.
func New(ctx context.Context, cfg Config) (*Store, error) {
	primary, err := newPool(ctx, cfg.PrimaryDSN, cfg.MaxConns)
	if err != nil {
		return nil, fmt.Errorf("connect primary: %w", err)
	}

	reader := primary
	if cfg.ReaderDSN != "" && cfg.ReaderDSN != cfg.PrimaryDSN {
		reader, err = newPool(ctx, cfg.ReaderDSN, cfg.MaxConns)
		if err != nil {
			primary.Close()
			return nil, fmt.Errorf("connect reader: %w", err)
		}
	}

	return &Store{primary: primary, reader: reader}, nil
}

func newPool(ctx context.Context, dsn string, maxConns int32) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if maxConns > 0 {
		poolCfg.MaxConns = maxConns
	}
	poolCfg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// Close releases all pooled connections.
func (s *Store) Close() {
	if s.reader != nil && s.reader != s.primary {
		s.reader.Close()
	}
	if s.primary != nil {
		s.primary.Close()
	}
}

// Ping verifies primary connectivity for health checks.
func (s *Store) Ping(ctx context.Context) error {
	return s.primary.Ping(ctx)
}

const integrationTestLockKey int64 = 8675309

type integrationTestLock struct {
	conn *pgxpool.Conn
}

// HoldIntegrationTestLock serializes integration/e2e tests that share one database.
// Call once per test; release is automatic via cleanup.
func (s *Store) HoldIntegrationTestLock(ctx context.Context) (*integrationTestLock, error) {
	conn, err := s.primary.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire conn: %w", err)
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, integrationTestLockKey); err != nil {
		conn.Release()
		return nil, fmt.Errorf("advisory lock: %w", err)
	}
	return &integrationTestLock{conn: conn}, nil
}

// Release unlocks the advisory lock and returns the connection to the pool.
func (l *integrationTestLock) Release(ctx context.Context) {
	if l == nil || l.conn == nil {
		return
	}
	_, _ = l.conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, integrationTestLockKey)
	l.conn.Release()
	l.conn = nil
}

// ResetIntegrationDB truncates operational tables and re-seeds the default template.
// It is intended for integration and end-to-end tests only.
func (s *Store) ResetIntegrationDB(ctx context.Context) error {
	if _, err := s.primary.Exec(ctx, `
		TRUNCATE outbox, notification_deliveries, notification_requests,
		         notification_batches, templates RESTART IDENTITY CASCADE`); err != nil {
		return fmt.Errorf("truncate: %w", err)
	}
	if _, err := s.primary.Exec(ctx, `
		INSERT INTO templates (id, name, channel, content, required_variables)
		VALUES (
			'00000000-0000-0000-0000-000000000001',
			'verification_code', 'sms',
			'Your code is {{code}}', '["code"]'::jsonb
		) ON CONFLICT (id) DO NOTHING`); err != nil {
		return fmt.Errorf("seed template: %w", err)
	}
	return nil
}

// releaseIntegrationLock is a test helper cleanup.
func releaseIntegrationLock(ctx context.Context, lock *integrationTestLock) {
	if lock != nil {
		lock.Release(ctx)
	}
}

// withTx runs fn inside a serializable-safe transaction, committing on success and
// rolling back on error or panic.
func (s *Store) withTx(ctx context.Context, fn func(pgx.Tx) error) (err error) {
	tx, err := s.primary.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(ctx)
			panic(p)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
