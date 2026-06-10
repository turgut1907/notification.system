package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/turgut1907/notification.system/internal/domain"
)

// dueRow is an internal projection of a delivery being (re)enqueued.
type dueRow struct {
	id        uuid.UUID
	createdAt time.Time
	requestID uuid.UUID
	channel   domain.Channel
	priority  domain.Priority
}

// PromoteDue moves due SCHEDULED deliveries to PENDING and writes their outbox rows
// in one transaction. Returns the number promoted.
func (s *Store) PromoteDue(ctx context.Context, limit int) (int, error) {
	return s.transitionAndEnqueue(ctx,
		`WITH due AS (
		    SELECT id, created_at
		    FROM notification_deliveries
		    WHERE status='SCHEDULED' AND send_at <= now()
		    ORDER BY send_at
		    LIMIT $1
		    FOR UPDATE SKIP LOCKED
		 )
		 UPDATE notification_deliveries d
		 SET status='PENDING', updated_at=now()
		 FROM due
		 WHERE d.id=due.id AND d.created_at=due.created_at
		 RETURNING d.id, d.created_at, d.request_id, d.channel, d.priority`, limit)
}

// SweepRetries moves due RETRYING deliveries back to PENDING and enqueues them.
func (s *Store) SweepRetries(ctx context.Context, limit int) (int, error) {
	return s.transitionAndEnqueue(ctx,
		`WITH due AS (
		    SELECT id, created_at
		    FROM notification_deliveries
		    WHERE status='RETRYING' AND next_retry_at <= now()
		    ORDER BY next_retry_at
		    LIMIT $1
		    FOR UPDATE SKIP LOCKED
		 )
		 UPDATE notification_deliveries d
		 SET status='PENDING', updated_at=now()
		 FROM due
		 WHERE d.id=due.id AND d.created_at=due.created_at
		 RETURNING d.id, d.created_at, d.request_id, d.channel, d.priority`, limit)
}

// ReconcilePending re-enqueues PENDING deliveries that have no unpublished outbox row
// and have been idle longer than olderThan. This heals the rare case where a state
// change committed but the outbox insert/publish was lost.
func (s *Store) ReconcilePending(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	cutoff := time.Now().UTC().Add(-olderThan)
	var count int
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT d.id, d.created_at, d.request_id, d.channel, d.priority
			 FROM notification_deliveries d
			 WHERE d.status='PENDING' AND d.updated_at < $1
			   AND NOT EXISTS (
			       SELECT 1 FROM outbox o
			       WHERE o.delivery_id = d.id AND o.published_at IS NULL)
			 ORDER BY d.updated_at
			 LIMIT $2
			 FOR UPDATE SKIP LOCKED`, cutoff, limit)
		if err != nil {
			return fmt.Errorf("reconcile query: %w", err)
		}
		due, err := scanDueRows(rows)
		if err != nil {
			return err
		}
		for _, d := range due {
			if err := enqueueDueTx(ctx, tx, d); err != nil {
				return err
			}
		}
		count = len(due)
		return nil
	})
	return count, err
}

// ReapStuck returns PROCESSING deliveries whose lease expired (crashed workers) to
// RETRYING so the next retry sweep re-enqueues them. Returns the number reclaimed.
func (s *Store) ReapStuck(ctx context.Context) (int, error) {
	tag, err := s.primary.Exec(ctx,
		`UPDATE notification_deliveries
		 SET status='RETRYING', next_retry_at=now(), locked_until=NULL, updated_at=now()
		 WHERE status='PROCESSING' AND locked_until < now()`)
	if err != nil {
		return 0, fmt.Errorf("reap stuck: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// transitionAndEnqueue runs a status-transition query returning due rows and writes
// an outbox row for each in the same transaction.
func (s *Store) transitionAndEnqueue(ctx context.Context, query string, limit int) (int, error) {
	var count int
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, query, limit)
		if err != nil {
			return fmt.Errorf("transition query: %w", err)
		}
		due, err := scanDueRows(rows)
		if err != nil {
			return err
		}
		for _, d := range due {
			if err := enqueueDueTx(ctx, tx, d); err != nil {
				return err
			}
		}
		count = len(due)
		return nil
	})
	return count, err
}

// enqueueDueTx inserts an outbox row for a due delivery.
func enqueueDueTx(ctx context.Context, tx pgx.Tx, d dueRow) error {
	return insertOutbox(ctx, tx, domain.OutboxRecord{
		ID:                uuid.New(),
		DeliveryID:        d.id,
		DeliveryCreatedAt: d.createdAt,
		Stream:            domain.Stream(d.priority, d.channel),
		Message: domain.DeliveryMessage{
			DeliveryID:        d.id,
			DeliveryCreatedAt: d.createdAt,
			RequestID:         d.requestID,
			Channel:           d.channel,
			Priority:          d.priority,
		},
	})
}

func scanDueRows(rows pgx.Rows) ([]dueRow, error) {
	defer rows.Close()
	var out []dueRow
	for rows.Next() {
		var d dueRow
		if err := rows.Scan(&d.id, &d.createdAt, &d.requestID, &d.channel, &d.priority); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// --- Partition & retention maintenance ---

// CreateDeliveryPartition ensures a daily partition exists for the given day.
func (s *Store) CreateDeliveryPartition(ctx context.Context, day time.Time) error {
	_, err := s.primary.Exec(ctx, `SELECT create_delivery_partition($1::date)`, day)
	if err != nil {
		return fmt.Errorf("create partition: %w", err)
	}
	return nil
}

// ArchiveAndDropPartition archives terminal rows from a day's partition then drops it.
func (s *Store) ArchiveAndDropPartition(ctx context.Context, day time.Time) error {
	_, err := s.primary.Exec(ctx, `SELECT archive_and_drop_partition($1::date)`, day)
	if err != nil {
		return fmt.Errorf("archive/drop partition: %w", err)
	}
	return nil
}

// OldestDueLagSeconds returns seconds since the oldest due scheduled or retrying
// delivery that has not yet been promoted. Zero when nothing is overdue.
func (s *Store) OldestDueLagSeconds(ctx context.Context) (float64, error) {
	var lag *float64
	err := s.primary.QueryRow(ctx, `
		SELECT EXTRACT(EPOCH FROM (now() - sub.oldest))
		FROM (
		    SELECT MIN(ts) AS oldest FROM (
		        SELECT send_at AS ts FROM notification_deliveries
		         WHERE status = 'SCHEDULED' AND send_at <= now()
		        UNION ALL
		        SELECT next_retry_at AS ts FROM notification_deliveries
		         WHERE status = 'RETRYING' AND next_retry_at <= now()
		    ) due
		) sub
		WHERE sub.oldest IS NOT NULL`).Scan(&lag)
	if err != nil {
		return 0, fmt.Errorf("oldest due lag: %w", err)
	}
	if lag == nil {
		return 0, nil
	}
	return *lag, nil
}

// PurgePublishedOutbox deletes published outbox rows older than before.
func (s *Store) PurgePublishedOutbox(ctx context.Context, before time.Time) (int64, error) {
	var deleted int64
	err := s.primary.QueryRow(ctx, `SELECT purge_published_outbox($1)`, before).Scan(&deleted)
	if err != nil {
		return 0, fmt.Errorf("purge outbox: %w", err)
	}
	return deleted, nil
}
