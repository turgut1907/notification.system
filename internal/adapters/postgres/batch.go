package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/turgut1907/notification.system/internal/domain"
)

const batchColumns = `id, user_id, idempotency_key, total_count, pending_count, sent_count, failed_count, cancelled_count, created_at`

// CreateBatch inserts the batch and all items in one transaction. Per-item
// idempotency keys are honored via ON CONFLICT DO NOTHING; duplicate items reuse the
// existing request and are not re-enqueued. Counters reflect only newly created rows.
func (s *Store) CreateBatch(ctx context.Context, batch domain.Batch, items []domain.BatchItem) (domain.BatchResult, error) {
	var res domain.BatchResult
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		// Insert the batch row first so notification_requests.batch_id FK is satisfied.
		if _, err := tx.Exec(ctx,
			`INSERT INTO notification_batches
			 (id, user_id, idempotency_key, total_count, pending_count, sent_count, failed_count, cancelled_count, created_at)
			 VALUES ($1,$2,$3,0,0,0,0,0,$4)`,
			batch.ID, batch.UserID, batch.IdempotencyKey, batch.CreatedAt); err != nil {
			if isUniqueViolation(err) {
				return domain.ErrConflict
			}
			return fmt.Errorf("insert batch: %w", err)
		}

		created := 0
		for _, item := range items {
			inserted, err := insertRequestOnConflict(ctx, tx, item.Request)
			if err != nil {
				return err
			}
			if !inserted {
				continue // duplicate idempotency key: skip delivery + outbox
			}
			created++
			if err := insertDelivery(ctx, tx, item.Delivery); err != nil {
				return err
			}
			if item.Outbox != nil {
				if err := insertOutbox(ctx, tx, *item.Outbox); err != nil {
					return err
				}
			}
		}

		batch.TotalCount = created
		batch.PendingCount = created
		if _, err := tx.Exec(ctx,
			`UPDATE notification_batches SET total_count = $2, pending_count = $2 WHERE id = $1`,
			batch.ID, created); err != nil {
			return fmt.Errorf("update batch counts: %w", err)
		}

		res = domain.BatchResult{Batch: batch, CreatedCount: created, DuplicateCount: len(items) - created}
		return nil
	})
	return res, err
}

// insertRequestOnConflict inserts a request, returning whether a new row was created.
// A conflict on idempotency_key means the request already exists.
func insertRequestOnConflict(ctx context.Context, tx pgx.Tx, r domain.Request) (bool, error) {
	tag, err := tx.Exec(ctx,
		`INSERT INTO notification_requests
		 (id, user_id, batch_id, recipient, channel, priority, template_id, rendered_content, idempotency_key, status, created_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		 ON CONFLICT (user_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`,
		r.ID, r.UserID, r.BatchID, r.Recipient, r.Channel, r.Priority, r.TemplateID,
		r.RenderedContent, r.IdempotencyKey, r.Status, r.CreatedAt)
	if err != nil {
		return false, fmt.Errorf("insert request on conflict: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// GetBatchByID returns a batch or domain.ErrNotFound.
func (s *Store) GetBatchByID(ctx context.Context, id uuid.UUID) (domain.Batch, error) {
	return scanBatch(s.reader.QueryRow(ctx,
		`SELECT `+batchColumns+` FROM notification_batches WHERE id = $1`, id))
}

// GetBatchByIdempotencyKey returns an existing batch for the user and key, or ErrNotFound.
func (s *Store) GetBatchByIdempotencyKey(ctx context.Context, userID, key string) (domain.Batch, error) {
	return scanBatch(s.primary.QueryRow(ctx,
		`SELECT `+batchColumns+` FROM notification_batches WHERE user_id = $1 AND idempotency_key = $2`, userID, key))
}

// adjustBatchCountersTx moves one unit between status buckets when a request in a
// batch changes status.
func adjustBatchCountersTx(ctx context.Context, tx pgx.Tx, batchID uuid.UUID, from, to domain.RequestStatus) error {
	fromCol := bucketColumn(from)
	toCol := bucketColumn(to)
	if fromCol == toCol {
		return nil
	}
	_, err := tx.Exec(ctx, fmt.Sprintf(
		`UPDATE notification_batches
		 SET %s = GREATEST(%s - 1, 0), %s = %s + 1
		 WHERE id = $1`, fromCol, fromCol, toCol, toCol), batchID)
	if err != nil {
		return fmt.Errorf("adjust batch counters: %w", err)
	}
	return nil
}

// bucketColumn maps a request status to its counter column. PARTIAL is counted as
// sent (at least partially delivered) for reporting simplicity.
func bucketColumn(s domain.RequestStatus) string {
	switch s {
	case domain.RequestSent, domain.RequestPartial:
		return "sent_count"
	case domain.RequestFailed:
		return "failed_count"
	case domain.RequestCancelled:
		return "cancelled_count"
	default:
		return "pending_count"
	}
}

func scanBatch(row rowScanner) (domain.Batch, error) {
	var b domain.Batch
	err := row.Scan(&b.ID, &b.UserID, &b.IdempotencyKey, &b.TotalCount, &b.PendingCount,
		&b.SentCount, &b.FailedCount, &b.CancelledCount, &b.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Batch{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Batch{}, fmt.Errorf("scan batch: %w", err)
	}
	return b, nil
}
