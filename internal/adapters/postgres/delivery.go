package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/turgut1907/notification.system/internal/domain"
)

const deliveryColumns = `id, request_id, channel, priority, status, attempt_count, provider_message_id, last_error, next_retry_at, locked_until, send_at, created_at, updated_at`

func insertDelivery(ctx context.Context, tx pgx.Tx, d domain.Delivery) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO notification_deliveries
		 (id, request_id, channel, priority, status, attempt_count, provider_message_id, last_error, next_retry_at, locked_until, send_at, created_at, updated_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		d.ID, d.RequestID, d.Channel, d.Priority, d.Status, d.AttemptCount,
		d.ProviderMessageID, d.LastError, d.NextRetryAt, d.LockedUntil, d.SendAt, d.CreatedAt, d.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert delivery: %w", err)
	}
	return nil
}

// GetDelivery returns a delivery by composite key (id, created_at) for partition pruning.
func (s *Store) GetDelivery(ctx context.Context, id uuid.UUID, createdAt time.Time) (domain.Delivery, error) {
	row := s.primary.QueryRow(ctx,
		`SELECT `+deliveryColumns+` FROM notification_deliveries WHERE id = $1 AND created_at = $2`, id, createdAt)
	return scanDelivery(row)
}

// GetDeliveriesByRequest returns all deliveries for a request.
func (s *Store) GetDeliveriesByRequest(ctx context.Context, requestID uuid.UUID) ([]domain.Delivery, error) {
	rows, err := s.reader.Query(ctx,
		`SELECT `+deliveryColumns+` FROM notification_deliveries WHERE request_id = $1 ORDER BY created_at`, requestID)
	if err != nil {
		return nil, fmt.Errorf("get deliveries: %w", err)
	}
	defer rows.Close()

	var out []domain.Delivery
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// ClaimDelivery atomically transitions a delivery to PROCESSING with a lease. The
// status guard makes this the idempotency point: a duplicate message loses the race
// and returns claimed=false.
func (s *Store) ClaimDelivery(ctx context.Context, id uuid.UUID, createdAt time.Time, leaseUntil time.Time) (domain.Delivery, bool, error) {
	row := s.primary.QueryRow(ctx,
		`UPDATE notification_deliveries
		 SET status = 'PROCESSING', locked_until = $3, updated_at = now()
		 WHERE id = $1 AND created_at = $2 AND status IN ('PENDING','RETRYING')
		 RETURNING `+deliveryColumns,
		id, createdAt, leaseUntil)

	d, err := scanDelivery(row)
	if errors.Is(err, domain.ErrNotFound) {
		return domain.Delivery{}, false, nil
	}
	if err != nil {
		return domain.Delivery{}, false, err
	}
	return d, true, nil
}

// MarkSent finalizes a delivery as SENT and recomputes the parent request status.
func (s *Store) MarkSent(ctx context.Context, id uuid.UUID, createdAt time.Time, requestID uuid.UUID, providerMessageID string) error {
	return s.withTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`UPDATE notification_deliveries
			 SET status='SENT', provider_message_id=$3, last_error=NULL, locked_until=NULL, updated_at=now()
			 WHERE id=$1 AND created_at=$2`, id, createdAt, providerMessageID); err != nil {
			return fmt.Errorf("mark sent: %w", err)
		}
		return recomputeRequestStatusTx(ctx, tx, requestID)
	})
}

// MarkRetrying schedules a delivery for a future retry. The request stays PENDING,
// so no status recompute is required.
func (s *Store) MarkRetrying(ctx context.Context, id uuid.UUID, createdAt time.Time, attemptCount int, nextRetryAt time.Time, lastErr string) error {
	_, err := s.primary.Exec(ctx,
		`UPDATE notification_deliveries
		 SET status='RETRYING', attempt_count=$3, next_retry_at=$4, last_error=$5, locked_until=NULL, updated_at=now()
		 WHERE id=$1 AND created_at=$2`, id, createdAt, attemptCount, nextRetryAt, lastErr)
	if err != nil {
		return fmt.Errorf("mark retrying: %w", err)
	}
	return nil
}

// MarkFailed finalizes a delivery as FAILED and recomputes the parent request status.
func (s *Store) MarkFailed(ctx context.Context, id uuid.UUID, createdAt time.Time, requestID uuid.UUID, attemptCount int, lastErr string) error {
	return s.withTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`UPDATE notification_deliveries
			 SET status='FAILED', attempt_count=$3, last_error=$4, locked_until=NULL, updated_at=now()
			 WHERE id=$1 AND created_at=$2`, id, createdAt, attemptCount, lastErr); err != nil {
			return fmt.Errorf("mark failed: %w", err)
		}
		return recomputeRequestStatusTx(ctx, tx, requestID)
	})
}

// recomputeRequestStatusTx derives the request status from its deliveries and, when
// it changes, updates the request and adjusts the denormalized batch counters.
func recomputeRequestStatusTx(ctx context.Context, tx pgx.Tx, requestID uuid.UUID) error {
	var oldStatus domain.RequestStatus
	var batchID *uuid.UUID
	if err := tx.QueryRow(ctx,
		`SELECT status, batch_id FROM notification_requests WHERE id=$1 FOR UPDATE`, requestID).
		Scan(&oldStatus, &batchID); err != nil {
		return fmt.Errorf("load request for recompute: %w", err)
	}

	rows, err := tx.Query(ctx,
		`SELECT status FROM notification_deliveries WHERE request_id=$1`, requestID)
	if err != nil {
		return fmt.Errorf("load delivery statuses: %w", err)
	}
	var statuses []domain.DeliveryStatus
	for rows.Next() {
		var st domain.DeliveryStatus
		if err := rows.Scan(&st); err != nil {
			rows.Close()
			return err
		}
		statuses = append(statuses, st)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	newStatus := domain.AggregateRequestStatus(statuses)
	if newStatus == oldStatus {
		return nil
	}

	if _, err := tx.Exec(ctx,
		`UPDATE notification_requests SET status=$2 WHERE id=$1`, requestID, newStatus); err != nil {
		return fmt.Errorf("update request status: %w", err)
	}

	if batchID != nil {
		if err := adjustBatchCountersTx(ctx, tx, *batchID, oldStatus, newStatus); err != nil {
			return err
		}
	}
	return nil
}

func scanDelivery(row rowScanner) (domain.Delivery, error) {
	var d domain.Delivery
	err := row.Scan(&d.ID, &d.RequestID, &d.Channel, &d.Priority, &d.Status, &d.AttemptCount,
		&d.ProviderMessageID, &d.LastError, &d.NextRetryAt, &d.LockedUntil, &d.SendAt, &d.CreatedAt, &d.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Delivery{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Delivery{}, fmt.Errorf("scan delivery: %w", err)
	}
	return d, nil
}
