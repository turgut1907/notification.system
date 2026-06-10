package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/turgut1907/notification.system/internal/domain"
)

// insertOutbox writes a pending enqueue intent within the caller's transaction.
func insertOutbox(ctx context.Context, tx pgx.Tx, o domain.OutboxRecord) error {
	payload, err := marshalMessage(o.Message)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO outbox (id, delivery_id, delivery_created_at, stream, payload, created_at)
		 VALUES ($1,$2,$3,$4,$5, now())`,
		o.ID, o.DeliveryID, o.DeliveryCreatedAt, o.Stream, payload)
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}
	return nil
}

// FetchUnpublishedOutbox locks and returns up to limit unpublished rows using
// SKIP LOCKED so multiple relays can run concurrently without contention.
func (s *Store) FetchUnpublishedOutbox(ctx context.Context, limit int) ([]domain.OutboxEntry, error) {
	rows, err := s.primary.Query(ctx,
		`SELECT id, stream, payload
		 FROM outbox
		 WHERE published_at IS NULL
		 ORDER BY created_at
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch outbox: %w", err)
	}
	defer rows.Close()

	var out []domain.OutboxEntry
	for rows.Next() {
		var e domain.OutboxEntry
		var payload []byte
		if err := rows.Scan(&e.ID, &e.Stream, &payload); err != nil {
			return nil, err
		}
		if err := unmarshalMessage(payload, &e.Message); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// MarkOutboxPublished marks the given outbox rows as published.
func (s *Store) MarkOutboxPublished(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.primary.Exec(ctx,
		`UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids)
	if err != nil {
		return fmt.Errorf("mark outbox published: %w", err)
	}
	return nil
}

// CountUnpublishedOutbox returns the current outbox backlog (for metrics).
func (s *Store) CountUnpublishedOutbox(ctx context.Context) (int64, error) {
	var n int64
	err := s.primary.QueryRow(ctx,
		`SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
	return n, err
}
