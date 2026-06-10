package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/turgut1907/notification.system/internal/domain"
)

// uniqueViolationCode is the PostgreSQL SQLSTATE for a unique constraint violation.
const uniqueViolationCode = "23505"

// isUniqueViolation reports whether err is a unique-constraint violation.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == uniqueViolationCode
}

const requestColumns = `id, user_id, batch_id, recipient, channel, priority, template_id, rendered_content, idempotency_key, status, created_at`

// requestListColumns omits rendered_content so list pages avoid reading large TEXT
// blobs across many rows.
const requestListColumns = `id, user_id, batch_id, recipient, channel, priority, template_id, idempotency_key, status, created_at`

// CreateNotification inserts a request, its delivery, and (when immediate) an outbox
// row in a single transaction. A nil outbox means the delivery is SCHEDULED for the
// future and will be enqueued later by the scheduler.
func (s *Store) CreateNotification(ctx context.Context, req domain.Request, del domain.Delivery, outbox *domain.OutboxRecord) error {
	return s.withTx(ctx, func(tx pgx.Tx) error {
		if err := insertRequest(ctx, tx, req); err != nil {
			return err
		}
		if err := insertDelivery(ctx, tx, del); err != nil {
			return err
		}
		if outbox != nil {
			if err := insertOutbox(ctx, tx, *outbox); err != nil {
				return err
			}
		}
		return nil
	})
}

func insertRequest(ctx context.Context, tx pgx.Tx, r domain.Request) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO notification_requests
		 (id, user_id, batch_id, recipient, channel, priority, template_id, rendered_content, idempotency_key, status, created_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		r.ID, r.UserID, r.BatchID, r.Recipient, r.Channel, r.Priority, r.TemplateID,
		r.RenderedContent, r.IdempotencyKey, r.Status, r.CreatedAt,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return domain.ErrConflict
		}
		return fmt.Errorf("insert request: %w", err)
	}
	return nil
}

// GetRequestByID returns a single request or domain.ErrNotFound.
func (s *Store) GetRequestByID(ctx context.Context, id uuid.UUID) (domain.Request, error) {
	row := s.reader.QueryRow(ctx,
		`SELECT `+requestColumns+` FROM notification_requests WHERE id = $1`, id)
	return scanRequest(row)
}

// GetRequestByIdempotencyKey returns an existing request for the user and key, or ErrNotFound.
func (s *Store) GetRequestByIdempotencyKey(ctx context.Context, userID, key string) (domain.Request, error) {
	row := s.primary.QueryRow(ctx,
		`SELECT `+requestColumns+` FROM notification_requests WHERE user_id = $1 AND idempotency_key = $2`, userID, key)
	return scanRequest(row)
}

// ListRequests returns a page of requests filtered and keyset-paginated by
// (created_at, id) descending.
func (s *Store) ListRequests(ctx context.Context, f domain.ListFilter) (domain.Page, error) {
	f.Normalize()

	args := make([]any, 0, 8)
	where := "WHERE user_id = $1"
	args = append(args, f.UserID)
	add := func(cond string, val any) {
		args = append(args, val)
		where += fmt.Sprintf(" AND %s$%d", cond, len(args))
	}

	if f.Status != nil {
		add("status = ", string(*f.Status))
	}
	if f.Channel != nil {
		add("channel = ", string(*f.Channel))
	}
	if f.From != nil {
		add("created_at >= ", *f.From)
	}
	if f.To != nil {
		add("created_at <= ", *f.To)
	}
	if f.Cursor != nil && f.CursorAt != nil {
		// keyset: rows strictly "older" than the cursor in (created_at, id) order
		args = append(args, *f.CursorAt, *f.Cursor)
		where += fmt.Sprintf(" AND (created_at, id) < ($%d, $%d)", len(args)-1, len(args))
	}

	args = append(args, f.Limit+1) // fetch one extra to detect a next page
	query := fmt.Sprintf(
		`SELECT %s FROM notification_requests %s ORDER BY created_at DESC, id DESC LIMIT $%d`,
		requestListColumns, where, len(args))

	rows, err := s.reader.Query(ctx, query, args...)
	if err != nil {
		return domain.Page{}, fmt.Errorf("list requests: %w", err)
	}
	defer rows.Close()

	var items []domain.Request
	for rows.Next() {
		r, err := scanRequestList(rows)
		if err != nil {
			return domain.Page{}, err
		}
		items = append(items, r)
	}
	if err := rows.Err(); err != nil {
		return domain.Page{}, err
	}

	page := domain.Page{Items: items}
	if len(items) > f.Limit {
		last := items[f.Limit-1]
		page.Items = items[:f.Limit]
		page.NextCursor = &last.ID
		page.NextAt = &last.CreatedAt
	}
	return page, nil
}

// CancelRequest cancels all still-cancellable deliveries of a request and returns
// the updated request. It is atomic and races safely against worker claims because
// the UPDATE is guarded by status.
func (s *Store) CancelRequest(ctx context.Context, id uuid.UUID) (domain.Request, error) {
	var result domain.Request
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		// Lock the request row to serialize against concurrent status recompute.
		if _, err := scanRequest(tx.QueryRow(ctx,
			`SELECT `+requestColumns+` FROM notification_requests WHERE id = $1 FOR UPDATE`, id)); err != nil {
			return err
		}

		tag, err := tx.Exec(ctx,
			`UPDATE notification_deliveries
			 SET status = 'CANCELLED', updated_at = now()
			 WHERE request_id = $1 AND status IN ('SCHEDULED','PENDING','RETRYING')`, id)
		if err != nil {
			return fmt.Errorf("cancel deliveries: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return domain.ErrCancelNotAllowed
		}

		if err := recomputeRequestStatusTx(ctx, tx, id); err != nil {
			return err
		}
		result, err = scanRequest(tx.QueryRow(ctx,
			`SELECT `+requestColumns+` FROM notification_requests WHERE id = $1`, id))
		return err
	})
	return result, err
}

// rowScanner abstracts pgx.Row and pgx.Rows for shared scan helpers.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanRequest(row rowScanner) (domain.Request, error) {
	var r domain.Request
	err := row.Scan(&r.ID, &r.UserID, &r.BatchID, &r.Recipient, &r.Channel, &r.Priority,
		&r.TemplateID, &r.RenderedContent, &r.IdempotencyKey, &r.Status, &r.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Request{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Request{}, fmt.Errorf("scan request: %w", err)
	}
	return r, nil
}

func scanRequestList(row rowScanner) (domain.Request, error) {
	var r domain.Request
	err := row.Scan(&r.ID, &r.UserID, &r.BatchID, &r.Recipient, &r.Channel, &r.Priority,
		&r.TemplateID, &r.IdempotencyKey, &r.Status, &r.CreatedAt)
	if err != nil {
		return domain.Request{}, fmt.Errorf("scan request list: %w", err)
	}
	return r, nil
}

// marshalMessage serializes a delivery message for an outbox payload.
func marshalMessage(m domain.DeliveryMessage) ([]byte, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal message: %w", err)
	}
	return b, nil
}

// unmarshalMessage deserializes an outbox payload into a delivery message.
func unmarshalMessage(b []byte, m *domain.DeliveryMessage) error {
	if err := json.Unmarshal(b, m); err != nil {
		return fmt.Errorf("unmarshal message: %w", err)
	}
	return nil
}
