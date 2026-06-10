package notification

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/turgut1907/notification.system/internal/domain"
)

// BatchCreateResult is the outcome of a batch create.
type BatchCreateResult struct {
	Batch          domain.Batch
	Duplicate      bool // true when an idempotent batch replay returned an existing batch
	CreatedCount   int
	DuplicateCount int
}

// CreateBatch validates and persists up to 1000 notifications in one transaction.
// A batch-level idempotency key makes the whole submission idempotent; per-item keys
// deduplicate individual notifications.
func (s *Service) CreateBatch(ctx context.Context, in BatchInput) (BatchCreateResult, error) {
	if err := in.validate(); err != nil {
		return BatchCreateResult{}, err
	}

	if in.IdempotencyKey != nil {
		if existing, err := s.repo.GetBatchByIdempotencyKey(ctx, in.UserID, *in.IdempotencyKey); err == nil {
			return BatchCreateResult{Batch: existing, Duplicate: true}, nil
		} else if !errors.Is(err, domain.ErrNotFound) {
			return BatchCreateResult{}, err
		}
	}

	batchID := uuid.New()
	now := s.clock.Now()
	batch := domain.Batch{
		ID:             batchID,
		UserID:         in.UserID,
		IdempotencyKey: in.IdempotencyKey,
		CreatedAt:      now,
	}

	items := make([]domain.BatchItem, 0, len(in.Items))
	for i := range in.Items {
		item := in.Items[i]
		item.UserID = in.UserID
		built, err := s.build(ctx, item, &batchID)
		if err != nil {
			return BatchCreateResult{}, err
		}
		items = append(items, domain.BatchItem{
			Request:  built.Request,
			Delivery: built.Delivery,
			Outbox:   built.Outbox,
		})
	}

	res, err := s.repo.CreateBatch(ctx, batch, items)
	if err != nil {
		if errors.Is(err, domain.ErrConflict) && in.IdempotencyKey != nil {
			existing, gerr := s.repo.GetBatchByIdempotencyKey(ctx, in.UserID, *in.IdempotencyKey)
			if gerr != nil {
				return BatchCreateResult{}, gerr
			}
			return BatchCreateResult{Batch: existing, Duplicate: true}, nil
		}
		return BatchCreateResult{}, err
	}

	for i := range items {
		s.metrics.IncCreated(items[i].Request.Channel, items[i].Request.Priority)
	}

	return BatchCreateResult{
		Batch:          res.Batch,
		CreatedCount:   res.CreatedCount,
		DuplicateCount: res.DuplicateCount,
	}, nil
}
