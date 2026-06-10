// Package notification holds the core application service: creating, querying, and
// cancelling notifications. It depends only on ports (interfaces), never on
// infrastructure, so the business rules are isolated and testable.
package notification

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/turgut1907/notification.system/internal/domain"
	"github.com/turgut1907/notification.system/internal/platform/clock"
	"github.com/turgut1907/notification.system/internal/platform/logging"
)

// Repository is the persistence port required by the service.
type Repository interface {
	CreateNotification(ctx context.Context, req domain.Request, del domain.Delivery, outbox *domain.OutboxRecord) error
	GetRequestByID(ctx context.Context, id uuid.UUID) (domain.Request, error)
	GetRequestByIdempotencyKey(ctx context.Context, userID, key string) (domain.Request, error)
	ListRequests(ctx context.Context, f domain.ListFilter) (domain.Page, error)
	CancelRequest(ctx context.Context, id uuid.UUID) (domain.Request, error)
	GetDeliveriesByRequest(ctx context.Context, id uuid.UUID) ([]domain.Delivery, error)
	CreateBatch(ctx context.Context, batch domain.Batch, items []domain.BatchItem) (domain.BatchResult, error)
	GetBatchByID(ctx context.Context, id uuid.UUID) (domain.Batch, error)
	GetBatchByIdempotencyKey(ctx context.Context, userID, key string) (domain.Batch, error)
}

// Renderer resolves a template and renders its content.
type Renderer interface {
	Render(ctx context.Context, templateID uuid.UUID, channel domain.Channel, variables map[string]string) (domain.Template, string, error)
}

// MetricsRecorder is the narrow metrics port this service needs.
type MetricsRecorder interface {
	IncCreated(channel domain.Channel, priority domain.Priority)
}

// Service implements notification use cases.
type Service struct {
	repo     Repository
	renderer Renderer
	clock    clock.Clock
	metrics  MetricsRecorder
}

// New constructs the notification Service.
func New(repo Repository, renderer Renderer, clk clock.Clock, m MetricsRecorder) *Service {
	return &Service{repo: repo, renderer: renderer, clock: clk, metrics: m}
}

// View is a request together with its deliveries.
type View struct {
	Request    domain.Request
	Deliveries []domain.Delivery
}

// CreateResult is the outcome of a single create, flagging idempotent replays.
type CreateResult struct {
	Request   domain.Request
	Duplicate bool
}

// Create validates input, renders content, and persists the request, its delivery,
// and (for immediate sends) an outbox row in one transaction. Duplicate idempotency
// keys return the existing notification rather than creating a new one.
func (s *Service) Create(ctx context.Context, in CreateInput) (CreateResult, error) {
	if err := in.validate(); err != nil {
		return CreateResult{}, err
	}

	if in.IdempotencyKey != nil {
		if existing, err := s.repo.GetRequestByIdempotencyKey(ctx, in.UserID, *in.IdempotencyKey); err == nil {
			return CreateResult{Request: existing, Duplicate: true}, nil
		} else if !errors.Is(err, domain.ErrNotFound) {
			return CreateResult{}, err
		}
	}

	built, err := s.build(ctx, in, nil)
	if err != nil {
		return CreateResult{}, err
	}

	if err := s.repo.CreateNotification(ctx, built.Request, built.Delivery, built.Outbox); err != nil {
		// Lost an idempotency race: another writer inserted the same key first.
		if errors.Is(err, domain.ErrConflict) && in.IdempotencyKey != nil {
			existing, gerr := s.repo.GetRequestByIdempotencyKey(ctx, in.UserID, *in.IdempotencyKey)
			if gerr != nil {
				return CreateResult{}, gerr
			}
			return CreateResult{Request: existing, Duplicate: true}, nil
		}
		return CreateResult{}, err
	}

	s.metrics.IncCreated(built.Request.Channel, built.Request.Priority)
	return CreateResult{Request: built.Request}, nil
}

// Get returns a request with its deliveries for the authenticated user.
func (s *Service) Get(ctx context.Context, userID string, id uuid.UUID) (View, error) {
	req, err := s.repo.GetRequestByID(ctx, id)
	if err != nil {
		return View{}, err
	}
	if req.UserID != userID {
		return View{}, domain.ErrNotFound
	}
	deliveries, err := s.repo.GetDeliveriesByRequest(ctx, id)
	if err != nil {
		return View{}, err
	}
	return View{Request: req, Deliveries: deliveries}, nil
}

// List returns a filtered, keyset-paginated page of requests.
func (s *Service) List(ctx context.Context, f domain.ListFilter) (domain.Page, error) {
	return s.repo.ListRequests(ctx, f)
}

// Cancel cancels a notification if any of its deliveries are still cancellable.
func (s *Service) Cancel(ctx context.Context, userID string, id uuid.UUID) (domain.Request, error) {
	req, err := s.repo.GetRequestByID(ctx, id)
	if err != nil {
		return domain.Request{}, err
	}
	if req.UserID != userID {
		return domain.Request{}, domain.ErrNotFound
	}
	return s.repo.CancelRequest(ctx, id)
}

// GetBatch returns a batch summary by ID for the authenticated user.
func (s *Service) GetBatch(ctx context.Context, userID string, id uuid.UUID) (domain.Batch, error) {
	batch, err := s.repo.GetBatchByID(ctx, id)
	if err != nil {
		return domain.Batch{}, err
	}
	if batch.UserID != userID {
		return domain.Batch{}, domain.ErrNotFound
	}
	return batch, nil
}

// builtNotification bundles the rows produced from one create input.
type builtNotification struct {
	Request  domain.Request
	Delivery domain.Delivery
	Outbox   *domain.OutboxRecord
}

// build renders content and constructs the request, delivery, and (for immediate
// sends) outbox rows. batchID is set for batch items.
func (s *Service) build(ctx context.Context, in CreateInput, batchID *uuid.UUID) (builtNotification, error) {
	content := in.Content
	var templateID *uuid.UUID
	if in.TemplateID != nil {
		_, rendered, err := s.renderer.Render(ctx, *in.TemplateID, in.Channel, in.Variables)
		if err != nil {
			return builtNotification{}, err
		}
		content = rendered
		templateID = in.TemplateID
	}

	now := s.clock.Now()
	requestID := uuid.New()
	deliveryID := uuid.New()

	req := domain.Request{
		ID:              requestID,
		UserID:          in.UserID,
		BatchID:         batchID,
		Recipient:       in.Recipient,
		Channel:         in.Channel,
		Priority:        in.Priority,
		TemplateID:      templateID,
		RenderedContent: content,
		IdempotencyKey:  in.IdempotencyKey,
		Status:          domain.RequestPending,
		CreatedAt:       now,
	}

	del := domain.Delivery{
		ID:        deliveryID,
		RequestID: requestID,
		Channel:   in.Channel,
		Priority:  in.Priority,
		CreatedAt: now,
		UpdatedAt: now,
	}

	var outbox *domain.OutboxRecord
	if in.SendAt != nil && in.SendAt.After(now) {
		// Future send: park as SCHEDULED; the scheduler enqueues it when due.
		del.Status = domain.DeliveryScheduled
		del.SendAt = in.SendAt
	} else {
		// Immediate: PENDING with an outbox row so the relay enqueues it.
		del.Status = domain.DeliveryPending
		outbox = &domain.OutboxRecord{
			ID:                uuid.New(),
			DeliveryID:        deliveryID,
			DeliveryCreatedAt: now,
			Stream:            domain.Stream(in.Priority, in.Channel),
			Message: domain.DeliveryMessage{
				DeliveryID:        deliveryID,
				DeliveryCreatedAt: now,
				RequestID:         requestID,
				Channel:           in.Channel,
				Priority:          in.Priority,
				CorrelationID:     logging.CorrelationID(ctx),
			},
		}
	}

	return builtNotification{Request: req, Delivery: del, Outbox: outbox}, nil
}
