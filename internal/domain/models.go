package domain

import (
	"time"

	"github.com/google/uuid"
)

// Batch groups notifications created together. Counters are denormalized so batch
// status can be served in O(1) without scanning delivery rows.
type Batch struct {
	ID             uuid.UUID
	UserID         string
	IdempotencyKey *string
	TotalCount     int
	PendingCount   int
	SentCount      int
	FailedCount    int
	CancelledCount int
	CreatedAt      time.Time
}

// Template is a reusable message body with variable placeholders ({{var}}) and a
// declared set of required variables.
type Template struct {
	ID                uuid.UUID
	Name              string
	Channel           Channel
	Content           string
	RequiredVariables []string
	CreatedAt         time.Time
}

// Request represents the business intent: "send this notification". Its rendered
// content is immutable once created so later template edits never change queued work.
type Request struct {
	ID              uuid.UUID
	UserID          string
	BatchID         *uuid.UUID
	Recipient       string
	Channel         Channel
	Priority        Priority
	TemplateID      *uuid.UUID
	RenderedContent string
	IdempotencyKey  *string
	Status          RequestStatus
	CreatedAt       time.Time
}

// Delivery represents the operational state of sending a request through a channel.
// This is the source of truth for the processing pipeline.
type Delivery struct {
	ID                uuid.UUID
	RequestID         uuid.UUID
	Channel           Channel
	Priority          Priority
	Status            DeliveryStatus
	AttemptCount      int
	ProviderMessageID *string
	LastError         *string
	NextRetryAt       *time.Time
	LockedUntil       *time.Time
	SendAt            *time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
}
