package domain

import (
	"time"

	"github.com/google/uuid"
)

// DLQStream is the Redis stream for messages that cannot be processed.
const DLQStream = "notifications.dlq"

// DeliveryMessage is the event payload published to a Redis stream and stored as
// the outbox payload. It carries delivery_created_at so the worker's claim can
// prune to a single partition (the deliveries table is partitioned by created_at).
type DeliveryMessage struct {
	DeliveryID        uuid.UUID `json:"delivery_id"`
	DeliveryCreatedAt time.Time `json:"delivery_created_at"`
	RequestID         uuid.UUID `json:"request_id"`
	Channel           Channel   `json:"channel"`
	Priority          Priority  `json:"priority"`
	CorrelationID     string    `json:"correlation_id,omitempty"`
}

// DLQMessage is the payload written to the dead-letter stream.
type DLQMessage struct {
	Original     DeliveryMessage `json:"original"`
	Reason       string          `json:"reason"`
	Error        string          `json:"error,omitempty"`
	AttemptCount int             `json:"attempt_count,omitempty"`
	Stream       string          `json:"stream,omitempty"`
	EntryID      string          `json:"entry_id,omitempty"`
	Timestamp    time.Time       `json:"timestamp"`
}

// StreamMessage is a delivery message as consumed from a stream, including the
// stream name and entry ID needed to acknowledge it.
type StreamMessage struct {
	Stream   string
	ID       string
	Delivery DeliveryMessage
}

// OutboxRecord is a pending enqueue intent written transactionally with a delivery
// state change and later published by the relay.
type OutboxRecord struct {
	ID                uuid.UUID
	DeliveryID        uuid.UUID
	DeliveryCreatedAt time.Time
	Stream            string
	Message           DeliveryMessage
}

// BatchItem is one notification within a batch create, paired with its outbox row
// (nil when scheduled for the future).
type BatchItem struct {
	Request  Request
	Delivery Delivery
	Outbox   *OutboxRecord
}

// BatchResult reports how many items were newly created vs. deduplicated by key.
type BatchResult struct {
	Batch          Batch
	CreatedCount   int
	DuplicateCount int
}

// OutboxEntry is an unpublished outbox row ready to be relayed to its stream.
type OutboxEntry struct {
	ID      uuid.UUID
	Stream  string
	Message DeliveryMessage
}
