package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/turgut1907/notification.system/internal/domain"
)

const messageField = "data"

// DLQPublisher publishes messages that cannot be processed to the dead-letter stream.
type DLQPublisher interface {
	Publish(ctx context.Context, msg domain.DLQMessage) error
}

// PoisonReporter records poison-pill drops for metrics.
type PoisonReporter interface {
	IncPoison()
	IncDLQ(reason string)
}

// Queue is a Redis Streams-backed implementation of the queue port. Each
// priority/channel pair is its own stream with a single consumer group.
type Queue struct {
	client  *goredis.Client
	group   string
	dlq     DLQPublisher
	metrics PoisonReporter
}

// NewQueue builds a Queue using the given consumer group name.
func NewQueue(client *goredis.Client, group string) *Queue {
	return &Queue{client: client, group: group}
}

// WithDLQ attaches optional DLQ publishing and metrics for poison-pill handling.
func (q *Queue) WithDLQ(dlq DLQPublisher, metrics PoisonReporter) *Queue {
	q.dlq = dlq
	q.metrics = metrics
	return q
}

// EnsureGroup creates the consumer group (and the stream) if it does not exist.
func (q *Queue) EnsureGroup(ctx context.Context, stream string) error {
	err := q.client.XGroupCreateMkStream(ctx, stream, q.group, "0").Err()
	if err != nil && !isBusyGroup(err) {
		return fmt.Errorf("ensure group %s: %w", stream, err)
	}
	return nil
}

// Publish appends a delivery message to a stream.
func (q *Queue) Publish(ctx context.Context, stream string, msg domain.DeliveryMessage) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	if err := q.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: stream,
		Values: map[string]any{messageField: payload},
	}).Err(); err != nil {
		return fmt.Errorf("xadd %s: %w", stream, err)
	}
	return nil
}

// Read performs a blocking consumer-group read across the given streams (listed in
// priority order, so higher-priority entries appear first in the result). New
// messages only (">").
func (q *Queue) Read(ctx context.Context, consumer string, streams []string, count int, block time.Duration) ([]domain.StreamMessage, error) {
	if len(streams) == 0 {
		return nil, nil
	}

	args := make([]string, 0, len(streams)*2)
	args = append(args, streams...)
	for range streams {
		args = append(args, ">")
	}

	res, err := q.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    q.group,
		Consumer: consumer,
		Streams:  args,
		Count:    int64(count),
		Block:    block,
	}).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, nil // no messages within the block window
		}
		return nil, fmt.Errorf("xreadgroup: %w", err)
	}

	var out []domain.StreamMessage
	for _, stream := range res {
		for _, entry := range stream.Messages {
			raw, ok := entry.Values[messageField].(string)
			if !ok {
				q.dropPoison(ctx, stream.Stream, entry.ID, "missing_data_field", "stream entry missing data field")
				continue
			}
			var dm domain.DeliveryMessage
			if err := json.Unmarshal([]byte(raw), &dm); err != nil {
				q.dropPoison(ctx, stream.Stream, entry.ID, "invalid_json", err.Error())
				continue
			}
			out = append(out, domain.StreamMessage{Stream: stream.Stream, ID: entry.ID, Delivery: dm})
		}
	}
	return out, nil
}

// Ack acknowledges a processed message.
func (q *Queue) Ack(ctx context.Context, stream, id string) error {
	return q.client.XAck(ctx, stream, q.group, id).Err()
}

// Depth returns the number of entries in a stream (for the queue_depth metric).
func (q *Queue) Depth(ctx context.Context, stream string) (int64, error) {
	n, err := q.client.XLen(ctx, stream).Result()
	if err != nil {
		return 0, err
	}
	return n, nil
}

// Pending returns the number of delivered-but-unacked entries for the group.
func (q *Queue) Pending(ctx context.Context, stream string) (int64, error) {
	res, err := q.client.XPending(ctx, stream, q.group).Result()
	if err != nil {
		return 0, err
	}
	return res.Count, nil
}

func isBusyGroup(err error) bool {
	return err != nil && (err.Error() == "BUSYGROUP Consumer Group name already exists")
}

func (q *Queue) dropPoison(ctx context.Context, stream, entryID, reason, errMsg string) {
	if q.dlq != nil {
		_ = q.dlq.Publish(ctx, domain.DLQMessage{
			Reason:    reason,
			Error:     errMsg,
			Stream:    stream,
			EntryID:   entryID,
			Timestamp: time.Now().UTC(),
		})
	}
	if q.metrics != nil {
		q.metrics.IncPoison()
		q.metrics.IncDLQ("poison_pill")
	}
	_ = q.Ack(ctx, stream, entryID)
}
