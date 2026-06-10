package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/turgut1907/notification.system/internal/domain"
)

// DLQ publishes dead-letter messages to the notifications.dlq stream.
type DLQ struct {
	client *goredis.Client
}

// NewDLQ builds a DLQ publisher.
func NewDLQ(client *goredis.Client) *DLQ {
	return &DLQ{client: client}
}

// Publish appends a DLQ entry to the dead-letter stream.
func (d *DLQ) Publish(ctx context.Context, msg domain.DLQMessage) error {
	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now().UTC()
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal dlq message: %w", err)
	}
	if err := d.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: domain.DLQStream,
		Values: map[string]any{"data": string(payload)},
	}).Err(); err != nil {
		return fmt.Errorf("xadd dlq: %w", err)
	}
	return nil
}
