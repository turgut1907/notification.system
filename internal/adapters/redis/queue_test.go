package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/turgut1907/notification.system/internal/domain"
)

func newTestQueue(t *testing.T) (*Queue, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	return NewQueue(client, "test-group"), mr
}

func TestQueuePublishReadAck(t *testing.T) {
	q, _ := newTestQueue(t)
	ctx := context.Background()
	stream := domain.Stream(domain.PriorityHigh, domain.ChannelSMS)

	if err := q.EnsureGroup(ctx, stream); err != nil {
		t.Fatal(err)
	}

	msg := domain.DeliveryMessage{
		DeliveryID:        uuid.New(),
		DeliveryCreatedAt: time.Now().UTC(),
		RequestID:         uuid.New(),
		Channel:           domain.ChannelSMS,
		Priority:          domain.PriorityHigh,
	}
	if err := q.Publish(ctx, stream, msg); err != nil {
		t.Fatal(err)
	}

	depth, err := q.Depth(ctx, stream)
	if err != nil || depth != 1 {
		t.Fatalf("depth %d err=%v", depth, err)
	}

	msgs, err := q.Read(ctx, "c1", []string{stream}, 1, time.Second)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("read: len=%d err=%v", len(msgs), err)
	}
	if msgs[0].Delivery.DeliveryID != msg.DeliveryID {
		t.Fatal("delivery id mismatch")
	}

	if err := q.Ack(ctx, stream, msgs[0].ID); err != nil {
		t.Fatal(err)
	}
}

func TestQueueReadEmpty(t *testing.T) {
	q, _ := newTestQueue(t)
	ctx := context.Background()
	stream := domain.Stream(domain.PriorityLow, domain.ChannelPush)
	_ = q.EnsureGroup(ctx, stream)

	msgs, err := q.Read(ctx, "c1", []string{stream}, 1, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 0 {
		t.Fatalf("expected empty, got %d", len(msgs))
	}
}

func TestEnsureGroupIdempotent(t *testing.T) {
	q, _ := newTestQueue(t)
	ctx := context.Background()
	stream := "notifications.normal.email"
	if err := q.EnsureGroup(ctx, stream); err != nil {
		t.Fatal(err)
	}
	if err := q.EnsureGroup(ctx, stream); err != nil {
		t.Fatal(err)
	}
}

func TestQueueSkipsBadPayload(t *testing.T) {
	q, mr := newTestQueue(t)
	ctx := context.Background()
	stream := domain.Stream(domain.PriorityNormal, domain.ChannelEmail)
	_ = q.EnsureGroup(ctx, stream)
	mr.XAdd(stream, "1-0", []string{"data", "not-json"})

	msgs, err := q.Read(ctx, "c1", []string{stream}, 1, time.Second)
	if err != nil || len(msgs) != 0 {
		t.Fatalf("bad payload should be acked and skipped: len=%d err=%v", len(msgs), err)
	}
}

func TestQueuePending(t *testing.T) {
	q, _ := newTestQueue(t)
	ctx := context.Background()
	stream := domain.Stream(domain.PriorityHigh, domain.ChannelPush)
	_ = q.EnsureGroup(ctx, stream)
	n, err := q.Pending(ctx, stream)
	if err != nil || n != 0 {
		t.Fatalf("pending %d err=%v", n, err)
	}
}

func TestPublish_RedisUnavailable(t *testing.T) {
	q, mr := newTestQueue(t)
	mr.Close()
	ctx := context.Background()
	err := q.Publish(ctx, domain.Stream(domain.PriorityHigh, domain.ChannelSMS), domain.DeliveryMessage{
		DeliveryID: uuid.New(), RequestID: uuid.New(), Channel: domain.ChannelSMS, Priority: domain.PriorityHigh,
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRead_RedisUnavailable(t *testing.T) {
	q, mr := newTestQueue(t)
	ctx := context.Background()
	stream := domain.Stream(domain.PriorityHigh, domain.ChannelSMS)
	_ = q.EnsureGroup(ctx, stream)
	mr.Close()
	_, err := q.Read(ctx, "c1", []string{stream}, 1, time.Millisecond)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRead_MissingDataField(t *testing.T) {
	q, mr := newTestQueue(t)
	ctx := context.Background()
	stream := domain.Stream(domain.PriorityNormal, domain.ChannelEmail)
	_ = q.EnsureGroup(ctx, stream)
	mr.XAdd(stream, "1-0", []string{"wrong_field", "value"})

	msgs, err := q.Read(ctx, "c1", []string{stream}, 1, time.Second)
	if err != nil || len(msgs) != 0 {
		t.Fatalf("missing field should be dropped: len=%d err=%v", len(msgs), err)
	}
}

func TestRead_EmptyStreams(t *testing.T) {
	q, _ := newTestQueue(t)
	msgs, err := q.Read(context.Background(), "c1", nil, 1, time.Millisecond)
	if err != nil || msgs != nil {
		t.Fatalf("got msgs=%v err=%v", msgs, err)
	}
}

func TestAck_RedisUnavailable(t *testing.T) {
	q, mr := newTestQueue(t)
	mr.Close()
	err := q.Ack(context.Background(), "stream", "1-0")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestIsBusyGroup(t *testing.T) {
	if !isBusyGroup(errors.New("BUSYGROUP Consumer Group name already exists")) {
		t.Fatal("expected busy group")
	}
	if isBusyGroup(errors.New("other")) {
		t.Fatal("unexpected match")
	}
}
