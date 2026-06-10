package redis

import (
	"context"
	"testing"

	miniredis "github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/turgut1907/notification.system/internal/domain"
)

func TestRedisRateLimiterAllow(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	rl := NewRedisRateLimiter(client, 10, 10, true)

	ctx := context.Background()
	ok, _, err := rl.Allow(ctx, domain.ChannelSMS)
	if err != nil || !ok {
		t.Fatalf("allow: ok=%v err=%v", ok, err)
	}
}

func TestRedisRateLimiter_Deny(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	rl := NewRedisRateLimiter(client, 1, 1, true)
	ctx := context.Background()
	if ok, _, err := rl.Allow(ctx, domain.ChannelSMS); err != nil || !ok {
		t.Fatalf("first allow: ok=%v err=%v", ok, err)
	}
	ok, retryAfter, err := rl.Allow(ctx, domain.ChannelSMS)
	if err != nil {
		t.Fatal(err)
	}
	if ok || retryAfter <= 0 {
		t.Fatalf("expected deny with retryAfter, ok=%v retry=%v", ok, retryAfter)
	}
}

func TestRedisRateLimiter_FailClosed(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	rl := NewRedisRateLimiter(client, 10, 10, false)
	ctx := context.Background()
	mr.Close()
	ok, _, err := rl.Allow(ctx, domain.ChannelSMS)
	if err == nil || ok {
		t.Fatalf("expected deny with error, ok=%v err=%v", ok, err)
	}
}

func TestInProcessRateLimiter_Deny(t *testing.T) {
	rl := NewInProcessRateLimiter(1, 1)
	ctx := context.Background()
	if ok, _, err := rl.Allow(ctx, domain.ChannelEmail); err != nil || !ok {
		t.Fatalf("first: ok=%v err=%v", ok, err)
	}
	ok, retryAfter, err := rl.Allow(ctx, domain.ChannelEmail)
	if err != nil {
		t.Fatal(err)
	}
	if ok || retryAfter <= 0 {
		t.Fatalf("expected deny, ok=%v retry=%v", ok, retryAfter)
	}
}

func TestInProcessRateLimiterAllow(t *testing.T) {
	rl := NewInProcessRateLimiter(100, 100)
	ctx := context.Background()
	ok, _, err := rl.Allow(ctx, domain.ChannelEmail)
	if err != nil || !ok {
		t.Fatalf("allow: ok=%v err=%v", ok, err)
	}
}
