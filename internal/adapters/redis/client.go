// Package redis implements the Queue and RateLimiter ports using Redis (Streams
// for the queue, a GCRA Lua script via redis_rate for fleet-wide rate limiting).
package redis

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// NewClient constructs and verifies a Redis client.
func NewClient(ctx context.Context, addr, password string, db int) (*goredis.Client, error) {
	client := goredis.NewClient(&goredis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}
