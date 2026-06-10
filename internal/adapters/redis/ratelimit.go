package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis_rate/v10"
	goredis "github.com/redis/go-redis/v9"
	xrate "golang.org/x/time/rate"

	"github.com/turgut1907/notification.system/internal/domain"
)

// RedisRateLimiter enforces a per-channel rate limit across the whole fleet using
// an atomic GCRA Lua script in Redis.
type RedisRateLimiter struct {
	limiter  *redis_rate.Limiter
	limit    redis_rate.Limit
	failOpen bool
}

// NewRedisRateLimiter builds a Redis-backed limiter.
func NewRedisRateLimiter(client *goredis.Client, perSecond, burst int, failOpen bool) *RedisRateLimiter {
	return &RedisRateLimiter{
		limiter:  redis_rate.NewLimiter(client),
		limit:    redis_rate.Limit{Rate: perSecond, Burst: burst, Period: time.Second},
		failOpen: failOpen,
	}
}

// Allow reports whether a send on the channel may proceed now. On Redis errors it
// fails open (allow) or closed (deny) per configuration.
func (r *RedisRateLimiter) Allow(ctx context.Context, channel domain.Channel) (bool, time.Duration, error) {
	res, err := r.limiter.Allow(ctx, key(channel), r.limit)
	if err != nil {
		if r.failOpen {
			return true, 0, fmt.Errorf("rate limiter (fail-open): %w", err)
		}
		return false, 0, fmt.Errorf("rate limiter (fail-closed): %w", err)
	}
	if res.Allowed > 0 {
		return true, 0, nil
	}
	return false, res.RetryAfter, nil
}

func key(channel domain.Channel) string {
	return "ratelimit:" + string(channel)
}

// InProcessRateLimiter enforces a per-channel rate limit within a single process.
// Correct only with a single worker replica; useful for local/dev runs.
type InProcessRateLimiter struct {
	perSecond int
	burst     int
	mu        sync.Mutex
	limiters  map[domain.Channel]*xrate.Limiter
}

// NewInProcessRateLimiter builds an in-process limiter.
func NewInProcessRateLimiter(perSecond, burst int) *InProcessRateLimiter {
	return &InProcessRateLimiter{
		perSecond: perSecond,
		burst:     burst,
		limiters:  make(map[domain.Channel]*xrate.Limiter),
	}
}

// Allow reports whether a send on the channel may proceed now.
func (r *InProcessRateLimiter) Allow(_ context.Context, channel domain.Channel) (bool, time.Duration, error) {
	lim := r.limiterFor(channel)
	res := lim.Reserve()
	if !res.OK() {
		return false, time.Second, nil
	}
	if d := res.Delay(); d > 0 {
		res.Cancel()
		return false, d, nil
	}
	return true, 0, nil
}

func (r *InProcessRateLimiter) limiterFor(channel domain.Channel) *xrate.Limiter {
	r.mu.Lock()
	defer r.mu.Unlock()
	if lim, ok := r.limiters[channel]; ok {
		return lim
	}
	lim := xrate.NewLimiter(xrate.Limit(r.perSecond), r.burst)
	r.limiters[channel] = lim
	return lim
}
