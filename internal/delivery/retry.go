// Package delivery holds the operational rules for processing a delivery: how a
// failed send is classified and when (or whether) it should be retried. It is pure
// logic with no infrastructure dependencies so it can be tested deterministically.
package delivery

import (
	"math/rand"
	"time"

	"github.com/turgut1907/notification.system/internal/domain"
)

// Policy encodes the retry strategy.
type Policy struct {
	MaxAttempts int
	Backoff     []time.Duration
	JitterRatio float64
	rng         *rand.Rand
}

// NewPolicy builds a Policy. A nil rng uses a time-seeded source; tests may pass a
// seeded source for deterministic jitter.
func NewPolicy(maxAttempts int, backoff []time.Duration, jitterRatio float64, rng *rand.Rand) Policy {
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return Policy{
		MaxAttempts: maxAttempts,
		Backoff:     backoff,
		JitterRatio: jitterRatio,
		rng:         rng,
	}
}

// Outcome is the next persisted state for a delivery after a failed send.
type Outcome struct {
	Status       domain.DeliveryStatus
	AttemptCount int
	NextRetryAt  *time.Time
}

// EvaluateFailure decides the next state after the provider call for this attempt
// failed. currentAttempts is the attempt_count BEFORE this attempt.
//
//   - Permanent errors fail immediately (no point retrying).
//   - Otherwise the attempt is counted; if attempts are exhausted the delivery
//     fails, else it is scheduled for retry. A provider-supplied RetryAfter takes
//     precedence over the computed backoff.
func (p Policy) EvaluateFailure(now time.Time, currentAttempts int, perr *domain.ProviderError) Outcome {
	attempts := currentAttempts + 1

	if perr != nil && perr.Permanent {
		return Outcome{Status: domain.DeliveryFailed, AttemptCount: attempts}
	}

	if attempts >= p.MaxAttempts {
		return Outcome{Status: domain.DeliveryFailed, AttemptCount: attempts}
	}

	var wait time.Duration
	if perr != nil && perr.RetryAfter != nil && *perr.RetryAfter > 0 {
		wait = *perr.RetryAfter
	} else {
		wait = p.backoffFor(attempts)
	}

	next := now.Add(wait)
	return Outcome{
		Status:       domain.DeliveryRetrying,
		AttemptCount: attempts,
		NextRetryAt:  &next,
	}
}

// backoffFor returns the jittered backoff for the given attempt number (1-based).
// Attempts beyond the schedule reuse the final entry.
func (p Policy) backoffFor(attempt int) time.Duration {
	if len(p.Backoff) == 0 {
		return time.Minute
	}
	idx := attempt - 1
	if idx >= len(p.Backoff) {
		idx = len(p.Backoff) - 1
	}
	base := p.Backoff[idx]
	return p.applyJitter(base)
}

// applyJitter spreads the value by +/- JitterRatio to avoid synchronized retry
// storms when a provider recovers.
func (p Policy) applyJitter(d time.Duration) time.Duration {
	if p.JitterRatio <= 0 {
		return d
	}
	delta := float64(d) * p.JitterRatio
	// random offset in [-delta, +delta]
	offset := (p.rng.Float64()*2 - 1) * delta
	jittered := time.Duration(float64(d) + offset)
	if jittered < 0 {
		return 0
	}
	return jittered
}
