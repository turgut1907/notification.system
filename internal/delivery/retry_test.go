package delivery

import (
	"math/rand"
	"testing"
	"time"

	"github.com/turgut1907/notification.system/internal/domain"
)

func newTestPolicy() Policy {
	return NewPolicy(3, []time.Duration{time.Minute, 5 * time.Minute}, 0, rand.New(rand.NewSource(1)))
}

func TestEvaluateFailure_PermanentFailsImmediately(t *testing.T) {
	p := newTestPolicy()
	out := p.EvaluateFailure(time.Now(), 0, &domain.ProviderError{Permanent: true})

	if out.Status != domain.DeliveryFailed {
		t.Fatalf("expected FAILED, got %s", out.Status)
	}
	if out.AttemptCount != 1 {
		t.Fatalf("expected attempt 1, got %d", out.AttemptCount)
	}
	if out.NextRetryAt != nil {
		t.Fatalf("permanent failure must not schedule a retry")
	}
}

func TestEvaluateFailure_RetriesThenExhausts(t *testing.T) {
	p := newTestPolicy()
	now := time.Now()

	// attempt 1 -> retry
	out := p.EvaluateFailure(now, 0, &domain.ProviderError{Permanent: false})
	if out.Status != domain.DeliveryRetrying || out.AttemptCount != 1 {
		t.Fatalf("attempt 1 should retry, got %s/%d", out.Status, out.AttemptCount)
	}

	// attempt 2 -> retry
	out = p.EvaluateFailure(now, 1, &domain.ProviderError{Permanent: false})
	if out.Status != domain.DeliveryRetrying || out.AttemptCount != 2 {
		t.Fatalf("attempt 2 should retry, got %s/%d", out.Status, out.AttemptCount)
	}

	// attempt 3 -> exhausted (MaxAttempts=3)
	out = p.EvaluateFailure(now, 2, &domain.ProviderError{Permanent: false})
	if out.Status != domain.DeliveryFailed || out.AttemptCount != 3 {
		t.Fatalf("attempt 3 should fail, got %s/%d", out.Status, out.AttemptCount)
	}
}

func TestEvaluateFailure_RetryAfterTakesPrecedence(t *testing.T) {
	p := newTestPolicy()
	now := time.Now()
	retryAfter := 90 * time.Second

	out := p.EvaluateFailure(now, 0, &domain.ProviderError{
		Permanent:  false,
		StatusCode: 429,
		RetryAfter: &retryAfter,
	})
	if out.NextRetryAt == nil {
		t.Fatal("expected a scheduled retry")
	}
	got := out.NextRetryAt.Sub(now)
	if got != retryAfter {
		t.Fatalf("expected retry-after %s to win over backoff, got %s", retryAfter, got)
	}
}

func TestApplyJitter_WithinBounds(t *testing.T) {
	p := NewPolicy(5, []time.Duration{time.Minute}, 0.2, rand.New(rand.NewSource(42)))
	base := time.Minute
	for i := 0; i < 1000; i++ {
		j := p.applyJitter(base)
		lo := time.Duration(float64(base) * 0.8)
		hi := time.Duration(float64(base) * 1.2)
		if j < lo || j > hi {
			t.Fatalf("jittered value %s out of [%s,%s]", j, lo, hi)
		}
	}
}
