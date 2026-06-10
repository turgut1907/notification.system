package domain

import "time"

// ProviderRequest is the channel-agnostic payload handed to an external provider.
type ProviderRequest struct {
	DeliveryID string // used as the provider Idempotency-Key
	To         string
	Channel    Channel
	Content    string
}

// ProviderResult is a successful provider response.
type ProviderResult struct {
	MessageID string
	Status    string
	Timestamp time.Time
}

// ProviderError classifies a provider failure so the worker can decide whether to
// retry. Permanent errors (e.g. invalid recipient) must not be retried; transient
// errors (timeouts, 5xx, 429) are retried with backoff. RetryAfter, when set,
// overrides the computed backoff (e.g. from a 429 Retry-After header).
type ProviderError struct {
	Permanent  bool
	StatusCode int
	RetryAfter *time.Duration
	// BreakerOpen is set when the call was short-circuited by an open circuit
	// breaker (the provider was never contacted). Such failures must not consume a
	// retry attempt, since nothing was actually attempted against the provider.
	BreakerOpen bool
	Err         error
}

func (e *ProviderError) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	return "provider error"
}

func (e *ProviderError) Unwrap() error { return e.Err }
