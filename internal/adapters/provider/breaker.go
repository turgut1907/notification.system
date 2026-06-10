package provider

import (
	"context"
	"errors"
	"time"

	"github.com/sony/gobreaker/v2"

	"github.com/turgut1907/notification.system/internal/domain"
)

// Sender is the minimal port the breaker decorates.
type Sender interface {
	Send(ctx context.Context, req domain.ProviderRequest) (domain.ProviderResult, error)
}

// BreakerConfig configures the per-channel circuit breakers.
type BreakerConfig struct {
	FailureRateThreshold float64
	MinRequests          uint32
	Window               time.Duration
	OpenTimeout          time.Duration
	HalfOpenMax          uint32
}

// StateChangeFunc is notified when a channel's breaker changes state.
type StateChangeFunc func(channel domain.Channel, state gobreaker.State)

// Breaker wraps a Sender with one circuit breaker per channel so a single failing
// channel does not impact the others.
type Breaker struct {
	inner    Sender
	breakers map[domain.Channel]*gobreaker.CircuitBreaker[domain.ProviderResult]
}

// NewBreaker builds a per-channel circuit-breaker decorator around inner.
func NewBreaker(inner Sender, cfg BreakerConfig, onChange StateChangeFunc) *Breaker {
	breakers := make(map[domain.Channel]*gobreaker.CircuitBreaker[domain.ProviderResult])
	for _, ch := range domain.AllChannels() {
		channel := ch
		settings := gobreaker.Settings{
			Name:        string(channel),
			MaxRequests: cfg.HalfOpenMax,
			Interval:    cfg.Window,
			Timeout:     cfg.OpenTimeout,
			ReadyToTrip: func(counts gobreaker.Counts) bool {
				if counts.Requests < cfg.MinRequests {
					return false
				}
				failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
				return failureRatio >= cfg.FailureRateThreshold
			},
			// Permanent (4xx) errors reflect bad input, not provider health, so they
			// are excluded from the breaker's failure accounting entirely.
			IsExcluded: func(err error) bool {
				var perr *domain.ProviderError
				return errors.As(err, &perr) && perr.Permanent
			},
			OnStateChange: func(_ string, _ gobreaker.State, to gobreaker.State) {
				if onChange != nil {
					onChange(channel, to)
				}
			},
		}
		breakers[channel] = gobreaker.NewCircuitBreaker[domain.ProviderResult](settings)
	}
	return &Breaker{inner: inner, breakers: breakers}
}

// Send routes through the channel's breaker. When the breaker is open the call is
// short-circuited and returned as a transient, attempt-preserving error.
func (b *Breaker) Send(ctx context.Context, req domain.ProviderRequest) (domain.ProviderResult, error) {
	cb, ok := b.breakers[req.Channel]
	if !ok {
		return b.inner.Send(ctx, req)
	}

	res, err := cb.Execute(func() (domain.ProviderResult, error) {
		return b.inner.Send(ctx, req)
	})
	if err != nil {
		if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
			return domain.ProviderResult{}, &domain.ProviderError{
				Permanent:   false,
				BreakerOpen: true,
				Err:         err,
			}
		}
		return domain.ProviderResult{}, err
	}
	return res, nil
}
