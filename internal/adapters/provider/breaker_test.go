package provider

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/turgut1907/notification.system/internal/domain"
)

// scriptedSender returns a queued sequence of errors (nil = success).
type scriptedSender struct {
	errs  []error
	calls int
}

func (s *scriptedSender) Send(_ context.Context, _ domain.ProviderRequest) (domain.ProviderResult, error) {
	i := s.calls
	s.calls++
	if i < len(s.errs) && s.errs[i] != nil {
		return domain.ProviderResult{}, s.errs[i]
	}
	return domain.ProviderResult{MessageID: "ok"}, nil
}

func testBreaker(inner Sender) *Breaker {
	return NewBreaker(inner, BreakerConfig{
		FailureRateThreshold: 0.5,
		MinRequests:          2,
		Window:               time.Minute,
		OpenTimeout:          time.Minute,
		HalfOpenMax:          1,
	}, nil)
}

func TestBreaker_OpensAndShortCircuits(t *testing.T) {
	transient := &domain.ProviderError{Permanent: false, Err: errors.New("boom")}
	sender := &scriptedSender{errs: []error{transient, transient}}
	b := testBreaker(sender)

	req := domain.ProviderRequest{Channel: domain.ChannelEmail}

	// Two transient failures should trip the breaker.
	for i := 0; i < 2; i++ {
		if _, err := b.Send(context.Background(), req); err == nil {
			t.Fatalf("call %d expected failure", i)
		}
	}

	// Next call must be short-circuited without reaching the inner sender.
	callsBefore := sender.calls
	_, err := b.Send(context.Background(), req)
	var perr *domain.ProviderError
	if !errors.As(err, &perr) || !perr.BreakerOpen {
		t.Fatalf("expected BreakerOpen error, got %v", err)
	}
	if sender.calls != callsBefore {
		t.Fatal("open breaker must not call the inner sender")
	}
}

func TestBreaker_PermanentErrorsExcludedFromTripping(t *testing.T) {
	permanent := &domain.ProviderError{Permanent: true, StatusCode: 400, Err: errors.New("bad input")}
	// Many permanent failures; breaker should stay closed (these are excluded).
	sender := &scriptedSender{errs: []error{permanent, permanent, permanent, permanent}}
	b := testBreaker(sender)
	req := domain.ProviderRequest{Channel: domain.ChannelSMS}

	for i := 0; i < 4; i++ {
		_, err := b.Send(context.Background(), req)
		var perr *domain.ProviderError
		if !errors.As(err, &perr) || perr.BreakerOpen {
			t.Fatalf("call %d: permanent error should pass through, got %v", i, err)
		}
	}
}

func TestBreaker_SuccessPassesThrough(t *testing.T) {
	sender := &scriptedSender{errs: nil}
	b := testBreaker(sender)
	res, err := b.Send(context.Background(), domain.ProviderRequest{Channel: domain.ChannelPush})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.MessageID != "ok" {
		t.Fatalf("unexpected result %+v", res)
	}
}
