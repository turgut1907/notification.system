package logging

import (
	"context"
	"testing"
)

func TestCorrelationIDRoundTrip(t *testing.T) {
	ctx := WithCorrelationID(context.Background(), "abc-123")
	if CorrelationID(ctx) != "abc-123" {
		t.Fatal("id mismatch")
	}
	if CorrelationID(context.Background()) != "" {
		t.Fatal("empty ctx")
	}
}

func TestNewLogger(t *testing.T) {
	log := New("info")
	if log == nil {
		t.Fatal("nil logger")
	}
	_ = FromContext(WithCorrelationID(context.Background(), "x"), log)
}
