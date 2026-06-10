package httpapi

import (
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestParseListFilter_FromToAndLimit(t *testing.T) {
	from := "2026-06-01T00:00:00Z"
	to := "2026-06-10T23:59:59Z"
	req := httptest.NewRequest("GET", "/api/v1/notifications?"+url.Values{
		"status":  {"SENT"},
		"channel": {"sms"},
		"from":    {from},
		"to":      {to},
		"limit":   {"25"},
	}.Encode(), nil)

	f, err := parseListFilter(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f.Status == nil || *f.Status != "SENT" {
		t.Fatalf("unexpected status: %v", f.Status)
	}
	if f.Channel == nil || string(*f.Channel) != "sms" {
		t.Fatalf("unexpected channel: %v", f.Channel)
	}
	if f.From == nil || !f.From.Equal(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("unexpected from: %v", f.From)
	}
	if f.To == nil || !f.To.Equal(time.Date(2026, 6, 10, 23, 59, 59, 0, time.UTC)) {
		t.Fatalf("unexpected to: %v", f.To)
	}
	if f.Limit != 25 {
		t.Fatalf("expected limit 25, got %d", f.Limit)
	}
}

func TestParseListFilter_InvalidFrom(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v1/notifications?from=not-a-date", nil)
	_, err := parseListFilter(req)
	if err == nil {
		t.Fatal("expected validation error for invalid from")
	}
}

func TestParseListFilter_FromAfterTo(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v1/notifications?from=2026-06-10T00:00:00Z&to=2026-06-01T00:00:00Z", nil)
	_, err := parseListFilter(req)
	if err == nil {
		t.Fatal("expected validation error when from > to")
	}
}
