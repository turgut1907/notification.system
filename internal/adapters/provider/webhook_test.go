package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/turgut1907/notification.system/internal/domain"
)

func TestWebhookClient_SendSuccess202(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Idempotency-Key") != "del-1" {
			t.Fatalf("missing idempotency key")
		}
		var body requestBody
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.To != "user@test.com" || body.Channel != "email" {
			t.Fatalf("body %+v", body)
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(responseBody{
			MessageID: "mid-1", Status: "accepted", Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	}))
	defer srv.Close()

	c := NewWebhookClient(srv.URL, time.Second)
	res, err := c.Send(context.Background(), domain.ProviderRequest{
		DeliveryID: "del-1", To: "user@test.com", Channel: domain.ChannelEmail, Content: "hi",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.MessageID != "mid-1" {
		t.Fatalf("message id %q", res.MessageID)
	}
}

func TestWebhookClient_Permanent4xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	c := NewWebhookClient(srv.URL, time.Second)
	_, err := c.Send(context.Background(), domain.ProviderRequest{
		DeliveryID: "d", To: "x", Channel: domain.ChannelSMS, Content: "c",
	})
	var perr *domain.ProviderError
	if err == nil || !errors.As(err, &perr) || !perr.Permanent {
		t.Fatalf("expected permanent error, got %v", err)
	}
}

func TestWebhookClient_RateLimited429(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := NewWebhookClient(srv.URL, time.Second)
	_, err := c.Send(context.Background(), domain.ProviderRequest{
		DeliveryID: "d", To: "x", Channel: domain.ChannelPush, Content: "c",
	})
	var perr *domain.ProviderError
	if err == nil || !errors.As(err, &perr) || perr.Permanent || perr.RetryAfter == nil {
		t.Fatalf("expected transient 429 with retry-after, got %v", err)
	}
}

func TestWebhookClient_Transient5xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewWebhookClient(srv.URL, time.Second)
	_, err := c.Send(context.Background(), domain.ProviderRequest{
		DeliveryID: "d", To: "x", Channel: domain.ChannelSMS, Content: "c",
	})
	var perr *domain.ProviderError
	if err == nil || !errors.As(err, &perr) || perr.Permanent {
		t.Fatalf("expected transient 5xx, got %v", err)
	}
}

func TestWebhookClient_EmptyBody200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewWebhookClient(srv.URL, time.Second)
	res, err := c.Send(context.Background(), domain.ProviderRequest{
		DeliveryID: "d", To: "x", Channel: domain.ChannelSMS, Content: "c",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != "accepted" {
		t.Fatalf("status %q", res.Status)
	}
}

func TestParseRetryAfter(t *testing.T) {
	d := parseRetryAfter("10")
	if d == nil || *d != 10*time.Second {
		t.Fatalf("retry-after %v", d)
	}
}
