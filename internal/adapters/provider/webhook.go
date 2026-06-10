// Package provider implements the external send port. The webhook.site client is
// the concrete integration; a circuit-breaker decorator wraps it for resilience.
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/turgut1907/notification.system/internal/domain"
	"github.com/turgut1907/notification.system/internal/platform/tracing"
)

// WebhookClient sends notifications to a webhook.site URL (the simulated provider).
type WebhookClient struct {
	url     string
	client  *http.Client
	timeout time.Duration
}

// NewWebhookClient builds a client targeting the given webhook URL.
func NewWebhookClient(url string, timeout time.Duration) *WebhookClient {
	return &WebhookClient{
		url:     url,
		client:  &http.Client{Timeout: timeout},
		timeout: timeout,
	}
}

// requestBody matches the provider contract exactly: {to, channel, content}.
type requestBody struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

// responseBody matches the expected 202 response: {messageId, status, timestamp}.
type responseBody struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

// Send delivers a notification. Errors are returned as *domain.ProviderError so the
// worker can distinguish permanent failures (no retry) from transient ones (retry).
// The delivery ID is sent as Idempotency-Key so an idempotency-aware provider
// deduplicates re-sends.
func (c *WebhookClient) Send(ctx context.Context, req domain.ProviderRequest) (domain.ProviderResult, error) {
	ctx, span := tracing.StartSpan(ctx, "provider.send",
		attribute.String("delivery_id", req.DeliveryID),
		attribute.String("channel", string(req.Channel)),
	)
	defer span.End()

	body, err := json.Marshal(requestBody{
		To:      req.To,
		Channel: string(req.Channel),
		Content: req.Content,
	})
	if err != nil {
		return domain.ProviderResult{}, &domain.ProviderError{Permanent: true, Err: fmt.Errorf("marshal request: %w", err)}
	}

	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(callCtx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return domain.ProviderResult{}, &domain.ProviderError{Permanent: true, Err: fmt.Errorf("build request: %w", err)}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", req.DeliveryID)

	resp, err := c.client.Do(httpReq)
	if err != nil {
		// Network failures / timeouts are transient.
		return domain.ProviderResult{}, &domain.ProviderError{Permanent: false, Err: fmt.Errorf("provider call: %w", err)}
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return parseSuccess(respBytes)
	case resp.StatusCode == http.StatusTooManyRequests:
		return domain.ProviderResult{}, &domain.ProviderError{
			Permanent:  false,
			StatusCode: resp.StatusCode,
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
			Err:        fmt.Errorf("provider rate limited: %s", string(respBytes)),
		}
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		// Client errors (bad recipient, validation) will never succeed on retry.
		return domain.ProviderResult{}, &domain.ProviderError{
			Permanent:  true,
			StatusCode: resp.StatusCode,
			Err:        fmt.Errorf("provider rejected (%d): %s", resp.StatusCode, string(respBytes)),
		}
	default:
		// 5xx and anything else: transient.
		return domain.ProviderResult{}, &domain.ProviderError{
			Permanent:  false,
			StatusCode: resp.StatusCode,
			Err:        fmt.Errorf("provider error (%d): %s", resp.StatusCode, string(respBytes)),
		}
	}
}

func parseSuccess(body []byte) (domain.ProviderResult, error) {
	var rb responseBody
	// webhook.site may not echo our configured JSON; tolerate an empty/!json body.
	_ = json.Unmarshal(body, &rb)

	ts := time.Now().UTC()
	if rb.Timestamp != "" {
		if parsed, err := time.Parse(time.RFC3339, rb.Timestamp); err == nil {
			ts = parsed
		}
	}
	status := rb.Status
	if status == "" {
		status = "accepted"
	}
	return domain.ProviderResult{
		MessageID: rb.MessageID,
		Status:    status,
		Timestamp: ts,
	}, nil
}

func parseRetryAfter(header string) *time.Duration {
	if header == "" {
		return nil
	}
	if secs, err := strconv.Atoi(header); err == nil {
		d := time.Duration(secs) * time.Second
		return &d
	}
	if t, err := http.ParseTime(header); err == nil {
		d := time.Until(t)
		if d < 0 {
			d = 0
		}
		return &d
	}
	return nil
}
