package httpapi

import (
	"time"

	"github.com/google/uuid"

	"github.com/turgut1907/notification.system/internal/domain"
	"github.com/turgut1907/notification.system/internal/notification"
)

// createRequest is the JSON body for creating a single notification.
type createRequest struct {
	Recipient      string            `json:"recipient"`
	Channel        string            `json:"channel"`
	Priority       string            `json:"priority"`
	Content        string            `json:"content,omitempty"`
	TemplateID     *string           `json:"template_id,omitempty"`
	Variables      map[string]string `json:"variables,omitempty"`
	IdempotencyKey *string           `json:"idempotency_key,omitempty"`
	SendAt         *time.Time        `json:"send_at,omitempty"`
}

// toInput converts the wire request to a service input, parsing the optional
// template ID. The idempotency key may also arrive via the Idempotency-Key header.
func (r createRequest) toInput(headerKey string) (notification.CreateInput, error) {
	in := notification.CreateInput{
		Recipient: r.Recipient,
		Channel:   domain.Channel(r.Channel),
		Priority:  domain.Priority(r.Priority),
		Content:   r.Content,
		Variables: r.Variables,
		SendAt:    r.SendAt,
	}
	if r.TemplateID != nil && *r.TemplateID != "" {
		id, err := uuid.Parse(*r.TemplateID)
		if err != nil {
			return in, domain.NewValidationError("template_id", "must be a valid UUID")
		}
		in.TemplateID = &id
	}
	switch {
	case r.IdempotencyKey != nil && *r.IdempotencyKey != "":
		in.IdempotencyKey = r.IdempotencyKey
	case headerKey != "":
		in.IdempotencyKey = &headerKey
	}
	return in, nil
}

// batchRequest is the JSON body for creating many notifications at once.
type batchRequest struct {
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	Notifications  []createRequest `json:"notifications"`
}

func (r batchRequest) toInput(headerKey string) (notification.BatchInput, error) {
	in := notification.BatchInput{}
	switch {
	case r.IdempotencyKey != nil && *r.IdempotencyKey != "":
		in.IdempotencyKey = r.IdempotencyKey
	case headerKey != "":
		in.IdempotencyKey = &headerKey
	}
	in.Items = make([]notification.CreateInput, 0, len(r.Notifications))
	for i := range r.Notifications {
		item, err := r.Notifications[i].toInput("")
		if err != nil {
			return in, err
		}
		in.Items = append(in.Items, item)
	}
	return in, nil
}

// notificationResponse is the API view of a request.
type notificationResponse struct {
	ID         uuid.UUID          `json:"id"`
	BatchID    *uuid.UUID         `json:"batch_id,omitempty"`
	Recipient  string             `json:"recipient"`
	Channel    string             `json:"channel"`
	Priority   string             `json:"priority"`
	Status     string             `json:"status"`
	Content    string             `json:"content,omitempty"`
	CreatedAt  time.Time          `json:"created_at"`
	Deliveries []deliveryResponse `json:"deliveries,omitempty"`
}

// deliveryResponse is the API view of a delivery attempt.
type deliveryResponse struct {
	ID                uuid.UUID  `json:"id"`
	Status            string     `json:"status"`
	AttemptCount      int        `json:"attempt_count"`
	ProviderMessageID *string    `json:"provider_message_id,omitempty"`
	LastError         *string    `json:"last_error,omitempty"`
	NextRetryAt       *time.Time `json:"next_retry_at,omitempty"`
	SendAt            *time.Time `json:"send_at,omitempty"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

func toNotificationResponse(req domain.Request, deliveries []domain.Delivery) notificationResponse {
	resp := toNotificationSummaryResponse(req)
	resp.Content = req.RenderedContent
	for _, d := range deliveries {
		resp.Deliveries = append(resp.Deliveries, deliveryResponse{
			ID:                d.ID,
			Status:            string(d.Status),
			AttemptCount:      d.AttemptCount,
			ProviderMessageID: d.ProviderMessageID,
			LastError:         d.LastError,
			NextRetryAt:       d.NextRetryAt,
			SendAt:            d.SendAt,
			UpdatedAt:         d.UpdatedAt,
		})
	}
	return resp
}

// toNotificationSummaryResponse maps a request without rendered content (list pages).
func toNotificationSummaryResponse(req domain.Request) notificationResponse {
	return notificationResponse{
		ID:        req.ID,
		BatchID:   req.BatchID,
		Recipient: req.Recipient,
		Channel:   string(req.Channel),
		Priority:  string(req.Priority),
		Status:    string(req.Status),
		CreatedAt: req.CreatedAt,
	}
}

// batchResponse is the API view of a batch summary.
type batchResponse struct {
	ID             uuid.UUID `json:"id"`
	TotalCount     int       `json:"total_count"`
	PendingCount   int       `json:"pending_count"`
	SentCount      int       `json:"sent_count"`
	FailedCount    int       `json:"failed_count"`
	CancelledCount int       `json:"cancelled_count"`
	CreatedAt      time.Time `json:"created_at"`
}

func toBatchResponse(b domain.Batch) batchResponse {
	return batchResponse{
		ID:             b.ID,
		TotalCount:     b.TotalCount,
		PendingCount:   b.PendingCount,
		SentCount:      b.SentCount,
		FailedCount:    b.FailedCount,
		CancelledCount: b.CancelledCount,
		CreatedAt:      b.CreatedAt,
	}
}

// listResponse is a page of notifications with an opaque next cursor.
type listResponse struct {
	Items      []notificationResponse `json:"items"`
	NextCursor *string                `json:"next_cursor,omitempty"`
}
