package notification

import (
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/turgut1907/notification.system/internal/domain"
)

// maxContentLength is the per-channel maximum rendered content length.
var maxContentLength = map[domain.Channel]int{
	domain.ChannelSMS:   1600,
	domain.ChannelEmail: 100000,
	domain.ChannelPush:  4000,
}

const maxBatchSize = 1000

// CreateInput is the validated input for creating one notification. Content may be
// supplied directly or produced from a template + variables.
type CreateInput struct {
	UserID         string
	Recipient      string
	Channel        domain.Channel
	Priority       domain.Priority
	TemplateID     *uuid.UUID
	Content        string
	Variables      map[string]string
	IdempotencyKey *string
	SendAt         *time.Time
}

func (in CreateInput) validate() error {
	if strings.TrimSpace(in.Recipient) == "" {
		return domain.NewValidationError("recipient", "recipient is required")
	}
	if !in.Channel.Valid() {
		return domain.NewValidationError("channel", "channel must be one of sms, email, push")
	}
	if !in.Priority.Valid() {
		return domain.NewValidationError("priority", "priority must be one of high, normal, low")
	}
	if in.TemplateID == nil && strings.TrimSpace(in.Content) == "" {
		return domain.NewValidationError("content", "either content or template_id is required")
	}
	if in.TemplateID == nil {
		if max := maxContentLength[in.Channel]; len(in.Content) > max {
			return domain.NewValidationError("content", "content exceeds maximum length for channel")
		}
	}
	if in.IdempotencyKey != nil && len(*in.IdempotencyKey) > 255 {
		return domain.NewValidationError("idempotency_key", "idempotency_key too long")
	}
	return nil
}

// BatchInput is the validated input for creating up to maxBatchSize notifications.
type BatchInput struct {
	UserID         string
	IdempotencyKey *string
	Items          []CreateInput
}

func (in BatchInput) validate() error {
	if len(in.Items) == 0 {
		return domain.NewValidationError("notifications", "at least one notification is required")
	}
	if len(in.Items) > maxBatchSize {
		return domain.NewValidationError("notifications", "batch exceeds maximum of 1000 notifications")
	}
	for i := range in.Items {
		if err := in.Items[i].validate(); err != nil {
			return err
		}
	}
	return nil
}
