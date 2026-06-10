package notification

import (
	"testing"

	"github.com/turgut1907/notification.system/internal/domain"
)

func TestCreateInputValidate(t *testing.T) {
	if err := (CreateInput{Recipient: "a", Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Content: "x"}).validate(); err != nil {
		t.Fatal(err)
	}
	if err := (CreateInput{Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Content: "x"}).validate(); err == nil {
		t.Fatal("missing recipient")
	}
}

func TestBatchInputValidate(t *testing.T) {
	items := make([]CreateInput, 1001)
	for i := range items {
		items[i] = CreateInput{Recipient: "a", Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Content: "x"}
	}
	if err := (BatchInput{Items: items}).validate(); err == nil {
		t.Fatal("batch too large")
	}
}
