package domain

import "testing"

func TestAggregateRequestStatus(t *testing.T) {
	tests := []struct {
		name string
		in   []DeliveryStatus
		want RequestStatus
	}{
		{"empty is pending", nil, RequestPending},
		{"in progress is pending", []DeliveryStatus{DeliverySent, DeliveryPending}, RequestPending},
		{"all sent", []DeliveryStatus{DeliverySent, DeliverySent}, RequestSent},
		{"all cancelled", []DeliveryStatus{DeliveryCancelled}, RequestCancelled},
		{"all failed none sent", []DeliveryStatus{DeliveryFailed, DeliveryFailed}, RequestFailed},
		{"mixed sent and failed is partial", []DeliveryStatus{DeliverySent, DeliveryFailed}, RequestPartial},
		{"sent and cancelled is partial", []DeliveryStatus{DeliverySent, DeliveryCancelled}, RequestPartial},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AggregateRequestStatus(tt.in); got != tt.want {
				t.Fatalf("got %s want %s", got, tt.want)
			}
		})
	}
}

func TestStreamNaming(t *testing.T) {
	if got := Stream(PriorityHigh, ChannelSMS); got != "notifications.high.sms" {
		t.Fatalf("unexpected stream name %q", got)
	}
}

func TestDeliveryStatusIsTerminal(t *testing.T) {
	terminal := []DeliveryStatus{DeliverySent, DeliveryFailed, DeliveryCancelled}
	for _, s := range terminal {
		if !s.IsTerminal() {
			t.Fatalf("%s should be terminal", s)
		}
	}
	nonTerminal := []DeliveryStatus{DeliveryScheduled, DeliveryPending, DeliveryProcessing, DeliveryRetrying}
	for _, s := range nonTerminal {
		if s.IsTerminal() {
			t.Fatalf("%s should not be terminal", s)
		}
	}
}
