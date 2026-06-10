package domain

import "fmt"

// Channel is the delivery medium for a notification.
type Channel string

const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// AllChannels enumerates every supported channel.
func AllChannels() []Channel {
	return []Channel{ChannelSMS, ChannelEmail, ChannelPush}
}

// Valid reports whether the channel is one the system supports.
func (c Channel) Valid() bool {
	switch c {
	case ChannelSMS, ChannelEmail, ChannelPush:
		return true
	default:
		return false
	}
}

func (c Channel) String() string { return string(c) }

// Priority controls which stream a delivery is routed to and how eagerly it is consumed.
type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

// AllPriorities enumerates every supported priority, ordered from most to least urgent.
func AllPriorities() []Priority {
	return []Priority{PriorityHigh, PriorityNormal, PriorityLow}
}

// Valid reports whether the priority is supported.
func (p Priority) Valid() bool {
	switch p {
	case PriorityHigh, PriorityNormal, PriorityLow:
		return true
	default:
		return false
	}
}

func (p Priority) String() string { return string(p) }

// RequestStatus is the aggregated business status of a notification request,
// derived from the statuses of its deliveries.
type RequestStatus string

const (
	RequestPending   RequestStatus = "PENDING"
	RequestPartial   RequestStatus = "PARTIAL"
	RequestSent      RequestStatus = "SENT"
	RequestFailed    RequestStatus = "FAILED"
	RequestCancelled RequestStatus = "CANCELLED"
)

// DeliveryStatus is the operational state of a single delivery attempt lifecycle.
// The notification_deliveries row is the source of truth for processing.
type DeliveryStatus string

const (
	DeliveryScheduled  DeliveryStatus = "SCHEDULED"
	DeliveryPending    DeliveryStatus = "PENDING"
	DeliveryProcessing DeliveryStatus = "PROCESSING"
	DeliveryRetrying   DeliveryStatus = "RETRYING"
	DeliverySent       DeliveryStatus = "SENT"
	DeliveryFailed     DeliveryStatus = "FAILED"
	DeliveryCancelled  DeliveryStatus = "CANCELLED"
)

// IsTerminal reports whether the delivery has reached a final state that will not change.
func (s DeliveryStatus) IsTerminal() bool {
	switch s {
	case DeliverySent, DeliveryFailed, DeliveryCancelled:
		return true
	default:
		return false
	}
}

// Stream returns the Redis stream name for a priority/channel combination,
// e.g. "notifications.high.sms". This is the single place the topology naming lives.
func Stream(p Priority, c Channel) string {
	return fmt.Sprintf("notifications.%s.%s", p, c)
}
