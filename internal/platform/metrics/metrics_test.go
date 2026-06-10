package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/turgut1907/notification.system/internal/domain"
)

func TestMetricsHelpers(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	m.IncCreated(domain.ChannelSMS, domain.PriorityHigh)
	m.IncSent(domain.ChannelSMS, domain.PriorityHigh)
	m.IncFailed(domain.ChannelEmail, domain.PriorityLow)
	m.IncRetry(domain.ChannelPush, domain.PriorityNormal)
	m.ObserveProviderLatency(domain.ChannelSMS, 0.1)
	m.ObserveProcessing(domain.ChannelSMS, domain.PriorityHigh, 0.2)
	m.ObserveE2ELatency(domain.ChannelSMS, domain.PriorityHigh, 1.5)
	m.SetCircuitState(domain.ChannelSMS, 0)
	m.SetQueueDepth("stream", 5)
	m.SetOutboxUnpublished(3)
	m.SetSchedulerLag(1.5)
	m.AddPromoted(2)
	m.AddReclaimed(1)
	m.AddReconciled(4)
}
