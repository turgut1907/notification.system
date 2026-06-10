// Package metrics defines the Prometheus instruments used across all binaries and
// a small Recorder facade so business code depends on an interface, not Prometheus.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/turgut1907/notification.system/internal/domain"
)

// Metrics holds all Prometheus collectors. Labels intentionally exclude high-
// cardinality values like correlation_id.
type Metrics struct {
	NotificationsCreated *prometheus.CounterVec
	DeliveriesSent       *prometheus.CounterVec
	DeliveriesFailed     *prometheus.CounterVec
	RetriesTotal         *prometheus.CounterVec
	DeliveriesPromoted   prometheus.Counter
	LocksReclaimed       prometheus.Counter
	ReconciledTotal      prometheus.Counter

	QueueDepth        *prometheus.GaugeVec
	OutboxUnpublished prometheus.Gauge
	SchedulerLag      prometheus.Gauge
	CircuitState      *prometheus.GaugeVec

	ProviderLatency    *prometheus.HistogramVec
	ProcessingDuration *prometheus.HistogramVec
	E2ELatency         *prometheus.HistogramVec

	DLQMessages    *prometheus.CounterVec
	PoisonMessages prometheus.Counter
	QueuePending   *prometheus.GaugeVec
}

// New registers and returns the metric set against the given registry.
func New(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		NotificationsCreated: f.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_created_total",
			Help: "Total notification requests created.",
		}, []string{"channel", "priority"}),
		DeliveriesSent: f.NewCounterVec(prometheus.CounterOpts{
			Name: "deliveries_sent_total",
			Help: "Total deliveries sent successfully.",
		}, []string{"channel", "priority"}),
		DeliveriesFailed: f.NewCounterVec(prometheus.CounterOpts{
			Name: "deliveries_failed_total",
			Help: "Total deliveries that reached the FAILED terminal state.",
		}, []string{"channel", "priority"}),
		RetriesTotal: f.NewCounterVec(prometheus.CounterOpts{
			Name: "retry_total",
			Help: "Total delivery retries scheduled.",
		}, []string{"channel", "priority"}),
		DeliveriesPromoted: f.NewCounter(prometheus.CounterOpts{
			Name: "deliveries_promoted_total",
			Help: "Total scheduled deliveries promoted to pending.",
		}),
		LocksReclaimed: f.NewCounter(prometheus.CounterOpts{
			Name: "locks_reclaimed_total",
			Help: "Total stuck PROCESSING deliveries reclaimed by the reaper.",
		}),
		ReconciledTotal: f.NewCounter(prometheus.CounterOpts{
			Name: "reconciled_total",
			Help: "Total orphaned PENDING deliveries re-enqueued by reconciliation.",
		}),
		QueueDepth: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "queue_depth",
			Help: "Number of entries per stream.",
		}, []string{"stream"}),
		OutboxUnpublished: f.NewGauge(prometheus.GaugeOpts{
			Name: "outbox_unpublished",
			Help: "Number of unpublished outbox rows (relay backlog).",
		}),
		SchedulerLag: f.NewGauge(prometheus.GaugeOpts{
			Name: "scheduler_lag_seconds",
			Help: "Seconds between now and the oldest due, unpromoted delivery.",
		}),
		CircuitState: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "provider_circuit_state",
			Help: "Circuit breaker state per channel (0 closed, 1 half-open, 2 open).",
		}, []string{"channel"}),
		ProviderLatency: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "provider_latency_seconds",
			Help:    "Latency of provider calls.",
			Buckets: prometheus.DefBuckets,
		}, []string{"channel"}),
		ProcessingDuration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "delivery_processing_seconds",
			Help:    "End-to-end worker processing time per delivery.",
			Buckets: prometheus.DefBuckets,
		}, []string{"channel", "priority"}),
		E2ELatency: f.NewHistogramVec(prometheus.HistogramOpts{
			Name: "delivery_e2e_latency_seconds",
			Help: "Time from API create (delivery created_at) to successful send.",
			Buckets: []float64{
				0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600,
			},
		}, []string{"channel", "priority"}),
		DLQMessages: f.NewCounterVec(prometheus.CounterOpts{
			Name: "dlq_messages_total",
			Help: "Total messages published to the dead-letter stream.",
		}, []string{"reason"}),
		PoisonMessages: f.NewCounter(prometheus.CounterOpts{
			Name: "poison_messages_total",
			Help: "Total unparseable stream entries dropped to DLQ.",
		}),
		QueuePending: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "queue_pending",
			Help: "Pending (unacked) entries per stream for the consumer group.",
		}, []string{"stream"}),
	}
}

// Helpers that encapsulate label wiring so call sites stay terse.

func (m *Metrics) IncCreated(c domain.Channel, p domain.Priority) {
	m.NotificationsCreated.WithLabelValues(string(c), string(p)).Inc()
}

func (m *Metrics) IncSent(c domain.Channel, p domain.Priority) {
	m.DeliveriesSent.WithLabelValues(string(c), string(p)).Inc()
}

func (m *Metrics) IncFailed(c domain.Channel, p domain.Priority) {
	m.DeliveriesFailed.WithLabelValues(string(c), string(p)).Inc()
}

func (m *Metrics) IncRetry(c domain.Channel, p domain.Priority) {
	m.RetriesTotal.WithLabelValues(string(c), string(p)).Inc()
}

func (m *Metrics) ObserveProviderLatency(c domain.Channel, seconds float64) {
	m.ProviderLatency.WithLabelValues(string(c)).Observe(seconds)
}

func (m *Metrics) ObserveProcessing(c domain.Channel, p domain.Priority, seconds float64) {
	m.ProcessingDuration.WithLabelValues(string(c), string(p)).Observe(seconds)
}

func (m *Metrics) ObserveE2ELatency(c domain.Channel, p domain.Priority, seconds float64) {
	m.E2ELatency.WithLabelValues(string(c), string(p)).Observe(seconds)
}

func (m *Metrics) SetCircuitState(c domain.Channel, state float64) {
	m.CircuitState.WithLabelValues(string(c)).Set(state)
}

func (m *Metrics) SetQueueDepth(stream string, n float64) {
	m.QueueDepth.WithLabelValues(stream).Set(n)
}

func (m *Metrics) SetOutboxUnpublished(n float64) { m.OutboxUnpublished.Set(n) }

func (m *Metrics) SetSchedulerLag(seconds float64) { m.SchedulerLag.Set(seconds) }

func (m *Metrics) AddPromoted(n float64) { m.DeliveriesPromoted.Add(n) }

func (m *Metrics) AddReclaimed(n float64) { m.LocksReclaimed.Add(n) }

func (m *Metrics) AddReconciled(n float64) { m.ReconciledTotal.Add(n) }

func (m *Metrics) IncDLQ(reason string) { m.DLQMessages.WithLabelValues(reason).Inc() }

func (m *Metrics) IncPoison() { m.PoisonMessages.Inc() }

func (m *Metrics) SetQueuePending(stream string, n float64) {
	m.QueuePending.WithLabelValues(stream).Set(n)
}
