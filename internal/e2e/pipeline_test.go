package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	dto "github.com/prometheus/client_model/go"

	"github.com/turgut1907/notification.system/internal/adapters/provider"
	redisadapter "github.com/turgut1907/notification.system/internal/adapters/redis"
	"github.com/turgut1907/notification.system/internal/bootstrap"
	"github.com/turgut1907/notification.system/internal/delivery"
	"github.com/turgut1907/notification.system/internal/domain"
	"github.com/turgut1907/notification.system/internal/notification"
	"github.com/turgut1907/notification.system/internal/platform/auth"
	"github.com/turgut1907/notification.system/internal/platform/clock"
	"github.com/turgut1907/notification.system/internal/platform/logging"
	"github.com/turgut1907/notification.system/internal/scheduler"
	"github.com/turgut1907/notification.system/internal/template"
	httpapi "github.com/turgut1907/notification.system/internal/transport/http"
	"github.com/turgut1907/notification.system/internal/worker"
)

const e2eJWTSecret = "01234567890123456789012345678901"

type webhookCapture struct {
	mu      sync.Mutex
	headers http.Header
	body    []byte
}

func (c *webhookCapture) handler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	c.mu.Lock()
	c.headers = r.Header.Clone()
	c.body = body
	c.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"messageId":"e2e-msg-1","status":"accepted","timestamp":"2026-01-01T00:00:00Z"}`))
}

func resetE2EDB(t *testing.T) {
	t.Helper()
	if err := envStore.ResetIntegrationDB(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestGoldenPath_API_Outbox_Worker_Sent(t *testing.T) {
	requireInfra(t)
	ctx := context.Background()
	lock, err := envStore.HoldIntegrationTestLock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lock.Release(ctx) })
	if err := envRedis.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush redis: %v", err)
	}
	resetE2EDB(t)

	var capture webhookCapture
	providerSrv := httptest.NewServer(http.HandlerFunc(capture.handler))
	defer providerSrv.Close()

	log := logging.New("error")
	metricSet, registry := bootstrap.Metrics()
	templates := template.New(envStore)
	svc := notification.New(envStore, templates, clock.System{}, metricSet)

	handler := httpapi.NewHandler(svc, log)
	router := httpapi.NewRouter(httpapi.RouterConfig{
		Handler:   handler,
		Registry:  registry,
		JWTSecret: e2eJWTSecret,
		Readiness: map[string]httpapi.HealthChecker{
			"postgres": envStore,
			"redis":    bootstrap.RedisHealth{Client: envRedis},
		},
		Log: log,
	})
	apiSrv := httptest.NewServer(router)
	defer apiSrv.Close()

	token, err := auth.SignToken(e2eJWTSecret, "e2e-user", time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	group := "e2e-" + uuid.New().String()
	queue := redisadapter.NewQueue(envRedis, group)
	sched := scheduler.New(scheduler.Config{OutboxBatchSize: 64}, envStore, queue, metricSet, log)

	webhook := provider.NewWebhookClient(providerSrv.URL, 5*time.Second)
	prov := provider.NewBreaker(webhook, provider.BreakerConfig{
		FailureRateThreshold: 0.5,
		MinRequests:          1,
		Window:               time.Minute,
		OpenTimeout:          time.Second,
		HalfOpenMax:          1,
	}, nil)
	limiter := redisadapter.NewInProcessRateLimiter(1000, 1000)
	policy := delivery.NewPolicy(3, []time.Duration{time.Minute}, 0, nil)
	dlq := redisadapter.NewDLQ(envRedis)
	w := worker.NewWithDLQ(worker.Config{
		Priorities:  []domain.Priority{domain.PriorityHigh, domain.PriorityNormal, domain.PriorityLow},
		Channels:    domain.AllChannels(),
		Concurrency: 1,
		Consumer:    "e2e-consumer",
		BatchSize:   1,
		LeaseTTL:    2 * time.Minute,
		BlockTime:   100 * time.Millisecond,
	}, queue, envStore, prov, limiter, policy, clock.System{}, metricSet, dlq, log)

	workerCtx, stopWorker := context.WithCancel(context.Background())
	defer stopWorker()
	workerDone := make(chan error, 1)
	go func() { workerDone <- w.Run(workerCtx) }()

	tmplID := "00000000-0000-0000-0000-000000000001"
	createBody, _ := json.Marshal(map[string]any{
		"recipient":   "+15551234567",
		"channel":     "sms",
		"priority":    "high",
		"template_id": tmplID,
		"variables":   map[string]string{"code": "123456"},
	})
	createReq, err := http.NewRequest(http.MethodPost, apiSrv.URL+"/api/v1/notifications", bytes.NewReader(createBody))
	if err != nil {
		t.Fatal(err)
	}
	createReq.Header.Set("Authorization", "Bearer "+token)
	createReq.Header.Set("Content-Type", "application/json")
	createReq.Header.Set("X-Correlation-ID", "e2e-corr-1")

	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatal(err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(createResp.Body)
		t.Fatalf("create status %d body %s", createResp.StatusCode, body)
	}

	var created struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(createResp.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	requestID, err := uuid.Parse(created.ID)
	if err != nil {
		t.Fatal(err)
	}

	dels, err := envStore.GetDeliveriesByRequest(context.Background(), requestID)
	if err != nil || len(dels) != 1 {
		t.Fatalf("deliveries: %v len=%d", err, len(dels))
	}
	if dels[0].Status != domain.DeliveryPending {
		t.Fatalf("expected PENDING, got %s", dels[0].Status)
	}
	n, err := envStore.CountUnpublishedOutbox(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("unpublished outbox %d err=%v", n, err)
	}

	sched.RunOutboxRelay(context.Background())

	deadline := time.Now().Add(10 * time.Second)
	var sent bool
	for time.Now().Before(deadline) {
		getReq, _ := http.NewRequest(http.MethodGet, apiSrv.URL+"/api/v1/notifications/"+requestID.String(), nil)
		getReq.Header.Set("Authorization", "Bearer "+token)
		getResp, err := http.DefaultClient.Do(getReq)
		if err != nil {
			t.Fatal(err)
		}
		var view struct {
			Status     string `json:"status"`
			Deliveries []struct {
				Status string `json:"status"`
			} `json:"deliveries"`
		}
		_ = json.NewDecoder(getResp.Body).Decode(&view)
		getResp.Body.Close()
		if view.Status == string(domain.RequestSent) && len(view.Deliveries) == 1 &&
			view.Deliveries[0].Status == string(domain.DeliverySent) {
			sent = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	stopWorker()
	<-workerDone

	if !sent {
		t.Fatal("delivery did not reach SENT within timeout")
	}

	var idemKey string
	var body []byte
	webhookDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(webhookDeadline) {
		capture.mu.Lock()
		idemKey = capture.headers.Get("Idempotency-Key")
		body = append([]byte(nil), capture.body...)
		capture.mu.Unlock()
		if idemKey != "" && len(body) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if idemKey != dels[0].ID.String() {
		t.Fatalf("idempotency key %q want %s", idemKey, dels[0].ID)
	}
	if len(body) == 0 {
		t.Fatal("webhook received no body")
	}

	// Metric smoke check: counter should have been incremented.
	var mfs []*dto.MetricFamily
	mfs, err = registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	foundSent := false
	for _, mf := range mfs {
		if mf.GetName() == "deliveries_sent_total" && len(mf.GetMetric()) > 0 {
			foundSent = true
			break
		}
	}
	if !foundSent {
		t.Fatal("deliveries_sent_total not recorded")
	}
}
