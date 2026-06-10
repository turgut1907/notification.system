package bootstrap

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sony/gobreaker/v2"

	"github.com/turgut1907/notification.system/internal/platform/config"
)

func TestMetricsServerEndpoints(t *testing.T) {
	reg := prometheus.NewRegistry()
	srv := MetricsServer(reg, nil)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz %d", rec.Code)
	}
	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec = httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics %d", rec.Code)
	}
}

func TestCircuitStateValue(t *testing.T) {
	if circuitStateValue(gobreaker.StateClosed) != 0 {
		t.Fatal("closed")
	}
	if circuitStateValue(gobreaker.StateHalfOpen) != 1 {
		t.Fatal("half-open")
	}
	if circuitStateValue(gobreaker.StateOpen) != 2 {
		t.Fatal("open")
	}
}

func TestRetryPolicyAndRateLimiter(t *testing.T) {
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if RetryPolicy(cfg).MaxAttempts == 0 {
		t.Fatal("retry policy")
	}
	if RateLimiter(cfg, nil) == nil {
		t.Fatal("inproc limiter")
	}
	t.Setenv("RATE_LIMITER", "redis")
	cfg, err = config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if RateLimiter(cfg, nil) == nil {
		t.Fatal("redis limiter")
	}
}
