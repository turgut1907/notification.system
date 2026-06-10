package httpapi

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HealthChecker reports the liveness of a dependency (DB, Redis).
type HealthChecker interface {
	Ping(ctx context.Context) error
}

// RouterConfig wires the router's dependencies.
type RouterConfig struct {
	Handler   *Handler
	Registry  *prometheus.Registry
	JWTSecret string
	Readiness map[string]HealthChecker
	Log       *slog.Logger
	SwaggerUI bool
}

// NewRouter builds the full HTTP routing tree with middleware applied.
func NewRouter(cfg RouterConfig) http.Handler {
	mux := http.NewServeMux()

	// API v1
	mux.HandleFunc("POST /api/v1/notifications", cfg.Handler.createNotification)
	mux.HandleFunc("POST /api/v1/notifications/batch", cfg.Handler.createBatch)
	mux.HandleFunc("GET /api/v1/notifications", cfg.Handler.listNotifications)
	mux.HandleFunc("GET /api/v1/notifications/{id}", cfg.Handler.getNotification)
	mux.HandleFunc("POST /api/v1/notifications/{id}/cancel", cfg.Handler.cancelNotification)
	mux.HandleFunc("GET /api/v1/batches/{id}", cfg.Handler.getBatch)

	// Operational endpoints
	mux.HandleFunc("GET /healthz", handleLive)
	mux.HandleFunc("GET /readyz", handleReady(cfg.Readiness))
	mux.Handle("GET /metrics", promhttp.HandlerFor(cfg.Registry, promhttp.HandlerOpts{}))

	// API docs
	mux.HandleFunc("GET /openapi.yaml", handleOpenAPISpec)
	if cfg.SwaggerUI {
		mux.HandleFunc("GET /docs", handleSwaggerUI)
	}

	return chain(mux,
		recoverMiddleware(cfg.Log),
		correlationMiddleware,
		authMiddleware(cfg.JWTSecret),
		loggingMiddleware(cfg.Log),
	)
}

// handleLive is a trivial liveness probe.
func handleLive(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReady pings every registered dependency and reports per-dependency status.
func handleReady(checks map[string]HealthChecker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		results := make(map[string]string, len(checks))
		ready := true
		for name, check := range checks {
			if err := check.Ping(r.Context()); err != nil {
				results[name] = "down: " + err.Error()
				ready = false
			} else {
				results[name] = "ok"
			}
		}
		status := http.StatusOK
		if !ready {
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, map[string]any{"ready": ready, "checks": results})
	}
}
