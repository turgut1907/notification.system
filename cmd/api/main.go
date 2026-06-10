// Command api runs the HTTP API server: it accepts notifications, persists them
// with their outbox rows, and serves status/list/cancel plus health, metrics, and
// API docs. Asynchronous processing is handled by the worker and scheduler binaries.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/turgut1907/notification.system/internal/bootstrap"
	"github.com/turgut1907/notification.system/internal/notification"
	"github.com/turgut1907/notification.system/internal/platform/auth"
	"github.com/turgut1907/notification.system/internal/platform/clock"
	"github.com/turgut1907/notification.system/internal/platform/config"
	"github.com/turgut1907/notification.system/internal/platform/tracing"
	"github.com/turgut1907/notification.system/internal/template"
	httpapi "github.com/turgut1907/notification.system/internal/transport/http"
)

func main() {
	if err := run(); err != nil {
		slog.Error("api exited with error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if err := auth.ValidateSecret(cfg.Auth.JWTSecret); err != nil {
		return err
	}
	log := bootstrap.Logger(cfg)
	metricSet, registry := bootstrap.Metrics()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.Tracing.ServiceName == "" {
		cfg.Tracing.ServiceName = "api"
	}
	traceShutdown, err := tracing.Init(ctx, tracing.Config{
		Enabled:     cfg.Tracing.Enabled,
		Endpoint:    cfg.Tracing.Endpoint,
		ServiceName: cfg.Tracing.ServiceName,
	})
	if err != nil {
		return err
	}
	defer func() { _ = traceShutdown(context.Background()) }()

	store, err := bootstrap.Postgres(ctx, cfg)
	if err != nil {
		return err
	}
	defer store.Close()

	redisClient, err := bootstrap.Redis(ctx, cfg)
	if err != nil {
		return err
	}
	defer redisClient.Close()

	// Assemble the application service from ports.
	templates := template.New(store)
	svc := notification.New(store, templates, clock.System{}, metricSet)

	handler := httpapi.NewHandler(svc, log)
	router := httpapi.NewRouter(httpapi.RouterConfig{
		Handler:   handler,
		Registry:  registry,
		JWTSecret: cfg.Auth.JWTSecret,
		Readiness: map[string]httpapi.HealthChecker{
			"postgres": store,
			"redis":    bootstrap.RedisHealth{Client: redisClient},
		},
		Log:       log,
		SwaggerUI: true,
	})

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("api listening", slog.String("addr", cfg.HTTPAddr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		log.Info("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}
