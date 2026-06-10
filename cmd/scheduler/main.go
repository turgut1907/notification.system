// Command scheduler runs the background maintenance jobs: the outbox relay, due and
// retry promotion, the stuck-lease reaper, reconciliation, and partition/retention
// maintenance. It exposes /metrics and /healthz.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/turgut1907/notification.system/internal/bootstrap"
	"github.com/turgut1907/notification.system/internal/platform/config"
	"github.com/turgut1907/notification.system/internal/platform/tracing"
	"github.com/turgut1907/notification.system/internal/scheduler"
)

func main() {
	if err := run(); err != nil {
		slog.Error("scheduler exited with error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := bootstrap.Logger(cfg)
	metricSet, registry := bootstrap.Metrics()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.Tracing.ServiceName == "" {
		cfg.Tracing.ServiceName = "scheduler"
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

	queue := bootstrap.QueueWithDLQ(redisClient, cfg, metricSet)

	sched := scheduler.New(scheduler.Config{
		PollInterval:    cfg.Scheduler.PollInterval,
		OutboxInterval:  cfg.Scheduler.OutboxInterval,
		OutboxBatchSize: cfg.Scheduler.OutboxBatchSize,
		PromoteBatch:    cfg.Scheduler.PromoteBatch,
		ReaperInterval:  cfg.Scheduler.ReaperInterval,
		ReconcileAfter:  cfg.Scheduler.ReconcileAfter,
	}, store, queue, metricSet, log)

	ready := bootstrap.Readiness{
		"postgres": store.Ping,
		"redis":    func(ctx context.Context) error { return redisClient.Ping(ctx).Err() },
	}
	metricsSrv := bootstrap.MetricsServer(registry, ready)
	go func() {
		log.Info("scheduler metrics listening", slog.String("addr", metricsSrv.Addr))
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server error", slog.String("error", err.Error()))
		}
	}()

	err = sched.Run(ctx)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)
	return err
}
