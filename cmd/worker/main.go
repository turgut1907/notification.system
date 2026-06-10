// Command worker consumes delivery messages from the priority/channel streams and
// runs the send pipeline (claim -> rate limit -> circuit breaker -> provider ->
// persist). It also exposes /metrics and /healthz for observability.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	redisadapter "github.com/turgut1907/notification.system/internal/adapters/redis"
	"github.com/turgut1907/notification.system/internal/bootstrap"
	"github.com/turgut1907/notification.system/internal/domain"
	"github.com/turgut1907/notification.system/internal/platform/clock"
	"github.com/turgut1907/notification.system/internal/platform/config"
	"github.com/turgut1907/notification.system/internal/platform/tracing"
	"github.com/turgut1907/notification.system/internal/worker"
)

func main() {
	if err := run(); err != nil {
		slog.Error("worker exited with error", slog.String("error", err.Error()))
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
		cfg.Tracing.ServiceName = "worker"
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
	provider := bootstrap.Provider(cfg, metricSet, log)
	limiter := bootstrap.RateLimiter(cfg, redisClient)
	policy := bootstrap.RetryPolicy(cfg)
	dlq := redisadapter.NewDLQ(redisClient)

	w := worker.NewWithDLQ(worker.Config{
		Priorities:   cfg.Worker.Priorities,
		Channels:     domain.AllChannels(),
		Concurrency:  cfg.Worker.Concurrency,
		Consumer:     cfg.Worker.Consumer,
		BatchSize:    cfg.Worker.BatchSize,
		LeaseTTL:     cfg.Worker.LeaseTTL,
		BlockTime:    cfg.Worker.BlockTime,
		RateWaitMax:  cfg.Worker.LeaseTTL / 2,
		RateWaitStep: cfg.RateLimiter.WaitInterval,
	}, queue, store, provider, limiter, policy, clock.System{}, metricSet, dlq, log)

	ready := bootstrap.Readiness{
		"postgres": store.Ping,
		"redis":    func(ctx context.Context) error { return redisClient.Ping(ctx).Err() },
	}
	metricsSrv := bootstrap.MetricsServer(registry, ready)
	go func() {
		log.Info("worker metrics listening", slog.String("addr", metricsSrv.Addr))
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server error", slog.String("error", err.Error()))
		}
	}()

	err = w.Run(ctx)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)
	return err
}
