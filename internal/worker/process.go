package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/turgut1907/notification.system/internal/domain"
	"github.com/turgut1907/notification.system/internal/platform/logging"
	"github.com/turgut1907/notification.system/internal/platform/tracing"
)

// process runs the full pipeline for one consumed message. It always acknowledges
// the stream entry afterwards because the delivery's next state is durably recorded
// in the database (the source of truth); retries are re-enqueued by the scheduler.
func (w *Worker) process(ctx context.Context, msg domain.StreamMessage) {
	start := w.clock.Now()
	dm := msg.Delivery
	if dm.CorrelationID != "" {
		ctx = logging.WithCorrelationID(ctx, dm.CorrelationID)
	}
	ctx, span := tracing.StartSpan(ctx, "worker.process",
		attribute.String("delivery_id", dm.DeliveryID.String()),
		attribute.String("request_id", dm.RequestID.String()),
		attribute.String("channel", string(dm.Channel)),
		attribute.String("priority", string(dm.Priority)),
		attribute.String("correlation_id", dm.CorrelationID),
	)
	defer span.End()
	log := logging.FromContext(ctx, w.logger(msg))

	defer func() {
		if err := w.queue.Ack(ctx, msg.Stream, msg.ID); err != nil {
			log.Error("ack failed", slog.String("error", err.Error()))
		}
		w.metrics.ObserveProcessing(dm.Channel, dm.Priority, w.clock.Now().Sub(start).Seconds())
	}()

	// 1. Claim: the status-guarded UPDATE is the idempotency gate. A lost race means
	// the delivery is already being handled, so we simply drop this duplicate.
	leaseUntil := w.clock.Now().Add(w.cfg.LeaseTTL)
	del, claimed, err := w.repo.ClaimDelivery(ctx, dm.DeliveryID, dm.DeliveryCreatedAt, leaseUntil)
	if err != nil {
		log.Error("claim failed", slog.String("error", err.Error()))
		return
	}
	if !claimed {
		log.Debug("delivery already claimed or terminal; skipping")
		return
	}

	// 2. Rate limit per channel. If the budget is exhausted we requeue without
	// burning an attempt rather than dropping the notification.
	if !w.waitForRateLimit(ctx, dm.Channel, log) {
		w.requeueWithoutAttempt(ctx, del, "rate limited", log)
		return
	}

	// Load the request for recipient + immutable rendered content.
	req, err := w.repo.GetRequestByID(ctx, del.RequestID)
	if err != nil {
		log.Error("load request failed", slog.String("error", err.Error()))
		return
	}

	// 3 & 4. Circuit breaker + provider call.
	res, sendErr := w.send(ctx, del, req.Recipient, req.RenderedContent)
	if sendErr == nil {
		if err := w.repo.MarkSent(ctx, del.ID, del.CreatedAt, del.RequestID, res.MessageID); err != nil {
			log.Error("mark sent failed", slog.String("error", err.Error()))
			return
		}
		w.metrics.IncSent(del.Channel, del.Priority)
		w.metrics.ObserveE2ELatency(del.Channel, del.Priority, w.clock.Now().Sub(del.CreatedAt).Seconds())
		log.Info("delivery sent", slog.String("provider_message_id", res.MessageID))
		return
	}

	w.handleFailure(ctx, del, msg, sendErr, log)
}

// send performs the timed provider call and records latency.
func (w *Worker) send(ctx context.Context, del domain.Delivery, recipient, content string) (domain.ProviderResult, error) {
	begin := time.Now()
	res, err := w.provider.Send(ctx, domain.ProviderRequest{
		DeliveryID: del.ID.String(),
		To:         recipient,
		Channel:    del.Channel,
		Content:    content,
	})
	w.metrics.ObserveProviderLatency(del.Channel, time.Since(begin).Seconds())
	return res, err
}

// handleFailure classifies the error and persists the next state.
func (w *Worker) handleFailure(ctx context.Context, del domain.Delivery, msg domain.StreamMessage, sendErr error, log *slog.Logger) {
	var perr *domain.ProviderError
	errors.As(sendErr, &perr)

	// Breaker open: the provider was never contacted, so do not burn an attempt.
	if perr != nil && perr.BreakerOpen {
		w.requeueWithoutAttempt(ctx, del, "circuit breaker open", log)
		return
	}

	outcome := w.policy.EvaluateFailure(w.clock.Now(), del.AttemptCount, perr)
	switch outcome.Status {
	case domain.DeliveryFailed:
		if err := w.repo.MarkFailed(ctx, del.ID, del.CreatedAt, del.RequestID, outcome.AttemptCount, sendErr.Error()); err != nil {
			log.Error("mark failed failed", slog.String("error", err.Error()))
			return
		}
		w.metrics.IncFailed(del.Channel, del.Priority)
		w.publishDLQ(ctx, del, msg, outcome.AttemptCount, sendErr.Error())
		log.Warn("delivery failed permanently",
			slog.Int("attempts", outcome.AttemptCount),
			slog.String("error", sendErr.Error()))
	default:
		next := w.clock.Now()
		if outcome.NextRetryAt != nil {
			next = *outcome.NextRetryAt
		}
		if err := w.repo.MarkRetrying(ctx, del.ID, del.CreatedAt, outcome.AttemptCount, next, sendErr.Error()); err != nil {
			log.Error("mark retrying failed", slog.String("error", err.Error()))
			return
		}
		w.metrics.IncRetry(del.Channel, del.Priority)
		log.Info("delivery scheduled for retry",
			slog.Int("attempt", outcome.AttemptCount),
			slog.Time("next_retry_at", next))
	}
}

func (w *Worker) publishDLQ(ctx context.Context, del domain.Delivery, msg domain.StreamMessage, attempts int, errMsg string) {
	if w.dlq == nil {
		return
	}
	reason := "permanent_failure"
	if attempts > 0 {
		reason = "exhausted_retries"
	}
	dlqMsg := domain.DLQMessage{
		Original:     msg.Delivery,
		Reason:       reason,
		Error:        errMsg,
		AttemptCount: attempts,
		Stream:       msg.Stream,
		EntryID:      msg.ID,
		Timestamp:    w.clock.Now(),
	}
	if err := w.dlq.Publish(ctx, dlqMsg); err != nil {
		w.log.Warn("dlq publish failed", slog.String("error", err.Error()))
		return
	}
	w.metrics.IncDLQ(reason)
}

// requeueWithoutAttempt returns a delivery to RETRYING soon without incrementing
// attempt_count (used for backpressure: rate limiting and open circuit breaker).
func (w *Worker) requeueWithoutAttempt(ctx context.Context, del domain.Delivery, reason string, log *slog.Logger) {
	next := w.clock.Now().Add(w.cfg.RateWaitStep)
	if err := w.repo.MarkRetrying(ctx, del.ID, del.CreatedAt, del.AttemptCount, next, reason); err != nil {
		log.Error("requeue failed", slog.String("error", err.Error()))
		return
	}
	log.Info("delivery requeued without attempt", slog.String("reason", reason))
}

// waitForRateLimit blocks (bounded) until a token is available. Returns false if the
// budget could not be obtained within RateWaitMax.
func (w *Worker) waitForRateLimit(ctx context.Context, channel domain.Channel, log *slog.Logger) bool {
	deadline := time.Now().Add(w.cfg.RateWaitMax)
	for {
		allowed, retryAfter, err := w.limiter.Allow(ctx, channel)
		if err != nil {
			log.Warn("rate limiter error", slog.String("error", err.Error()))
		}
		if allowed {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		wait := w.cfg.RateWaitStep
		if retryAfter > 0 && retryAfter < wait {
			wait = retryAfter
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(wait):
		}
	}
}
