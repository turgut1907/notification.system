// Package logging provides structured logging built on the standard library slog,
// plus helpers for propagating a correlation ID through context.
package logging

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

type contextKey string

const correlationIDKey contextKey = "correlation_id"

// New builds a structured JSON logger at the given level ("debug", "info", "warn", "error").
func New(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(handler)
}

// WithCorrelationID stores a correlation ID in the context for downstream logging.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDKey, id)
}

// CorrelationID extracts the correlation ID from context, or "" if absent.
func CorrelationID(ctx context.Context) string {
	if v, ok := ctx.Value(correlationIDKey).(string); ok {
		return v
	}
	return ""
}

// FromContext returns a logger enriched with the context's correlation ID.
func FromContext(ctx context.Context, base *slog.Logger) *slog.Logger {
	if id := CorrelationID(ctx); id != "" {
		return base.With(slog.String("correlation_id", id))
	}
	return base
}
