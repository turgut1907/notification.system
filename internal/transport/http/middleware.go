package httpapi

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/turgut1907/notification.system/internal/platform/auth"
	"github.com/turgut1907/notification.system/internal/platform/logging"
)

const correlationHeader = "X-Correlation-ID"

// statusRecorder captures the response status code for access logging.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// correlationMiddleware ensures every request has a correlation ID, stores it in the
// context, and echoes it back in the response header.
func correlationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(correlationHeader)
		if id == "" {
			id = uuid.NewString()
		}
		ctx := logging.WithCorrelationID(r.Context(), id)
		w.Header().Set(correlationHeader, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// loggingMiddleware emits a structured access log with latency and status.
func loggingMiddleware(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			attrs := []slog.Attr{
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", rec.status),
				slog.Duration("duration", time.Since(start)),
			}
			if uid := auth.UserID(r.Context()); uid != "" {
				attrs = append(attrs, slog.String("user_id", uid))
			}
			logging.FromContext(r.Context(), log).LogAttrs(r.Context(), slog.LevelInfo, "http request", attrs...)
		})
	}
}

// recoverMiddleware converts panics into 500s instead of crashing the server.
func recoverMiddleware(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					logging.FromContext(r.Context(), log).Error("panic recovered",
						slog.Any("panic", rec))
					writeJSON(w, http.StatusInternalServerError, errorResponse{
						Error: "internal_error", Message: "an unexpected error occurred",
					})
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// chain applies middlewares in order (outermost first).
func chain(h http.Handler, mws ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}
