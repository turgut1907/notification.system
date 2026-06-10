package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"go.opentelemetry.io/otel/attribute"

	"github.com/turgut1907/notification.system/internal/domain"
	"github.com/turgut1907/notification.system/internal/notification"
	"github.com/turgut1907/notification.system/internal/platform/auth"
	"github.com/turgut1907/notification.system/internal/platform/tracing"
)

const maxBodyBytes = 1 << 20 // 1 MiB request body cap

// NotificationService is the inbound port the HTTP handlers depend on.
type NotificationService interface {
	Create(ctx context.Context, in notification.CreateInput) (notification.CreateResult, error)
	CreateBatch(ctx context.Context, in notification.BatchInput) (notification.BatchCreateResult, error)
	Get(ctx context.Context, userID string, id uuid.UUID) (notification.View, error)
	List(ctx context.Context, f domain.ListFilter) (domain.Page, error)
	Cancel(ctx context.Context, userID string, id uuid.UUID) (domain.Request, error)
	GetBatch(ctx context.Context, userID string, id uuid.UUID) (domain.Batch, error)
}

// Handler holds the dependencies for the HTTP API.
type Handler struct {
	svc NotificationService
	log *slog.Logger
}

// NewHandler builds an API Handler.
func NewHandler(svc NotificationService, log *slog.Logger) *Handler {
	return &Handler{svc: svc, log: log}
}

// createNotification handles POST /api/v1/notifications.
func (h *Handler) createNotification(w http.ResponseWriter, r *http.Request) {
	userID, ok := userIDFromContext(w, r)
	if !ok {
		return
	}

	var body createRequest
	if err := decodeJSON(w, r, &body); err != nil {
		writeError(w, err)
		return
	}

	in, err := body.toInput(r.Header.Get("Idempotency-Key"))
	if err != nil {
		writeError(w, err)
		return
	}
	in.UserID = userID

	ctx, span := tracing.StartSpan(r.Context(), "notification.create",
		attribute.String("user_id", userID),
		attribute.String("channel", string(in.Channel)),
		attribute.String("priority", string(in.Priority)),
	)
	defer span.End()

	res, err := h.svc.Create(ctx, in)
	if err != nil {
		writeError(w, err)
		return
	}

	status := http.StatusAccepted
	if res.Duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, toNotificationResponse(res.Request, nil))
}

// createBatch handles POST /api/v1/notifications/batch.
func (h *Handler) createBatch(w http.ResponseWriter, r *http.Request) {
	userID, ok := userIDFromContext(w, r)
	if !ok {
		return
	}

	var body batchRequest
	if err := decodeJSON(w, r, &body); err != nil {
		writeError(w, err)
		return
	}

	in, err := body.toInput(r.Header.Get("Idempotency-Key"))
	if err != nil {
		writeError(w, err)
		return
	}
	in.UserID = userID

	res, err := h.svc.CreateBatch(r.Context(), in)
	if err != nil {
		writeError(w, err)
		return
	}

	status := http.StatusAccepted
	if res.Duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, struct {
		batchResponse
		CreatedCount   int `json:"created_count"`
		DuplicateCount int `json:"duplicate_count"`
	}{
		batchResponse:  toBatchResponse(res.Batch),
		CreatedCount:   res.CreatedCount,
		DuplicateCount: res.DuplicateCount,
	})
}

// getNotification handles GET /api/v1/notifications/{id}.
func (h *Handler) getNotification(w http.ResponseWriter, r *http.Request) {
	userID, ok := userIDFromContext(w, r)
	if !ok {
		return
	}

	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, domain.NewValidationError("id", "must be a valid UUID"))
		return
	}
	view, err := h.svc.Get(r.Context(), userID, id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toNotificationResponse(view.Request, view.Deliveries))
}

// cancelNotification handles POST /api/v1/notifications/{id}/cancel.
func (h *Handler) cancelNotification(w http.ResponseWriter, r *http.Request) {
	userID, ok := userIDFromContext(w, r)
	if !ok {
		return
	}

	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, domain.NewValidationError("id", "must be a valid UUID"))
		return
	}
	req, err := h.svc.Cancel(r.Context(), userID, id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toNotificationResponse(req, nil))
}

// listNotifications handles GET /api/v1/notifications.
func (h *Handler) listNotifications(w http.ResponseWriter, r *http.Request) {
	userID, ok := userIDFromContext(w, r)
	if !ok {
		return
	}

	filter, err := parseListFilter(r)
	if err != nil {
		writeError(w, err)
		return
	}
	filter.UserID = userID
	page, err := h.svc.List(r.Context(), filter)
	if err != nil {
		writeError(w, err)
		return
	}

	resp := listResponse{Items: make([]notificationResponse, 0, len(page.Items))}
	for i := range page.Items {
		resp.Items = append(resp.Items, toNotificationSummaryResponse(page.Items[i]))
	}
	if page.NextCursor != nil {
		cursor := encodeCursor(*page.NextCursor, page.NextAt)
		resp.NextCursor = &cursor
	}
	writeJSON(w, http.StatusOK, resp)
}

// getBatch handles GET /api/v1/batches/{id}.
func (h *Handler) getBatch(w http.ResponseWriter, r *http.Request) {
	userID, ok := userIDFromContext(w, r)
	if !ok {
		return
	}

	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, domain.NewValidationError("id", "must be a valid UUID"))
		return
	}
	batch, err := h.svc.GetBatch(r.Context(), userID, id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toBatchResponse(batch))
}

// parseListFilter builds a ListFilter from query parameters.
func parseListFilter(r *http.Request) (domain.ListFilter, error) {
	q := r.URL.Query()
	var f domain.ListFilter

	if s := q.Get("status"); s != "" {
		st := domain.RequestStatus(s)
		f.Status = &st
	}
	if c := q.Get("channel"); c != "" {
		ch := domain.Channel(c)
		if !ch.Valid() {
			return f, domain.NewValidationError("channel", "invalid channel")
		}
		f.Channel = &ch
	}
	if from := q.Get("from"); from != "" {
		t, err := parseQueryTime(from)
		if err != nil {
			return f, domain.NewValidationError("from", "must be an RFC3339 datetime (e.g. 2026-06-10T00:00:00Z)")
		}
		f.From = &t
	}
	if to := q.Get("to"); to != "" {
		t, err := parseQueryTime(to)
		if err != nil {
			return f, domain.NewValidationError("to", "must be an RFC3339 datetime (e.g. 2026-06-10T23:59:59Z)")
		}
		f.To = &t
	}
	if f.From != nil && f.To != nil && f.From.After(*f.To) {
		return f, domain.NewValidationError("from", "must be before or equal to to")
	}
	if l := q.Get("limit"); l != "" {
		n, err := parseInt(l)
		if err != nil {
			return f, domain.NewValidationError("limit", "must be an integer")
		}
		f.Limit = n
	}
	if c := q.Get("cursor"); c != "" {
		id, at, err := decodeCursor(c)
		if err != nil {
			return f, domain.NewValidationError("cursor", "invalid cursor")
		}
		f.Cursor = &id
		f.CursorAt = at
	}
	f.Normalize()
	return f, nil
}

func userIDFromContext(w http.ResponseWriter, r *http.Request) (string, bool) {
	userID := auth.UserID(r.Context())
	if userID == "" {
		writeJSON(w, http.StatusUnauthorized, errorResponse{
			Error: "unauthorized", Message: "authentication required",
		})
		return "", false
	}
	return userID, true
}

// decodeJSON strictly decodes the request body into v with a size cap.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		if errors.Is(err, io.EOF) {
			return domain.NewValidationError("body", "request body is required")
		}
		return domain.NewValidationError("body", "invalid JSON: "+err.Error())
	}
	return nil
}
