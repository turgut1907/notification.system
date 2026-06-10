package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/turgut1907/notification.system/internal/domain"
)

// errorResponse is the uniform error envelope returned to clients.
type errorResponse struct {
	Error   string `json:"error"`
	Field   string `json:"field,omitempty"`
	Message string `json:"message"`
}

// writeJSON serializes v as JSON with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

// writeError maps domain errors to HTTP status codes and a JSON envelope.
func writeError(w http.ResponseWriter, err error) {
	var ve *domain.ValidationError
	switch {
	case errors.As(err, &ve):
		writeJSON(w, http.StatusBadRequest, errorResponse{
			Error: "validation_error", Field: ve.Field, Message: ve.Message,
		})
	case errors.Is(err, domain.ErrValidation):
		writeJSON(w, http.StatusBadRequest, errorResponse{
			Error: "validation_error", Message: err.Error(),
		})
	case errors.Is(err, domain.ErrTemplateVariablesMissing):
		writeJSON(w, http.StatusBadRequest, errorResponse{
			Error: "template_variables_missing", Message: err.Error(),
		})
	case errors.Is(err, domain.ErrNotFound):
		writeJSON(w, http.StatusNotFound, errorResponse{
			Error: "not_found", Message: "resource not found",
		})
	case errors.Is(err, domain.ErrCancelNotAllowed):
		writeJSON(w, http.StatusConflict, errorResponse{
			Error: "cancel_not_allowed", Message: err.Error(),
		})
	case errors.Is(err, domain.ErrConflict):
		writeJSON(w, http.StatusConflict, errorResponse{
			Error: "conflict", Message: err.Error(),
		})
	default:
		writeJSON(w, http.StatusInternalServerError, errorResponse{
			Error: "internal_error", Message: "an unexpected error occurred",
		})
	}
}
