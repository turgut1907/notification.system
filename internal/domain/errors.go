package domain

import "errors"

// Sentinel errors expressed in domain terms. Adapters and services translate
// infrastructure errors into these so callers (including HTTP transport) can map
// them to responses without importing infrastructure packages.
var (
	// ErrNotFound is returned when a requested entity does not exist.
	ErrNotFound = errors.New("not found")

	// ErrConflict indicates a uniqueness conflict (e.g. duplicate idempotency key).
	ErrConflict = errors.New("conflict")

	// ErrValidation indicates the caller supplied invalid input.
	ErrValidation = errors.New("validation error")

	// ErrTemplateVariablesMissing indicates required template variables were not provided.
	ErrTemplateVariablesMissing = errors.New("required template variables missing")

	// ErrCancelNotAllowed indicates a delivery is in a state that cannot be cancelled.
	ErrCancelNotAllowed = errors.New("notification cannot be cancelled in its current state")
)

// ValidationError carries a human-readable, field-level validation message while
// still unwrapping to ErrValidation for errors.Is checks.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	if e.Field == "" {
		return e.Message
	}
	return e.Field + ": " + e.Message
}

func (e *ValidationError) Unwrap() error { return ErrValidation }

// NewValidationError builds a ValidationError for the given field and message.
func NewValidationError(field, message string) *ValidationError {
	return &ValidationError{Field: field, Message: message}
}
