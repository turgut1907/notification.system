package domain

import (
	"errors"
	"testing"
)

func TestValidationError(t *testing.T) {
	err := NewValidationError("email", "invalid")
	if err.Error() != "email: invalid" {
		t.Fatal(err.Error())
	}
	if !errors.Is(err, ErrValidation) {
		t.Fatal("should wrap ErrValidation")
	}
}
