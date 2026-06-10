package httpapi

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCursorRoundTrip(t *testing.T) {
	id := uuid.New()
	at := time.Date(2026, 6, 10, 9, 30, 0, 0, time.UTC)

	encoded := encodeCursor(id, &at)
	gotID, gotAt, err := decodeCursor(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if gotID != id {
		t.Fatalf("id mismatch: got %s want %s", gotID, id)
	}
	if gotAt == nil || !gotAt.Equal(at) {
		t.Fatalf("time mismatch: got %v want %v", gotAt, at)
	}
}

func TestDecodeCursor_Invalid(t *testing.T) {
	if _, _, err := decodeCursor("!!!not-base64!!!"); err == nil {
		t.Fatal("expected error for malformed cursor")
	}
}
