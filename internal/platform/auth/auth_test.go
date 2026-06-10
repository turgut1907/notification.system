package auth

import (
	"strings"
	"testing"
	"time"
)

const testSecret = "01234567890123456789012345678901"

func TestSignAndParseToken(t *testing.T) {
	token, err := SignToken(testSecret, "alice", time.Hour)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	userID, err := ParseToken(testSecret, token)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if userID != "alice" {
		t.Fatalf("got user_id %q", userID)
	}
}

func TestParseTokenWrongSecret(t *testing.T) {
	token, err := SignToken(testSecret, "alice", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseToken(strings.Repeat("x", 32), token); err == nil {
		t.Fatal("expected invalid token")
	}
}

func TestParseTokenExpired(t *testing.T) {
	token, err := SignToken(testSecret, "alice", -time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseToken(testSecret, token); err == nil {
		t.Fatal("expected expired token error")
	}
}

func TestValidateSecret(t *testing.T) {
	if err := ValidateSecret(""); err != ErrEmptySecret {
		t.Fatalf("empty: %v", err)
	}
	if err := ValidateSecret("short"); err != ErrSecretTooShort {
		t.Fatalf("short: %v", err)
	}
	if err := ValidateSecret(testSecret); err != nil {
		t.Fatalf("valid: %v", err)
	}
}

func TestSignTokenRejectsEmptyUserID(t *testing.T) {
	if _, err := SignToken(testSecret, "  ", time.Hour); err != ErrMissingUserID {
		t.Fatalf("got %v", err)
	}
}

func TestUserIDContext(t *testing.T) {
	ctx := WithUserID(t.Context(), "bob")
	if got := UserID(ctx); got != "bob" {
		t.Fatalf("got %q", got)
	}
	if got := UserID(t.Context()); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}
