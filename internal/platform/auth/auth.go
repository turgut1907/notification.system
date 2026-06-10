// Package auth provides JWT signing/parsing and request-scoped user identity.
package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const minSecretLen = 32

// DemoTokenTTL is the default lifetime for locally generated demo tokens.
const DemoTokenTTL = 365 * 24 * time.Hour

var (
	ErrEmptySecret   = errors.New("auth: JWT_SECRET is required")
	ErrSecretTooShort = fmt.Errorf("auth: JWT_SECRET must be at least %d characters", minSecretLen)
	ErrInvalidToken  = errors.New("auth: invalid token")
	ErrMissingUserID = errors.New("auth: token missing user_id claim")
)

type contextKey string

const userIDKey contextKey = "user_id"

type claims struct {
	UserID string `json:"user_id"`
	jwt.RegisteredClaims
}

// ValidateSecret checks that secret is suitable for HMAC signing.
func ValidateSecret(secret string) error {
	if secret == "" {
		return ErrEmptySecret
	}
	if len(secret) < minSecretLen {
		return ErrSecretTooShort
	}
	return nil
}

// SignToken issues an HS256 JWT with the given user_id claim.
func SignToken(secret, userID string, ttl time.Duration) (string, error) {
	if err := ValidateSecret(secret); err != nil {
		return "", err
	}
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return "", ErrMissingUserID
	}

	now := time.Now()
	c := claims{
		UserID: userID,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, c).SignedString([]byte(secret))
}

// ParseToken validates a bearer token and returns the user_id claim.
func ParseToken(secret, tokenString string) (string, error) {
	if err := ValidateSecret(secret); err != nil {
		return "", err
	}

	tokenString = strings.TrimSpace(tokenString)
	if tokenString == "" {
		return "", ErrInvalidToken
	}

	parsed, err := jwt.ParseWithClaims(tokenString, &claims{}, func(t *jwt.Token) (any, error) {
		if t.Method != jwt.SigningMethodHS256 {
			return nil, ErrInvalidToken
		}
		return []byte(secret), nil
	})
	if err != nil || !parsed.Valid {
		return "", ErrInvalidToken
	}

	c, ok := parsed.Claims.(*claims)
	if !ok || strings.TrimSpace(c.UserID) == "" {
		return "", ErrMissingUserID
	}
	return c.UserID, nil
}

// WithUserID stores the authenticated user ID in context.
func WithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, userIDKey, userID)
}

// UserID returns the authenticated user ID from context, or "" if absent.
func UserID(ctx context.Context) string {
	if v, ok := ctx.Value(userIDKey).(string); ok {
		return v
	}
	return ""
}
