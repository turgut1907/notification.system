package httpapi

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/turgut1907/notification.system/internal/domain"
	"github.com/turgut1907/notification.system/internal/platform/auth"
)

const testJWTSecret = "01234567890123456789012345678901"

func TestAuthMiddleware_RejectsMissingToken(t *testing.T) {
	h := authMiddleware(testJWTSecret)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/notifications", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestAuthMiddleware_AllowsPublicPaths(t *testing.T) {
	h := authMiddleware(testJWTSecret)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestAuthMiddleware_SetsUserID(t *testing.T) {
	token, err := auth.SignToken(testJWTSecret, "alice", auth.DemoTokenTTL)
	if err != nil {
		t.Fatal(err)
	}

	var gotUser string
	h := authMiddleware(testJWTSecret)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser = auth.UserID(r.Context())
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || gotUser != "alice" {
		t.Fatalf("status=%d user=%q", rec.Code, gotUser)
	}
}

func TestNewRouter_APIRequiresAuth(t *testing.T) {
	h := NewHandler(&mockNotifSvc{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	router := NewRouter(RouterConfig{
		Handler:   h,
		JWTSecret: testJWTSecret,
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/notifications", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestAuthMiddleware_RejectsInvalidToken(t *testing.T) {
	h := authMiddleware(testJWTSecret)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications", nil)
	req.Header.Set("Authorization", "Bearer not-a-valid-jwt")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestAuthMiddleware_RejectsMalformedBearer(t *testing.T) {
	h := authMiddleware(testJWTSecret)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications", nil)
	req.Header.Set("Authorization", "Token abc")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestBearerToken(t *testing.T) {
	if got := bearerToken("Bearer abc.def"); got != "abc.def" {
		t.Fatalf("got %q", got)
	}
	if bearerToken("Basic x") != "" {
		t.Fatal("expected empty for non-bearer")
	}
	if bearerToken("") != "" {
		t.Fatal("expected empty")
	}
}

func TestNewRouter_APIAllowsValidToken(t *testing.T) {
	token, err := auth.SignToken(testJWTSecret, "demo-user", auth.DemoTokenTTL)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(&mockNotifSvc{page: domain.Page{}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	router := NewRouter(RouterConfig{
		Handler:   h,
		JWTSecret: testJWTSecret,
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
}
