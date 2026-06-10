package httpapi

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewRouter(t *testing.T) {
	h := NewHandler(&mockNotifSvc{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	reg := prometheus.NewRegistry()
	router := NewRouter(RouterConfig{
		Handler:   h,
		Registry:  reg,
		JWTSecret: "01234567890123456789012345678901",
		Readiness: map[string]HealthChecker{"db": healthOK{}},
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		SwaggerUI: true,
	})

	for _, path := range []string{"/healthz", "/openapi.yaml", "/docs"} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status %d", path, rec.Code)
		}
	}
}

func TestDTOToInput(t *testing.T) {
	key := "k1"
	tid := "00000000-0000-0000-0000-000000000001"
	in, err := (createRequest{
		Recipient: "a@b.com", Channel: "email", Priority: "normal", Content: "hi",
		TemplateID: &tid, IdempotencyKey: &key,
	}).toInput("")
	if err != nil || in.TemplateID == nil || in.IdempotencyKey == nil {
		t.Fatalf("input: %+v err=%v", in, err)
	}
}

func TestDecodeJSONInvalid(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{bad"))
	err := decodeJSON(rec, req, &struct{}{})
	if err == nil {
		t.Fatal("expected error")
	}
}
