package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/turgut1907/notification.system/internal/domain"
	"github.com/turgut1907/notification.system/internal/notification"
	"github.com/turgut1907/notification.system/internal/platform/auth"
)

type mockNotifSvc struct {
	lastCreateIn notification.CreateInput
	lastListFilter domain.ListFilter
	createRes  notification.CreateResult
	createErr  error
	batchRes   notification.BatchCreateResult
	batchErr   error
	view       notification.View
	getErr     error
	page       domain.Page
	listErr    error
	cancelReq  domain.Request
	cancelErr  error
	batch      domain.Batch
	batchGetErr error
}

func (m *mockNotifSvc) Create(_ context.Context, in notification.CreateInput) (notification.CreateResult, error) {
	m.lastCreateIn = in
	return m.createRes, m.createErr
}
func (m *mockNotifSvc) CreateBatch(context.Context, notification.BatchInput) (notification.BatchCreateResult, error) {
	return m.batchRes, m.batchErr
}
func (m *mockNotifSvc) Get(context.Context, string, uuid.UUID) (notification.View, error) {
	return m.view, m.getErr
}
func (m *mockNotifSvc) List(_ context.Context, f domain.ListFilter) (domain.Page, error) {
	m.lastListFilter = f
	return m.page, m.listErr
}
func (m *mockNotifSvc) Cancel(context.Context, string, uuid.UUID) (domain.Request, error) {
	return m.cancelReq, m.cancelErr
}
func (m *mockNotifSvc) GetBatch(context.Context, string, uuid.UUID) (domain.Batch, error) {
	return m.batch, m.batchGetErr
}

func testHandler(svc NotificationService) *Handler {
	return NewHandler(svc, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func authedRequest(method, target string, body io.Reader) *http.Request {
	req := httptest.NewRequest(method, target, body)
	return req.WithContext(auth.WithUserID(req.Context(), "test-user"))
}

func TestCreateNotification_UnauthorizedWithoutUserID(t *testing.T) {
	h := testHandler(&mockNotifSvc{})
	body := `{"recipient":"a@b.com","channel":"email","priority":"normal","content":"hi"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	h.createNotification(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestCreateNotification_PassesUserIDToService(t *testing.T) {
	svc := &mockNotifSvc{createRes: notification.CreateResult{
		Request: domain.Request{ID: uuid.New()},
	}}
	h := testHandler(svc)
	body := `{"recipient":"a@b.com","channel":"email","priority":"normal","content":"hi"}`
	req := authedRequest(http.MethodPost, "/api/v1/notifications", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	h.createNotification(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status %d", rec.Code)
	}
	if svc.lastCreateIn.UserID != "test-user" {
		t.Fatalf("user_id %q", svc.lastCreateIn.UserID)
	}
}

func TestListNotifications_PassesUserIDFilter(t *testing.T) {
	svc := &mockNotifSvc{page: domain.Page{Items: []domain.Request{{ID: uuid.New()}}}}
	h := testHandler(svc)
	req := authedRequest(http.MethodGet, "/api/v1/notifications?limit=10", nil)
	rec := httptest.NewRecorder()
	h.listNotifications(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if svc.lastListFilter.UserID != "test-user" {
		t.Fatalf("filter user_id %q", svc.lastListFilter.UserID)
	}
}

func TestCreateNotificationHandler(t *testing.T) {
	id := uuid.New()
	h := testHandler(&mockNotifSvc{createRes: notification.CreateResult{
		Request: domain.Request{ID: id, Recipient: "a@b.com", Channel: domain.ChannelEmail, Priority: domain.PriorityNormal, Status: domain.RequestPending, CreatedAt: time.Now()},
	}})
	body := `{"recipient":"a@b.com","channel":"email","priority":"normal","content":"hi"}`
	req := authedRequest(http.MethodPost, "/api/v1/notifications", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	h.createNotification(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
}

func TestCreateNotificationDuplicate(t *testing.T) {
	h := testHandler(&mockNotifSvc{createRes: notification.CreateResult{Duplicate: true, Request: domain.Request{ID: uuid.New()}}})
	body := `{"recipient":"a@b.com","channel":"email","priority":"normal","content":"hi"}`
	req := authedRequest(http.MethodPost, "/api/v1/notifications", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	h.createNotification(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestGetNotificationNotFound(t *testing.T) {
	h := testHandler(&mockNotifSvc{getErr: domain.ErrNotFound})
	req := authedRequest(http.MethodGet, "/api/v1/notifications/"+uuid.New().String(), nil)
	req.SetPathValue("id", uuid.New().String())
	rec := httptest.NewRecorder()
	h.getNotification(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestListNotifications(t *testing.T) {
	h := testHandler(&mockNotifSvc{page: domain.Page{Items: []domain.Request{{
		ID: uuid.New(), Recipient: "x", RenderedContent: "secret-body",
	}}}})
	req := authedRequest(http.MethodGet, "/api/v1/notifications?limit=10", nil)
	rec := httptest.NewRecorder()
	h.listNotifications(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	var resp listResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if len(resp.Items) != 1 {
		t.Fatalf("items %d", len(resp.Items))
	}
	if resp.Items[0].Content != "" {
		t.Fatalf("list must omit content, got %q", resp.Items[0].Content)
	}
}

func TestCancelNotification(t *testing.T) {
	id := uuid.New()
	h := testHandler(&mockNotifSvc{cancelReq: domain.Request{ID: id, Status: domain.RequestCancelled}})
	req := authedRequest(http.MethodPost, "/api/v1/notifications/"+id.String()+"/cancel", nil)
	req.SetPathValue("id", id.String())
	rec := httptest.NewRecorder()
	h.cancelNotification(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestCreateBatchHandler(t *testing.T) {
	id := uuid.New()
	h := testHandler(&mockNotifSvc{batchRes: notification.BatchCreateResult{
		Batch: domain.Batch{ID: id, TotalCount: 2}, CreatedCount: 2,
	}})
	body := `{"notifications":[{"recipient":"a@b.com","channel":"email","priority":"normal","content":"hi"}]}`
	req := authedRequest(http.MethodPost, "/api/v1/notifications/batch", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	h.createBatch(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
}

func TestGetBatch(t *testing.T) {
	id := uuid.New()
	h := testHandler(&mockNotifSvc{batch: domain.Batch{ID: id, TotalCount: 5}})
	req := authedRequest(http.MethodGet, "/api/v1/batches/"+id.String(), nil)
	req.SetPathValue("id", id.String())
	rec := httptest.NewRecorder()
	h.getBatch(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestWriteErrorValidation(t *testing.T) {
	rec := httptest.NewRecorder()
	writeError(rec, domain.NewValidationError("field", "bad"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestHandleLive(t *testing.T) {
	rec := httptest.NewRecorder()
	handleLive(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatal(rec.Code)
	}
}

func TestHandleReady(t *testing.T) {
	rec := httptest.NewRecorder()
	h := handleReady(map[string]HealthChecker{
		"ok":  healthOK{},
		"bad": healthBad{},
	})
	h(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d", rec.Code)
	}
}

type healthOK struct{}

func (healthOK) Ping(context.Context) error { return nil }

type healthBad struct{}

func (healthBad) Ping(context.Context) error { return domain.ErrNotFound }

func TestCorrelationMiddleware(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	correlationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(rec, req)
	if rec.Header().Get(correlationHeader) == "" {
		t.Fatal("missing correlation header")
	}
}

func TestOpenAPISpecAndSwagger(t *testing.T) {
	rec := httptest.NewRecorder()
	handleOpenAPISpec(rec, httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil))
	if rec.Code != http.StatusOK || len(rec.Body.Bytes()) == 0 {
		t.Fatal("empty spec")
	}
	rec = httptest.NewRecorder()
	handleSwaggerUI(rec, httptest.NewRequest(http.MethodGet, "/docs", nil))
	if rec.Code != http.StatusOK {
		t.Fatal(rec.Code)
	}
}
