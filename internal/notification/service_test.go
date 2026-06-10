package notification

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/turgut1907/notification.system/internal/domain"
	"github.com/turgut1907/notification.system/internal/platform/clock"
)

// --- fakes ---

type fakeRepo struct {
	created       *builtCapture
	byKey         map[string]domain.Request
	byID          map[uuid.UUID]domain.Request
	deliveries    map[uuid.UUID][]domain.Delivery
	batches       map[uuid.UUID]domain.Batch
	batchByKey    map[string]domain.Batch
	createErr     error
	batchResult   domain.BatchResult
	batchItems    []domain.BatchItem
	batchErr      error
	createdCalled int
	cancelReq     domain.Request
	cancelErr     error
	listPage      domain.Page
	listFilter    domain.ListFilter
	listErr       error
	// conflictMode hides existing keys from the pre-check until a conflicting
	// insert is observed, simulating a lost idempotency race.
	conflictMode bool
	conflictSeen bool
}

type builtCapture struct {
	req    domain.Request
	del    domain.Delivery
	outbox *domain.OutboxRecord
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		byKey:      map[string]domain.Request{},
		byID:       map[uuid.UUID]domain.Request{},
		deliveries: map[uuid.UUID][]domain.Delivery{},
		batches:    map[uuid.UUID]domain.Batch{},
		batchByKey: map[string]domain.Batch{},
	}
}

func (f *fakeRepo) CreateNotification(_ context.Context, req domain.Request, del domain.Delivery, outbox *domain.OutboxRecord) error {
	if f.createErr != nil {
		if errors.Is(f.createErr, domain.ErrConflict) {
			f.conflictSeen = true
		}
		return f.createErr
	}
	f.createdCalled++
	f.created = &builtCapture{req: req, del: del, outbox: outbox}
	if req.IdempotencyKey != nil {
		f.byKey[*req.IdempotencyKey] = req
	}
	f.byID[req.ID] = req
	return nil
}

func (f *fakeRepo) GetRequestByID(_ context.Context, id uuid.UUID) (domain.Request, error) {
	if r, ok := f.byID[id]; ok {
		return r, nil
	}
	return domain.Request{}, domain.ErrNotFound
}

func (f *fakeRepo) GetRequestByIdempotencyKey(_ context.Context, _, key string) (domain.Request, error) {
	if f.conflictMode && !f.conflictSeen {
		return domain.Request{}, domain.ErrNotFound
	}
	if r, ok := f.byKey[key]; ok {
		return r, nil
	}
	return domain.Request{}, domain.ErrNotFound
}

func (f *fakeRepo) ListRequests(_ context.Context, filter domain.ListFilter) (domain.Page, error) {
	f.listFilter = filter
	return f.listPage, f.listErr
}
func (f *fakeRepo) CancelRequest(_ context.Context, id uuid.UUID) (domain.Request, error) {
	if f.cancelErr != nil {
		return domain.Request{}, f.cancelErr
	}
	if f.cancelReq.ID != uuid.Nil {
		return f.cancelReq, nil
	}
	if r, ok := f.byID[id]; ok {
		r.Status = domain.RequestCancelled
		return r, nil
	}
	return domain.Request{}, domain.ErrNotFound
}
func (f *fakeRepo) GetDeliveriesByRequest(_ context.Context, id uuid.UUID) ([]domain.Delivery, error) {
	return f.deliveries[id], nil
}
func (f *fakeRepo) CreateBatch(_ context.Context, batch domain.Batch, items []domain.BatchItem) (domain.BatchResult, error) {
	f.batchItems = items
	if f.batchErr != nil {
		if errors.Is(f.batchErr, domain.ErrConflict) {
			f.conflictSeen = true
		}
		return domain.BatchResult{}, f.batchErr
	}
	f.batches[batch.ID] = batch
	if batch.IdempotencyKey != nil {
		f.batchByKey[*batch.IdempotencyKey] = batch
	}
	if f.batchResult.Batch.ID == uuid.Nil {
		f.batchResult = domain.BatchResult{Batch: batch, CreatedCount: 1}
	}
	return f.batchResult, nil
}
func (f *fakeRepo) GetBatchByID(_ context.Context, id uuid.UUID) (domain.Batch, error) {
	if b, ok := f.batches[id]; ok {
		return b, nil
	}
	return domain.Batch{}, domain.ErrNotFound
}
func (f *fakeRepo) GetBatchByIdempotencyKey(_ context.Context, _, key string) (domain.Batch, error) {
	if f.conflictMode && !f.conflictSeen {
		return domain.Batch{}, domain.ErrNotFound
	}
	if b, ok := f.batchByKey[key]; ok {
		return b, nil
	}
	return domain.Batch{}, domain.ErrNotFound
}

type fakeMetrics struct{ created int }

func (m *fakeMetrics) IncCreated(_ domain.Channel, _ domain.Priority) { m.created++ }

type fakeRenderer struct{}

func (fakeRenderer) Render(_ context.Context, _ uuid.UUID, _ domain.Channel, _ map[string]string) (domain.Template, string, error) {
	return domain.Template{}, "rendered", nil
}

func newService(repo Repository) (*Service, *clock.Fake, *fakeMetrics) {
	clk := clock.NewFake(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	m := &fakeMetrics{}
	return New(repo, fakeRenderer{}, clk, m), clk, m
}

// --- tests ---

func TestCreate_ValidationError(t *testing.T) {
	svc, _, _ := newService(newFakeRepo())
	_, err := svc.Create(context.Background(), CreateInput{
		Channel:  domain.ChannelEmail,
		Priority: domain.PriorityHigh,
		Content:  "hi",
		// missing recipient
	})
	var ve *domain.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected validation error, got %v", err)
	}
}

func TestCreate_ImmediateProducesOutbox(t *testing.T) {
	repo := newFakeRepo()
	svc, _, m := newService(repo)

	res, err := svc.Create(context.Background(), CreateInput{
		Recipient: "user@example.com",
		Channel:   domain.ChannelEmail,
		Priority:  domain.PriorityHigh,
		Content:   "hello",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Duplicate {
		t.Fatal("new create should not be a duplicate")
	}
	if repo.created.outbox == nil {
		t.Fatal("immediate send must enqueue an outbox row")
	}
	if repo.created.del.Status != domain.DeliveryPending {
		t.Fatalf("expected PENDING delivery, got %s", repo.created.del.Status)
	}
	if repo.created.outbox.Stream != domain.Stream(domain.PriorityHigh, domain.ChannelEmail) {
		t.Fatalf("unexpected stream %s", repo.created.outbox.Stream)
	}
	if m.created != 1 {
		t.Fatalf("expected IncCreated called once, got %d", m.created)
	}
}

func TestCreate_ScheduledHasNoOutbox(t *testing.T) {
	repo := newFakeRepo()
	svc, clk, _ := newService(repo)
	future := clk.Now().Add(time.Hour)

	_, err := svc.Create(context.Background(), CreateInput{
		Recipient: "user@example.com",
		Channel:   domain.ChannelSMS,
		Priority:  domain.PriorityLow,
		Content:   "later",
		SendAt:    &future,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.created.outbox != nil {
		t.Fatal("scheduled send must not enqueue immediately")
	}
	if repo.created.del.Status != domain.DeliveryScheduled {
		t.Fatalf("expected SCHEDULED, got %s", repo.created.del.Status)
	}
}

func TestCreate_IdempotentReplayReturnsExisting(t *testing.T) {
	repo := newFakeRepo()
	svc, _, _ := newService(repo)
	key := "abc-123"

	in := CreateInput{
		Recipient:      "user@example.com",
		Channel:        domain.ChannelEmail,
		Priority:       domain.PriorityHigh,
		Content:        "hello",
		IdempotencyKey: &key,
	}

	first, err := svc.Create(context.Background(), in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	second, err := svc.Create(context.Background(), in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !second.Duplicate {
		t.Fatal("second create with same key must be a duplicate")
	}
	if second.Request.ID != first.Request.ID {
		t.Fatal("duplicate must return the original request")
	}
	if repo.createdCalled != 1 {
		t.Fatalf("expected exactly one persist, got %d", repo.createdCalled)
	}
}

func TestCreate_IdempotencyRaceFallsBackToExisting(t *testing.T) {
	repo := newFakeRepo()
	key := "race-key"
	// Pre-seed the existing row and force the insert to report a conflict. The
	// pre-check is hidden until the conflict is observed (conflictMode).
	repo.byKey[key] = domain.Request{ID: uuid.New(), IdempotencyKey: &key}
	repo.createErr = domain.ErrConflict
	repo.conflictMode = true

	svc, _, _ := newService(repo)
	res, err := svc.Create(context.Background(), CreateInput{
		Recipient:      "user@example.com",
		Channel:        domain.ChannelEmail,
		Priority:       domain.PriorityHigh,
		Content:        "hello",
		IdempotencyKey: &key,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Duplicate || res.Request.ID != repo.byKey[key].ID {
		t.Fatal("conflict should resolve to the existing request")
	}
}

func TestGet_ReturnsView(t *testing.T) {
	repo := newFakeRepo()
	id := uuid.New()
	repo.byID[id] = domain.Request{ID: id, UserID: "alice", Recipient: "a@b.com", Channel: domain.ChannelEmail, Priority: domain.PriorityNormal}
	repo.deliveries[id] = []domain.Delivery{{ID: uuid.New(), Status: domain.DeliveryPending}}
	svc, _, _ := newService(repo)

	view, err := svc.Get(context.Background(), "alice", id)
	if err != nil || len(view.Deliveries) != 1 {
		t.Fatalf("view: %v err=%v", view, err)
	}
}

func TestList_DelegatesToRepo(t *testing.T) {
	repo := newFakeRepo()
	repo.listPage = domain.Page{Items: []domain.Request{{ID: uuid.New()}}}
	svc, _, _ := newService(repo)
	page, err := svc.List(context.Background(), domain.ListFilter{Limit: 10})
	if err != nil || len(page.Items) != 1 {
		t.Fatal(err)
	}
}

func TestCancel_Success(t *testing.T) {
	repo := newFakeRepo()
	id := uuid.New()
	repo.byID[id] = domain.Request{ID: id, UserID: "alice", Status: domain.RequestPending}
	svc, _, _ := newService(repo)
	req, err := svc.Cancel(context.Background(), "alice", id)
	if err != nil || req.Status != domain.RequestCancelled {
		t.Fatalf("cancel: %v err=%v", req.Status, err)
	}
}

func TestCreateBatch_Success(t *testing.T) {
	repo := newFakeRepo()
	svc, _, m := newService(repo)
	res, err := svc.CreateBatch(context.Background(), BatchInput{Items: []CreateInput{{
		Recipient: "+1", Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Content: "hi",
	}}})
	if err != nil || res.CreatedCount != 1 || m.created != 1 {
		t.Fatalf("batch: %+v err=%v created=%d", res, err, m.created)
	}
}

func TestCreateBatch_IdempotentReplay(t *testing.T) {
	repo := newFakeRepo()
	key := "batch-key"
	existing := domain.Batch{ID: uuid.New(), IdempotencyKey: &key, TotalCount: 3}
	repo.batchByKey[key] = existing
	svc, _, m := newService(repo)
	res, err := svc.CreateBatch(context.Background(), BatchInput{
		IdempotencyKey: &key,
		Items:          []CreateInput{{Recipient: "a@b.com", Channel: domain.ChannelEmail, Priority: domain.PriorityNormal, Content: "x"}},
	})
	if err != nil || !res.Duplicate || res.Batch.ID != existing.ID || m.created != 0 {
		t.Fatalf("replay: %+v err=%v created=%d", res, err, m.created)
	}
}

func TestGetBatch(t *testing.T) {
	repo := newFakeRepo()
	id := uuid.New()
	repo.batches[id] = domain.Batch{ID: id, UserID: "alice", TotalCount: 2}
	svc, _, _ := newService(repo)
	b, err := svc.GetBatch(context.Background(), "alice", id)
	if err != nil || b.TotalCount != 2 {
		t.Fatal(err)
	}
}

func TestGet_WrongUserReturnsNotFound(t *testing.T) {
	repo := newFakeRepo()
	id := uuid.New()
	repo.byID[id] = domain.Request{ID: id, UserID: "bob"}
	svc, _, _ := newService(repo)
	_, err := svc.Get(context.Background(), "alice", id)
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("got %v", err)
	}
}

func TestCreate_PersistsUserID(t *testing.T) {
	repo := newFakeRepo()
	svc, _, _ := newService(repo)
	_, err := svc.Create(context.Background(), CreateInput{
		UserID:    "alice",
		Recipient: "user@example.com",
		Channel:   domain.ChannelEmail,
		Priority:  domain.PriorityNormal,
		Content:   "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	if repo.created.req.UserID != "alice" {
		t.Fatalf("got user_id %q", repo.created.req.UserID)
	}
}

func TestCancel_WrongUserReturnsNotFound(t *testing.T) {
	repo := newFakeRepo()
	id := uuid.New()
	repo.byID[id] = domain.Request{ID: id, UserID: "bob", Status: domain.RequestPending}
	svc, _, _ := newService(repo)
	_, err := svc.Cancel(context.Background(), "alice", id)
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("got %v", err)
	}
}

func TestGetBatch_WrongUserReturnsNotFound(t *testing.T) {
	repo := newFakeRepo()
	id := uuid.New()
	repo.batches[id] = domain.Batch{ID: id, UserID: "bob", TotalCount: 1}
	svc, _, _ := newService(repo)
	_, err := svc.GetBatch(context.Background(), "alice", id)
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("got %v", err)
	}
}

func TestList_PassesUserIDFilter(t *testing.T) {
	repo := newFakeRepo()
	repo.listPage = domain.Page{Items: []domain.Request{{ID: uuid.New()}}}
	svc, _, _ := newService(repo)
	_, err := svc.List(context.Background(), domain.ListFilter{UserID: "alice", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if repo.listFilter.UserID != "alice" {
		t.Fatalf("filter user_id %q", repo.listFilter.UserID)
	}
}

func TestCreateBatch_AssignsBatchIDToItems(t *testing.T) {
	repo := newFakeRepo()
	svc, _, _ := newService(repo)
	res, err := svc.CreateBatch(context.Background(), BatchInput{
		UserID: "alice",
		Items: []CreateInput{
			{Recipient: "+15550000001", Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Content: "one"},
			{Recipient: "a@example.com", Channel: domain.ChannelEmail, Priority: domain.PriorityNormal, Content: "two"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(repo.batchItems) != 2 {
		t.Fatalf("expected 2 batch items, got %d", len(repo.batchItems))
	}
	for i, item := range repo.batchItems {
		if item.Request.BatchID == nil {
			t.Fatalf("item %d: batch_id is nil", i)
		}
		if *item.Request.BatchID != res.Batch.ID {
			t.Fatalf("item %d: batch_id %s, want %s", i, *item.Request.BatchID, res.Batch.ID)
		}
	}
}

func TestCreateBatch_PersistsUserIDOnBatch(t *testing.T) {
	repo := newFakeRepo()
	svc, _, _ := newService(repo)
	res, err := svc.CreateBatch(context.Background(), BatchInput{
		UserID: "alice",
		Items: []CreateInput{{
			Recipient: "+1", Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Content: "hi",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Batch.UserID != "alice" {
		t.Fatalf("result batch user_id %q", res.Batch.UserID)
	}
	for _, b := range repo.batches {
		if b.UserID != "alice" {
			t.Fatalf("stored batch user_id %q", b.UserID)
		}
	}
}
