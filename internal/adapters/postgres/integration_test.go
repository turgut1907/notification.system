package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/turgut1907/notification.system/internal/domain"
)

func TestCreateAndGetRequest(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	req, del, outbox := sampleRequestDelivery(now)
	if err := intStore.CreateNotification(ctx, req, del, outbox); err != nil {
		t.Fatalf("create: %v", err)
	}

	got, err := intStore.GetRequestByID(ctx, req.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Recipient != req.Recipient {
		t.Fatalf("recipient mismatch")
	}

	dels, err := intStore.GetDeliveriesByRequest(ctx, req.ID)
	if err != nil || len(dels) != 1 {
		t.Fatalf("deliveries: %v %d", err, len(dels))
	}
}

func TestIdempotencyKeyScopedPerUser(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	now := time.Now().UTC()
	key := "shared-key"

	req1, del1, out1 := sampleRequestDelivery(now)
	req1.UserID = "user-a"
	req1.IdempotencyKey = &key
	if err := intStore.CreateNotification(ctx, req1, del1, out1); err != nil {
		t.Fatal(err)
	}

	req2, del2, out2 := sampleRequestDelivery(now)
	req2.UserID = "user-b"
	req2.IdempotencyKey = &key
	if err := intStore.CreateNotification(ctx, req2, del2, out2); err != nil {
		t.Fatalf("same key for different users should succeed: %v", err)
	}

	gotA, err := intStore.GetRequestByIdempotencyKey(ctx, "user-a", key)
	if err != nil || gotA.ID != req1.ID {
		t.Fatalf("user-a lookup: %v", err)
	}
	gotB, err := intStore.GetRequestByIdempotencyKey(ctx, "user-b", key)
	if err != nil || gotB.ID != req2.ID {
		t.Fatalf("user-b lookup: %v", err)
	}
}

func TestListRequestsScopedByUserID(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	now := time.Now().UTC()

	reqA, delA, outA := sampleRequestDelivery(now)
	reqA.UserID = "user-a"
	_ = intStore.CreateNotification(ctx, reqA, delA, outA)

	reqB, delB, outB := sampleRequestDelivery(now)
	reqB.UserID = "user-b"
	_ = intStore.CreateNotification(ctx, reqB, delB, outB)

	page, err := intStore.ListRequests(ctx, domain.ListFilter{UserID: "user-a", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].ID != reqA.ID {
		t.Fatalf("expected only user-a request, got %d items", len(page.Items))
	}
	if page.Items[0].RenderedContent != "" {
		t.Fatal("list query must not load rendered_content")
	}
	got, err := intStore.GetRequestByID(ctx, reqA.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.RenderedContent != reqA.RenderedContent {
		t.Fatalf("get by id should return content, got %q", got.RenderedContent)
	}
}

func TestIdempotencyKeyConflict(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	now := time.Now().UTC()
	key := "idem-1"

	req1, del1, out1 := sampleRequestDelivery(now)
	req1.IdempotencyKey = &key
	if err := intStore.CreateNotification(ctx, req1, del1, out1); err != nil {
		t.Fatal(err)
	}

	req2, del2, out2 := sampleRequestDelivery(now)
	req2.IdempotencyKey = &key
	if err := intStore.CreateNotification(ctx, req2, del2, out2); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}

	got, err := intStore.GetRequestByIdempotencyKey(ctx, req1.UserID, key)
	if err != nil || got.ID != req1.ID {
		t.Fatalf("idempotent lookup failed: %v", err)
	}
}

func TestClaimAndMarkSent(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	now := time.Now().UTC()

	req, del, outbox := sampleRequestDelivery(now)
	if err := intStore.CreateNotification(ctx, req, del, outbox); err != nil {
		t.Fatal(err)
	}

	lease := now.Add(time.Minute)
	claimed, ok, err := intStore.ClaimDelivery(ctx, del.ID, del.CreatedAt, lease)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if claimed.Status != domain.DeliveryProcessing {
		t.Fatalf("status %s", claimed.Status)
	}

	if err := intStore.MarkSent(ctx, del.ID, del.CreatedAt, req.ID, "msg-1"); err != nil {
		t.Fatal(err)
	}

	dels, _ := intStore.GetDeliveriesByRequest(ctx, req.ID)
	if dels[0].Status != domain.DeliverySent {
		t.Fatalf("expected SENT, got %s", dels[0].Status)
	}
	got, _ := intStore.GetRequestByID(ctx, req.ID)
	if got.Status != domain.RequestSent {
		t.Fatalf("request status %s", got.Status)
	}
}

func TestMarkRetryingAndFailed(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	now := time.Now().UTC()

	req, del, outbox := sampleRequestDelivery(now)
	_ = intStore.CreateNotification(ctx, req, del, outbox)

	lease := now.Add(time.Minute)
	_, ok, _ := intStore.ClaimDelivery(ctx, del.ID, del.CreatedAt, lease)
	if !ok {
		t.Fatal("claim failed")
	}

	next := now.Add(time.Minute)
	if err := intStore.MarkRetrying(ctx, del.ID, del.CreatedAt, 1, next, "transient"); err != nil {
		t.Fatal(err)
	}

	_, ok, _ = intStore.ClaimDelivery(ctx, del.ID, del.CreatedAt, lease)
	if !ok {
		t.Fatal("re-claim failed")
	}
	if err := intStore.MarkFailed(ctx, del.ID, del.CreatedAt, req.ID, 3, "permanent"); err != nil {
		t.Fatal(err)
	}
	got, _ := intStore.GetRequestByID(ctx, req.ID)
	if got.Status != domain.RequestFailed {
		t.Fatalf("got %s", got.Status)
	}
}

func TestListRequestsFilters(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	now := time.Now().UTC()

	req, del, outbox := sampleRequestDelivery(now)
	_ = intStore.CreateNotification(ctx, req, del, outbox)

	st := domain.RequestPending
	ch := domain.ChannelSMS
	page, err := intStore.ListRequests(ctx, domain.ListFilter{
		UserID: req.UserID, Status: &st, Channel: &ch, From: &now, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 {
		t.Fatalf("want 1 item, got %d", len(page.Items))
	}
}

func TestCancelRequest(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	now := time.Now().UTC()

	req, del, outbox := sampleRequestDelivery(now)
	_ = intStore.CreateNotification(ctx, req, del, outbox)

	cancelled, err := intStore.CancelRequest(ctx, req.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Status != domain.RequestCancelled {
		t.Fatalf("status %s", cancelled.Status)
	}
}

func TestOutboxFetchAndPublish(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	now := time.Now().UTC()

	req, del, outbox := sampleRequestDelivery(now)
	_ = intStore.CreateNotification(ctx, req, del, outbox)

	entries, err := intStore.FetchUnpublishedOutbox(ctx, 10)
	if err != nil || len(entries) != 1 {
		t.Fatalf("fetch: %v len=%d", err, len(entries))
	}

	if err := intStore.MarkOutboxPublished(ctx, []uuid.UUID{entries[0].ID}); err != nil {
		t.Fatal(err)
	}

	n, err := intStore.CountUnpublishedOutbox(ctx)
	if err != nil || n != 0 {
		t.Fatalf("unpublished %d err=%v", n, err)
	}
}

func TestCreateBatch(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	now := time.Now().UTC()

	req, del, outbox := sampleRequestDelivery(now)
	batch := domain.Batch{ID: uuid.New(), UserID: req.UserID, CreatedAt: now, TotalCount: 1, PendingCount: 1}
	req.BatchID = &batch.ID
	res, err := intStore.CreateBatch(ctx, batch, []domain.BatchItem{{Request: req, Delivery: del, Outbox: outbox}})
	if err != nil {
		t.Fatal(err)
	}
	if res.CreatedCount != 1 {
		t.Fatalf("created %d", res.CreatedCount)
	}
	got, err := intStore.GetBatchByID(ctx, batch.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalCount != 1 {
		t.Fatalf("total %d", got.TotalCount)
	}
	if got.UserID != req.UserID {
		t.Fatalf("batch user_id %q", got.UserID)
	}
}

func TestCreateBatch_MultipleItemsWithBatchID(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	now := time.Now().UTC()

	batch := domain.Batch{ID: uuid.New(), UserID: "test-user", CreatedAt: now}
	items := make([]domain.BatchItem, 0, 3)
	for i := range 3 {
		req, del, outbox := sampleRequestDelivery(now)
		key := fmt.Sprintf("batch-item-%d", i)
		req.IdempotencyKey = &key
		req.Recipient = fmt.Sprintf("+1555%07d", i+1)
		req.BatchID = &batch.ID
		items = append(items, domain.BatchItem{Request: req, Delivery: del, Outbox: outbox})
	}

	res, err := intStore.CreateBatch(ctx, batch, items)
	if err != nil {
		t.Fatal(err)
	}
	if res.CreatedCount != 3 || res.DuplicateCount != 0 {
		t.Fatalf("created=%d duplicate=%d", res.CreatedCount, res.DuplicateCount)
	}

	got, err := intStore.GetBatchByID(ctx, batch.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalCount != 3 || got.PendingCount != 3 {
		t.Fatalf("batch counts total=%d pending=%d", got.TotalCount, got.PendingCount)
	}

	for _, item := range items {
		stored, err := intStore.GetRequestByID(ctx, item.Request.ID)
		if err != nil {
			t.Fatalf("get request %s: %v", item.Request.ID, err)
		}
		if stored.BatchID == nil || *stored.BatchID != batch.ID {
			t.Fatalf("request %s batch_id=%v want %s", item.Request.ID, stored.BatchID, batch.ID)
		}
	}
}

func TestCreateBatch_DeduplicatesItems(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	now := time.Now().UTC()
	dupKey := "shared-item-key"

	existingReq, existingDel, existingOutbox := sampleRequestDelivery(now)
	existingReq.IdempotencyKey = &dupKey
	if err := intStore.CreateNotification(ctx, existingReq, existingDel, existingOutbox); err != nil {
		t.Fatal(err)
	}

	batch := domain.Batch{ID: uuid.New(), UserID: "test-user", CreatedAt: now}
	newReq, newDel, newOutbox := sampleRequestDelivery(now)
	newKey := "new-item-key"
	newReq.IdempotencyKey = &newKey
	newReq.BatchID = &batch.ID

	dupReq, dupDel, dupOutbox := sampleRequestDelivery(now)
	dupReq.IdempotencyKey = &dupKey
	dupReq.BatchID = &batch.ID

	res, err := intStore.CreateBatch(ctx, batch, []domain.BatchItem{
		{Request: dupReq, Delivery: dupDel, Outbox: dupOutbox},
		{Request: newReq, Delivery: newDel, Outbox: newOutbox},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.CreatedCount != 1 || res.DuplicateCount != 1 {
		t.Fatalf("created=%d duplicate=%d", res.CreatedCount, res.DuplicateCount)
	}

	got, err := intStore.GetBatchByID(ctx, batch.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalCount != 1 || got.PendingCount != 1 {
		t.Fatalf("batch counts total=%d pending=%d", got.TotalCount, got.PendingCount)
	}

	stored, err := intStore.GetRequestByID(ctx, newReq.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.BatchID == nil || *stored.BatchID != batch.ID {
		t.Fatalf("new request batch_id=%v want %s", stored.BatchID, batch.ID)
	}
}

func TestBatchIdempotencyScopedPerUser(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	now := time.Now().UTC()
	key := "batch-key"

	makeBatch := func(userID string) domain.Batch {
		req, del, outbox := sampleRequestDelivery(now)
		req.UserID = userID
		batch := domain.Batch{ID: uuid.New(), UserID: userID, IdempotencyKey: &key, CreatedAt: now}
		req.BatchID = &batch.ID
		if _, err := intStore.CreateBatch(ctx, batch, []domain.BatchItem{{Request: req, Delivery: del, Outbox: outbox}}); err != nil {
			t.Fatalf("create batch for %s: %v", userID, err)
		}
		return batch
	}

	batchA := makeBatch("user-a")
	batchB := makeBatch("user-b")

	gotA, err := intStore.GetBatchByIdempotencyKey(ctx, "user-a", key)
	if err != nil || gotA.ID != batchA.ID {
		t.Fatalf("user-a batch: %v", err)
	}
	gotB, err := intStore.GetBatchByIdempotencyKey(ctx, "user-b", key)
	if err != nil || gotB.ID != batchB.ID {
		t.Fatalf("user-b batch: %v", err)
	}
}

func TestGetTemplateByID(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	tmpl, err := intStore.GetTemplateByID(ctx, uuid.MustParse("00000000-0000-0000-0000-000000000001"))
	if err != nil {
		t.Fatal(err)
	}
	if tmpl.Channel != domain.ChannelSMS {
		t.Fatalf("channel %s", tmpl.Channel)
	}
}

func TestPromoteDueAndSweepRetries(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// Scheduled delivery
	reqID := uuid.New()
	delID := uuid.New()
	past := now.Add(-time.Minute)
	req := domain.Request{
		ID: reqID, UserID: "test-user", Recipient: "a@b.com", Channel: domain.ChannelEmail,
		Priority: domain.PriorityNormal, RenderedContent: "x",
		Status: domain.RequestPending, CreatedAt: now,
	}
	del := domain.Delivery{
		ID: delID, RequestID: reqID, Channel: domain.ChannelEmail,
		Priority: domain.PriorityNormal, Status: domain.DeliveryScheduled,
		SendAt: &past, CreatedAt: now, UpdatedAt: now,
	}
	if err := intStore.CreateNotification(ctx, req, del, nil); err != nil {
		t.Fatal(err)
	}

	n, err := intStore.PromoteDue(ctx, 10)
	if err != nil || n != 1 {
		t.Fatalf("promote: n=%d err=%v", n, err)
	}

	// Retry sweep
	req2, del2, outbox2 := sampleRequestDelivery(now)
	_ = intStore.CreateNotification(ctx, req2, del2, outbox2)
	_, _, _ = intStore.ClaimDelivery(ctx, del2.ID, del2.CreatedAt, now.Add(time.Minute))
	retryAt := now.Add(-time.Second)
	_ = intStore.MarkRetrying(ctx, del2.ID, del2.CreatedAt, 1, retryAt, "err")

	n2, err := intStore.SweepRetries(ctx, 10)
	if err != nil || n2 != 1 {
		t.Fatalf("sweep: n=%d err=%v", n2, err)
	}
}

func TestReapStuck(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	now := time.Now().UTC()

	req, del, outbox := sampleRequestDelivery(now)
	_ = intStore.CreateNotification(ctx, req, del, outbox)
	past := now.Add(-time.Hour)
	_, err := intStore.primary.Exec(ctx,
		`UPDATE notification_deliveries SET status='PROCESSING', locked_until=$3, updated_at=now()
		 WHERE id=$1 AND created_at=$2`,
		del.ID, del.CreatedAt, past)
	if err != nil {
		t.Fatal(err)
	}

	n, err := intStore.ReapStuck(ctx)
	if err != nil || n != 1 {
		t.Fatalf("reap: n=%d err=%v", n, err)
	}
}

func TestIsUniqueViolationHelper(t *testing.T) {
	if isUniqueViolation(nil) {
		t.Fatal("nil should be false")
	}
}

func TestGetNotFound(t *testing.T) {
	resetDB(t)
	_, err := intStore.GetRequestByID(context.Background(), uuid.New())
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("got %v", err)
	}
}

func TestReconcilePending(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	now := time.Now().UTC()
	req, del, outbox := sampleRequestDelivery(now)
	_ = intStore.CreateNotification(ctx, req, del, outbox)
	_ = intStore.MarkOutboxPublished(ctx, []uuid.UUID{outbox.ID})

	old := now.Add(-2 * time.Hour)
	_, err := intStore.primary.Exec(ctx,
		`UPDATE notification_deliveries SET updated_at=$3 WHERE id=$1 AND created_at=$2`,
		del.ID, del.CreatedAt, old)
	if err != nil {
		t.Fatal(err)
	}

	n, err := intStore.ReconcilePending(ctx, time.Minute, 10)
	if err != nil || n < 1 {
		t.Fatalf("reconcile n=%d err=%v", n, err)
	}
}

func TestStorePing(t *testing.T) {
	if err := intStore.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestClaimDelivery_AlreadyProcessing(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	now := time.Now().UTC()

	req, del, outbox := sampleRequestDelivery(now)
	_ = intStore.CreateNotification(ctx, req, del, outbox)

	lease := now.Add(time.Minute)
	_, ok, err := intStore.ClaimDelivery(ctx, del.ID, del.CreatedAt, lease)
	if err != nil || !ok {
		t.Fatalf("first claim: ok=%v err=%v", ok, err)
	}

	_, ok2, err := intStore.ClaimDelivery(ctx, del.ID, del.CreatedAt, lease)
	if err != nil {
		t.Fatalf("second claim err: %v", err)
	}
	if ok2 {
		t.Fatal("expected lost race on second claim")
	}
}

func TestCancelRequest_NotFound(t *testing.T) {
	resetDB(t)
	_, err := intStore.CancelRequest(context.Background(), uuid.New())
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("got %v", err)
	}
}

func TestCancelRequest_NotAllowed(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	now := time.Now().UTC()

	req, del, outbox := sampleRequestDelivery(now)
	_ = intStore.CreateNotification(ctx, req, del, outbox)

	lease := now.Add(time.Minute)
	_, ok, _ := intStore.ClaimDelivery(ctx, del.ID, del.CreatedAt, lease)
	if !ok {
		t.Fatal("claim failed")
	}

	_, err := intStore.CancelRequest(ctx, req.ID)
	if !errors.Is(err, domain.ErrCancelNotAllowed) {
		t.Fatalf("got %v", err)
	}
}

func TestCreateBatch_IdempotencyConflict(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	now := time.Now().UTC()
	key := "batch-dup"

	req, del, outbox := sampleRequestDelivery(now)
	batch := domain.Batch{ID: uuid.New(), UserID: req.UserID, IdempotencyKey: &key, CreatedAt: now}
	req.BatchID = &batch.ID
	if _, err := intStore.CreateBatch(ctx, batch, []domain.BatchItem{{Request: req, Delivery: del, Outbox: outbox}}); err != nil {
		t.Fatal(err)
	}

	req2, del2, outbox2 := sampleRequestDelivery(now)
	batch2 := domain.Batch{ID: uuid.New(), UserID: req.UserID, IdempotencyKey: &key, CreatedAt: now}
	req2.BatchID = &batch2.ID
	if _, err := intStore.CreateBatch(ctx, batch2, []domain.BatchItem{{Request: req2, Delivery: del2, Outbox: outbox2}}); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
}

func TestGetTemplateByID_NotFound(t *testing.T) {
	resetDB(t)
	_, err := intStore.GetTemplateByID(context.Background(), uuid.New())
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("got %v", err)
	}
}

func TestFetchUnpublishedOutbox_CorruptPayload(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	now := time.Now().UTC()

	req, del, outbox := sampleRequestDelivery(now)
	_ = intStore.CreateNotification(ctx, req, del, outbox)

	_, err := intStore.primary.Exec(ctx,
		`UPDATE outbox SET payload = '[]'::jsonb WHERE delivery_id = $1`, del.ID)
	if err != nil {
		t.Fatal(err)
	}

	_, err = intStore.FetchUnpublishedOutbox(ctx, 10)
	if err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestPurgePublishedOutbox(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	now := time.Now().UTC()
	req, del, outbox := sampleRequestDelivery(now)
	_ = intStore.CreateNotification(ctx, req, del, outbox)
	entries, _ := intStore.FetchUnpublishedOutbox(ctx, 1)
	_ = intStore.MarkOutboxPublished(ctx, []uuid.UUID{entries[0].ID})

	n, err := intStore.PurgePublishedOutbox(ctx, now.Add(time.Hour))
	if err != nil || n < 1 {
		t.Fatalf("purged %d err=%v", n, err)
	}
}
