-- Core business tables: batches, templates, and notification requests.

CREATE TABLE notification_batches (
    id              UUID PRIMARY KEY,
    user_id         VARCHAR(255) NOT NULL,
    idempotency_key VARCHAR(255),
    total_count     INTEGER NOT NULL DEFAULT 0,
    pending_count   INTEGER NOT NULL DEFAULT 0,
    sent_count      INTEGER NOT NULL DEFAULT 0,
    failed_count    INTEGER NOT NULL DEFAULT 0,
    cancelled_count INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE templates (
    id                 UUID PRIMARY KEY,
    name               VARCHAR(255) NOT NULL,
    channel            VARCHAR(16)  NOT NULL,
    content            TEXT NOT NULL,
    required_variables JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE notification_requests (
    id               UUID PRIMARY KEY,
    user_id          VARCHAR(255) NOT NULL,
    batch_id         UUID REFERENCES notification_batches(id),
    recipient        VARCHAR(255) NOT NULL,
    channel          VARCHAR(16)  NOT NULL,
    priority         VARCHAR(16)  NOT NULL,
    template_id      UUID REFERENCES templates(id),
    rendered_content TEXT NOT NULL,
    idempotency_key  VARCHAR(255),
    status           VARCHAR(16)  NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Per-user idempotency and list pagination.
CREATE UNIQUE INDEX idx_batches_idempotency ON notification_batches (user_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE UNIQUE INDEX idx_requests_idempotency ON notification_requests (user_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX idx_requests_list    ON notification_requests (user_id, created_at DESC, id DESC);
CREATE INDEX idx_requests_batch   ON notification_requests (batch_id);
CREATE INDEX idx_requests_status  ON notification_requests (status);
CREATE INDEX idx_requests_channel ON notification_requests (channel);
