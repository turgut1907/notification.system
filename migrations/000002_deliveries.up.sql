-- notification_deliveries is the operational source of truth and the hottest table.
-- It is RANGE-partitioned by created_at so the active (non-terminal) working set
-- stays in the newest partitions, retention is an O(1) partition drop, and the
-- hot partial indexes stay small.

CREATE TABLE notification_deliveries (
    id                  UUID NOT NULL,
    request_id          UUID NOT NULL,
    channel             VARCHAR(16) NOT NULL,
    priority            VARCHAR(16) NOT NULL,
    status              VARCHAR(16) NOT NULL,
    attempt_count       INTEGER NOT NULL DEFAULT 0,
    provider_message_id VARCHAR(255),
    last_error          TEXT,
    next_retry_at       TIMESTAMPTZ,
    locked_until        TIMESTAMPTZ,
    send_at             TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

-- Partial indexes scoped to non-terminal statuses: terminal rows are never indexed,
-- which keeps write amplification low. Created on the parent so every current and
-- future partition inherits them.
CREATE INDEX idx_deliveries_scheduled  ON notification_deliveries (send_at)      WHERE status = 'SCHEDULED';
CREATE INDEX idx_deliveries_retrying   ON notification_deliveries (next_retry_at) WHERE status = 'RETRYING';
CREATE INDEX idx_deliveries_processing ON notification_deliveries (locked_until) WHERE status = 'PROCESSING';
CREATE INDEX idx_deliveries_pending    ON notification_deliveries (updated_at)   WHERE status = 'PENDING';
CREATE INDEX idx_deliveries_request    ON notification_deliveries (request_id);

-- Default partition is a safety net so inserts never fail even if a dated partition
-- is missing. The maintenance job creates dated daily partitions ahead of time.
CREATE TABLE notification_deliveries_default
    PARTITION OF notification_deliveries DEFAULT;

-- Cold archive tier: terminal rows from aged partitions are copied here before the
-- partition is dropped. Same shape, fewer indexes, cheaper to retain.
CREATE TABLE notification_deliveries_archive (
    id                  UUID NOT NULL,
    request_id          UUID NOT NULL,
    channel             VARCHAR(16) NOT NULL,
    priority            VARCHAR(16) NOT NULL,
    status              VARCHAR(16) NOT NULL,
    attempt_count       INTEGER NOT NULL DEFAULT 0,
    provider_message_id VARCHAR(255),
    last_error          TEXT,
    next_retry_at       TIMESTAMPTZ,
    locked_until        TIMESTAMPTZ,
    send_at             TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL,
    archived_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, created_at)
);
CREATE INDEX idx_deliveries_archive_request ON notification_deliveries_archive (request_id);
