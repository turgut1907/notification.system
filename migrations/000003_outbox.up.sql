-- Transactional outbox: enqueue intents written in the same transaction as the
-- delivery state change. A relay publishes unpublished rows to Redis Streams, so
-- there is never a direct DB/queue dual-write on the request path.

CREATE TABLE outbox (
    id                  UUID PRIMARY KEY,
    delivery_id         UUID NOT NULL,
    delivery_created_at TIMESTAMPTZ NOT NULL,
    stream              VARCHAR(64) NOT NULL,
    payload             JSONB NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at        TIMESTAMPTZ
);

-- Hot query: "the next unpublished rows in creation order".
CREATE INDEX idx_outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;

-- High update/insert churn: reserve page space for HOT updates and vacuum eagerly.
ALTER TABLE outbox SET (fillfactor = 80, autovacuum_vacuum_scale_factor = 0.05);
