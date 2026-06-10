# Notification System

Go implementation of an event-driven notification platform (take-home assessment).

The API accepts notification requests and persists them in PostgreSQL. Workers
consume Redis Streams and deliver messages through an external webhook provider, with
retries, idempotency, rate limiting, circuit breaking, scheduling, and observability.

**Architecture:** ports-and-adapters (hexagonal). Domain and application services
depend on interfaces; PostgreSQL, Redis, HTTP, and the provider are wired in `cmd/*`
and `internal/adapters/*`.

---

## Contents

- [Architecture](#architecture)
- [Why it is event-driven](#why-it-is-event-driven)
- [Project structure](#project-structure)
- [Quick start](#quick-start)
- [Authentication](#authentication)
- [API](#api)
- [How the core requirements are met](#how-the-core-requirements-are-met)
- [Database scalability & retention](#database-scalability--retention)
- [Observability](#observability)
- [Configuration](#configuration)
- [Testing](#testing)
- [Design tradeoffs](#design-tradeoffs)
- [Future improvements](#future-improvements)

---

## Architecture

```
                 ┌──────────────┐     POST /notifications
   client ─────▶ │   API (HTTP) │ ◀───────────────────────
                 └──────┬───────┘
                        │ write request + delivery + outbox row
                        ▼  (single transaction)
                 ┌──────────────┐
                 │  PostgreSQL  │  source of truth (partitioned deliveries)
                 └──────┬───────┘
                        │ outbox rows
                        ▼
                 ┌──────────────┐  relay publishes to streams
                 │  Scheduler   │──────────────┐
                 │  + Outbox    │              │ promote SCHEDULED→PENDING,
                 │  + Reaper    │              │ sweep RETRYING, reap stuck,
                 └──────────────┘              │ reconcile, partition maint.
                                               ▼
                 ┌─────────────────────────────────────────┐
                 │      Redis Streams (9 streams)            │
                 │  notifications.{high|normal|low}.{sms|email|push}
                 └──────┬────────────────────────────────────┘
                        │ XREADGROUP (priority order)
                        ▼
                 ┌──────────────┐  claim → rate limit → breaker → provider
                 │   Workers    │──────────────▶  external provider (webhook.site)
                 └──────────────┘                 (Idempotency-Key header)
```

Three independently deployable binaries share the same domain and adapters:

| Binary           | Responsibility                                                            |
| ---------------- | ------------------------------------------------------------------------- |
| `cmd/api`        | HTTP API: create/batch/get/list/cancel, health, metrics, Swagger UI       |
| `cmd/worker`     | Consume streams and run the send pipeline; horizontally scalable          |
| `cmd/scheduler`  | Outbox relay, due/retry promotion, stuck-lease reaper, reconciliation, partition/retention maintenance |

### The transactional outbox

The API never writes to Redis directly. It commits the `notification_requests`
row, the `notification_deliveries` row, **and** an `outbox` row in one PostgreSQL
transaction. The scheduler's relay then publishes outbox rows to Redis and marks
them published. This removes the dual-write problem (DB committed but enqueue lost,
or vice versa) and gives **at-least-once** enqueue semantics. The database — not
Redis — is the source of truth.

## Why it is event-driven

- **Decoupled producers/consumers.** The API only records intent and emits an
  event (an outbox row → a stream message). It never calls a provider inline.
- **Reactive workers.** Workers block on `XREADGROUP` and react to events as they
  arrive. Throughput scales by adding worker replicas (consumer-group members);
  Redis distributes stream entries among them.
- **Priority topology.** Nine streams (3 priorities × 3 channels) let workers read
  high-priority streams first and let operators dedicate worker pools to specific
  priorities via `WORKER_PRIORITIES`.
- **State changes are events too.** Retries, scheduled sends, and crash recovery
  all flow back through the same enqueue-via-outbox path, so there is exactly one
  way work enters a stream.

## Project structure

```
cmd/
  api/         worker/         scheduler/      # composition roots (main)
internal/
  domain/                      # entities, enums, DTOs, errors — no dependencies
  notification/                # application service (create/batch/get/list/cancel)
  delivery/                    # retry policy (pure logic)
  template/                    # template render + validation
  worker/                      # stream consumer + send pipeline
  scheduler/                   # background jobs + outbox relay
  transport/http/              # inbound HTTP adapter (handlers, router, OpenAPI)
  adapters/
    postgres/                  # repositories (pgx)
    redis/                     # streams queue + rate limiter
    provider/                  # webhook client + circuit-breaker decorator
  platform/                    # config, logging, clock, metrics (cross-cutting)
  bootstrap/                   # shared dependency construction
migrations/                    # golang-migrate SQL
deploy/                        # prometheus config
```

Dependencies always point **inward**: adapters import `domain`; services import
`domain` and declare the ports they need; `main` wires concrete adapters into
services. Adapters never import services.

## Quick start

**Prerequisites:** Docker, Docker Compose.

```bash
cp .env.example .env
# Set JWT_SECRET (≥32 chars) and PROVIDER_WEBHOOK_URL (e.g. a webhook.site inbox URL).

make up
make gentoken user_id=demo-user
export TOKEN=PASTE_TOKEN_HERE
make logs
```

| Service     | URL |
| ----------- | --- |
| API         | http://localhost:8080 |
| Swagger UI  | http://localhost:8080/docs |
| Metrics     | API `:8080/metrics`, worker `:9090`, scheduler `:9091` |
| Prometheus  | http://localhost:9092 |

Bulk load for demos: `./scripts/feed-notifications.sh -t "$TOKEN"`.

### API examples

Set `TOKEN` first. Examples use single-line JSON (`--data-raw`) so they paste cleanly
into zsh/bash.

Create with free-form `content`:

```bash
curl -i -X POST http://localhost:8080/api/v1/notifications \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-001" \
  --data-raw '{"recipient":"+15551234567","channel":"sms","priority":"high","content":"Your verification code is 123456."}'
```

Create with a seeded template (see [Templates](#templates)):

```bash
curl -X POST http://localhost:8080/api/v1/notifications \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  --data-raw '{"recipient":"+15551234567","channel":"sms","priority":"high","template_id":"00000000-0000-0000-0000-000000000001","variables":{"code":"123456","expire_minutes":"5"}}'
```

Schedule a future send:

```bash
curl -X POST http://localhost:8080/api/v1/notifications \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  --data-raw '{"recipient":"user@example.com","channel":"email","priority":"normal","content":"Welcome aboard!","send_at":"2030-01-01T00:00:00Z"}'
```

Get, list, cancel (`NOTIFICATION_ID` from the create response):

```bash
curl -H "Authorization: Bearer $TOKEN" "http://localhost:8080/api/v1/notifications/NOTIFICATION_ID"

curl -G -H "Authorization: Bearer $TOKEN" http://localhost:8080/api/v1/notifications \
  --data-urlencode "status=SENT" \
  --data-urlencode "channel=sms" \
  --data-urlencode "limit=20"

curl -G -H "Authorization: Bearer $TOKEN" http://localhost:8080/api/v1/notifications \
  --data-urlencode "from=2026-06-01T00:00:00Z" \
  --data-urlencode "to=2026-06-10T23:59:59Z" \
  --data-urlencode "limit=50"

curl -X POST -H "Authorization: Bearer $TOKEN" "http://localhost:8080/api/v1/notifications/NOTIFICATION_ID/cancel"
```

## Authentication

All `/api/v1/*` endpoints require a valid JWT in the `Authorization` header:

```http
Authorization: Bearer <token>
```

- Tokens are **HS256** JWTs signed with `JWT_SECRET` (minimum 32 characters).
- The `user_id` claim identifies the caller; it is **not** supplied in the request body.
- Every notification and batch is owned by that `user_id`. List, get, cancel, and batch
  queries are scoped to the authenticated user only.
- Accessing another user's resource returns **404 Not Found** (not 403), so resource
  existence is not leaked across tenants.
- Operational endpoints (`/healthz`, `/readyz`, `/metrics`, `/docs`, `/openapi.yaml`)
  remain public.

Demo tokens (must use the same `JWT_SECRET` as the API):

```bash
make gentoken user_id=demo-user
```

## API

Full, interactive documentation is served at `/docs` (Swagger UI) and the raw spec
at `/openapi.yaml`.

| Method & path                              | Description                              |
| ------------------------------------------ | ---------------------------------------- |
| `POST /api/v1/notifications`               | Create one notification                  |
| `POST /api/v1/notifications/batch`         | Create up to 1000 in one transaction     |
| `GET  /api/v1/notifications/{id}`          | Get a notification with its deliveries   |
| `GET  /api/v1/notifications`               | List metadata only — no `content` field (see tradeoffs) |
| `POST /api/v1/notifications/{id}/cancel`   | Cancel non-terminal deliveries           |
| `GET  /api/v1/batches/{id}`                | Batch summary (O(1) denormalized counts) |
| `GET  /healthz` `GET /readyz` `GET /metrics` | Operations                             |

A `202 Accepted` is returned for new work; an idempotent replay returns `200 OK`
with the existing resource.

## How the core requirements are met

### Idempotency (multi-layer)

1. **API layer** — an optional `Idempotency-Key` (header or body) is stored with a
   unique constraint. A replay returns the original notification (`200`) instead of
   creating a duplicate. Batches support a batch-level key plus per-item keys
   (`ON CONFLICT DO NOTHING`). A lost race (concurrent inserts) is caught via the
   unique violation and resolved to the existing row.
2. **Worker layer** — every delivery is claimed with a status-guarded conditional
   `UPDATE` (`PENDING/RETRYING → PROCESSING`). Only one worker can win the claim, so
   a duplicate stream message (Redis is at-least-once) is dropped harmlessly.
3. **Provider layer** — the delivery ID is sent as the provider's `Idempotency-Key`
   header, so an idempotency-aware provider deduplicates re-sends. This turns the
   pipeline's at-least-once delivery into **effectively exactly-once**.

### Delivery & retry

The retry policy (`internal/delivery`) is pure, deterministic logic:

- **Error classification** — provider errors are typed (`domain.ProviderError`).
  `4xx` (except `429`) are **permanent** and fail immediately; `5xx`, timeouts, and
  network errors are **transient** and retried.
- **Exponential backoff with jitter** — a configurable schedule
  (`RETRY_BACKOFF`) with `±RETRY_JITTER_RATIO` randomization to avoid retry storms.
- **`Retry-After` honored** — a `429`/`503` `Retry-After` takes precedence over the
  computed backoff.
- **Bounded attempts** — after `MAX_ATTEMPTS` the delivery becomes `FAILED`.
- **Lease > timeout invariant** — `WORKER_LEASE_TTL` must exceed `PROVIDER_TIMEOUT`
  (enforced at startup), so the stuck-processing reaper can never reclaim a delivery
  that is still in flight and cause a double-send.

### Priority handling

Work is routed to one of nine streams by `(priority, channel)`. Workers read streams
in priority order. `WORKER_PRIORITIES=high,normal,low` allows dedicated worker pools
(e.g. a deployment that only drains `high`).

### Rate limiting

Per-channel limiting is centralized in Redis using a GCRA Lua script
(`redis_rate`), so the limit is enforced **fleet-wide** regardless of worker count
(default 100 msg/s/channel). When the budget is exhausted the worker briefly waits
and re-checks; if it still cannot proceed it requeues the delivery **without
consuming a retry attempt**. An in-process limiter (`RATE_LIMITER=inproc`) is
available as a single-replica fallback.

### Circuit breaker

Each channel gets its own circuit breaker (`gobreaker`) wrapping the provider, so a
single failing channel cannot exhaust resources or burn attempts for others. The
trip policy is configurable at deploy time:

- `CB_FAILURE_RATE_THRESHOLD` (default `0.5`), `CB_MIN_REQUESTS` (default `20`),
  `CB_WINDOW`, `CB_OPEN_TIMEOUT`, `CB_HALF_OPEN_MAX`.

Permanent (`4xx`) errors are excluded from the breaker's accounting (they reflect
bad input, not provider health). When the breaker is open, calls are short-circuited
and the delivery is requeued **without** burning a retry attempt — nothing was
actually attempted.

### External provider integration

The assessment specifies a webhook provider that returns **HTTP 202** with a JSON
body `{messageId, status, timestamp}`. The adapter (`internal/adapters/provider`)
**intentionally relaxes this** for local development with [webhook.site](https://webhook.site):

- **Any 2xx** status is treated as success (webhook.site often returns `200`, not `202`).
- **Missing or non-JSON bodies** are tolerated; `status` defaults to `"accepted"` and
  `messageId` may be empty.

This keeps the compose demo reliable without a custom mock server. A production
deployment would require `202`, validate the response schema, and map parse failures
to the retry policy. Outbound format: `POST` with `{to, channel, content}` and an
`Idempotency-Key` header (per assessment spec).

### Scheduled notifications

`send_at` in the future parks the delivery as `SCHEDULED` (no outbox row). The
scheduler promotes due rows to `PENDING` and enqueues them transactionally.

### Templates

`template_id` is **optional**. Each request must include either `content` (free-form
text) or a `template_id` with `variables` — not both.

Templates carry `{{variable}}` placeholders and a declared required-variable set.
Content is rendered **once at creation time** and stored immutably, so later
template edits never change already-queued work.

Migration `000005_seed` inserts a sample SMS template (`verification_code`, id
`00000000-0000-0000-0000-000000000001`) for template-render examples.

## Database scalability & retention

`notification_deliveries` is the hottest table (one row per delivery, updated on
every state transition). It is engineered for write/update churn:

- **Range partitioning by `created_at`** (daily). The active working set stays in
  the newest partitions; old data is isolated.
- **Two-stage retention** — terminal rows from an aged partition are copied to a
  cold `notification_deliveries_archive` tier, then the whole partition is
  **dropped** (`O(1)` reclaim, no mass `DELETE`/vacuum churn). A `DEFAULT` partition
  guarantees inserts never fail even if a dated partition is missing.
- **HOT-update tuning** — `fillfactor=80` leaves free space on each page so status
  updates can be written in-place (Heap-Only Tuples) without index churn; per-table
  **aggressive autovacuum** keeps bloat down under high update rates.
- **Minimal partial indexes** — indexes are scoped to non-terminal statuses only
  (`WHERE status='RETRYING'`, etc.), so terminal rows are never indexed and write
  amplification stays low.
- **Denormalized batch counters** — batch status is served in `O(1)` from counters
  on `notification_batches` instead of scanning deliveries.
- **Read/write split ready** — the store accepts an optional replica DSN for
  read-only list/status queries.

Partition creation, archival, retention, and outbox purging are driven by the
scheduler (`internal/scheduler`); the same SQL functions can alternatively be
scheduled with `pg_cron`/`pg_partman` in a managed environment.

## Observability

- **Prometheus metrics** at `/metrics` on every binary: created/sent/failed/retry
  counters (labeled by channel+priority), provider latency and processing-duration
  histograms, queue depth and PEL pending depth, outbox backlog, scheduler lag,
  DLQ/poison counters, and per-channel circuit state. Prometheus + Alertmanager
  configs are in `deploy/` (`prometheus.yml`, `alerts.yml`, `alertmanager.yml`).
- **Structured logging** (`slog`, JSON) with a per-request **correlation ID**
  (`X-Correlation-ID`) stored on outbox/stream messages and attached to worker logs.
- **OpenTelemetry tracing** (optional, off by default): set `OTEL_ENABLED=true` and
  `OTEL_EXPORTER_OTLP_ENDPOINT` (e.g. `http://localhost:4318`) to export spans for
  API create, outbox relay, worker processing, and provider calls.
- **Dead-letter stream** — terminal failures and poison-pill stream entries are
  published to `notifications.dlq` with reason metadata (`dlq_messages_total`).
- **Health probes** — `/healthz` (liveness) and `/readyz` (Postgres + Redis on API,
  worker, and scheduler metrics servers).

## Configuration

All configuration is via environment variables (see `internal/platform/config`).
Highlights (all have sensible defaults for `docker compose`):

| Variable                     | Default                | Purpose                              |
| ---------------------------- | ---------------------- | ------------------------------------ |
| `JWT_SECRET`                 | (required for API)     | HMAC secret for JWT sign/validate; ≥32 chars |
| `POSTGRES_DSN`               | local compose DSN      | Primary database                     |
| `POSTGRES_REPLICA_DSN`       | (empty)                | Optional read replica                |
| `REDIS_ADDR`                 | `localhost:6379`       | Redis                                |
| `WORKER_PRIORITIES`          | `high,normal,low`      | Which streams this worker drains      |
| `WORKER_CONCURRENCY`         | `8`                    | In-process send concurrency          |
| `WORKER_LEASE_TTL`           | `2m`                   | Processing lease (must > timeout)    |
| `RATE_LIMITER`               | `redis`                | `redis` or `inproc`                  |
| `RATE_LIMIT_PER_SECOND`      | `100`                  | Per-channel limit                    |
| `MAX_ATTEMPTS`               | `5`                    | Retry ceiling                        |
| `RETRY_BACKOFF`              | `1m,5m,15m,30m,60m`    | Backoff schedule                     |
| `CB_FAILURE_RATE_THRESHOLD`  | `0.5`                  | Circuit-breaker trip ratio           |
| `PROVIDER_WEBHOOK_URL`       | (empty)                | External provider endpoint           |
| `OTEL_ENABLED`               | `false`                | Enable OpenTelemetry trace export    |
| `OTEL_EXPORTER_OTLP_ENDPOINT`| `http://localhost:4318` | OTLP HTTP collector endpoint      |
| `OTEL_SERVICE_NAME`          | (per binary)           | Trace service name (`api`, etc.)     |

## Testing

```bash
make test        # go test -race ./internal/... (Postgres + Redis integration)
make test-e2e    # golden-path: API → outbox → worker → SENT
make cover       # coverage report -> coverage.html
```

Unit tests cover the pure, high-value logic without infrastructure: the retry
policy, template rendering/validation, idempotency replay and race handling,
request-status aggregation, the per-channel circuit breaker, config invariants
(including the lease > timeout rule), and pagination cursor encoding. Postgres
and Redis adapter tests cover failure paths (claim races, cancel errors, Redis
outages, rate-limit deny). `internal/e2e` runs one full pipeline test against
real Postgres and Redis. CI (`.github/workflows/ci.yml`) additionally runs
`go vet`, `gofmt` checks, `golangci-lint`, migrations, and a Docker build.

## Design tradeoffs

- **At-least-once + idempotency, not distributed transactions.** Rather than a 2PC
  across Postgres and Redis, the outbox gives at-least-once enqueue and the
  claim + provider `Idempotency-Key` make delivery effectively exactly-once. Simpler,
  more available, and the standard industry pattern — at the cost of occasionally
  re-publishing a message that the claim then discards.
- **Database as source of truth; Redis as transport.** Retries and recovery are
  driven from Postgres (reaper + sweep re-enqueue), so a Redis flush loses no work.
  The tradeoff is more DB load and a small polling latency on retries.
- **Scheduler-driven maintenance over `pg_cron`.** Keeping partition/retention logic
  in the Go scheduler makes the stack run anywhere (including managed Postgres
  without extensions) and keeps it testable. The SQL functions remain `pg_cron`-ready
  for teams that prefer DB-native scheduling.
- **Render-on-create (immutable content).** Guarantees what was queued is what is
  sent, at the cost of storing rendered content per request.
- **Content on single-row reads only.** `GET /notifications/{id}`, create, and cancel
  responses include `content`. List pages omit it (SQL does not select
  `rendered_content`) to keep list queries fast on wide rows.
- **Polling streams + DB** instead of a heavier broker (Kafka/RabbitMQ). Redis
  Streams with consumer groups cover priority, fan-out, and at-least-once with far
  less operational weight; a higher-throughput deployment might graduate to Kafka.
- **Caller identity is JWT-authenticated; recipient is caller-supplied.** The API
  validates a bearer JWT and scopes all resources to the token's `user_id`. There is
  no user-directory lookup for delivery targets — the recipient is taken directly from
  the request body. A `RecipientResolver` port could be added without touching the
  pipeline.

## Future improvements

- **`XAUTOCLAIM` for crashed consumers** to reclaim pending-entry-list messages
  directly, complementing the DB reaper and trimming the PEL faster.
- **Admin requeue API** for messages on the `notifications.dlq` stream.
- **Provider abstraction per channel** (real SMS/email/push vendors) selected by
  channel, each with its own adapter and credentials.
- **Outbox relay scale-out** via `SKIP LOCKED` (already in place) across multiple
  relay replicas, and partitioning the outbox table under extreme load.
- **Per-tenant rate limits and quotas**, and priority-aware fair scheduling.
- **Webhook delivery receipts** to reconcile provider-side status asynchronously.
- **`pg_partman`/`pg_cron`** wired in the Postgres image for DB-native partition
  lifecycle in environments that allow extensions.
