# Multi-stage build producing a single image that contains all three binaries
# (api, worker, scheduler). The compose file selects which one to run per service.
FROM golang:1.26 AS builder

WORKDIR /src

# Cache dependencies first.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build static binaries for all entrypoints.
ARG VERSION=dev
ENV CGO_ENABLED=0 GOOS=linux
RUN go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/api ./cmd/api && \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/worker ./cmd/worker && \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/scheduler ./cmd/scheduler

FROM alpine:3.20

RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
USER app
WORKDIR /app

COPY --from=builder /out/api /app/api
COPY --from=builder /out/worker /app/worker
COPY --from=builder /out/scheduler /app/scheduler

EXPOSE 8080 9090
# Default binary; compose sets entrypoint per service (api / worker / scheduler).
CMD ["/app/api"]
