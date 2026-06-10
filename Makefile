.DEFAULT_GOAL := help

DB_DSN ?= postgres://notif:notif@localhost:5432/notifications?sslmode=disable
MIGRATE_IMAGE := migrate/migrate:v4.18.1

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

## --- Local development ---

.PHONY: build
build: ## Build all binaries into ./bin
	go build -o bin/api ./cmd/api
	go build -o bin/worker ./cmd/worker
	go build -o bin/scheduler ./cmd/scheduler
	go build -o bin/gentoken ./cmd/gentoken

.PHONY: gentoken
gentoken: ## Generate a long-lived demo JWT (usage: make gentoken user_id=demo-user)
	@test -n "$(user_id)" || (echo "usage: make gentoken user_id=YOUR_USER_ID" && exit 1)
	@set -a && [ -f .env ] && . ./.env; set +a; \
		go run ./cmd/gentoken $(user_id)

.PHONY: tidy
tidy: ## Sync go.mod/go.sum
	go mod tidy

.PHONY: fmt
fmt: ## Format the code
	gofmt -s -w .

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: test
test: ## Run tests with race detector (postgres integration tests need DB up)
	POSTGRES_DSN=$(DB_DSN) REDIS_ADDR=localhost:6379 go test -race -count=1 ./internal/...

.PHONY: test-e2e
test-e2e: ## Run end-to-end golden-path test (needs Postgres + Redis)
	POSTGRES_DSN=$(DB_DSN) REDIS_ADDR=localhost:6379 go test -race -count=1 ./internal/e2e/...

.PHONY: cover
cover: ## Run tests, enforce 70%% coverage on internal/, write coverage.html
	POSTGRES_DSN=$(DB_DSN) go test -race -covermode=atomic -coverprofile=coverage.out ./internal/...
	@pct=$$(go tool cover -func=coverage.out | awk '/^total:/ {print $$3}' | tr -d '%'); \
		echo "coverage: $${pct}%"; \
		awk -v p="$$pct" 'BEGIN {exit !(p+0 >= 70)}' || (echo "coverage below 70%"; exit 1)
	go tool cover -html=coverage.out -o coverage.html
	@echo "open coverage.html"

## --- Docker stack ---

.PHONY: up
up: ## Build images and start the full stack
	docker compose up --build -d

.PHONY: down
down: ## Stop the stack (keep volumes)
	docker compose down

.PHONY: clean
clean: ## Stop the stack and remove volumes
	docker compose down -v

.PHONY: logs
logs: ## Tail logs from all services
	docker compose logs -f

.PHONY: ps
ps: ## Show running services
	docker compose ps

## --- Migrations ---

.PHONY: migrate-up
migrate-up: ## Apply all migrations
	docker run --rm --network host -v $(PWD)/migrations:/migrations $(MIGRATE_IMAGE) \
		-path=/migrations -database "$(DB_DSN)" up

.PHONY: migrate-down
migrate-down: ## Roll back the last migration
	docker run --rm --network host -v $(PWD)/migrations:/migrations $(MIGRATE_IMAGE) \
		-path=/migrations -database "$(DB_DSN)" down 1

.PHONY: migrate-create
migrate-create: ## Create a new migration: make migrate-create name=foo
	docker run --rm -v $(PWD)/migrations:/migrations $(MIGRATE_IMAGE) \
		create -ext sql -dir /migrations -seq $(name)
