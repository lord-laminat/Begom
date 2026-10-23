export GOCACHE ?= /tmp/begom-go-build
export GOMODCACHE ?= /tmp/begom-go-mod
GO ?= go
BINS := coordinator agent begomctl mockdonor node-helper

.PHONY: build test vet check integration init up down acceptance fmt-check
build:
	@mkdir -p bin
	@for app in $(BINS); do CGO_ENABLED=0 $(GO) build -trimpath -o bin/$$app ./cmd/$$app || exit; done
test:
	$(GO) test -race ./...
vet:
	$(GO) vet ./...
fmt-check:
	@test -z "$$(gofmt -l $$(rg --files -g '*.go' cmd internal tests tools))"
check: fmt-check vet test build
integration:
	$(GO) test -race -tags integration ./tests/integration/... -count=1 -timeout=10m
init:
	$(GO) run ./cmd/begomctl init --dir .local
up:
	docker compose up -d --build
down:
	docker compose down
acceptance:
	$(GO) run ./cmd/begomctl acceptance --config .local/client.json --output artifacts/acceptance
