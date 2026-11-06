export GOCACHE ?= /tmp/begom-go-build
export GOMODCACHE ?= /tmp/begom-go-mod
GO ?= go
BINS := coordinator agent begomctl mockdonor node-helper

.PHONY: build test vet check integration init up down status logs acceptance fmt-check
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
	bash deploy/local-v2.sh init
up:
	bash deploy/local-v2.sh up
down:
	bash deploy/local-v2.sh down
status:
	bash deploy/local-v2.sh status
logs:
	bash deploy/local-v2.sh logs
acceptance:
	BEGOM_SOURCE=mock bash deploy/local-v2.sh acceptance
