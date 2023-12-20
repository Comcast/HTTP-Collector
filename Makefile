GOARCH ?= amd64
GOOS ?= linux
GOHOSTARCH = $(shell go env GOHOSTARCH)
GOHOSTOS = $(shell go env GOHOSTOS)

all: unittest build

.PHONY: build
build:  ## Build a version
		GOOS=${GOOS} GOARCH=${GOARCH} go build -v -o bin/hc-${GOOS}-${GOARCH} \
		  -ldflags="-X main.AppVersion=${Version} -X main.BuildTime=`date -u +%Y%m%d.%H%M%S`" ./cmd/hc

.PHONY: local
local: GOARCH = $(GOHOSTARCH)
local: GOOS = $(GOHOSTOS)
local: Version = local
local: build

.PHONY: release
release:
	GOOS=${GOOS} GOARCH=${GOARCH} go build -v -o bin/hc-${GOOS}-${GOARCH} \
		  -ldflags="-X main.AppVersion=${Version} -X main.BuildTime=`date -u +%Y%m%d.%H%M%S`" ./cmd/hc

.PHONY: perftest
perftest:
	go build -v -o bin/perftest ./cmd/perftest

.PHONY: unittest
unittest:
	GOOS=${GOHOSTOS} GOARCH=${GOHOSTARCH} go test ./...

.PHONY: buildsmoketest
buildsmoketest:
	GOOS=${GOOS} GOARCH=${GOARCH} go build -v -o bin/smoketest-${GOOS}-${GOARCH} ./test

.PHONY: clean
clean: ## Remove temporary files
	go clean
	go clean --testcache
