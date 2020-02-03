GOARCH := amd64
GOOS := linux

all: deps build
build:  ## Build a version
		GOOS=${GOOS} GOARCH=${GOARCH} go build -v -o bin/hc-${GOOS}-${GOARCH} ./...

deps:	## Using module, remove depds here

clean: ## Remove temporary files
		go clean
