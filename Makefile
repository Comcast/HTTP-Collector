GOARCH ?= amd64
GOOS ?= linux

all: build
build:  ## Build a version
		GOOS=${GOOS} GOARCH=${GOARCH} go build -v -o bin/hc-${GOOS}-${GOARCH} \
		  -ldflags="-X main.AppVersion=${Version} -X main.BuildTime=`date -u +%Y%m%d.%H%M%S`" ./...

clean: ## Remove temporary files
		go clean
