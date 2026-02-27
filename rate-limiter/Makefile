.PHONY: all build run test bench proto proto-install docker clean lint help

MODULE   := rate-limiter
BINARY   := rate-limiter
PROTO_DIR := proto
PB_DIR   := pb

## Default target
all: proto build

## Build the server binary
build:
	@echo ">>> Building $(BINARY)..."
	go build -ldflags="-s -w" -o bin/$(BINARY) ./cmd/server

## Run the server
run: build
	./bin/$(BINARY)

## Run all tests with race detector
test:
	go test -v -race -count=1 ./...

## Run benchmarks
bench:
	@echo ">>> Running benchmarks..."
	go test -bench=. -benchmem -benchtime=3s ./pkg/limiter/

## Generate protobuf code
proto:
	@echo ">>> Generating protobuf code..."
	@mkdir -p $(PB_DIR)
	protoc -I $(PROTO_DIR) \
		--go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		$(PROTO_DIR)/ratelimit.proto
	@echo ">>> Proto generation complete"

## Install protoc Go plugins
proto-install:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

## Build Docker image
docker:
	docker build -t $(BINARY):latest .

## Clean build artifacts
clean:
	rm -rf bin/
	rm -f $(PB_DIR)/*.pb.go

## Run golangci-lint
lint:
	golangci-lint run ./...

## Benchmark HTTP endpoints (requires 'hey': go install github.com/rakyll/hey@latest)
bench-http:
	@echo ">>> HTTP Benchmark - Token Bucket"
	hey -z 10s -c 100 -m POST \
		-H "Content-Type: application/json" \
		-d '{"key":"bench","tokens":1,"algorithm":"token_bucket"}' \
		http://localhost:8080/api/v1/allow
	@echo ""
	@echo ">>> HTTP Benchmark - Leaky Bucket"
	hey -z 10s -c 100 -m POST \
		-H "Content-Type: application/json" \
		-d '{"key":"bench","tokens":1,"algorithm":"leaky_bucket"}' \
		http://localhost:8080/api/v1/allow
	@echo ""
	@echo ">>> HTTP Benchmark - Sliding Window"
	hey -z 10s -c 100 -m POST \
		-H "Content-Type: application/json" \
		-d '{"key":"bench","tokens":1,"algorithm":"sliding_window"}' \
		http://localhost:8080/api/v1/allow

## Show help
help:
	@echo "Available targets:"
	@echo "  all           - Generate proto and build (default)"
	@echo "  build         - Build the server binary"
	@echo "  run           - Build and run the server"
	@echo "  test          - Run all tests with race detector"
	@echo "  bench         - Run Go benchmarks"
	@echo "  bench-http    - Run HTTP benchmarks (requires hey)"
	@echo "  proto         - Generate protobuf Go code"
	@echo "  proto-install - Install protoc Go plugins"
	@echo "  docker        - Build Docker image"
	@echo "  clean         - Remove build artifacts"
	@echo "  lint          - Run golangci-lint"
	@echo "  help          - Show this help"
