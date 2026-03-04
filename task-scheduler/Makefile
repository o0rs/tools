.PHONY: build scheduler worker client clean test

build: scheduler worker client

scheduler:
	go build -o bin/scheduler ./cmd/scheduler

worker:
	go build -o bin/worker ./cmd/worker

client:
	go build -o bin/client ./cmd/client

clean:
	rm -rf bin/

run-scheduler:
	go run ./cmd/scheduler

run-worker:
	go run ./cmd/worker

run-client:
	go run ./cmd/client

test:
	go test -v -race ./...
