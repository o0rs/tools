# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /rate-limiter ./cmd/server

# Runtime stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /rate-limiter /usr/local/bin/rate-limiter

EXPOSE 8080 9090

# Default environment variables
ENV HTTP_PORT=8080 \
    GRPC_PORT=9090 \
    DEFAULT_ALGORITHM=token_bucket \
    DEFAULT_RATE=100 \
    DEFAULT_CAPACITY=200 \
    DEFAULT_WINDOW=1s

ENTRYPOINT ["rate-limiter"]
