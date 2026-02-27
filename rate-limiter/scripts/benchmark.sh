#!/usr/bin/env bash
#
# Benchmark script for the rate limiter HTTP API.
# Requires: hey (go install github.com/rakyll/hey@latest)
#
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
DURATION="${DURATION:-10s}"
CONCURRENCY="${CONCURRENCY:-100}"

command -v hey >/dev/null 2>&1 || {
    echo "Error: 'hey' is not installed."
    echo "Install it with: go install github.com/rakyll/hey@latest"
    exit 1
}

echo "============================================"
echo " Rate Limiter HTTP Benchmark"
echo "============================================"
echo " URL:         ${BASE_URL}"
echo " Duration:    ${DURATION}"
echo " Concurrency: ${CONCURRENCY}"
echo "============================================"
echo ""

for algo in token_bucket leaky_bucket sliding_window; do
    echo "--------------------------------------------"
    echo " Algorithm: ${algo}"
    echo "--------------------------------------------"
    hey -z "${DURATION}" -c "${CONCURRENCY}" \
        -m POST \
        -H "Content-Type: application/json" \
        -d "{\"key\":\"bench-key\",\"tokens\":1,\"algorithm\":\"${algo}\"}" \
        "${BASE_URL}/api/v1/allow"
    echo ""
done

echo "============================================"
echo " gRPC Benchmark (requires grpcurl)"
echo "============================================"

if command -v grpcurl >/dev/null 2>&1; then
    GRPC_ADDR="${GRPC_ADDR:-localhost:9090}"
    echo " gRPC Address: ${GRPC_ADDR}"
    echo ""
    for algo in token_bucket leaky_bucket sliding_window; do
        echo "--- Algorithm: ${algo} ---"
        echo "Sending 1000 sequential gRPC requests..."
        start=$(date +%s%N)
        for i in $(seq 1 1000); do
            grpcurl -plaintext -d "{\"key\":\"bench-key\",\"tokens\":1,\"algorithm\":\"${algo}\"}" \
                "${GRPC_ADDR}" ratelimit.v1.RateLimitService/Allow > /dev/null 2>&1
        done
        end=$(date +%s%N)
        elapsed=$(( (end - start) / 1000000 ))
        echo "  1000 requests in ${elapsed}ms ($(( 1000000 / elapsed )) req/s)"
        echo ""
    done
else
    echo "Skipping gRPC benchmark (grpcurl not installed)"
    echo "Install it with: brew install grpcurl"
fi

echo "============================================"
echo " Benchmark Complete!"
echo "============================================"
