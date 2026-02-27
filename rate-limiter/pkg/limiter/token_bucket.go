package limiter

import (
	"math"
	"sync"
	"time"
)

// TokenBucket implements the token bucket rate limiting algorithm.
//
// Tokens are added at a constant rate and consumed by requests.
// Requests are allowed as long as tokens are available, enabling burst traffic
// up to the bucket capacity while maintaining a long-term average rate.
type TokenBucket struct {
	mu        sync.Mutex
	tokens    float64
	maxTokens float64
	rate      float64   // tokens added per second
	lastTime  time.Time // last time tokens were refilled
}

// NewTokenBucket creates a new token bucket limiter.
//   - rate: tokens added per second
//   - capacity: maximum number of tokens (burst size)
func NewTokenBucket(rate float64, capacity int) *TokenBucket {
	return &TokenBucket{
		tokens:    float64(capacity),
		maxTokens: float64(capacity),
		rate:      rate,
		lastTime:  time.Now(),
	}
}

func (tb *TokenBucket) Allow() Result {
	return tb.AllowN(1)
}

func (tb *TokenBucket) AllowN(n int) Result {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastTime).Seconds()

	// Refill tokens based on elapsed time
	tb.tokens = math.Min(tb.maxTokens, tb.tokens+elapsed*tb.rate)
	tb.lastTime = now

	requested := float64(n)
	if tb.tokens >= requested {
		tb.tokens -= requested
		return Result{
			Allowed:   true,
			Remaining: int(tb.tokens),
		}
	}

	// Calculate how long until enough tokens are available
	deficit := requested - tb.tokens
	waitSeconds := deficit / tb.rate
	return Result{
		Allowed:    false,
		Remaining:  int(tb.tokens),
		RetryAfter: time.Duration(waitSeconds * float64(time.Second)),
	}
}
