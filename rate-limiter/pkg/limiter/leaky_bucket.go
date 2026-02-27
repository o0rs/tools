package limiter

import (
	"math"
	"sync"
	"time"
)

// LeakyBucket implements the leaky bucket rate limiting algorithm.
//
// Requests fill the bucket, and the bucket leaks (drains) at a constant rate.
// If the bucket is full, incoming requests are rejected.
// This produces a smooth output rate regardless of input burstiness.
type LeakyBucket struct {
	mu       sync.Mutex
	water    float64   // current water level (pending requests)
	capacity float64   // maximum water level
	rate     float64   // leak rate per second
	lastTime time.Time // last time water was drained
}

// NewLeakyBucket creates a new leaky bucket limiter.
//   - rate: how many requests drain per second
//   - capacity: maximum number of queued requests
func NewLeakyBucket(rate float64, capacity int) *LeakyBucket {
	return &LeakyBucket{
		water:    0,
		capacity: float64(capacity),
		rate:     rate,
		lastTime: time.Now(),
	}
}

func (lb *LeakyBucket) Allow() Result {
	return lb.AllowN(1)
}

func (lb *LeakyBucket) AllowN(n int) Result {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(lb.lastTime).Seconds()

	// Drain water based on elapsed time
	lb.water = math.Max(0, lb.water-elapsed*lb.rate)
	lb.lastTime = now

	requested := float64(n)
	if lb.water+requested <= lb.capacity {
		lb.water += requested
		return Result{
			Allowed:   true,
			Remaining: int(lb.capacity - lb.water),
		}
	}

	// Calculate how long until enough capacity is available
	overflow := lb.water + requested - lb.capacity
	waitSeconds := overflow / lb.rate
	return Result{
		Allowed:    false,
		Remaining:  0,
		RetryAfter: time.Duration(waitSeconds * float64(time.Second)),
	}
}
