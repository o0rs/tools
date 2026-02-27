package limiter

import "time"

// Algorithm represents the rate limiting algorithm type.
type Algorithm string

const (
	AlgoTokenBucket   Algorithm = "token_bucket"
	AlgoLeakyBucket   Algorithm = "leaky_bucket"
	AlgoSlidingWindow Algorithm = "sliding_window"
)

// ParseAlgorithm converts a string to an Algorithm, defaulting to token bucket.
func ParseAlgorithm(s string) Algorithm {
	switch s {
	case string(AlgoTokenBucket):
		return AlgoTokenBucket
	case string(AlgoLeakyBucket):
		return AlgoLeakyBucket
	case string(AlgoSlidingWindow):
		return AlgoSlidingWindow
	default:
		return AlgoTokenBucket
	}
}

// Result contains the outcome of a rate limit check.
type Result struct {
	Allowed    bool
	Remaining  int
	RetryAfter time.Duration
}

// Limiter defines the interface for all rate limiting algorithms.
type Limiter interface {
	// Allow checks if a single request is allowed.
	Allow() Result
	// AllowN checks if n requests are allowed.
	AllowN(n int) Result
}

// Options holds configuration for creating a limiter.
type Options struct {
	Rate     float64       // requests per second (token bucket, leaky bucket)
	Capacity int           // max burst size / bucket capacity / window max requests
	Window   time.Duration // window size (sliding window only)
}

// New creates a new Limiter based on the specified algorithm and options.
func New(algo Algorithm, opts Options) Limiter {
	switch algo {
	case AlgoTokenBucket:
		return NewTokenBucket(opts.Rate, opts.Capacity)
	case AlgoLeakyBucket:
		return NewLeakyBucket(opts.Rate, opts.Capacity)
	case AlgoSlidingWindow:
		return NewSlidingWindow(opts.Capacity, opts.Window)
	default:
		return NewTokenBucket(opts.Rate, opts.Capacity)
	}
}
