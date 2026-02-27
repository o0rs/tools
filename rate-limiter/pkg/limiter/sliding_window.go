package limiter

import (
	"sync"
	"time"
)

const defaultNumBuckets = 10

// SlidingWindow implements the sliding window counter rate limiting algorithm.
//
// The time window is divided into sub-buckets for granularity. Each bucket tracks
// the number of requests in its time slice. The total count across all non-expired
// buckets determines whether new requests are allowed.
//
// This provides a good balance between memory efficiency and accuracy compared
// to exact sliding window logs.
type SlidingWindow struct {
	mu          sync.Mutex
	windowSize  time.Duration
	maxRequests int
	counters    []int64
	timestamps  []int64 // bucket start time in nanoseconds
	numBuckets  int
	bucketSize  time.Duration
}

// NewSlidingWindow creates a new sliding window counter limiter.
//   - maxRequests: maximum requests allowed within the window
//   - windowSize: the duration of the sliding window
func NewSlidingWindow(maxRequests int, windowSize time.Duration) *SlidingWindow {
	if windowSize <= 0 {
		windowSize = time.Second
	}
	numBuckets := defaultNumBuckets
	bucketSize := windowSize / time.Duration(numBuckets)
	if bucketSize <= 0 {
		bucketSize = time.Millisecond
		numBuckets = int(windowSize / bucketSize)
		if numBuckets <= 0 {
			numBuckets = 1
		}
	}

	return &SlidingWindow{
		windowSize:  windowSize,
		maxRequests: maxRequests,
		counters:    make([]int64, numBuckets),
		timestamps:  make([]int64, numBuckets),
		numBuckets:  numBuckets,
		bucketSize:  bucketSize,
	}
}

func (sw *SlidingWindow) Allow() Result {
	return sw.AllowN(1)
}

func (sw *SlidingWindow) AllowN(n int) Result {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now().UnixNano()

	// Determine current bucket
	bucketIdx := int((now / int64(sw.bucketSize)) % int64(sw.numBuckets))
	bucketStart := (now / int64(sw.bucketSize)) * int64(sw.bucketSize)

	// Reset current bucket if it belongs to a previous period
	if sw.timestamps[bucketIdx] != bucketStart {
		sw.counters[bucketIdx] = 0
		sw.timestamps[bucketIdx] = bucketStart
	}

	// Sum all non-expired bucket counts
	windowStart := now - int64(sw.windowSize)
	var total int64
	for i := 0; i < sw.numBuckets; i++ {
		if sw.timestamps[i] > windowStart {
			total += sw.counters[i]
		}
	}

	if total+int64(n) <= int64(sw.maxRequests) {
		sw.counters[bucketIdx] += int64(n)
		return Result{
			Allowed:   true,
			Remaining: int(int64(sw.maxRequests) - total - int64(n)),
		}
	}

	return Result{
		Allowed:    false,
		Remaining:  int(int64(sw.maxRequests) - total),
		RetryAfter: sw.bucketSize, // at minimum, wait for one bucket to expire
	}
}
