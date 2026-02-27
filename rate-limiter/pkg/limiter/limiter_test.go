package limiter

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestTokenBucket_AllowBurst(t *testing.T) {
	tb := NewTokenBucket(10, 10)
	for i := 0; i < 10; i++ {
		result := tb.Allow()
		if !result.Allowed {
			t.Errorf("request %d should be allowed", i+1)
		}
	}
	result := tb.Allow()
	if result.Allowed {
		t.Error("request beyond capacity should be denied")
	}
	if result.RetryAfter <= 0 {
		t.Error("RetryAfter should be positive when denied")
	}
}

func TestTokenBucket_Refill(t *testing.T) {
	tb := NewTokenBucket(100, 10)
	for i := 0; i < 10; i++ {
		tb.Allow()
	}
	time.Sleep(50 * time.Millisecond)
	result := tb.Allow()
	if !result.Allowed {
		t.Error("should be allowed after refill period")
	}
}

func TestTokenBucket_AllowN(t *testing.T) {
	tb := NewTokenBucket(10, 10)
	result := tb.AllowN(5)
	if !result.Allowed {
		t.Error("AllowN(5) should succeed with capacity 10")
	}
	if result.Remaining != 5 {
		t.Errorf("expected 5 remaining, got %d", result.Remaining)
	}
	result = tb.AllowN(6)
	if result.Allowed {
		t.Error("AllowN(6) should fail with only 5 remaining")
	}
}

func TestLeakyBucket_AllowUpToCapacity(t *testing.T) {
	lb := NewLeakyBucket(10, 10)
	for i := 0; i < 10; i++ {
		result := lb.Allow()
		if !result.Allowed {
			t.Errorf("request %d should be allowed", i+1)
		}
	}
	result := lb.Allow()
	if result.Allowed {
		t.Error("request should be denied when bucket is full")
	}
	if result.RetryAfter <= 0 {
		t.Error("RetryAfter should be positive when denied")
	}
}

func TestLeakyBucket_Drain(t *testing.T) {
	lb := NewLeakyBucket(100, 10)
	for i := 0; i < 10; i++ {
		lb.Allow()
	}
	time.Sleep(50 * time.Millisecond)
	result := lb.Allow()
	if !result.Allowed {
		t.Error("should be allowed after drain period")
	}
}

func TestLeakyBucket_AllowN(t *testing.T) {
	lb := NewLeakyBucket(10, 10)
	result := lb.AllowN(7)
	if !result.Allowed {
		t.Error("AllowN(7) should succeed with capacity 10")
	}
	result = lb.AllowN(4)
	if result.Allowed {
		t.Error("AllowN(4) should fail with only 3 capacity left")
	}
}

func TestSlidingWindow_Basic(t *testing.T) {
	sw := NewSlidingWindow(10, time.Second)
	for i := 0; i < 10; i++ {
		result := sw.Allow()
		if !result.Allowed {
			t.Errorf("request %d should be allowed", i+1)
		}
	}
	result := sw.Allow()
	if result.Allowed {
		t.Error("request beyond window limit should be denied")
	}
}

func TestSlidingWindow_WindowExpiry(t *testing.T) {
	sw := NewSlidingWindow(10, 100*time.Millisecond)
	for i := 0; i < 10; i++ {
		sw.Allow()
	}
	time.Sleep(150 * time.Millisecond)
	result := sw.Allow()
	if !result.Allowed {
		t.Error("should be allowed after window expiry")
	}
}

func TestSlidingWindow_AllowN(t *testing.T) {
	sw := NewSlidingWindow(10, time.Second)
	result := sw.AllowN(8)
	if !result.Allowed {
		t.Error("AllowN(8) should succeed with limit 10")
	}
	result = sw.AllowN(3)
	if result.Allowed {
		t.Error("AllowN(3) should fail with only 2 remaining")
	}
}

func TestNew_AllAlgorithms(t *testing.T) {
	opts := Options{Rate: 100, Capacity: 50, Window: time.Second}
	tests := []struct {
		algo Algorithm
		name string
	}{
		{AlgoTokenBucket, "token_bucket"},
		{AlgoLeakyBucket, "leaky_bucket"},
		{AlgoSlidingWindow, "sliding_window"},
		{Algorithm("unknown"), "unknown_defaults_to_token_bucket"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := New(tt.algo, opts)
			if l == nil {
				t.Fatal("New() returned nil")
			}
			result := l.Allow()
			if !result.Allowed {
				t.Error("first request should always be allowed")
			}
		})
	}
}

func TestManager_PerKeyIsolation(t *testing.T) {
	mgr := NewManager(AlgoTokenBucket, Options{Rate: 10, Capacity: 5})
	for i := 0; i < 5; i++ {
		mgr.Allow("key1", 1)
	}
	result := mgr.Allow("key1", 1)
	if result.Allowed {
		t.Error("key1 should be denied after exhausting capacity")
	}
	result = mgr.Allow("key2", 1)
	if !result.Allowed {
		t.Error("key2 should be allowed (separate limiter)")
	}
}

func TestManager_DifferentAlgorithms(t *testing.T) {
	mgr := NewManager(AlgoTokenBucket, Options{Rate: 10, Capacity: 5, Window: time.Second})
	r1 := mgr.AllowWithAlgo("key1", AlgoTokenBucket, 1)
	r2 := mgr.AllowWithAlgo("key1", AlgoLeakyBucket, 1)
	if !r1.Allowed || !r2.Allowed {
		t.Error("first request to each algorithm should be allowed")
	}
}

func TestManager_Reset(t *testing.T) {
	mgr := NewManager(AlgoTokenBucket, Options{Rate: 10, Capacity: 2})
	mgr.Allow("key1", 2)
	result := mgr.Allow("key1", 1)
	if result.Allowed {
		t.Error("should be denied after exhausting capacity")
	}
	mgr.Reset("key1", AlgoTokenBucket)
	result = mgr.Allow("key1", 1)
	if !result.Allowed {
		t.Error("should be allowed after reset")
	}
}

func TestConcurrency_AllAlgorithms(t *testing.T) {
	algos := []struct {
		name string
		l    Limiter
	}{
		{"TokenBucket", NewTokenBucket(1e6, 1e6)},
		{"LeakyBucket", NewLeakyBucket(1e6, 1e6)},
		{"SlidingWindow", NewSlidingWindow(1e6, time.Second)},
	}
	for _, algo := range algos {
		algo := algo
		t.Run(algo.name, func(t *testing.T) {
			var wg sync.WaitGroup
			for i := 0; i < 100; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < 1000; j++ {
						algo.l.Allow()
					}
				}()
			}
			wg.Wait()
		})
	}
}

func TestConcurrency_Manager(t *testing.T) {
	mgr := NewManager(AlgoTokenBucket, Options{Rate: 1e6, Capacity: 1e6})
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				key := fmt.Sprintf("key-%d", j%10)
				mgr.Allow(key, 1)
			}
		}(i)
	}
	wg.Wait()
}
