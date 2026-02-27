package limiter

import (
	"fmt"
	"testing"
	"time"
)

const benchCapacity = 1 << 30

func BenchmarkTokenBucket(b *testing.B) {
	tb := NewTokenBucket(float64(benchCapacity), benchCapacity)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tb.Allow()
	}
}

func BenchmarkLeakyBucket(b *testing.B) {
	lb := NewLeakyBucket(float64(benchCapacity), benchCapacity)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lb.Allow()
	}
}

func BenchmarkSlidingWindow(b *testing.B) {
	sw := NewSlidingWindow(benchCapacity, time.Second)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sw.Allow()
	}
}

func BenchmarkTokenBucket_Parallel(b *testing.B) {
	tb := NewTokenBucket(float64(benchCapacity), benchCapacity)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tb.Allow()
		}
	})
}

func BenchmarkLeakyBucket_Parallel(b *testing.B) {
	lb := NewLeakyBucket(float64(benchCapacity), benchCapacity)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lb.Allow()
		}
	})
}

func BenchmarkSlidingWindow_Parallel(b *testing.B) {
	sw := NewSlidingWindow(benchCapacity, time.Second)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sw.Allow()
		}
	})
}

func BenchmarkManager_SingleKey(b *testing.B) {
	mgr := NewManager(AlgoTokenBucket, Options{
		Rate:     float64(benchCapacity),
		Capacity: benchCapacity,
	})
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mgr.Allow("single-key", 1)
		}
	})
}

func BenchmarkManager_MultiKey(b *testing.B) {
	for _, numKeys := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("keys=%d", numKeys), func(b *testing.B) {
			mgr := NewManager(AlgoTokenBucket, Options{
				Rate:     float64(benchCapacity),
				Capacity: benchCapacity,
			})
			keys := make([]string, numKeys)
			for i := range keys {
				keys[i] = fmt.Sprintf("key-%d", i)
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					mgr.Allow(keys[i%numKeys], 1)
					i++
				}
			})
		})
	}
}

func BenchmarkComparison(b *testing.B) {
	algos := []struct {
		name string
		l    Limiter
	}{
		{"TokenBucket", NewTokenBucket(float64(benchCapacity), benchCapacity)},
		{"LeakyBucket", NewLeakyBucket(float64(benchCapacity), benchCapacity)},
		{"SlidingWindow", NewSlidingWindow(benchCapacity, time.Second)},
	}
	for _, algo := range algos {
		algo := algo
		b.Run(algo.name, func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					algo.l.Allow()
				}
			})
		})
	}
}
