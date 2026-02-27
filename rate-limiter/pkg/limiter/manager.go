package limiter

import (
	"sync"
)

// Manager manages per-key rate limiters with thread-safe access.
// It uses sync.Map for high-performance concurrent reads (common path)
// and lazy initialization of per-key limiters.
type Manager struct {
	limiters sync.Map // map[string]Limiter
	algo     Algorithm
	opts     Options
}

// NewManager creates a new limiter manager with default algorithm and options.
func NewManager(algo Algorithm, opts Options) *Manager {
	return &Manager{
		algo: algo,
		opts: opts,
	}
}

// Allow checks if a single request for the given key is allowed using the default algorithm.
func (m *Manager) Allow(key string, n int) Result {
	return m.AllowWithAlgo(key, m.algo, n)
}

// AllowWithAlgo checks if n requests for the given key are allowed using the specified algorithm.
// If no limiter exists for the key+algorithm combination, one is created atomically.
func (m *Manager) AllowWithAlgo(key string, algo Algorithm, n int) Result {
	compositeKey := string(algo) + ":" + key

	if l, ok := m.limiters.Load(compositeKey); ok {
		return l.(Limiter).AllowN(n)
	}

	// Create a new limiter if one doesn't exist
	newLimiter := New(algo, m.opts)
	actual, _ := m.limiters.LoadOrStore(compositeKey, newLimiter)
	return actual.(Limiter).AllowN(n)
}

// Reset removes the limiter for a specific key and algorithm.
func (m *Manager) Reset(key string, algo Algorithm) {
	compositeKey := string(algo) + ":" + key
	m.limiters.Delete(compositeKey)
}

// ResetAll removes all limiters.
func (m *Manager) ResetAll() {
	m.limiters.Range(func(key, _ any) bool {
		m.limiters.Delete(key)
		return true
	})
}
