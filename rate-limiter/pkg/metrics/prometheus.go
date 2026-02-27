package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metric collectors for the rate limiter service.
type Metrics struct {
	// RequestsTotal counts total rate limit check requests by key, algorithm, and result.
	RequestsTotal *prometheus.CounterVec
	// RequestDuration tracks the duration of rate limit checks.
	RequestDuration *prometheus.HistogramVec
	// TokensRemaining shows the current remaining capacity per key.
	TokensRemaining *prometheus.GaugeVec
	// ActiveKeys tracks the number of active rate-limited keys.
	ActiveKeys prometheus.Gauge
}

// New creates and registers all Prometheus metrics.
func New() *Metrics {
	return &Metrics{
		RequestsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "ratelimiter",
				Name:      "requests_total",
				Help:      "Total number of rate limit check requests",
			},
			[]string{"key", "algorithm", "result"},
		),
		RequestDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "ratelimiter",
				Name:      "request_duration_seconds",
				Help:      "Histogram of rate limit check durations in seconds",
				Buckets:   []float64{.000001, .000005, .00001, .00005, .0001, .0005, .001, .005, .01},
			},
			[]string{"algorithm"},
		),
		TokensRemaining: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "ratelimiter",
				Name:      "tokens_remaining",
				Help:      "Current remaining tokens/capacity per key",
			},
			[]string{"key", "algorithm"},
		),
		ActiveKeys: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "ratelimiter",
				Name:      "active_keys",
				Help:      "Number of active rate-limited keys",
			},
		),
	}
}

// RecordRequest increments the request counter.
func (m *Metrics) RecordRequest(key, algorithm, result string) {
	m.RequestsTotal.WithLabelValues(key, algorithm, result).Inc()
}

// ObserveDuration records a rate limit check duration.
func (m *Metrics) ObserveDuration(algorithm string, seconds float64) {
	m.RequestDuration.WithLabelValues(algorithm).Observe(seconds)
}

// SetRemaining updates the remaining capacity gauge for a key.
func (m *Metrics) SetRemaining(key, algorithm string, remaining float64) {
	m.TokensRemaining.WithLabelValues(key, algorithm).Set(remaining)
}
