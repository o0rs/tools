package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"rate-limiter/pkg/config"
	"rate-limiter/pkg/limiter"
	"rate-limiter/pkg/metrics"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HTTPServer provides a REST API for rate limiting.
type HTTPServer struct {
	cfg     *config.Config
	manager *limiter.Manager
	metrics *metrics.Metrics
	server  *http.Server
}

// AllowRequest is the JSON request body for the /api/v1/allow endpoint.
type AllowRequest struct {
	Key       string `json:"key"`
	Tokens    int    `json:"tokens"`
	Algorithm string `json:"algorithm"`
}

// AllowResponse is the JSON response for rate limit checks.
type AllowResponse struct {
	Allowed    bool    `json:"allowed"`
	Remaining  int     `json:"remaining"`
	RetryAfter float64 `json:"retry_after_ms"`
	Message    string  `json:"message"`
}

// NewHTTPServer creates a new HTTP server instance.
func NewHTTPServer(cfg *config.Config, mgr *limiter.Manager, m *metrics.Metrics) *HTTPServer {
	return &HTTPServer{
		cfg:     cfg,
		manager: mgr,
		metrics: m,
	}
}

// Start begins listening and serving HTTP requests.
func (s *HTTPServer) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/allow", s.handleAllow)
	mux.HandleFunc("/health", s.handleHealth)
	mux.Handle("/metrics", promhttp.Handler())

	s.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", s.cfg.HTTPPort),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("[HTTP] listening on :%d", s.cfg.HTTPPort)
	return s.server.ListenAndServe()
}

// Stop gracefully shuts down the HTTP server.
func (s *HTTPServer) Stop(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *HTTPServer) handleAllow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, AllowResponse{
			Message: "method not allowed, use POST",
		})
		return
	}

	var req AllowRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, AllowResponse{
			Message: "invalid request body: " + err.Error(),
		})
		return
	}

	if req.Key == "" {
		writeJSON(w, http.StatusBadRequest, AllowResponse{
			Message: "key is required",
		})
		return
	}
	if req.Tokens <= 0 {
		req.Tokens = 1
	}
	if req.Algorithm == "" {
		req.Algorithm = s.cfg.DefaultAlgorithm
	}

	algo := limiter.ParseAlgorithm(req.Algorithm)

	start := time.Now()
	result := s.manager.AllowWithAlgo(req.Key, algo, req.Tokens)
	duration := time.Since(start).Seconds()

	// Record Prometheus metrics
	resultLabel := "allowed"
	if !result.Allowed {
		resultLabel = "denied"
	}
	s.metrics.RecordRequest(req.Key, req.Algorithm, resultLabel)
	s.metrics.ObserveDuration(req.Algorithm, duration)
	s.metrics.SetRemaining(req.Key, req.Algorithm, float64(result.Remaining))

	resp := AllowResponse{
		Allowed:    result.Allowed,
		Remaining:  result.Remaining,
		RetryAfter: float64(result.RetryAfter.Milliseconds()),
	}
	if result.Allowed {
		resp.Message = "request allowed"
	} else {
		resp.Message = "rate limit exceeded"
	}

	status := http.StatusOK
	if !result.Allowed {
		status = http.StatusTooManyRequests
	}
	writeJSON(w, status, resp)
}

func (s *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"service": "rate-limiter",
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
