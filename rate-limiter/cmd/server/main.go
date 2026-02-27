package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"rate-limiter/pkg/config"
	"rate-limiter/pkg/limiter"
	"rate-limiter/pkg/metrics"
	"rate-limiter/pkg/server"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)

	// Load configuration
	cfg := config.Load()
	log.Printf("Configuration loaded:")
	log.Printf("  HTTP Port:    %d", cfg.HTTPPort)
	log.Printf("  gRPC Port:    %d", cfg.GRPCPort)
	log.Printf("  Algorithm:    %s", cfg.DefaultAlgorithm)
	log.Printf("  Rate:         %.0f req/s", cfg.DefaultRate)
	log.Printf("  Capacity:     %d", cfg.DefaultCapacity)
	log.Printf("  Window:       %s", cfg.DefaultWindow)

	// Initialize components
	m := metrics.New()
	opts := limiter.Options{
		Rate:     cfg.DefaultRate,
		Capacity: cfg.DefaultCapacity,
		Window:   cfg.DefaultWindow,
	}
	algo := limiter.ParseAlgorithm(cfg.DefaultAlgorithm)
	mgr := limiter.NewManager(algo, opts)

	// Create servers
	httpServer := server.NewHTTPServer(cfg, mgr, m)
	grpcServer := server.NewGRPCServer(cfg, mgr, m)

	// Start servers in goroutines
	errCh := make(chan error, 2)

	go func() {
		if err := httpServer.Start(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("HTTP server error: %w", err)
		}
	}()

	go func() {
		if err := grpcServer.Start(); err != nil {
			errCh <- fmt.Errorf("gRPC server error: %w", err)
		}
	}()

	log.Println("Rate limiter service started successfully")

	// Wait for interrupt signal or server error
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-quit:
		log.Println("Received shutdown signal")
	case err := <-errCh:
		log.Printf("Server error: %v", err)
	}

	// Graceful shutdown
	log.Println("Shutting down servers...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	grpcServer.Stop()
	if err := httpServer.Stop(ctx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	log.Println("Server stopped gracefully")
}
