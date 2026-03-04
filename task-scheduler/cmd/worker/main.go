package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"task-scheduler/pkg/worker"
)

func main() {
	schedulerAddr := flag.String("scheduler", "http://localhost:8080", "scheduler address")
	pollInterval := flag.Duration("poll-interval", 2*time.Second, "task poll interval")
	heartbeatInterval := flag.Duration("heartbeat-interval", 5*time.Second, "heartbeat interval")
	flag.Parse()

	cfg := worker.Config{
		SchedulerAddr:     *schedulerAddr,
		PollInterval:      *pollInterval,
		HeartbeatInterval: *heartbeatInterval,
	}

	w := worker.New(cfg)

	// Register demo task handlers
	w.RegisterHandler("send_email", handleSendEmail)
	w.RegisterHandler("process_image", handleProcessImage)
	w.RegisterHandler("generate_report", handleGenerateReport)

	if err := w.Start(); err != nil {
		log.Fatalf("Failed to start worker: %v", err)
	}

	log.Println("🔧 Worker running. Press Ctrl+C to stop.")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down worker...")
	w.Stop()
}

// ──────────────────── Demo Handlers ────────────────────

func handleSendEmail(ctx context.Context, payload string) (string, error) {
	log.Printf("[send_email] processing: %s", payload)

	select {
	case <-time.After(time.Duration(rand.Intn(3)+1) * time.Second):
	case <-ctx.Done():
		return "", ctx.Err()
	}

	if rand.Float64() < 0.3 {
		return "", errors.New("SMTP connection refused")
	}

	return fmt.Sprintf("email sent: %s", payload), nil
}

func handleProcessImage(ctx context.Context, payload string) (string, error) {
	log.Printf("[process_image] processing: %s", payload)

	select {
	case <-time.After(time.Duration(rand.Intn(5)+2) * time.Second):
	case <-ctx.Done():
		return "", ctx.Err()
	}

	return fmt.Sprintf("image processed: %s", payload), nil
}

func handleGenerateReport(ctx context.Context, payload string) (string, error) {
	log.Printf("[generate_report] processing: %s", payload)

	select {
	case <-time.After(time.Duration(rand.Intn(4)+1) * time.Second):
	case <-ctx.Done():
		return "", ctx.Err()
	}

	if rand.Float64() < 0.2 {
		return "", errors.New("database connection timeout")
	}

	return fmt.Sprintf("report generated: %s", payload), nil
}
