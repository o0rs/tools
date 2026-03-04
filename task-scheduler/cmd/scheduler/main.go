package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"task-scheduler/pkg/scheduler"
)

func main() {
	addr := flag.String("addr", ":8080", "scheduler listen address")
	heartbeatTimeout := flag.Duration("heartbeat-timeout", 30*time.Second, "worker heartbeat timeout")
	checkInterval := flag.Duration("check-interval", 5*time.Second, "background check interval")
	flag.Parse()

	cfg := scheduler.Config{
		HeartbeatTimeout: *heartbeatTimeout,
		CheckInterval:    *checkInterval,
	}

	s := scheduler.New(cfg)
	s.Start()

	api := scheduler.NewAPI(s)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	srv := &http.Server{
		Addr:         *addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("🚀 Scheduler listening on %s", *addr)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Graceful shutdown on SIGINT/SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down scheduler...")
	s.Stop()
	srv.Close()
}
