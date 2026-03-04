package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"task-scheduler/pkg/model"
)

// TaskHandler processes a task payload and returns a result or an error.
// The context carries the per-attempt timeout; handlers MUST respect ctx.Done().
type TaskHandler func(ctx context.Context, payload string) (string, error)

// Config holds worker configuration.
type Config struct {
	SchedulerAddr     string
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
}

func DefaultConfig() Config {
	return Config{
		SchedulerAddr:     "http://localhost:8080",
		PollInterval:      2 * time.Second,
		HeartbeatInterval: 5 * time.Second,
	}
}

// Worker pulls tasks from the scheduler, executes them, and reports results.
type Worker struct {
	id                string
	schedulerAddr     string
	handlers          map[string]TaskHandler
	mu                sync.RWMutex
	pollInterval      time.Duration
	heartbeatInterval time.Duration
	client            *http.Client
	stopCh            chan struct{}
	wg                sync.WaitGroup
}

func New(cfg Config) *Worker {
	return &Worker{
		schedulerAddr:     cfg.SchedulerAddr,
		handlers:          make(map[string]TaskHandler),
		pollInterval:      cfg.PollInterval,
		heartbeatInterval: cfg.HeartbeatInterval,
		client:            &http.Client{Timeout: 10 * time.Second},
		stopCh:            make(chan struct{}),
	}
}

// RegisterHandler maps a task name to its handler function.
func (w *Worker) RegisterHandler(name string, handler TaskHandler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.handlers[name] = handler
	log.Printf("[Worker] registered handler: %s", name)
}

// Start registers with the scheduler and begins polling for tasks.
func (w *Worker) Start() error {
	// Register with scheduler
	resp, err := w.client.Post(w.schedulerAddr+"/api/workers/register", "application/json", nil)
	if err != nil {
		return fmt.Errorf("register failed: %w", err)
	}
	defer resp.Body.Close()

	var info model.WorkerInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return fmt.Errorf("decode register response: %w", err)
	}
	w.id = info.ID
	log.Printf("[Worker %s] registered with scheduler at %s", w.id, w.schedulerAddr)

	// Launch heartbeat loop
	w.wg.Add(1)
	go w.heartbeatLoop()

	// Launch task polling loop
	w.wg.Add(1)
	go w.pollLoop()

	return nil
}

// Stop gracefully shuts down the worker and waits for in-flight work to finish.
func (w *Worker) Stop() {
	close(w.stopCh)
	w.wg.Wait()
	log.Printf("[Worker %s] stopped", w.id)
}

// ID returns the worker's assigned ID.
func (w *Worker) ID() string { return w.id }

// ──────────────────── Heartbeat ────────────────────

func (w *Worker) heartbeatLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			url := fmt.Sprintf("%s/api/workers/%s/heartbeat", w.schedulerAddr, w.id)
			resp, err := w.client.Post(url, "application/json", nil)
			if err != nil {
				log.Printf("[Worker %s] heartbeat failed: %v", w.id, err)
				continue
			}
			resp.Body.Close()
		}
	}
}

// ──────────────────── Task Polling ────────────────────

func (w *Worker) pollLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.pullAndExecute()
		}
	}
}

func (w *Worker) pullAndExecute() {
	url := fmt.Sprintf("%s/api/tasks/pull?worker_id=%s", w.schedulerAddr, w.id)
	resp, err := w.client.Post(url, "application/json", nil)
	if err != nil {
		log.Printf("[Worker %s] pull failed: %v", w.id, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return // No tasks available
	}

	var task model.Task
	if err := json.NewDecoder(resp.Body).Decode(&task); err != nil {
		log.Printf("[Worker %s] decode task failed: %v", w.id, err)
		return
	}

	log.Printf("[Worker %s] pulled task %s (%s)", w.id, task.ID, task.Name)

	// Look up the handler
	w.mu.RLock()
	handler, ok := w.handlers[task.Name]
	w.mu.RUnlock()

	if !ok {
		w.reportFailure(task.ID, fmt.Sprintf("no handler registered for task type: %s", task.Name))
		return
	}

	// Execute with timeout
	timeout := time.Duration(task.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 60 * time.Second // default 60s if not specified
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	type execResult struct {
		result string
		err    error
	}
	ch := make(chan execResult, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				ch <- execResult{"", fmt.Errorf("handler panicked: %v", r)}
			}
		}()
		res, err := handler(ctx, task.Payload)
		ch <- execResult{res, err}
	}()

	select {
	case <-ctx.Done():
		w.reportFailure(task.ID, "execution timed out on worker side")
	case res := <-ch:
		if res.err != nil {
			w.reportFailure(task.ID, res.err.Error())
		} else {
			w.reportSuccess(task.ID, res.result)
		}
	}
}

// ──────────────────── Result Reporting ────────────────────

func (w *Worker) reportSuccess(taskID, result string) {
	body, _ := json.Marshal(map[string]string{
		"worker_id": w.id,
		"result":    result,
	})
	url := fmt.Sprintf("%s/api/tasks/%s/complete", w.schedulerAddr, taskID)

	resp, err := w.client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("[Worker %s] report success failed: %v", w.id, err)
		return
	}
	resp.Body.Close()
	log.Printf("[Worker %s] task %s → completed", w.id, taskID)
}

func (w *Worker) reportFailure(taskID, errMsg string) {
	body, _ := json.Marshal(map[string]string{
		"worker_id": w.id,
		"error":     errMsg,
	})
	url := fmt.Sprintf("%s/api/tasks/%s/fail", w.schedulerAddr, taskID)

	resp, err := w.client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("[Worker %s] report failure failed: %v", w.id, err)
		return
	}
	resp.Body.Close()
	log.Printf("[Worker %s] task %s → failed: %s", w.id, taskID, errMsg)
}
