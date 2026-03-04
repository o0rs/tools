package scheduler

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"task-scheduler/pkg/model"
)

// Config holds scheduler configuration.
type Config struct {
	HeartbeatTimeout time.Duration // How long before a worker is considered dead
	CheckInterval    time.Duration // How often to run background checks
}

func DefaultConfig() Config {
	return Config{
		HeartbeatTimeout: 30 * time.Second,
		CheckInterval:    5 * time.Second,
	}
}

// Scheduler is the central coordinator that manages tasks and workers.
type Scheduler struct {
	mu      sync.RWMutex
	queue   *model.TaskQueue
	tasks   map[string]*model.Task       // All tasks indexed by ID
	workers map[string]*model.WorkerInfo // All workers indexed by ID

	heartbeatTimeout time.Duration
	checkInterval    time.Duration

	stopCh chan struct{}
}

func New(cfg Config) *Scheduler {
	return &Scheduler{
		queue:            model.NewTaskQueue(),
		tasks:            make(map[string]*model.Task),
		workers:          make(map[string]*model.WorkerInfo),
		heartbeatTimeout: cfg.HeartbeatTimeout,
		checkInterval:    cfg.CheckInterval,
		stopCh:           make(chan struct{}),
	}
}

// Start launches background goroutines for timeout detection and worker health checks.
func (s *Scheduler) Start() {
	go s.timeoutChecker()
	go s.workerHealthChecker()
	log.Println("[Scheduler] started background checkers")
}

// Stop signals all background goroutines to exit.
func (s *Scheduler) Stop() {
	close(s.stopCh)
}

// ──────────────────── Task Operations ────────────────────

// SubmitTask adds a new task to the scheduling queue.
func (s *Scheduler) SubmitTask(task *model.Task) {
	s.mu.Lock()
	defer s.mu.Unlock()

	task.ID = model.GenerateID("task")
	task.Status = model.TaskPending
	task.RetryCount = 0
	task.CreatedAt = time.Now()
	task.UpdatedAt = time.Now()

	s.tasks[task.ID] = task
	s.queue.Enqueue(task)

	log.Printf("[Scheduler] task submitted: id=%s name=%s priority=%d", task.ID, task.Name, task.Priority)
}

// PullTask assigns the highest-priority pending task to the given worker.
// Returns nil if no tasks are available or the worker is not idle.
func (s *Scheduler) PullTask(workerID string) *model.Task {
	s.mu.Lock()
	defer s.mu.Unlock()

	worker, ok := s.workers[workerID]
	if !ok || worker.Status != model.WorkerIdle {
		return nil
	}

	task := s.queue.Dequeue()
	if task == nil {
		return nil
	}

	now := time.Now()
	task.Status = model.TaskRunning
	task.WorkerID = workerID
	task.StartedAt = &now
	task.UpdatedAt = now

	worker.Status = model.WorkerBusy
	worker.CurrentTaskID = task.ID

	log.Printf("[Scheduler] task %s assigned to worker %s", task.ID, workerID)
	return task
}

// CompleteTask marks a task as completed. Only the assigned worker may complete it.
func (s *Scheduler) CompleteTask(taskID, workerID, result string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, ok := s.tasks[taskID]
	if !ok || task.Status != model.TaskRunning || task.WorkerID != workerID {
		return false
	}

	task.Status = model.TaskCompleted
	task.Result = result
	task.UpdatedAt = time.Now()

	if worker, ok := s.workers[workerID]; ok {
		worker.Status = model.WorkerIdle
		worker.CurrentTaskID = ""
		worker.TasksCompleted++
	}

	log.Printf("[Scheduler] task %s completed by worker %s", taskID, workerID)
	return true
}

// FailTask marks a task as failed and handles retry logic.
// If retries remain, the task is re-queued; otherwise a failure callback is invoked.
func (s *Scheduler) FailTask(taskID, workerID, errMsg string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, ok := s.tasks[taskID]
	if !ok || task.Status != model.TaskRunning || task.WorkerID != workerID {
		return false
	}

	// Release the worker
	if worker, ok := s.workers[workerID]; ok {
		worker.Status = model.WorkerIdle
		worker.CurrentTaskID = ""
		worker.TasksFailed++
	}

	task.RetryCount++
	task.Error = errMsg
	task.UpdatedAt = time.Now()
	task.WorkerID = ""
	task.StartedAt = nil

	if task.RetryCount <= task.MaxRetries {
		task.Status = model.TaskPending
		s.queue.Enqueue(task)
		log.Printf("[Scheduler] task %s re-queued, retry %d/%d", taskID, task.RetryCount, task.MaxRetries)
	} else {
		task.Status = model.TaskFailed
		log.Printf("[Scheduler] task %s permanently failed after %d retries", taskID, task.RetryCount)
		go s.invokeFailCallback(task)
	}

	return true
}

// ──────────────────── Worker Operations ────────────────────

// RegisterWorker registers a new worker and returns its info.
func (s *Scheduler) RegisterWorker() *model.WorkerInfo {
	s.mu.Lock()
	defer s.mu.Unlock()

	worker := &model.WorkerInfo{
		ID:            model.GenerateID("worker"),
		Status:        model.WorkerIdle,
		LastHeartbeat: time.Now(),
		RegisteredAt:  time.Now(),
	}
	s.workers[worker.ID] = worker

	log.Printf("[Scheduler] worker registered: %s", worker.ID)
	return worker
}

// Heartbeat updates a worker's last-seen timestamp.
func (s *Scheduler) Heartbeat(workerID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	worker, ok := s.workers[workerID]
	if !ok {
		return false
	}
	worker.LastHeartbeat = time.Now()
	return true
}

// ──────────────────── Query Operations ────────────────────

func (s *Scheduler) GetTask(taskID string) *model.Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tasks[taskID]
}

func (s *Scheduler) GetAllTasks() []*model.Task {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tasks := make([]*model.Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		tasks = append(tasks, t)
	}
	return tasks
}

func (s *Scheduler) GetAllWorkers() []*model.WorkerInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	workers := make([]*model.WorkerInfo, 0, len(s.workers))
	for _, w := range s.workers {
		workers = append(workers, w)
	}
	return workers
}

// Stats returns a snapshot of scheduler metrics.
func (s *Scheduler) Stats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	counts := map[string]int{
		"pending": 0, "running": 0, "completed": 0, "failed": 0, "timeout": 0,
	}
	for _, t := range s.tasks {
		counts[string(t.Status)]++
	}

	activeWorkers := 0
	for _, w := range s.workers {
		if w.Status != model.WorkerOffline {
			activeWorkers++
		}
	}

	return map[string]interface{}{
		"total_tasks":    len(s.tasks),
		"pending_tasks":  counts["pending"],
		"running_tasks":  counts["running"],
		"completed_tasks": counts["completed"],
		"failed_tasks":   counts["failed"],
		"timeout_tasks":  counts["timeout"],
		"queue_length":   s.queue.Len(),
		"total_workers":  len(s.workers),
		"active_workers": activeWorkers,
	}
}

// ──────────────────── Background Checkers ────────────────────

// timeoutChecker periodically detects running tasks that have exceeded their timeout.
func (s *Scheduler) timeoutChecker() {
	ticker := time.NewTicker(s.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.checkTimeouts()
		}
	}
}

func (s *Scheduler) checkTimeouts() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for _, task := range s.tasks {
		if task.Status != model.TaskRunning || task.StartedAt == nil || task.TimeoutSeconds <= 0 {
			continue
		}

		elapsed := now.Sub(*task.StartedAt)
		if elapsed <= time.Duration(task.TimeoutSeconds)*time.Second {
			continue
		}

		log.Printf("[Scheduler] task %s timed out (elapsed %v, limit %ds)", task.ID, elapsed, task.TimeoutSeconds)

		// Release the worker
		if worker, ok := s.workers[task.WorkerID]; ok {
			worker.Status = model.WorkerIdle
			worker.CurrentTaskID = ""
			worker.TasksFailed++
		}

		task.RetryCount++
		task.Error = "task execution timed out"
		task.UpdatedAt = now
		task.WorkerID = ""
		task.StartedAt = nil

		if task.RetryCount <= task.MaxRetries {
			task.Status = model.TaskPending
			s.queue.Enqueue(task)
			log.Printf("[Scheduler] task %s re-queued for retry %d/%d", task.ID, task.RetryCount, task.MaxRetries)
		} else {
			task.Status = model.TaskTimeout
			log.Printf("[Scheduler] task %s permanently timed out", task.ID)
			go s.invokeFailCallback(task)
		}
	}
}

// workerHealthChecker periodically checks for workers that missed their heartbeat.
func (s *Scheduler) workerHealthChecker() {
	ticker := time.NewTicker(s.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.checkWorkerHealth()
		}
	}
}

func (s *Scheduler) checkWorkerHealth() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for _, worker := range s.workers {
		if worker.Status == model.WorkerOffline {
			continue
		}
		if now.Sub(worker.LastHeartbeat) <= s.heartbeatTimeout {
			continue
		}

		log.Printf("[Scheduler] worker %s offline (last heartbeat %v ago)", worker.ID, now.Sub(worker.LastHeartbeat).Round(time.Second))

		// Re-queue the task this worker was running
		if worker.CurrentTaskID != "" {
			if task, ok := s.tasks[worker.CurrentTaskID]; ok && task.Status == model.TaskRunning {
				task.RetryCount++
				task.Error = "worker went offline"
				task.UpdatedAt = now
				task.WorkerID = ""
				task.StartedAt = nil

				if task.RetryCount <= task.MaxRetries {
					task.Status = model.TaskPending
					s.queue.Enqueue(task)
					log.Printf("[Scheduler] task %s re-queued (worker offline)", task.ID)
				} else {
					task.Status = model.TaskFailed
					go s.invokeFailCallback(task)
				}
			}
		}

		worker.Status = model.WorkerOffline
		worker.CurrentTaskID = ""
	}
}

// ──────────────────── Failure Callback ────────────────────

// invokeFailCallback sends a POST to the task's fail_callback URL with failure details.
func (s *Scheduler) invokeFailCallback(task *model.Task) {
	if task.FailCallback == "" {
		return
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"task_id":     task.ID,
		"name":        task.Name,
		"error":       task.Error,
		"status":      task.Status,
		"retry_count": task.RetryCount,
	})

	resp, err := http.Post(task.FailCallback, "application/json", bytes.NewReader(payload))
	if err != nil {
		log.Printf("[Scheduler] fail callback error for task %s: %v", task.ID, err)
		return
	}
	defer resp.Body.Close()
	log.Printf("[Scheduler] fail callback invoked for task %s → HTTP %d", task.ID, resp.StatusCode)
}
