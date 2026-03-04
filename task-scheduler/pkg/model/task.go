package model

import (
	"container/heap"
	"crypto/rand"
	"fmt"
	"sync"
	"time"
)

// ---------- ID Generator ----------

// GenerateID creates a random ID with the given prefix (e.g. "task-a1b2c3d4").
func GenerateID(prefix string) string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("%s-%x", prefix, b)
}

// ---------- Task ----------

type TaskStatus string

const (
	TaskPending   TaskStatus = "pending"
	TaskRunning   TaskStatus = "running"
	TaskCompleted TaskStatus = "completed"
	TaskFailed    TaskStatus = "failed"
	TaskTimeout   TaskStatus = "timeout"
)

// Task represents a unit of work to be executed by a worker.
type Task struct {
	ID             string     `json:"id"`
	Name           string     `json:"name"`            // Task type name, maps to handler
	Payload        string     `json:"payload"`          // JSON-encoded task payload
	Priority       int        `json:"priority"`         // Higher value = higher priority
	MaxRetries     int        `json:"max_retries"`      // Max retry attempts on failure
	RetryCount     int        `json:"retry_count"`      // Current retry count
	TimeoutSeconds int        `json:"timeout_seconds"`  // Per-attempt timeout in seconds
	Status         TaskStatus `json:"status"`
	WorkerID       string     `json:"worker_id,omitempty"`
	Result         string     `json:"result,omitempty"`
	Error          string     `json:"error,omitempty"`
	FailCallback   string     `json:"fail_callback,omitempty"` // URL invoked on permanent failure
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
}

// ---------- Priority Queue (heap-based) ----------

// taskHeap implements heap.Interface for priority-based task scheduling.
type taskHeap []*Task

func (h taskHeap) Len() int            { return len(h) }
func (h taskHeap) Less(i, j int) bool  { return h[i].Priority > h[j].Priority } // Higher priority first
func (h taskHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *taskHeap) Push(x interface{}) { *h = append(*h, x.(*Task)) }
func (h *taskHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*h = old[:n-1]
	return item
}

// TaskQueue is a thread-safe priority queue for tasks.
type TaskQueue struct {
	mu   sync.Mutex
	heap taskHeap
}

func NewTaskQueue() *TaskQueue {
	tq := &TaskQueue{}
	heap.Init(&tq.heap)
	return tq
}

func (tq *TaskQueue) Enqueue(task *Task) {
	tq.mu.Lock()
	defer tq.mu.Unlock()
	heap.Push(&tq.heap, task)
}

func (tq *TaskQueue) Dequeue() *Task {
	tq.mu.Lock()
	defer tq.mu.Unlock()
	if len(tq.heap) == 0 {
		return nil
	}
	return heap.Pop(&tq.heap).(*Task)
}

func (tq *TaskQueue) Len() int {
	tq.mu.Lock()
	defer tq.mu.Unlock()
	return len(tq.heap)
}

// ---------- Worker ----------

type WorkerStatus string

const (
	WorkerIdle    WorkerStatus = "idle"
	WorkerBusy    WorkerStatus = "busy"
	WorkerOffline WorkerStatus = "offline"
)

// WorkerInfo tracks the state of a registered worker.
type WorkerInfo struct {
	ID             string       `json:"id"`
	Status         WorkerStatus `json:"status"`
	LastHeartbeat  time.Time    `json:"last_heartbeat"`
	CurrentTaskID  string       `json:"current_task_id,omitempty"`
	TasksCompleted int          `json:"tasks_completed"`
	TasksFailed    int          `json:"tasks_failed"`
	RegisteredAt   time.Time    `json:"registered_at"`
}
