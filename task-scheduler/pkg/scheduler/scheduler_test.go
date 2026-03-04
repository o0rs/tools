package scheduler

import (
	"testing"
	"time"

	"task-scheduler/pkg/model"
)

func newTestScheduler() *Scheduler {
	return New(Config{
		HeartbeatTimeout: 30 * time.Second,
		CheckInterval:    1 * time.Second,
	})
}

func TestSubmitAndPullTask(t *testing.T) {
	s := newTestScheduler()
	w := s.RegisterWorker()
	task := &model.Task{Name: "test_task", Payload: `{"key":"value"}`, Priority: 5, MaxRetries: 3, TimeoutSeconds: 30}
	s.SubmitTask(task)
	pulled := s.PullTask(w.ID)
	if pulled == nil {
		t.Fatal("expected a task, got nil")
	}
	if pulled.Name != "test_task" {
		t.Fatalf("expected name=test_task, got %s", pulled.Name)
	}
	if pulled.Status != model.TaskRunning {
		t.Fatalf("expected status=running, got %s", pulled.Status)
	}
	if pulled.WorkerID != w.ID {
		t.Fatalf("expected workerID=%s, got %s", w.ID, pulled.WorkerID)
	}
}

func TestPullNoTasks(t *testing.T) {
	s := newTestScheduler()
	w := s.RegisterWorker()
	if s.PullTask(w.ID) != nil {
		t.Fatal("expected nil when no tasks")
	}
}

func TestPullUnknownWorker(t *testing.T) {
	s := newTestScheduler()
	s.SubmitTask(&model.Task{Name: "test"})
	if s.PullTask("nonexistent") != nil {
		t.Fatal("expected nil for unknown worker")
	}
}

func TestBusyWorkerCannotPull(t *testing.T) {
	s := newTestScheduler()
	w := s.RegisterWorker()
	s.SubmitTask(&model.Task{Name: "task1", Priority: 5})
	s.SubmitTask(&model.Task{Name: "task2", Priority: 3})
	t1 := s.PullTask(w.ID)
	if t1 == nil {
		t.Fatal("first pull should succeed")
	}
	if s.PullTask(w.ID) != nil {
		t.Fatal("busy worker should not pull another task")
	}
	s.CompleteTask(t1.ID, w.ID, "done")
	if s.PullTask(w.ID) == nil {
		t.Fatal("idle worker should pull after completing task")
	}
}

func TestPriorityOrder(t *testing.T) {
	s := newTestScheduler()
	w := s.RegisterWorker()
	s.SubmitTask(&model.Task{Name: "low", Priority: 1})
	s.SubmitTask(&model.Task{Name: "high", Priority: 10})
	s.SubmitTask(&model.Task{Name: "medium", Priority: 5})

	t1 := s.PullTask(w.ID)
	if t1.Name != "high" {
		t.Fatalf("expected high first, got %s", t1.Name)
	}
	s.CompleteTask(t1.ID, w.ID, "done")

	t2 := s.PullTask(w.ID)
	if t2.Name != "medium" {
		t.Fatalf("expected medium second, got %s", t2.Name)
	}
	s.CompleteTask(t2.ID, w.ID, "done")

	t3 := s.PullTask(w.ID)
	if t3.Name != "low" {
		t.Fatalf("expected low third, got %s", t3.Name)
	}
}

func TestCompleteTask(t *testing.T) {
	s := newTestScheduler()
	w := s.RegisterWorker()
	s.SubmitTask(&model.Task{Name: "test", Priority: 5})
	pulled := s.PullTask(w.ID)
	if !s.CompleteTask(pulled.ID, w.ID, "all good") {
		t.Fatal("CompleteTask should return true")
	}
	task := s.GetTask(pulled.ID)
	if task.Status != model.TaskCompleted {
		t.Fatalf("expected completed, got %s", task.Status)
	}
	if task.Result != "all good" {
		t.Fatalf("expected result='all good', got %s", task.Result)
	}
	workers := s.GetAllWorkers()
	if workers[0].Status != model.WorkerIdle {
		t.Fatalf("expected worker idle, got %s", workers[0].Status)
	}
	if workers[0].TasksCompleted != 1 {
		t.Fatalf("expected tasks_completed=1, got %d", workers[0].TasksCompleted)
	}
}

func TestCompleteWrongWorker(t *testing.T) {
	s := newTestScheduler()
	w1 := s.RegisterWorker()
	w2 := s.RegisterWorker()
	s.SubmitTask(&model.Task{Name: "test"})
	pulled := s.PullTask(w1.ID)
	if s.CompleteTask(pulled.ID, w2.ID, "result") {
		t.Fatal("wrong worker should not complete the task")
	}
}

func TestRetryOnFailure(t *testing.T) {
	s := newTestScheduler()
	w := s.RegisterWorker()
	s.SubmitTask(&model.Task{Name: "flaky", MaxRetries: 2})

	t1 := s.PullTask(w.ID)
	s.FailTask(t1.ID, w.ID, "error 1")
	task := s.GetTask(t1.ID)
	if task.Status != model.TaskPending {
		t.Fatalf("after retry 1: expected pending, got %s", task.Status)
	}
	if task.RetryCount != 1 {
		t.Fatalf("expected retry_count=1, got %d", task.RetryCount)
	}

	t2 := s.PullTask(w.ID)
	s.FailTask(t2.ID, w.ID, "error 2")
	task = s.GetTask(t1.ID)
	if task.Status != model.TaskPending {
		t.Fatalf("after retry 2: expected pending, got %s", task.Status)
	}

	t3 := s.PullTask(w.ID)
	s.FailTask(t3.ID, w.ID, "error 3")
	task = s.GetTask(t1.ID)
	if task.Status != model.TaskFailed {
		t.Fatalf("after max retries: expected failed, got %s", task.Status)
	}
	if task.RetryCount != 3 {
		t.Fatalf("expected retry_count=3, got %d", task.RetryCount)
	}
	if s.PullTask(w.ID) != nil {
		t.Fatal("no tasks should remain after permanent failure")
	}
}

func TestRetryZeroMaxRetries(t *testing.T) {
	s := newTestScheduler()
	w := s.RegisterWorker()
	s.SubmitTask(&model.Task{Name: "no_retry", MaxRetries: 0})
	pulled := s.PullTask(w.ID)
	s.FailTask(pulled.ID, w.ID, "instant fail")
	task := s.GetTask(pulled.ID)
	if task.Status != model.TaskFailed {
		t.Fatalf("expected immediate failure, got %s", task.Status)
	}
}

func TestTaskTimeout(t *testing.T) {
	s := New(Config{HeartbeatTimeout: 30 * time.Second, CheckInterval: 200 * time.Millisecond})
	s.Start()
	defer s.Stop()
	w := s.RegisterWorker()
	s.SubmitTask(&model.Task{Name: "slow", TimeoutSeconds: 1, MaxRetries: 0})
	pulled := s.PullTask(w.ID)
	if pulled == nil {
		t.Fatal("expected a task")
	}
	time.Sleep(2 * time.Second)
	task := s.GetTask(pulled.ID)
	if task.Status != model.TaskTimeout {
		t.Fatalf("expected timeout, got %s", task.Status)
	}
}

func TestTaskTimeoutWithRetry(t *testing.T) {
	s := New(Config{HeartbeatTimeout: 30 * time.Second, CheckInterval: 200 * time.Millisecond})
	s.Start()
	defer s.Stop()
	w := s.RegisterWorker()
	s.SubmitTask(&model.Task{Name: "slow_retry", TimeoutSeconds: 1, MaxRetries: 1})
	pulled := s.PullTask(w.ID)
	time.Sleep(2 * time.Second)
	task := s.GetTask(pulled.ID)
	if task.Status != model.TaskPending {
		t.Fatalf("expected pending (re-queued), got %s", task.Status)
	}
	if task.RetryCount != 1 {
		t.Fatalf("expected retry_count=1, got %d", task.RetryCount)
	}
}

func TestWorkerOfflineDetection(t *testing.T) {
	s := New(Config{HeartbeatTimeout: 500 * time.Millisecond, CheckInterval: 200 * time.Millisecond})
	s.Start()
	defer s.Stop()
	w := s.RegisterWorker()
	time.Sleep(1 * time.Second)
	workers := s.GetAllWorkers()
	found := false
	for _, worker := range workers {
		if worker.ID == w.ID && worker.Status == model.WorkerOffline {
			found = true
		}
	}
	if !found {
		t.Fatal("worker should be marked offline")
	}
}

func TestWorkerOfflineRequeuesTask(t *testing.T) {
	s := New(Config{HeartbeatTimeout: 500 * time.Millisecond, CheckInterval: 200 * time.Millisecond})
	s.Start()
	defer s.Stop()
	w1 := s.RegisterWorker()
	s.SubmitTask(&model.Task{Name: "requeue_test", MaxRetries: 2})
	pulled := s.PullTask(w1.ID)
	if pulled == nil {
		t.Fatal("expected a task")
	}
	time.Sleep(1 * time.Second)
	w2 := s.RegisterWorker()
	repulled := s.PullTask(w2.ID)
	if repulled == nil {
		t.Fatal("task should be re-queued after worker offline")
	}
	if repulled.ID != pulled.ID {
		t.Fatal("re-queued task should be the same")
	}
}

func TestHeartbeat(t *testing.T) {
	s := newTestScheduler()
	w := s.RegisterWorker()
	if !s.Heartbeat(w.ID) {
		t.Fatal("heartbeat should succeed")
	}
	if s.Heartbeat("nonexistent") {
		t.Fatal("heartbeat should fail for unknown worker")
	}
}

func TestStats(t *testing.T) {
	s := newTestScheduler()
	w := s.RegisterWorker()
	s.SubmitTask(&model.Task{Name: "t1", Priority: 5})
	s.SubmitTask(&model.Task{Name: "t2", Priority: 3})
	stats := s.Stats()
	if stats["total_tasks"].(int) != 2 {
		t.Fatalf("expected 2 total, got %v", stats["total_tasks"])
	}
	if stats["pending_tasks"].(int) != 2 {
		t.Fatalf("expected 2 pending, got %v", stats["pending_tasks"])
	}
	if stats["active_workers"].(int) != 1 {
		t.Fatalf("expected 1 active worker, got %v", stats["active_workers"])
	}
	s.PullTask(w.ID)
	stats = s.Stats()
	if stats["running_tasks"].(int) != 1 {
		t.Fatalf("expected 1 running, got %v", stats["running_tasks"])
	}
}
