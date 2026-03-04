package scheduler

import (
	"encoding/json"
	"net/http"
	"strings"

	"task-scheduler/pkg/model"
)

// API exposes the scheduler functionality over HTTP.
type API struct {
	scheduler *Scheduler
}

func NewAPI(s *Scheduler) *API {
	return &API{scheduler: s}
}

// RegisterRoutes wires up all HTTP endpoints to the given ServeMux.
func (a *API) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/tasks", a.handleTasks)           // POST=submit, GET=list
	mux.HandleFunc("/api/tasks/pull", a.handlePullTask)   // POST=pull task
	mux.HandleFunc("/api/tasks/", a.handleTaskByID)       // GET=status, POST=complete/fail
	mux.HandleFunc("/api/workers/register", a.handleRegisterWorker) // POST=register
	mux.HandleFunc("/api/workers/", a.handleWorkerByID)   // POST=heartbeat
	mux.HandleFunc("/api/workers", a.handleListWorkers)   // GET=list
	mux.HandleFunc("/api/stats", a.handleStats)           // GET=stats
}

// POST /api/tasks         → submit a new task
// GET  /api/tasks         → list all tasks
func (a *API) handleTasks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			Name           string `json:"name"`
			Payload        string `json:"payload"`
			Priority       int    `json:"priority"`
			MaxRetries     int    `json:"max_retries"`
			TimeoutSeconds int    `json:"timeout_seconds"`
			FailCallback   string `json:"fail_callback"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if req.Name == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "task name is required"})
			return
		}

		task := &model.Task{
			Name:           req.Name,
			Payload:        req.Payload,
			Priority:       req.Priority,
			MaxRetries:     req.MaxRetries,
			TimeoutSeconds: req.TimeoutSeconds,
			FailCallback:   req.FailCallback,
		}
		a.scheduler.SubmitTask(task)
		writeJSON(w, http.StatusCreated, task)

	case http.MethodGet:
		writeJSON(w, http.StatusOK, a.scheduler.GetAllTasks())

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// POST /api/tasks/pull?worker_id=xxx → pull highest-priority pending task
func (a *API) handlePullTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	workerID := r.URL.Query().Get("worker_id")
	if workerID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "worker_id is required"})
		return
	}

	task := a.scheduler.PullTask(workerID)
	if task == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, task)
}

// Routes under /api/tasks/{id}:
//   GET  /api/tasks/{id}          → get task details
//   POST /api/tasks/{id}/complete → mark task complete
//   POST /api/tasks/{id}/fail     → mark task failed
func (a *API) handleTaskByID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/tasks/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	taskID := parts[0]

	// GET /api/tasks/{id}
	if len(parts) == 1 && r.Method == http.MethodGet {
		task := a.scheduler.GetTask(taskID)
		if task == nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "task not found"})
			return
		}
		writeJSON(w, http.StatusOK, task)
		return
	}

	// POST /api/tasks/{id}/complete or /api/tasks/{id}/fail
	if len(parts) == 2 && r.Method == http.MethodPost {
		switch parts[1] {
		case "complete":
			var req struct {
				WorkerID string `json:"worker_id"`
				Result   string `json:"result"`
			}
			json.NewDecoder(r.Body).Decode(&req)
			if a.scheduler.CompleteTask(taskID, req.WorkerID, req.Result) {
				writeJSON(w, http.StatusOK, map[string]string{"status": "completed"})
			} else {
				writeJSON(w, http.StatusConflict, map[string]string{"error": "task not found or not assigned to this worker"})
			}

		case "fail":
			var req struct {
				WorkerID string `json:"worker_id"`
				Error    string `json:"error"`
			}
			json.NewDecoder(r.Body).Decode(&req)
			if a.scheduler.FailTask(taskID, req.WorkerID, req.Error) {
				writeJSON(w, http.StatusOK, map[string]string{"status": "failed, retry scheduled if possible"})
			} else {
				writeJSON(w, http.StatusConflict, map[string]string{"error": "task not found or not assigned to this worker"})
			}

		default:
			w.WriteHeader(http.StatusNotFound)
		}
		return
	}

	w.WriteHeader(http.StatusNotFound)
}

// POST /api/workers/register → register a new worker
func (a *API) handleRegisterWorker(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	worker := a.scheduler.RegisterWorker()
	writeJSON(w, http.StatusCreated, worker)
}

// POST /api/workers/{id}/heartbeat → worker heartbeat
func (a *API) handleWorkerByID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/workers/")
	parts := strings.Split(path, "/")

	if len(parts) != 2 || parts[1] != "heartbeat" || r.Method != http.MethodPost {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if a.scheduler.Heartbeat(parts[0]) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	} else {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "worker not found"})
	}
}

// GET /api/workers → list all registered workers
func (a *API) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, a.scheduler.GetAllWorkers())
}

// GET /api/stats → scheduler statistics
func (a *API) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, a.scheduler.Stats())
}

// writeJSON encodes data as JSON and writes it to the response.
func writeJSON(w http.ResponseWriter, code int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if data != nil {
		json.NewEncoder(w).Encode(data)
	}
}
