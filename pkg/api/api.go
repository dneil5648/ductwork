package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/dneil5648/ductwork/pkg/config"
	"github.com/dneil5648/ductwork/pkg/history"
	"github.com/dneil5648/ductwork/pkg/orchestrator"
	"github.com/dneil5648/ductwork/pkg/scheduler"
	task "github.com/dneil5648/ductwork/pkg/tasks"
)

// API holds references to the orchestrator and scheduler so handlers can
// trigger tasks and inspect state.
type API struct {
	config       *config.Config
	orchestrator *orchestrator.Orchestrator
	scheduler    *scheduler.Scheduler
	historyStore history.Store
}

// SpawnRequest is the JSON body for POST /api/spawn
type SpawnRequest struct {
	Prompt string `json:"prompt"`
}

// SchedulerStatus is a single entry in the GET /api/scheduler/status response
type SchedulerStatus struct {
	Name     string `json:"name"`
	Schedule string `json:"schedule"`
	NextRun  string `json:"next_run"`
}

// Start creates the HTTP server and begins listening.
// This blocks — run it in a goroutine.
func Start(cfg *config.Config, orch *orchestrator.Orchestrator, sched *scheduler.Scheduler, historyStore history.Store) error {
	a := &API{
		config:       cfg,
		orchestrator: orch,
		scheduler:    sched,
		historyStore: historyStore,
	}

	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("GET /api/health", a.handleHealth)

	// Task endpoints
	mux.HandleFunc("GET /api/tasks", a.handleListTasks)
	mux.HandleFunc("GET /api/tasks/", a.handleGetTask)      // /api/tasks/{name}
	mux.HandleFunc("POST /api/tasks/", a.handleRunTask)      // /api/tasks/{name}/run

	// Spawn endpoint
	mux.HandleFunc("POST /api/spawn", a.handleSpawn)

	// Run history endpoints
	mux.HandleFunc("GET /api/runs", a.handleRecentRuns)
	mux.HandleFunc("GET /api/runs/", a.handleRunsByTask)     // /api/runs/{task-name}

	// Scheduler endpoints
	mux.HandleFunc("GET /api/scheduler/status", a.handleSchedulerStatus)
	mux.HandleFunc("POST /api/scheduler/add", a.handleSchedulerAdd)

	addr := fmt.Sprintf(":%d", cfg.APIPort)
	slog.Info("API server listening", "module", "api", "addr", addr)
	return http.ListenAndServe(addr, mux)
}

// GET /api/health
func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// GET /api/tasks — list all task definitions
func (a *API) handleListTasks(w http.ResponseWriter, r *http.Request) {
	tasks, err := task.LoadTasks(a.config.TasksDir)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, tasks)
}

// GET /api/tasks/{name} — get a specific task definition
func (a *API) handleGetTask(w http.ResponseWriter, r *http.Request) {
	// Only handle GET requests that don't end in /run
	name := extractTaskName(r.URL.Path)
	if name == "" || strings.HasSuffix(r.URL.Path, "/run") {
		// Let the POST handler deal with /run paths, return 404 for empty names
		if r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/run") {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "use POST for /run"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing task name"})
		return
	}

	taskPath := filepath.Join(a.config.TasksDir, name+".json")
	t, err := task.LoadTask(taskPath)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": fmt.Sprintf("task %q not found", name)})
		return
	}

	writeJSON(w, http.StatusOK, t)
}

// POST /api/tasks/{name}/run — trigger a task immediately
func (a *API) handleRunTask(w http.ResponseWriter, r *http.Request) {
	if !strings.HasSuffix(r.URL.Path, "/run") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "expected /api/tasks/{name}/run"})
		return
	}

	// Extract task name from path: /api/tasks/{name}/run
	path := strings.TrimSuffix(r.URL.Path, "/run")
	name := extractTaskName(path)
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing task name"})
		return
	}

	taskPath := filepath.Join(a.config.TasksDir, name+".json")
	t, err := task.LoadTask(taskPath)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": fmt.Sprintf("task %q not found", name)})
		return
	}

	// Resolve relative paths
	if t.MemoryDir != "" && !filepath.IsAbs(t.MemoryDir) {
		t.MemoryDir = filepath.Join(a.config.RootDir, t.MemoryDir)
	}
	for skillName, skillPath := range t.Skills {
		if !filepath.IsAbs(skillPath) {
			t.Skills[skillName] = filepath.Join(a.config.RootDir, skillPath)
		}
	}

	// Push to orchestrator channel (non-blocking via goroutine)
	go func() {
		a.orchestrator.TaskChan <- t
	}()

	writeJSON(w, http.StatusAccepted, map[string]string{
		"status": "accepted",
		"task":   name,
	})
}

// POST /api/spawn — run an ad-hoc task with a raw prompt
func (a *API) handleSpawn(w http.ResponseWriter, r *http.Request) {
	var req SpawnRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if req.Prompt == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "prompt is required"})
		return
	}

	// Fire and forget — spawn in background
	go func() {
		if _, err := a.orchestrator.SpawnAdhoc(context.Background(), req.Prompt); err != nil {
			slog.Error("spawn failed", "module", "api", "error", err)
		}
	}()

	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

// GET /api/scheduler/status — list all scheduled tasks with next run times
func (a *API) handleSchedulerStatus(w http.ResponseWriter, r *http.Request) {
	snapshot := a.scheduler.Status()

	statuses := make([]SchedulerStatus, len(snapshot))
	for i, st := range snapshot {
		statuses[i] = SchedulerStatus{
			Name:     st.Task.Name,
			Schedule: st.Task.Schedule,
			NextRun:  st.NextRun.Format("2006-01-02T15:04:05Z07:00"),
		}
	}

	writeJSON(w, http.StatusOK, statuses)
}

// POST /api/scheduler/add — add a new task to the scheduler at runtime
func (a *API) handleSchedulerAdd(w http.ResponseWriter, r *http.Request) {
	var t task.Task
	if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if t.Name == "" || t.Schedule == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name and schedule are required"})
		return
	}

	a.scheduler.AddTask(t)

	writeJSON(w, http.StatusAccepted, map[string]string{
		"status": "added",
		"task":   t.Name,
	})
}

// GET /api/runs — recent run history across all tasks
func (a *API) handleRecentRuns(w http.ResponseWriter, r *http.Request) {
	if a.historyStore == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "history store not configured"})
		return
	}
	records, err := a.historyStore.GetRecent(50)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, records)
}

// GET /api/runs/{task-name} — run history for a specific task
func (a *API) handleRunsByTask(w http.ResponseWriter, r *http.Request) {
	if a.historyStore == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "history store not configured"})
		return
	}
	taskName := strings.TrimPrefix(r.URL.Path, "/api/runs/")
	taskName = strings.TrimSuffix(taskName, "/")
	if taskName == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing task name"})
		return
	}
	records, err := a.historyStore.GetByTask(taskName, 50)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, records)
}

// extractTaskName pulls the task name from a URL path like /api/tasks/{name}
func extractTaskName(path string) string {
	path = strings.TrimPrefix(path, "/api/tasks/")
	path = strings.TrimSuffix(path, "/")
	if path == "" || strings.Contains(path, "/") {
		return ""
	}
	return path
}

// writeJSON is a helper to write a JSON response with a status code.
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
