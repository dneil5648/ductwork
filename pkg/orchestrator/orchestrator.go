package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/dneil5648/ductwork/pkg/agent"
	"github.com/dneil5648/ductwork/pkg/config"
	"github.com/dneil5648/ductwork/pkg/dependencies"
	"github.com/dneil5648/ductwork/pkg/history"
	"github.com/dneil5648/ductwork/pkg/logging"
	"github.com/dneil5648/ductwork/pkg/security"
	task "github.com/dneil5648/ductwork/pkg/tasks"
)

// Orchestrator owns the task channel and spawns agents to execute tasks.
// It is the single consumer of the channel — both the scheduler and
// immediate pushes (CLI, API) write to it.
//
// Instead of sharing a single Agent, the orchestrator creates a fresh Agent
// per task execution. This avoids concurrency issues with the Enforcer
// (which is task-specific) and keeps each execution isolated.
type Orchestrator struct {
	TaskChan       chan task.Task
	cfg            *config.Config
	securityConfig *security.SecurityConfig
	depPrompt      string        // pre-rendered dependency info for system prompt
	historyStore   history.Store // nil = no history recording
	sem            chan struct{} // concurrency semaphore
}

// NewOrchestrator creates an orchestrator with a buffered task channel.
// securityCfg, depCfg, and historyStore can be nil for backward compatibility.
func NewOrchestrator(cfg *config.Config, securityCfg *security.SecurityConfig, depCfg *dependencies.DependencyConfig, historyStore history.Store, bufferSize int) *Orchestrator {
	depPrompt := ""
	if depCfg != nil {
		depPrompt = depCfg.ToSystemPrompt()
	}

	maxConcurrent := cfg.MaxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = 5
	}

	return &Orchestrator{
		TaskChan:       make(chan task.Task, bufferSize),
		cfg:            cfg,
		securityConfig: securityCfg,
		depPrompt:      depPrompt,
		historyStore:   historyStore,
		sem:            make(chan struct{}, maxConcurrent),
	}
}

// newAgent creates a fresh Agent configured for a specific task.
// Each task gets its own Enforcer with merged security rules.
func (o *Orchestrator) newAgent(taskName string) (*agent.Agent, error) {
	a := &agent.Agent{
		SystemPrompt:       o.cfg.SystemPrompt,
		Model:              o.cfg.DefaultModel,
		TasksDir:           o.cfg.TasksDir,
		ScriptsDir:         o.cfg.ScriptsDir,
		DependenciesPrompt: o.depPrompt,
		ToolsFile:          o.cfg.ToolsFile,
	}

	// Create per-task Enforcer if security config exists
	if o.securityConfig != nil {
		enforcer, err := security.NewEnforcer(o.securityConfig, taskName)
		if err != nil {
			return nil, fmt.Errorf("failed to create enforcer for task %q: %w", taskName, err)
		}
		a.Enforcer = enforcer
	}

	return a, nil
}

// Run is the main orchestrator loop. It should be run as a goroutine.
// It reads tasks from the channel and spawns an agent goroutine for each one.
// The semaphore limits concurrent executions to MaxConcurrent.
func (o *Orchestrator) Run(ctx context.Context) {
	slog.Info("orchestrator started", "module", "orchestrator",
		"max_concurrent", cap(o.sem))

	for {
		select {
		case t := <-o.TaskChan:
			o.sem <- struct{}{} // acquire — blocks if at capacity
			go func(t task.Task) {
				defer func() { <-o.sem }() // release
				o.executeTask(t)
			}(t)
		case <-ctx.Done():
			slog.Info("orchestrator shutting down", "module", "orchestrator")
			return
		}
	}
}

// executeTask runs a single task via a freshly created agent with retry logic
// and history recording.
func (o *Orchestrator) executeTask(t task.Task) {
	runID := fmt.Sprintf("%s-%d", t.Name, time.Now().UnixMilli())
	ctx := logging.NewContext(context.Background(), t.Name, runID)
	logger := logging.FromContext(ctx)

	start := time.Now()
	logger.Info("executing task", "module", "orchestrator")

	// Create initial run record
	record := &history.RunRecord{
		RunID:     runID,
		TaskName:  t.Name,
		Status:    history.StatusRunning,
		StartedAt: start,
	}
	o.saveRecord(record)

	// Create agent
	a, err := o.newAgent(t.Name)
	if err != nil {
		logger.Error("failed to create agent", "module", "orchestrator", "error", err)
		o.finalizeRecord(record, start, nil, err, 0)
		return
	}

	// Resolve retry config
	retryCfg := o.resolveRetryConfig(t)

	var result *agent.RunResult
	var lastErr error
	retriesDone := 0

	for attempt := 0; attempt <= retryCfg.MaxRetries; attempt++ {
		if attempt > 0 {
			retriesDone++
			wait := retryCfg.Backoff(attempt - 1)
			logger.Info("retrying task", "attempt", attempt,
				"max_retries", retryCfg.MaxRetries, "backoff", wait)
			time.Sleep(wait)
		}

		result, lastErr = a.RunTask(ctx, t)
		if lastErr == nil {
			break // success
		}

		if !IsTransient(lastErr) {
			logger.Info("non-transient error, not retrying", "error", lastErr)
			break
		}

		logger.Warn("transient error", "attempt", attempt, "error", lastErr)
	}

	o.finalizeRecord(record, start, result, lastErr, retriesDone)

	elapsed := time.Since(start)
	if lastErr != nil {
		logger.Error("task failed", "module", "orchestrator",
			"elapsed", elapsed, "retries", retriesDone, "error", lastErr)
	} else {
		logFields := []any{"module", "orchestrator", "elapsed", elapsed, "retries", retriesDone}
		if result != nil {
			logFields = append(logFields,
				"input_tokens", result.InputTokens,
				"output_tokens", result.OutputTokens,
				"iterations", result.Iterations)
		}
		logger.Info("task completed", logFields...)
	}
}

// RunImmediate runs a defined task directly, bypassing the channel.
// Blocks until the task completes. Used by CLI `agent run` and the API.
// Does NOT go through the semaphore (user-initiated, should not be throttled).
func (o *Orchestrator) RunImmediate(ctx context.Context, t task.Task) (*agent.RunResult, error) {
	logger := logging.FromContext(ctx)
	start := time.Now()
	logger.Info("running task immediately", "module", "orchestrator", "task", t.Name)

	a, err := o.newAgent(t.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to create agent: %w", err)
	}

	result, err := a.RunTask(ctx, t)

	elapsed := time.Since(start)
	if err != nil {
		logger.Error("task failed", "module", "orchestrator", "elapsed", elapsed, "error", err)
		return result, err
	}

	logger.Info("task completed", "module", "orchestrator", "elapsed", elapsed)
	return result, nil
}

// SpawnAdhoc runs an ad-hoc task with a raw prompt string.
// Blocks until complete. Used by CLI `agent spawn` and the API.
func (o *Orchestrator) SpawnAdhoc(ctx context.Context, prompt string) (*agent.RunResult, error) {
	slog.Info("spawning ad-hoc agent", "module", "orchestrator")

	a, err := o.newAgent("adhoc")
	if err != nil {
		return nil, fmt.Errorf("failed to create agent: %w", err)
	}

	return a.Spawn(ctx, prompt)
}

// saveRecord persists a run record if history store is available.
func (o *Orchestrator) saveRecord(record *history.RunRecord) {
	if o.historyStore != nil {
		if err := o.historyStore.Save(record); err != nil {
			slog.Error("failed to save run record", "error", err)
		}
	}
}

// finalizeRecord updates a run record with completion data and persists it.
func (o *Orchestrator) finalizeRecord(record *history.RunRecord, start time.Time, result *agent.RunResult, err error, retries int) {
	now := time.Now()
	record.CompletedAt = &now
	record.Duration = time.Since(start).Round(time.Millisecond).String()
	record.Retries = retries

	if result != nil {
		record.InputTokens = result.InputTokens
		record.OutputTokens = result.OutputTokens
		record.Iterations = result.Iterations
	}

	if err != nil {
		record.Status = history.StatusFailed
		record.Error = err.Error()
	} else {
		record.Status = history.StatusCompleted
	}

	o.saveRecord(record)
}

// resolveRetryConfig merges task-level retry settings with config defaults.
func (o *Orchestrator) resolveRetryConfig(t task.Task) RetryConfig {
	maxRetries := o.cfg.DefaultMaxRetries
	if t.MaxRetries > 0 {
		maxRetries = t.MaxRetries
	}

	backoffStr := o.cfg.DefaultRetryBackoff
	if t.RetryBackoff != "" {
		backoffStr = t.RetryBackoff
	}

	baseBackoff, err := time.ParseDuration(backoffStr)
	if err != nil {
		baseBackoff = 2 * time.Second
	}

	return RetryConfig{
		MaxRetries:  maxRetries,
		BaseBackoff: baseBackoff,
	}
}
