package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/dneil5648/ductwork/pkg/agent"
	"github.com/dneil5648/ductwork/pkg/api"
	"github.com/dneil5648/ductwork/pkg/config"
	"github.com/dneil5648/ductwork/pkg/dependencies"
	"github.com/dneil5648/ductwork/pkg/history"
	"github.com/dneil5648/ductwork/pkg/logging"
	"github.com/dneil5648/ductwork/pkg/orchestrator"
	"github.com/dneil5648/ductwork/pkg/scheduler"
	"github.com/dneil5648/ductwork/pkg/security"
	task "github.com/dneil5648/ductwork/pkg/tasks"

	"github.com/spf13/cobra"
)

// agentDir is the default .agent/ directory path (relative to cwd)
const agentDir = ".agent"

func main() {
	rootCmd := &cobra.Command{
		Use:   "ductwork",
		Short: "AI Agent Orchestrator",
		Long:  "A Go-based platform for running AI agents on schedules with tasks, skills, and memory.",
	}

	rootCmd.AddCommand(initCmd())
	rootCmd.AddCommand(startCmd())
	rootCmd.AddCommand(runCmd())
	rootCmd.AddCommand(spawnCmd())
	rootCmd.AddCommand(listCmd())
	rootCmd.AddCommand(buildCmd())
	rootCmd.AddCommand(historyCmd())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// initCmd creates the .agent/ directory structure explicitly
func initCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "Initialize the .agent/ directory structure",
		RunE: func(cmd *cobra.Command, args []string) error {
			absDir, err := filepath.Abs(agentDir)
			if err != nil {
				return err
			}
			return config.Init(absDir)
		},
	}
}

// startCmd boots the scheduler, orchestrator, and API server as a long-running process
func startCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "start",
		Short: "Start the scheduler, orchestrator, and API server",
		Long:  "Boots all goroutines: scheduler polls and fires tasks, orchestrator spawns agents, API server listens for requests.",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Load config (auto-creates .agent/ if missing)
			cfg, err := config.LoadConfig(agentDir)
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			// Setup structured logging
			cleanup, err := logging.Setup(cfg.LogsDir, cfg.Debug)
			if err != nil {
				return fmt.Errorf("failed to setup logging: %w", err)
			}
			defer cleanup()

			// Load security config
			secCfg, err := security.LoadSecurityConfig(cfg.SecurityFile)
			if err != nil {
				return fmt.Errorf("failed to load security config: %w", err)
			}
			slog.Info("loaded security config", "file", cfg.SecurityFile)

			// Load dependency config
			depCfg, err := dependencies.LoadDependencies(cfg.DependenciesFile)
			if err != nil {
				return fmt.Errorf("failed to load dependencies config: %w", err)
			}
			slog.Info("loaded dependencies config", "runtimes", len(depCfg.Runtimes))

			// Create history store
			historyStore, err := history.NewFileStore(cfg.HistoryDir)
			if err != nil {
				return fmt.Errorf("failed to create history store: %w", err)
			}
			slog.Info("loaded history store", "dir", cfg.HistoryDir)

			// Create orchestrator (owns the task channel, creates per-task agents)
			orch := orchestrator.NewOrchestrator(cfg, secCfg, depCfg, historyStore, 10)

			// Create scheduler (writes to orchestrator's channel)
			sched := scheduler.NewScheduler(orch.TaskChan)

			// Load all task definitions and feed scheduled ones to the scheduler
			tasks, err := task.LoadTasks(cfg.TasksDir)
			if err != nil {
				slog.Warn("could not load tasks", "error", err)
				tasks = []task.Task{}
			}

			if len(tasks) > 0 {
				if err := sched.LoadTasks(tasks); err != nil {
					return fmt.Errorf("failed to load tasks into scheduler: %w", err)
				}
			}

			slog.Info("loaded tasks", "count", len(tasks), "dir", cfg.TasksDir)

			// Set up context with cancellation for graceful shutdown
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Start goroutines
			go orch.Run(ctx)
			go sched.Run(ctx)
			go func() {
				if err := api.Start(cfg, orch, sched, historyStore); err != nil {
					slog.Error("API server error", "error", err)
				}
			}()

			slog.Info("ductwork running", "api_port", cfg.APIPort)
			fmt.Println("Press Ctrl+C to stop")

			// Block until shutdown signal
			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
			sig := <-sigChan

			slog.Info("received shutdown signal", "signal", sig)
			cancel()

			return nil
		},
	}
}

// runCmd loads and runs a specific named task (immediate mode)
func runCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "run [task-name]",
		Short: "Run a defined task by name",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			taskName := args[0]

			cfg, err := config.LoadConfig(agentDir)
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			// Setup logging
			cleanup, err := logging.Setup(cfg.LogsDir, cfg.Debug)
			if err != nil {
				return fmt.Errorf("failed to setup logging: %w", err)
			}
			defer cleanup()

			// Load security config
			secCfg, err := security.LoadSecurityConfig(cfg.SecurityFile)
			if err != nil {
				return fmt.Errorf("failed to load security config: %w", err)
			}

			// Load dependency config
			depCfg, err := dependencies.LoadDependencies(cfg.DependenciesFile)
			if err != nil {
				return fmt.Errorf("failed to load dependencies config: %w", err)
			}

			// Load the specific task
			taskPath := filepath.Join(cfg.TasksDir, taskName+".json")
			t, err := task.LoadTask(taskPath)
			if err != nil {
				return fmt.Errorf("failed to load task %q: %w", taskName, err)
			}

			// Resolve memory_dir relative to .agent/ root
			if t.MemoryDir != "" && !filepath.IsAbs(t.MemoryDir) {
				t.MemoryDir = filepath.Join(cfg.RootDir, t.MemoryDir)
			}

			// Resolve skill paths relative to .agent/ root
			for name, path := range t.Skills {
				if !filepath.IsAbs(path) {
					t.Skills[name] = filepath.Join(cfg.RootDir, path)
				}
			}

			// Create per-task enforcer
			enforcer, err := security.NewEnforcer(secCfg, taskName)
			if err != nil {
				return fmt.Errorf("failed to create enforcer: %w", err)
			}

			a := &agent.Agent{
				SystemPrompt:       cfg.SystemPrompt,
				Model:              cfg.DefaultModel,
				Enforcer:           enforcer,
				TasksDir:           cfg.TasksDir,
				ScriptsDir:         cfg.ScriptsDir,
				DependenciesPrompt: depCfg.ToSystemPrompt(),
				ToolsFile:          cfg.ToolsFile,
			}

			ctx := context.Background()
			slog.Info("running task", "task", t.Name)
			result, err := a.RunTask(ctx, t)
			if err != nil {
				return fmt.Errorf("task failed: %w", err)
			}

			logFields := []any{"task", t.Name}
			if result != nil {
				logFields = append(logFields,
					"input_tokens", result.InputTokens,
					"output_tokens", result.OutputTokens,
					"iterations", result.Iterations)
			}
			slog.Info("task finished", logFields...)
			return nil
		},
	}
}

// spawnCmd runs an ad-hoc one-off task with a raw prompt
func spawnCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "spawn [prompt]",
		Short: "Run an ad-hoc agent with a raw prompt",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			prompt := args[0]

			cfg, err := config.LoadConfig(agentDir)
			if err != nil {
				return fmt.Errorf("failed to setup config: %w", err)
			}

			// Setup logging
			cleanup, err := logging.Setup(cfg.LogsDir, cfg.Debug)
			if err != nil {
				return fmt.Errorf("failed to setup logging: %w", err)
			}
			defer cleanup()

			// Load security config
			secCfg, err := security.LoadSecurityConfig(cfg.SecurityFile)
			if err != nil {
				return fmt.Errorf("failed to load security config: %w", err)
			}

			// Load dependency config
			depCfg, err := dependencies.LoadDependencies(cfg.DependenciesFile)
			if err != nil {
				return fmt.Errorf("failed to load dependencies config: %w", err)
			}

			// Create enforcer for ad-hoc tasks
			enforcer, err := security.NewEnforcer(secCfg, "adhoc")
			if err != nil {
				return fmt.Errorf("failed to create enforcer: %w", err)
			}

			a := &agent.Agent{
				SystemPrompt:       cfg.SystemPrompt,
				Model:              cfg.DefaultModel,
				Enforcer:           enforcer,
				TasksDir:           cfg.TasksDir,
				ScriptsDir:         cfg.ScriptsDir,
				DependenciesPrompt: depCfg.ToSystemPrompt(),
				ToolsFile:          cfg.ToolsFile,
			}

			ctx := context.Background()
			slog.Info("spawning agent")
			result, err := a.Spawn(ctx, prompt)
			if err != nil {
				return fmt.Errorf("agent failed: %w", err)
			}

			logFields := []any{}
			if result != nil {
				logFields = append(logFields,
					"input_tokens", result.InputTokens,
					"output_tokens", result.OutputTokens,
					"iterations", result.Iterations)
			}
			slog.Info("agent finished", logFields...)
			return nil
		},
	}
}

// listCmd lists all defined tasks from .agent/tasks/
func listCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all defined tasks",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.LoadConfig(agentDir)
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			tasks, err := task.LoadTasks(cfg.TasksDir)
			if err != nil {
				return fmt.Errorf("failed to load tasks: %w", err)
			}

			if len(tasks) == 0 {
				fmt.Println("No tasks defined. Add task JSON files to .agent/tasks/")
				return nil
			}

			fmt.Printf("%-20s %-12s %-10s %s\n", "NAME", "RUN MODE", "SCHEDULE", "DESCRIPTION")
			fmt.Printf("%-20s %-12s %-10s %s\n", "----", "--------", "--------", "-----------")
			for _, t := range tasks {
				schedule := t.Schedule
				if schedule == "" {
					schedule = "-"
				}
				fmt.Printf("%-20s %-12s %-10s %s\n", t.Name, t.RunMode, schedule, t.Description)
			}

			return nil
		},
	}
}

// buildCmd starts a specialized agent session focused on task creation.
// The agent gets a tailored system prompt and restricted tools (create_task + read_file only).
func buildCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "build [description]",
		Short: "Build a new task definition using an AI agent",
		Long:  "Starts an agent session that creates a task definition from a natural language description. The agent only has access to create_task and read_file tools.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			description := args[0]

			cfg, err := config.LoadConfig(agentDir)
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			// Setup logging
			cleanup, err := logging.Setup(cfg.LogsDir, cfg.Debug)
			if err != nil {
				return fmt.Errorf("failed to setup logging: %w", err)
			}
			defer cleanup()

			// Specialized system prompt for task building
			buildPrompt := `You are a task builder agent. Your job is to create well-structured task definitions from natural language descriptions.

You have access to:
- create_task: Create a new task JSON definition
- read_file: Read existing files for reference

When building a task:
1. Parse the user's description to understand the task's purpose
2. Choose an appropriate name (lowercase kebab-case, e.g. 'health-check')
3. Write a clear, actionable prompt that tells the executing agent exactly what to do
4. Decide if this is a scheduled or immediate task
5. If scheduled, pick an appropriate interval
6. Use create_task to save the definition

Be concise and create the task directly. Don't over-explain.`

			// Restricted enforcer — only create_task and read_file
			enforcer := security.NewStaticEnforcer([]string{"create_task", "read_file"})

			a := &agent.Agent{
				SystemPrompt: buildPrompt,
				Model:        cfg.DefaultModel,
				Enforcer:     enforcer,
				TasksDir:     cfg.TasksDir,
				ScriptsDir:   cfg.ScriptsDir,
			}

			ctx := context.Background()
			slog.Info("starting task builder", "description", description)

			prompt := fmt.Sprintf("Build a task for the following: %s", description)
			if _, err := a.Spawn(ctx, prompt); err != nil {
				return fmt.Errorf("build failed: %w", err)
			}

			slog.Info("task builder finished")
			return nil
		},
	}
}

// historyCmd shows recent run history
func historyCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "history [task-name]",
		Short: "Show recent run history",
		Long:  "Shows the most recent task runs with status, duration, and token usage. Optionally filter by task name.",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.LoadConfig(agentDir)
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			store, err := history.NewFileStore(cfg.HistoryDir)
			if err != nil {
				return fmt.Errorf("failed to open history store: %w", err)
			}

			var records []history.RunRecord
			if len(args) == 1 {
				records, err = store.GetByTask(args[0], 20)
			} else {
				records, err = store.GetRecent(20)
			}
			if err != nil {
				return fmt.Errorf("failed to load history: %w", err)
			}

			if len(records) == 0 {
				fmt.Println("No run history found.")
				return nil
			}

			fmt.Printf("%-36s %-20s %-10s %-12s %-8s %-8s %s\n",
				"RUN ID", "TASK", "STATUS", "DURATION", "IN TOK", "OUT TOK", "ERROR")
			fmt.Printf("%-36s %-20s %-10s %-12s %-8s %-8s %s\n",
				"------", "----", "------", "--------", "------", "-------", "-----")
			for _, r := range records {
				errStr := r.Error
				if len(errStr) > 30 {
					errStr = errStr[:30] + "..."
				}
				fmt.Printf("%-36s %-20s %-10s %-12s %-8d %-8d %s\n",
					r.RunID, r.TaskName, r.Status, r.Duration,
					r.InputTokens, r.OutputTokens, errStr)
			}

			return nil
		},
	}
}
