package taskbuilder

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	task "github.com/dneil5648/ductwork/pkg/tasks"
)

// namePattern enforces lowercase kebab-case task names (e.g. "health-check", "deploy-prod")
var namePattern = regexp.MustCompile(`^[a-z][a-z0-9]*(-[a-z0-9]+)*$`)

// ValidateAndCreate validates a task definition from tool input, writes it
// to the tasks directory as JSON, and returns the created Task and file path.
func ValidateAndCreate(input map[string]interface{}, tasksDir string) (task.Task, string, error) {
	// Extract required fields
	name, err := requireString(input, "name")
	if err != nil {
		return task.Task{}, "", err
	}
	description, err := requireString(input, "description")
	if err != nil {
		return task.Task{}, "", err
	}
	prompt, err := requireString(input, "prompt")
	if err != nil {
		return task.Task{}, "", err
	}
	runMode, err := requireString(input, "run_mode")
	if err != nil {
		return task.Task{}, "", err
	}

	// Validate name format
	if !namePattern.MatchString(name) {
		return task.Task{}, "", fmt.Errorf("invalid task name %q: must be lowercase kebab-case (e.g. 'health-check')", name)
	}

	// Validate run_mode
	if runMode != "scheduled" && runMode != "immediate" {
		return task.Task{}, "", fmt.Errorf("invalid run_mode %q: must be 'scheduled' or 'immediate'", runMode)
	}

	// Build the task
	t := task.Task{
		Name:        name,
		Description: description,
		Prompt:      prompt,
		RunMode:     task.RunMode(runMode),
	}

	// Optional fields
	if schedule, ok := input["schedule"].(string); ok && schedule != "" {
		if runMode != "scheduled" {
			return task.Task{}, "", fmt.Errorf("schedule field is only valid for run_mode 'scheduled'")
		}
		t.Schedule = schedule
	} else if runMode == "scheduled" {
		return task.Task{}, "", fmt.Errorf("scheduled tasks require a 'schedule' field (e.g. '30m', '1h', '24h')")
	}

	if model, ok := input["model"].(string); ok && model != "" {
		t.Model = model
	}

	if memoryDir, ok := input["memory_dir"].(string); ok && memoryDir != "" {
		t.MemoryDir = memoryDir
	}

	// Check for name collisions
	outPath := filepath.Join(tasksDir, name+".json")
	if _, err := os.Stat(outPath); err == nil {
		return task.Task{}, "", fmt.Errorf("task %q already exists at %s", name, outPath)
	}

	// Ensure tasks directory exists
	if err := os.MkdirAll(tasksDir, 0755); err != nil {
		return task.Task{}, "", fmt.Errorf("failed to create tasks directory: %w", err)
	}

	// Write the task JSON
	data, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return task.Task{}, "", fmt.Errorf("failed to marshal task: %w", err)
	}

	if err := os.WriteFile(outPath, data, 0644); err != nil {
		return task.Task{}, "", fmt.Errorf("failed to write task file: %w", err)
	}

	return t, outPath, nil
}

// requireString extracts a required string field from the input map.
func requireString(input map[string]interface{}, field string) (string, error) {
	val, ok := input[field].(string)
	if !ok || val == "" {
		return "", fmt.Errorf("missing or invalid required field %q", field)
	}
	return val, nil
}
