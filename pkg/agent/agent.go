package agent

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/dneil5648/ductwork/pkg/logging"
	"github.com/dneil5648/ductwork/pkg/security"
	"github.com/dneil5648/ductwork/pkg/taskbuilder"
	task "github.com/dneil5648/ductwork/pkg/tasks"
	"github.com/anthropics/anthropic-sdk-go"
)

//go:embed tools.json
var defaultToolsJSON []byte

// Tool types — these mirror the structure in tools.json

type Property struct {
	Type        string `json:"type"`
	Description string `json:"description"`
}

type InputSchema struct {
	Type       string              `json:"type"`
	Properties map[string]Property `json:"properties"`
	Required   []string            `json:"required"`
}

type Tool struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	InputSchema InputSchema `json:"input_schema"`
}

// Agent is the core runtime that talks to the Anthropic API and executes tools

type Agent struct {
	SystemPrompt       string
	Model              string
	Enforcer           *security.Enforcer // nil = no restrictions (backward compat)
	TasksDir           string             // path to .agent/tasks/ for create_task tool
	ScriptsDir         string             // path to .agent/scripts/ for save_script tool
	DependenciesPrompt string             // dependency info injected into system prompt
	ToolsFile          string             // path to .agent/tools.json (empty = use embedded default)
}

// RunResult holds the outcome of an agent execution, including token usage.
type RunResult struct {
	InputTokens  int64
	OutputTokens int64
	Iterations   int
}

// Spawn runs the agent loop with a raw task string.
// Use this for ad-hoc, one-off tasks.
func (a *Agent) Spawn(ctx context.Context, prompt string) (*RunResult, error) {
	runID := fmt.Sprintf("adhoc-%d", time.Now().UnixMilli())
	ctx = logging.NewContext(ctx, "adhoc", runID)
	return a.runLoop(ctx, a.SystemPrompt, prompt)
}

// RunTask runs the agent loop for a defined, repeatable task.
// It loads skill files into the system prompt and memory from previous runs
// into the user message, then delegates to the core loop.
func (a *Agent) RunTask(ctx context.Context, t task.Task) (*RunResult, error) {
	logger := logging.FromContext(ctx)

	// Use task-specific model if provided, otherwise fall back to agent default
	model := a.Model
	if t.Model != "" {
		model = t.Model
	}

	// Compose system prompt: base prompt + skills
	systemPrompt := a.SystemPrompt
	skills, err := t.LoadSkills()
	if err != nil {
		return nil, fmt.Errorf("failed to load skills: %w", err)
	}
	if skills != "" {
		systemPrompt += "\n" + skills
		logger.Debug("loaded skills into prompt")
	}

	// Compose user message: memory context + task prompt
	userMessage := t.Prompt
	memory, err := t.LoadMemory()
	if err != nil {
		return nil, fmt.Errorf("failed to load memory: %w", err)
	}
	if memory != "" {
		userMessage = memory + "\n---\n\n" + t.Prompt
		logger.Debug("loaded memory from previous runs")
	}

	// Tell the agent where its memory dir is so it can write to it
	if t.MemoryDir != "" {
		systemPrompt += fmt.Sprintf("\n\nYour memory directory is: %s\nYou can write files there to persist information for future runs of this task.", t.MemoryDir)
	}

	// Temporarily override model if task specifies one
	originalModel := a.Model
	a.Model = model
	defer func() { a.Model = originalModel }()

	return a.runLoop(ctx, systemPrompt, userMessage)
}

// runLoop is the core agent loop. It sends the task to Claude, executes any
// tool calls, feeds results back, and repeats until Claude responds with just text.
func (a *Agent) runLoop(ctx context.Context, systemPrompt string, userMessage string) (*RunResult, error) {
	logger := logging.FromContext(ctx)

	// Inject dependency info into system prompt if available
	if a.DependenciesPrompt != "" {
		systemPrompt += "\n" + a.DependenciesPrompt
	}

	// Inject available scripts into system prompt
	if a.ScriptsDir != "" {
		scripts := loadScriptsList(a.ScriptsDir)
		if scripts != "" {
			systemPrompt += "\n" + scripts
		}
	}

	// Load tool definitions — prefer ToolsFile if set, fall back to embedded default
	toolsData := defaultToolsJSON
	if a.ToolsFile != "" {
		fileData, err := os.ReadFile(a.ToolsFile)
		if err != nil {
			logger.Warn("failed to read tools file, using embedded default", "path", a.ToolsFile, "error", err)
		} else {
			toolsData = fileData
		}
	}

	var tools []Tool
	if err := json.Unmarshal(toolsData, &tools); err != nil {
		return nil, fmt.Errorf("failed to load tools: %w", err)
	}

	// Convert our tool definitions to SDK tool params
	sdkTools := make([]anthropic.ToolUnionParam, len(tools))
	for i, t := range tools {
		sdkTools[i] = anthropic.ToolUnionParam{
			OfTool: &anthropic.ToolParam{
				Name:        t.Name,
				Description: anthropic.String(t.Description),
				InputSchema: anthropic.ToolInputSchemaParam{
					Properties: t.InputSchema.Properties,
					Required:   t.InputSchema.Required,
				},
			},
		}
	}

	logger.Info("starting agent loop", "model", a.Model, "tools", len(tools))

	// Initialize the Anthropic client (reads ANTHROPIC_API_KEY from env)
	client := anthropic.NewClient()

	// Start the conversation with the user's task
	messages := []anthropic.MessageParam{
		anthropic.NewUserMessage(anthropic.NewTextBlock(userMessage)),
	}

	// Token usage accumulators
	var totalInputTokens, totalOutputTokens int64

	// Agent loop — runs until the model stops calling tools
	iteration := 0
	for {
		iteration++
		logger.Debug("API call", "iteration", iteration)

		// Call the Anthropic messages API
		response, err := client.Messages.New(ctx, anthropic.MessageNewParams{
			Model:     anthropic.Model(a.Model),
			MaxTokens: 4096,
			System: []anthropic.TextBlockParam{
				{Text: systemPrompt},
			},
			Messages: messages,
			Tools:    sdkTools,
		})
		if err != nil {
			return nil, fmt.Errorf("API error: %w", err)
		}

		// Accumulate token usage
		totalInputTokens += response.Usage.InputTokens
		totalOutputTokens += response.Usage.OutputTokens
		logger.Debug("token usage", "iteration", iteration,
			"input_tokens", response.Usage.InputTokens,
			"output_tokens", response.Usage.OutputTokens)

		// Walk through the response content blocks.
		// Each block is either text (Claude talking) or tool_use (Claude wants to run a tool).
		var assistantBlocks []anthropic.ContentBlockParamUnion
		var toolCalls []anthropic.ToolUseBlock

		for _, block := range response.Content {
			switch v := block.AsAny().(type) {
			case anthropic.TextBlock:
				fmt.Println(v.Text) // user-facing output stays on stdout
				logger.Debug("assistant text", "text", truncate(v.Text, 200))
				assistantBlocks = append(assistantBlocks, anthropic.NewTextBlock(v.Text))
			case anthropic.ToolUseBlock:
				logger.Info("tool call", "tool", v.Name)
				assistantBlocks = append(assistantBlocks, anthropic.NewToolUseBlock(v.ID, v.Input, v.Name))
				toolCalls = append(toolCalls, v)
			}
		}

		// Append the assistant's full response to the conversation history
		messages = append(messages, anthropic.NewAssistantMessage(assistantBlocks...))

		// If there were no tool calls, the agent is done
		if len(toolCalls) == 0 {
			result := &RunResult{
				InputTokens:  totalInputTokens,
				OutputTokens: totalOutputTokens,
				Iterations:   iteration,
			}
			logger.Info("agent loop complete", "iterations", iteration,
				"total_input_tokens", totalInputTokens,
				"total_output_tokens", totalOutputTokens)
			return result, nil
		}

		// Execute each tool call and build tool_result blocks
		var toolResults []anthropic.ContentBlockParamUnion

		for _, call := range toolCalls {
			// Parse the tool input JSON
			var input map[string]interface{}
			if err := json.Unmarshal(call.Input, &input); err != nil {
				toolResults = append(toolResults, anthropic.NewToolResultBlock(
					call.ID, fmt.Sprintf("error parsing input: %v", err), true,
				))
				continue
			}

			// Execute the tool
			result, err := a.executeTool(call.Name, input)
			if err != nil {
				logger.Error("tool failed", "tool", call.Name, "error", err)
				toolResults = append(toolResults, anthropic.NewToolResultBlock(
					call.ID, fmt.Sprintf("error: %v", err), true,
				))
			} else {
				logger.Debug("tool result", "tool", call.Name, "result", truncate(result, 100))
				toolResults = append(toolResults, anthropic.NewToolResultBlock(
					call.ID, result, false,
				))
			}
		}

		// Tool results go back as a "user" message (that's how the API expects it)
		messages = append(messages, anthropic.NewUserMessage(toolResults...))
	}
}

// executeTool dispatches a tool call to the right function based on name.
// Security checks are enforced before each tool execution.
func (a *Agent) executeTool(name string, input map[string]interface{}) (string, error) {
	// Security gate: check tool permission
	if a.Enforcer != nil {
		if err := a.Enforcer.CheckTool(name); err != nil {
			return "", err
		}
	}

	switch name {
	case "bash":
		cmd, ok := input["command"].(string)
		if !ok {
			return "", fmt.Errorf("bash: missing or invalid 'command' field")
		}
		// Security gate: check bash command
		if a.Enforcer != nil {
			if err := a.Enforcer.CheckBashCommand(cmd); err != nil {
				return "", err
			}
		}
		return executeBash(cmd)

	case "read_file":
		path, ok := input["path"].(string)
		if !ok {
			return "", fmt.Errorf("read_file: missing or invalid 'path' field")
		}
		// Security gate: check read path
		if a.Enforcer != nil {
			if err := a.Enforcer.CheckPath(path, "read"); err != nil {
				return "", err
			}
		}
		return readFile(path)

	case "write_file":
		path, ok := input["path"].(string)
		if !ok {
			return "", fmt.Errorf("write_file: missing or invalid 'path' field")
		}
		// Security gate: check write path
		if a.Enforcer != nil {
			if err := a.Enforcer.CheckPath(path, "write"); err != nil {
				return "", err
			}
		}
		content, ok := input["content"].(string)
		if !ok {
			return "", fmt.Errorf("write_file: missing or invalid 'content' field")
		}
		err := writeFile(path, content)
		if err != nil {
			return "", err
		}
		return "file written successfully", nil

	case "create_task":
		return a.executeCreateTask(input)

	case "save_script":
		return a.executeSaveScript(input)

	default:
		return "", fmt.Errorf("unknown tool: %s", name)
	}
}

// executeCreateTask delegates to the taskbuilder package to validate and create
// a new task definition file.
func (a *Agent) executeCreateTask(input map[string]interface{}) (string, error) {
	if a.TasksDir == "" {
		return "", fmt.Errorf("create_task: tasks directory not configured")
	}

	t, path, err := taskbuilder.ValidateAndCreate(input, a.TasksDir)
	if err != nil {
		return "", fmt.Errorf("create_task: %w", err)
	}

	return fmt.Sprintf("Task %q created at %s (run_mode: %s)", t.Name, path, t.RunMode), nil
}

// executeSaveScript writes a reusable script to the scripts directory with
// executable permissions. Prepends a description comment header if provided.
func (a *Agent) executeSaveScript(input map[string]interface{}) (string, error) {
	if a.ScriptsDir == "" {
		return "", fmt.Errorf("save_script: scripts directory not configured")
	}

	name, ok := input["name"].(string)
	if !ok || name == "" {
		return "", fmt.Errorf("save_script: missing or invalid 'name' field")
	}

	content, ok := input["content"].(string)
	if !ok || content == "" {
		return "", fmt.Errorf("save_script: missing or invalid 'content' field")
	}

	// Prepend description as comment if provided
	if desc, ok := input["description"].(string); ok && desc != "" {
		content = "# " + desc + "\n\n" + content
	}

	// Ensure scripts directory exists
	if err := os.MkdirAll(a.ScriptsDir, 0755); err != nil {
		return "", fmt.Errorf("save_script: failed to create scripts dir: %w", err)
	}

	scriptPath := filepath.Join(a.ScriptsDir, name)
	if err := os.WriteFile(scriptPath, []byte(content), 0755); err != nil {
		return "", fmt.Errorf("save_script: failed to write script: %w", err)
	}

	return fmt.Sprintf("Script saved to %s (executable)", scriptPath), nil
}

// Tool implementations

func executeBash(command string) (string, error) {
	cmd := exec.Command("bash", "-c", command)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), err
	}
	return string(output), nil
}

func readFile(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0644)
}

// truncate shortens a string for log output
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// loadScriptsList reads the scripts directory and returns a formatted list
// of available scripts for injection into the system prompt.
// Returns empty string if no scripts exist or directory doesn't exist.
func loadScriptsList(scriptsDir string) string {
	entries, err := os.ReadDir(scriptsDir)
	if err != nil {
		return ""
	}

	if len(entries) == 0 {
		return ""
	}

	var result string
	result = "\n## Available Scripts\n\n"
	result += fmt.Sprintf("Scripts directory: %s\n\n", scriptsDir)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		// Read first line to check for description comment
		content, err := os.ReadFile(filepath.Join(scriptsDir, entry.Name()))
		if err != nil {
			result += fmt.Sprintf("- `%s`\n", entry.Name())
			continue
		}

		// Extract description from comment header (first line starting with #)
		lines := string(content)
		desc := ""
		for _, line := range splitLines(lines) {
			if len(line) > 2 && line[0] == '#' && line[1] == ' ' {
				desc = line[2:]
				break
			}
			break // only check first line
		}

		if desc != "" {
			result += fmt.Sprintf("- `%s` — %s\n", entry.Name(), desc)
		} else {
			result += fmt.Sprintf("- `%s`\n", entry.Name())
		}
	}

	return result
}

// splitLines splits a string into lines.
func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}
