# Ductwork

A Go-based platform for running AI agents on schedules with tasks, skills, and persistent memory. Built on the Anthropic Claude API and Go's concurrency primitives.

## Architecture

```
                  ┌──────────────────────────────────────────────────┐
  ductwork start  │                     MAIN                         │
  ─────────────►  │  EnsureDir → Load config → Create agent → Boot   │
                  └────┬──────────────┬──────────────┬───────────────┘
                       │              │              │
               ┌───────▼──────┐  ┌────▼──────────┐  ┌▼──────────────┐
               │  SCHEDULER   │  │ ORCHESTRATOR   │  │  HTTP API     │
               │  goroutine   │  │ goroutine      │  │  :8080        │
               │              │  │                │  │               │
               │  Min-heap    │  │ Reads taskChan │  │ GET /tasks    │
               │  of tasks    │  │ Spawns agents  │  │ POST /run     │
               │  by NextRun  │  │                │  │ POST /spawn   │
               │              │  │ ┌────────────┐ │  │ GET /status   │
               │  Timer fires │  │ │go RunTask()│ │  │               │
               │  ──push──►───┼──┼►│ goroutine  │ │  │  Calls into   │
               │   taskChan   │  │ └────────────┘ │  │  orchestrator │
               │              │  │                │  │  & scheduler  │
               │  addChan ◄───┼──┼── runtime adds │  │───────────►───┤
               └──────────────┘  └────────────────┘  └───────────────┘
                                         ▲                   ▲
  ductwork run <task>                    │                   │
  ──────────── RunImmediate(t) ─────────┘                   │
                                                            │
  ductwork spawn "..."                                      │
  ──────────── SpawnAdhoc(prompt) ──────────────────────────┘
                                                            │
  Future UI / External Systems                              │
  ──────────── HTTP requests ───────────────────────────────┘
```

**Three goroutines** coordinate via channels:

- **Scheduler** — min-heap priority queue sorted by next run time. Sets a timer for the soonest task, fires it to the task channel, reschedules, and repeats.
- **Orchestrator** — reads from the task channel, spawns an agent goroutine per task. Includes concurrency limits via semaphore and retry with exponential backoff for transient errors.
- **API** — REST endpoints for triggering tasks, spawning ad-hoc agents, inspecting scheduler state, and viewing run history.

## Install

### Option 1: `go install` (recommended)

```bash
go install github.com/dneil5648/ductwork/cmd/ductwork@latest
```

This puts the `ductwork` binary in your `$GOPATH/bin` (or `$HOME/go/bin`). Make sure that's in your `$PATH`.

### Option 2: Build from source

```bash
git clone https://github.com/dneil5648/ductwork.git
cd ductwork
go build -o ductwork ./cmd/ductwork
```

### Prerequisites

- Go 1.23+
- An [Anthropic API key](https://console.anthropic.com/)

### Quick Start

```bash
# Set your API key
export ANTHROPIC_API_KEY="sk-ant-..."

# Initialize the .agent/ directory (also auto-created on first run)
ductwork init

# Ad-hoc task with a raw prompt
ductwork spawn "Create a file called hello.txt with 'hello world' in it"

# Run a defined task
ductwork run hello-world

# Build a task from a description
ductwork build "Monitor Bitcoin news every hour"

# Start the full system (scheduler + orchestrator + API)
ductwork start
```

## CLI

```
ductwork                          # prints help
ductwork init                     # creates .agent/ directory with default config
ductwork start                    # starts scheduler + orchestrator + API server
ductwork run <task-name>          # runs a defined task immediately
ductwork spawn "do something"     # runs an ad-hoc agent with a raw prompt
ductwork build "description"      # creates a task definition using an AI agent
ductwork list                     # lists all defined tasks
ductwork history [task-name]      # shows recent run history
```

### `ductwork start`

Boots the scheduler, orchestrator, and HTTP API as a long-running process. Loads all task definitions from `.agent/tasks/`, feeds scheduled ones into the min-heap, and begins firing them on interval.

Graceful shutdown via `Ctrl+C` (`SIGINT`) or `SIGTERM`.

### `ductwork list`

```
NAME                 RUN MODE     SCHEDULE   DESCRIPTION
----                 --------     --------   -----------
example-scheduled    scheduled    30m        Example scheduled task that runs every 30 minutes
hello-world          immediate    -          A simple test task that creates a file and reads it back
```

### `ductwork history`

```
RUN ID                               TASK                 STATUS     DURATION     IN TOK  OUT TOK ERROR
------                               ----                 ------     --------     ------  ------- -----
hello-world-1709312400000            hello-world          completed  3.2s         1024    256
example-scheduled-1709312100000      example-scheduled    failed     1.5s         512     128     connection refused...
```

## REST API

Starts automatically on `ductwork start`. Default port: `8080` (configurable in `.agent/config.json`).

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/health` | Health check |
| `GET` | `/api/tasks` | List all task definitions |
| `GET` | `/api/tasks/{name}` | Get a specific task |
| `POST` | `/api/tasks/{name}/run` | Trigger a task immediately |
| `POST` | `/api/spawn` | Ad-hoc agent — body: `{"prompt": "..."}` |
| `GET` | `/api/scheduler/status` | Scheduled tasks with next run times |
| `POST` | `/api/scheduler/add` | Add a task to the scheduler at runtime |
| `GET` | `/api/runs` | Recent run history (last 50) |
| `GET` | `/api/runs/{task-name}` | Run history for a specific task |

### Examples

```bash
# Health check
curl localhost:8080/api/health

# List tasks
curl localhost:8080/api/tasks

# Run a task
curl -X POST localhost:8080/api/tasks/hello-world/run

# Spawn ad-hoc
curl -X POST localhost:8080/api/spawn \
  -H "Content-Type: application/json" \
  -d '{"prompt": "What is 2+2?"}'

# Check scheduler
curl localhost:8080/api/scheduler/status

# View run history
curl localhost:8080/api/runs
```

## `.agent/` Directory

Auto-created on first boot. All task paths are relative to this root.

```
.agent/
├── config.json        # Global config (model, system prompt, ports, paths)
├── security.json      # Security rules (tool whitelist, path boundaries, bash filters)
├── dependencies.json  # Runtime dependency declarations
├── tools.json         # Agent tool definitions
├── tasks/             # Task definitions (JSON)
├── skills/            # Reusable skill files injected into system prompts
├── memory/            # Per-task persistent memory across runs
├── scripts/           # Agent-created scripts (global, reusable)
├── logs/              # Execution logs (structured JSON + text)
└── history/           # Run history records (one JSON file per run)
```

### `config.json`

```json
{
  "default_model": "claude-sonnet-4-6",
  "system_prompt": "You are an autonomous AI agent with access to...",
  "tasks_dir": "tasks",
  "skills_dir": "skills",
  "memory_dir": "memory",
  "logs_dir": "logs",
  "scripts_dir": "scripts",
  "history_dir": "history",
  "api_port": 8080,
  "max_concurrent": 5,
  "default_max_retries": 2,
  "default_retry_backoff": "2s"
}
```

## Task Definitions

Tasks are JSON files in `.agent/tasks/`. Two run modes:

- **`scheduled`** — runs on a recurring interval via the scheduler
- **`immediate`** — runs on demand via CLI or API

### Example: Scheduled Task

```json
{
  "name": "example-scheduled",
  "description": "Runs every 30 minutes",
  "prompt": "Check the current time and write it to a log file.",
  "skills": {},
  "memory_dir": "memory/example-scheduled",
  "run_mode": "scheduled",
  "model": "",
  "schedule": "30m"
}
```

### Example: Immediate Task

```json
{
  "name": "hello-world",
  "description": "A simple test task",
  "prompt": "Create hello.txt with 'hello world', read it back, save summary to memory.",
  "skills": {},
  "memory_dir": "memory/hello-world",
  "run_mode": "immediate",
  "model": "",
  "schedule": ""
}
```

### Task Fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Unique identifier (kebab-case) |
| `description` | string | Human-readable description |
| `prompt` | string | The instruction sent to the agent |
| `skills` | map[string]string | Skill name → file path (loaded into system prompt) |
| `memory_dir` | string | Directory for persistent memory across runs |
| `run_mode` | string | `"scheduled"` or `"immediate"` |
| `model` | string | Model override (empty = use default from config) |
| `schedule` | string | Go duration string: `"30m"`, `"1h"`, `"24h"` |
| `max_retries` | int | Override default retry count (0 = use config default) |
| `retry_backoff` | string | Override base backoff duration (e.g. `"5s"`) |

## Skills & Memory

### Skills

Skills are files (markdown, text, etc.) that get pre-loaded into the agent's system prompt before execution. This avoids wasting API calls on file discovery.

```json
{
  "skills": {
    "deploy": "skills/deploy-to-fly-io.md",
    "parse-csv": "skills/parse-csv.md"
  }
}
```

### Memory

Each task can have a persistent memory directory. On each run:

1. All files in the memory directory are loaded and prepended to the user message
2. The agent is told its memory directory path in the system prompt
3. The agent can write files there via `write_file` to persist information for future runs

Memory is created automatically on first run.

## Security

Security rules are defined in `.agent/security.json`. They control which tools each task can use, path boundaries for file access, and bash command filters.

```json
{
  "default": {
    "allowed_tools": ["bash", "read_file", "write_file", "create_task", "save_script"],
    "path_whitelist": ["."],
    "bash_blacklist": ["rm -rf /", "sudo"]
  }
}
```

Task-specific overrides can restrict permissions further:

```json
{
  "default": { ... },
  "tasks": {
    "read-only-task": {
      "allowed_tools": ["read_file"],
      "path_whitelist": [".agent/memory"]
    }
  }
}
```

## Agent Tools

The agent has five tools, defined in `.agent/tools.json`:

| Tool | Parameters | Description |
|------|-----------|-------------|
| `bash` | `command` (string) | Execute a bash command |
| `read_file` | `path` (string) | Read file contents |
| `write_file` | `path` (string), `content` (string) | Write content to a file |
| `create_task` | `name`, `description`, `prompt`, `run_mode`, etc. | Create a new task definition |
| `save_script` | `filename` (string), `content` (string) | Save a reusable script to scripts/ |

## Project Structure

```
ductwork/
├── cmd/
│   └── ductwork/
│       └── main.go              # Cobra CLI entry point (go install target)
├── pkg/
│   ├── agent/
│   │   ├── agent.go             # Core agent runtime (Spawn, RunTask, runLoop)
│   │   └── tools.json           # Tool definitions (embedded via //go:embed)
│   ├── tasks/
│   │   └── task.go              # Task struct, loaders, skill/memory pre-loading
│   ├── scheduler/
│   │   └── scheduler.go         # Min-heap priority queue scheduler
│   ├── orchestrator/
│   │   ├── orchestrator.go      # Task channel consumer, agent spawner, history, retry
│   │   └── retry.go             # Error classification, exponential backoff
│   ├── config/
│   │   ├── config.go            # .agent/ auto-init, config loading, path resolution
│   │   └── default_tools.json   # Default tools.json (embedded for bootstrap)
│   ├── api/
│   │   └── api.go               # HTTP REST API
│   ├── security/
│   │   └── security.go          # Enforcer, tool whitelist, path boundaries
│   ├── dependencies/
│   │   └── dependencies.go      # Runtime dependency config
│   ├── history/
│   │   └── history.go           # Run history store (FileStore)
│   ├── logging/
│   │   └── logging.go           # Structured logging (slog, dual-handler)
│   └── taskbuilder/
│       └── taskbuilder.go       # Task validation and creation
├── .agent/                      # Runtime directory (auto-created)
├── go.mod
└── README.md
```

## Dependencies

| Package | Purpose |
|---------|---------|
| [anthropic-sdk-go](https://github.com/anthropics/anthropic-sdk-go) v1.26.0 | Claude API client |
| [cobra](https://github.com/spf13/cobra) v1.10.2 | CLI framework |
| Go standard library | `container/heap`, `net/http`, `context`, `os/exec`, `encoding/json`, `log/slog` |
