package scheduler

import (
	"container/heap"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	task "github.com/dneil5648/ductwork/pkg/tasks"
)

// ScheduledTask wraps a task with its scheduling metadata
type ScheduledTask struct {
	Task     task.Task
	Interval time.Duration
	NextRun  time.Time
}

// TaskQueue is a min-heap of scheduled tasks, sorted by NextRun time.
// The soonest task to fire is always at index 0.
type TaskQueue []*ScheduledTask

// heap.Interface implementation

func (tq TaskQueue) Len() int           { return len(tq) }
func (tq TaskQueue) Less(i, j int) bool { return tq[i].NextRun.Before(tq[j].NextRun) }
func (tq TaskQueue) Swap(i, j int)      { tq[i], tq[j] = tq[j], tq[i] }

func (tq *TaskQueue) Push(x any) {
	*tq = append(*tq, x.(*ScheduledTask))
}

func (tq *TaskQueue) Pop() any {
	old := *tq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*tq = old[:n-1]
	return item
}

// Scheduler manages a priority queue of tasks and fires them on schedule.
// It pushes due tasks to the taskChan for the orchestrator to consume.
type Scheduler struct {
	queue    TaskQueue
	mu       sync.Mutex // protects queue for Status() reads while Run() is active
	taskChan chan<- task.Task // write-only ref to orchestrator's channel
	addChan  chan task.Task   // for adding new scheduled tasks at runtime
}

// NewScheduler creates a scheduler that pushes due tasks to the given channel.
func NewScheduler(taskChan chan<- task.Task) *Scheduler {
	s := &Scheduler{
		queue:    make(TaskQueue, 0),
		taskChan: taskChan,
		addChan:  make(chan task.Task, 10),
	}
	heap.Init(&s.queue)
	return s
}

// LoadTasks bulk-loads task definitions at startup.
// Filters for scheduled tasks, parses their Schedule field, and pushes to the heap.
func (s *Scheduler) LoadTasks(tasks []task.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, t := range tasks {
		if t.RunMode != task.RunModeScheduled {
			continue
		}
		if t.Schedule == "" {
			return fmt.Errorf("scheduled task %q has no schedule defined", t.Name)
		}

		interval, err := time.ParseDuration(t.Schedule)
		if err != nil {
			return fmt.Errorf("invalid schedule %q for task %q: %w", t.Schedule, t.Name, err)
		}

		st := &ScheduledTask{
			Task:     t,
			Interval: interval,
			NextRun:  time.Now().Add(interval),
		}
		heap.Push(&s.queue, st)
		slog.Info("loaded scheduled task", "module", "scheduler", "task", t.Name, "interval", interval)
	}

	return nil
}

// AddTask adds a new scheduled task at runtime (thread-safe).
// Called by the API or orchestrating agent.
func (s *Scheduler) AddTask(t task.Task) {
	s.addChan <- t
}

// Status returns a snapshot of all scheduled tasks and their next run times.
// Safe to call while the scheduler is running.
func (s *Scheduler) Status() []ScheduledTask {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot := make([]ScheduledTask, len(s.queue))
	for i, st := range s.queue {
		snapshot[i] = *st
	}
	return snapshot
}

// Run is the main scheduler loop. It should be run as a goroutine.
// It sets a timer for the soonest task and fires when due.
// It also listens for new tasks on addChan and shutdown on ctx.Done().
func (s *Scheduler) Run(ctx context.Context) {
	slog.Info("scheduler started", "module", "scheduler")

	for {
		s.mu.Lock()
		queueLen := s.queue.Len()
		s.mu.Unlock()

		if queueLen == 0 {
			// No tasks — block until one is added or shutdown
			select {
			case newTask := <-s.addChan:
				s.handleAdd(newTask)
			case <-ctx.Done():
				slog.Info("scheduler shutting down", "module", "scheduler")
				return
			}
			continue
		}

		// Peek at the soonest task
		s.mu.Lock()
		soonest := s.queue[0]
		s.mu.Unlock()

		waitDuration := time.Until(soonest.NextRun)
		if waitDuration < 0 {
			waitDuration = 0
		}

		timer := time.NewTimer(waitDuration)

		select {
		case <-timer.C:
			// Timer fired — pop the task, push to orchestrator, reschedule
			s.mu.Lock()
			fired := heap.Pop(&s.queue).(*ScheduledTask)
			s.mu.Unlock()

			slog.Info("firing task", "module", "scheduler", "task", fired.Task.Name)
			s.taskChan <- fired.Task

			// Reschedule: set next run and push back
			fired.NextRun = time.Now().Add(fired.Interval)
			s.mu.Lock()
			heap.Push(&s.queue, fired)
			s.mu.Unlock()

		case newTask := <-s.addChan:
			timer.Stop()
			s.handleAdd(newTask)

		case <-ctx.Done():
			timer.Stop()
			slog.Info("scheduler shutting down", "module", "scheduler")
			return
		}
	}
}

// handleAdd parses a new task's schedule and adds it to the heap.
func (s *Scheduler) handleAdd(t task.Task) {
	if t.Schedule == "" {
		slog.Warn("task has no schedule, skipping", "module", "scheduler", "task", t.Name)
		return
	}

	interval, err := time.ParseDuration(t.Schedule)
	if err != nil {
		slog.Error("failed to parse schedule", "module", "scheduler", "task", t.Name, "schedule", t.Schedule, "error", err)
		return
	}

	st := &ScheduledTask{
		Task:     t,
		Interval: interval,
		NextRun:  time.Now().Add(interval),
	}

	s.mu.Lock()
	heap.Push(&s.queue, st)
	s.mu.Unlock()

	slog.Info("added task", "module", "scheduler", "task", t.Name, "interval", interval)
}
