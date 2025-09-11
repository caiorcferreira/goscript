package interpreter

import "time"

// ExecutionOption defines a function that modifies ExecutionConfig
// for flexible configuration.
type ExecutionOption func(Routine) Routine

// WithConcurrency sets the concurrency level for ExecutionConfig.
func WithConcurrency(concurrency int) ExecutionOption {
	return func(r Routine) Routine {
		return NewParallel(r, concurrency)
	}
}

func WithDebounce(debounce time.Duration) ExecutionOption {
	return func(r Routine) Routine {
		return NewDebounce(r, debounce)
	}
}

type ExecutionConfig struct {
	Concurrency  int
	DebounceTime time.Duration
}

func ApplyExecutionOptions(r Routine, opts ...ExecutionOption) Routine {
	for _, opt := range opts {
		r = opt(r)
	}

	return r
}
