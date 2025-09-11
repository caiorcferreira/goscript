package interpreter

import (
	"context"
	"sync"
)

// ParallelOption defines a function that modifies ParallelConfig
// for flexible configuration.
type ParallelOption func(*ParallelConfig)

// WithConcurrency sets the concurrency level for ParallelConfig.
func WithConcurrency(concurrency int) ParallelOption {
	return func(cfg *ParallelConfig) {
		cfg.Concurrency = concurrency
	}
}

type ParallelConfig struct {
	Concurrency int
}

type Parallel struct {
	routine Routine
	config  ParallelConfig
}

func NewParallel(routine Routine, config ParallelConfig) *Parallel {
	return &Parallel{
		routine: routine,
		config:  config,
	}
}

func (p *Parallel) Run(ctx context.Context, pipe Pipe) error {
	var wg sync.WaitGroup

	wg.Add(p.config.Concurrency)

	for i := 0; i < p.config.Concurrency; i++ {
		go func() {
			p.routine.Run(ctx, pipe)
			wg.Done()
		}()
	}

	wg.Wait()

	return nil
}
