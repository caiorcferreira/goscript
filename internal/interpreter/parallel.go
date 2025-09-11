package interpreter

import (
	"context"
	"sync"
)

type Parallel struct {
	routine        Routine
	maxConcurrency int
}

func NewParallel(routine Routine, maxConcurrency int) Parallel {
	return Parallel{
		routine:        routine,
		maxConcurrency: maxConcurrency,
	}
}

func (p Parallel) Run(ctx context.Context, pipe Pipe) error {
	var wg sync.WaitGroup

	wg.Add(p.maxConcurrency)

	for i := 0; i < p.maxConcurrency; i++ {
		go func() {
			p.routine.Run(ctx, pipe)
			wg.Done()
		}()
	}

	wg.Wait()

	return nil
}
