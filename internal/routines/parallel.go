package routines

import (
	"context"
	"github.com/caiorcferreira/goscript/internal/interpreter"
)

type ParallelConfig struct {
	MaxInFlight int
}

type Parallel struct {
	routine interpreter.Routine
	config  ParallelConfig

	semaphore chan struct{}
}

func NewParallel(routine interpreter.Routine, config ParallelConfig) *Parallel {
	return &Parallel{
		routine:   routine,
		config:    config,
		semaphore: make(chan struct{}, config.MaxInFlight),
	}
}

func (p *Parallel) Run(ctx context.Context, pipe interpreter.Pipe) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case p.semaphore <- struct{}{}: // Acquire a slot, block if full
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
			//case data := <-pipe.In():
			//go p.routine.Run()
		}
	}
}
