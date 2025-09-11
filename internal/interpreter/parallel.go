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
	defer pipe.Close()

	subpipes := make([]*ChannelPipe, p.maxConcurrency)
	for i := 0; i < p.maxConcurrency; i++ {
		subpipes[i] = NewChanPipe()
		subpipes[i].SetInChan(pipe.In())
	}

	go func() {
		defer func() {
			for _, sp := range subpipes {
				sp.Close()
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case <-pipe.Done():
				return
			default:
				for _, sp := range subpipes {
					select {
					case data, open := <-sp.Out():
						if !open {
							continue
						}
						pipe.Out() <- data
					default:
						// no data available, move to the next subpipe
					}
				}
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(p.maxConcurrency)

	for i := 0; i < p.maxConcurrency; i++ {
		go func() {
			p.routine.Run(ctx, subpipes[i])
			wg.Done()
		}()
	}

	wg.Wait()

	return nil
}
