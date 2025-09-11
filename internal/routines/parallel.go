package routines

import (
	"context"
	"github.com/caiorcferreira/goscript/internal/interpreter"
	"sync"
)

type ParallelRoutine struct {
	routine        interpreter.Routine
	maxConcurrency int
}

func Parallel(r interpreter.Routine, maxConcurrency int) ParallelRoutine {
	return ParallelRoutine{
		routine:        r,
		maxConcurrency: maxConcurrency,
	}
}

func (p ParallelRoutine) Run(ctx context.Context, pipe interpreter.Pipe) error {
	defer pipe.Close()

	subpipes := make([]*interpreter.ChannelPipe, p.maxConcurrency)
	for i := 0; i < p.maxConcurrency; i++ {
		subpipes[i] = interpreter.NewChanPipe()
	}

	var wg sync.WaitGroup
	wg.Add(p.maxConcurrency)

	// fan-in from subpipes to output
	for _, sp := range subpipes {
		go func() {
			// we need to wait until all subpipes are drained
			defer func() {
				wg.Done()
			}()

			for data := range sp.Out() {
				select {
				case <-ctx.Done():
					return
				case pipe.Out() <- data:
				}
			}
		}()
	}

	// fan-out input to subpipes
	go func() {
		defer func() {
			for _, sp := range subpipes {
				close(sp.In())
			}
		}()

		roundRobinIndex := 0

		for data := range pipe.In() {
			select {
			case <-ctx.Done():
				return
			default:
				// trie to send msg to subpipe at roundRobinIndex
				// if it fails, try the next one in round-robin fashion
				// it will keep trying until it succeeds
				for {
					sent := false
					select {
					case <-ctx.Done():
						return
					case subpipes[roundRobinIndex].In() <- data:
						// data sent successfully
						sent = true
					default:
						sent = false
					}

					roundRobinIndex = (roundRobinIndex + 1) % p.maxConcurrency

					if sent {
						break
					}
				}
			}
		}
	}()

	// start worker goroutines
	for i := 0; i < p.maxConcurrency; i++ {
		go func() {
			p.routine.Run(ctx, subpipes[i])
		}()
	}

	wg.Wait()

	return nil
}
