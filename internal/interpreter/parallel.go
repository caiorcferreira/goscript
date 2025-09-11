package interpreter

import (
	"context"
	"sync"
)

type ParallelRoutine struct {
	routine        Routine
	maxConcurrency int
}

func Parallel(r Routine, maxConcurrency int) ParallelRoutine {
	return ParallelRoutine{
		routine:        r,
		maxConcurrency: maxConcurrency,
	}
}

func NewParallel(routine Routine, maxConcurrency int) ParallelRoutine {
	return ParallelRoutine{
		routine:        routine,
		maxConcurrency: maxConcurrency,
	}
}

func (p ParallelRoutine) Run(ctx context.Context, pipe Pipe) error {
	defer pipe.Close()
	defer func() {
		close(pipe.Out())
	}()

	subpipes := make([]*ChannelPipe, p.maxConcurrency)
	for i := 0; i < p.maxConcurrency; i++ {
		subpipes[i] = NewChanPipe()
		//subpipes[i].SetInChan(pipe.In())
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

		send := func(pipe *ChannelPipe, data any) bool {
			select {
			case <-ctx.Done():
				return false
			case pipe.In() <- data:
				// data sent successfully
				return true
			default:
				return false
			}
		}

		for data := range pipe.In() {
			select {
			case <-ctx.Done():
				return
			default:
				// send data to the first available subpipe
				for _, sp := range subpipes {
					if send(sp, data) {
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
			//wg.Done()
		}()
	}

	//go func() {
	//	defer func() {
	//		for _, sp := range subpipes {
	//			sp.Close()
	//		}
	//	}()
	//
	//	for {
	//		select {
	//		case <-ctx.Done():
	//			return
	//		case <-pipe.Done():
	//			return
	//		default:
	//			for _, sp := range subpipes {
	//				select {
	//				case data, open := <-sp.Out():
	//					if !open {
	//						continue
	//					}
	//					pipe.Out() <- data
	//				default:
	//					// no data available, move to the next subpipe
	//				}
	//			}
	//		}
	//	}
	//}()

	wg.Wait()

	return nil
}
