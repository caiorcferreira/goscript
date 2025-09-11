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
		//subpipes[i].SetInChan(pipe.In())
	}

	var wg sync.WaitGroup
	wg.Add(p.maxConcurrency)

	// start worker goroutines
	for i := 0; i < p.maxConcurrency; i++ {
		go func() {
			p.routine.Run(ctx, subpipes[i])
			wg.Done()
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

	// fan-in from subpipes to output
	for _, sp := range subpipes {
		go func() {
			//defer sp.Close()
			//defer func() {
			//	close(sp.Out())
			//}()

			for data := range sp.Out() {
				select {
				case <-ctx.Done():
					return
				case pipe.Out() <- data:
				}
			}
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
