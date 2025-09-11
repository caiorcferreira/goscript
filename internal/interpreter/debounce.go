package interpreter

import (
	"context"
	"time"
)

type Debounce struct {
	routine      Routine
	debounceTime time.Duration
}

func NewDebounce(routine Routine, debounceTime time.Duration) Debounce {
	return Debounce{
		routine:      routine,
		debounceTime: debounceTime,
	}
}

func (p Debounce) Run(ctx context.Context, pipe Pipe) error {
	slowPipe := NewChanPipe()
	slowPipe.SetOutChan(pipe.Out())

	go p.routine.Run(ctx, slowPipe)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pipe.Done():
			return nil
		case msg, open := <-pipe.In():
			if !open {
				return nil
			}

			time.Sleep(p.debounceTime)
			slowPipe.In() <- msg
		}
	}
}
