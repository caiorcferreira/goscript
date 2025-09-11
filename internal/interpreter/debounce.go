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
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pipe.Done():
			return nil
		case msg := <-pipe.In():
			time.Sleep(p.debounceTime)
			pipe.Out() <- msg
		}
	}
}
