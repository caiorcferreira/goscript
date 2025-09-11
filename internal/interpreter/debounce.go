package interpreter

import (
	"context"
	"time"
)

type DebounceRoutine struct {
	routine      Routine
	debounceTime time.Duration
}

func Debounce(debounceTime time.Duration) DebounceRoutine {
	return DebounceRoutine{
		debounceTime: debounceTime,
	}
}

func (p DebounceRoutine) Run(ctx context.Context, pipe Pipe) error {
	defer pipe.Close()

	for msg := range pipe.In() {
		time.Sleep(p.debounceTime)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case pipe.Out() <- msg:
		}
	}

	return nil
}
