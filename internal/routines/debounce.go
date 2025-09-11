package routines

import (
	"context"
	"github.com/caiorcferreira/goscript/internal/interpreter"
	"time"
)

type DebounceRoutine struct {
	routine      interpreter.Routine
	debounceTime time.Duration
}

func Debounce(debounceTime time.Duration) DebounceRoutine {
	return DebounceRoutine{
		debounceTime: debounceTime,
	}
}

func (p DebounceRoutine) Run(ctx context.Context, pipe interpreter.Pipe) error {
	defer pipe.Close()

	for msg := range pipe.In() {
		time.Sleep(p.debounceTime)

		select {
		case <-ctx.Done():
			return nil
		case pipe.Out() <- msg:
		}
	}

	return nil
}
