package routines

import (
	"context"
	"github.com/caiorcferreira/goscript/internal/pipeline"
	"time"
)

type DebounceRoutine struct {
	routine      pipeline.Routine
	debounceTime time.Duration
}

func Debounce(debounceTime time.Duration) DebounceRoutine {
	return DebounceRoutine{
		debounceTime: debounceTime,
	}
}

func (p DebounceRoutine) Start(ctx context.Context, pipe pipeline.Pipe) error {
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
