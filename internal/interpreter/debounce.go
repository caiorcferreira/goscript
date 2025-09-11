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

func NewDebounce(routine Routine, debounceTime time.Duration) DebounceRoutine {
	return DebounceRoutine{
		routine:      routine,
		debounceTime: debounceTime,
	}
}

func (p DebounceRoutine) Run(ctx context.Context, pipe Pipe) error {
	//slowPipe := NewChanPipe()
	//slowPipe.SetOutChan(pipe.Out())
	//
	//pipe.Chain(slowPipe)
	//slowPipe.Chain(pipe)

	//defer slowPipe.Close()
	defer pipe.Close()
	defer func() {
		close(pipe.Out())
	}()
	//
	//go p.routine.Run(ctx, slowPipe)

	for msg := range pipe.In() {
		time.Sleep(p.debounceTime)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case pipe.Out() <- msg:
		}
	}

	return nil

	//for {
	//	select {
	//	case <-ctx.Done():
	//		return ctx.Err()
	//	case <-pipe.Done():
	//		return nil
	//	case msg, open := <-pipe.In():
	//		if !open {
	//			continue
	//			//return nil
	//		}
	//
	//		time.Sleep(p.debounceTime)
	//		slowPipe.In() <- msg
	//	default:
	//		// no data available
	//	}
	//}
}
