package pipeline

import (
	"context"
	"log/slog"
)

// todo: implement Routine interface for Pipeline, so it can be nested with other Pipeline
type Pipeline struct {
	routines []Routine
}

// New creates a new instance of Pipeline with default values.
func New() *Pipeline {
	return &Pipeline{}
}

func (s *Pipeline) Chain(r Routine) *Pipeline {
	s.routines = append(s.routines, r)

	return s
}

func (s *Pipeline) Start(ctx context.Context, pipe Pipe) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	inPipe := NewChanPipe()
	previousPipe := inPipe

	for _, routine := range s.routines {
		stepPipe := NewChanPipe()

		previousPipe.Chain(stepPipe)
		previousPipe = stepPipe

		go func() {
			err := routine.Start(ctx, stepPipe)
			if err != nil {
				slog.Error("routine error", "error", err)
			}
		}()
	}

	go func() {
		defer inPipe.Close()

		for msg := range pipe.In() {
			slog.Debug("pipeline received message", "msg", msg)

			select {
			case <-ctx.Done():
				return
			case inPipe.Out() <- msg:
			}
		}
	}()

	go func() {
		defer pipe.Close()

		for msg := range previousPipe.Out() {
			slog.Debug("pipeline forwarding message", "msg", msg)

			select {
			case <-ctx.Done():
				return
			case pipe.Out() <- msg:
			}
		}
	}()

	<-pipe.Done()

	return nil
}

type memoizedPipeRoutine struct {
	routine Routine
	pipe    Pipe
}

func (m memoizedPipeRoutine) Run(ctx context.Context) error {
	return m.routine.Start(ctx, m.pipe)
}
