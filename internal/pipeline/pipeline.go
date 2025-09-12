package pipeline

import (
	"context"
	"log/slog"
)

// todo: implement Routine interface for Pipeline, so it can be nested with other Pipeline
type Pipeline struct {
	inputRoutine memoizedPipeRoutine
	inPipe       Pipe

	outputRoutine memoizedPipeRoutine
	outPipe       Pipe

	middlewareRoutines []memoizedPipeRoutine
	previousPipe       Pipe
}

// New creates a new instance of Pipeline with default values.
func New(in, out Routine) *Pipeline {
	inPipe := NewChanPipe()
	outPipe := NewChanPipe()

	inPipe.Chain(outPipe)

	return &Pipeline{
		inputRoutine: memoizedPipeRoutine{pipe: inPipe, routine: in},
		inPipe:       inPipe,

		outputRoutine: memoizedPipeRoutine{pipe: outPipe, routine: out},
		outPipe:       outPipe,

		previousPipe: inPipe,
	}
}

func (s *Pipeline) In(process Routine) *Pipeline {
	s.inputRoutine = memoizedPipeRoutine{pipe: s.inPipe, routine: process}

	return s
}

func (s *Pipeline) Out(process Routine) *Pipeline {
	s.outputRoutine = memoizedPipeRoutine{pipe: s.outPipe, routine: process}

	return s
}

func (s *Pipeline) Chain(r Routine) *Pipeline {
	stepPipe := NewChanPipe()
	previousPipe := s.previousPipe

	previousPipe.Chain(stepPipe)
	s.previousPipe = stepPipe

	s.middlewareRoutines = append(s.middlewareRoutines, memoizedPipeRoutine{
		pipe:    stepPipe,
		routine: r,
	})

	return s
}

func (s *Pipeline) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.previousPipe.Chain(s.outputRoutine.pipe)

	// start routines in reverse order: output, middlewares, input
	go func() {
		err := s.outputRoutine.Run(ctx)
		if err != nil {
			slog.Error("output routine error", "error", err)
		}
	}()

	for _, routine := range s.middlewareRoutines {
		go func() {
			err := routine.Run(ctx)
			if err != nil {
				slog.Error("routine error", "error", err)
			}
		}()
	}

	go func() {
		err := s.inputRoutine.Run(ctx)
		if err != nil {
			slog.Error("input routine error", "error", err)
		}
	}()

	// wait for input routine to finish
	<-s.outPipe.Done()

	// all routines should exit when context is cancelled
	return nil
}

type memoizedPipeRoutine struct {
	routine Routine
	pipe    Pipe
}

func (m memoizedPipeRoutine) Run(ctx context.Context) error {
	return m.routine.Start(ctx, m.pipe)
}
