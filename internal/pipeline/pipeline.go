package pipeline

import (
	"context"
	"log/slog"
)

// todo: implement Routine interface for Pipeline, so it can be nested with other Pipeline
type Pipeline struct {
	inputRoutine  memoizedPipeRoutine
	outputRoutine memoizedPipeRoutine

	middlewareRoutines []memoizedPipeRoutine
	previousPipe       Pipe

	routines []Routine
}

// New creates a new instance of Pipeline with default values.
func New(in, out Routine) *Pipeline {
	inPipe := NewChanPipe()
	outPipe := NewChanPipe()

	inPipe.Chain(outPipe)

	return &Pipeline{
		inputRoutine:  memoizedPipeRoutine{pipe: inPipe, routine: in},
		outputRoutine: memoizedPipeRoutine{pipe: outPipe, routine: out},

		previousPipe: inPipe,
	}
}

func New2() *Pipeline {
	inPipe := NewChanPipe()
	//outPipe := NewChanPipe()

	//inPipe.Chain(outPipe)

	return &Pipeline{
		//inputRoutine:  memoizedPipeRoutine{pipe: inPipe, routine: in},
		//outputRoutine: memoizedPipeRoutine{pipe: outPipe, routine: out},

		previousPipe: inPipe,
	}
}

func (s *Pipeline) In(r Routine) *Pipeline {
	s.inputRoutine.routine = r

	return s
}

func (s *Pipeline) Out(r Routine) *Pipeline {
	s.outputRoutine.routine = r

	return s
}

func (s *Pipeline) Chain(r Routine) *Pipeline {
	//stepPipe := NewChanPipe()
	//previousPipe := s.previousPipe
	//
	//previousPipe.Chain(stepPipe)
	//s.previousPipe = stepPipe
	//
	//s.middlewareRoutines = append(s.middlewareRoutines, memoizedPipeRoutine{
	//	pipe:    stepPipe,
	//	routine: r,
	//})

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

	//previousPipe.SetOutChan(pipe.Out())

	return nil
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
	<-s.outputRoutine.pipe.Done()

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
