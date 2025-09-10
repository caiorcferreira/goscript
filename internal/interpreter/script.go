package interpreter

import (
	"context"
)

type Script struct {
	inputRoutine Routine
	inPipe       Pipe

	outputRoutine Routine
	outPipe       Pipe

	middlewareRoutines []Routine
}

// NewScript creates a new instance of Script with default values.
func NewScript(in, out Routine) *Script {
	inPipe := NewChanPipe()
	outPipe := NewChanPipe()

	inPipe.Chain(outPipe)

	return &Script{
		inputRoutine: in,
		inPipe:       inPipe,

		outputRoutine: out,
		outPipe:       outPipe,
	}
}

func (s *Script) In(process Routine) *Script {
	s.inputRoutine = process
	s.inputRoutine.Pipe(s.inPipe)

	return s
}

func (s *Script) Out(process Routine) *Script {
	s.outputRoutine = process
	s.outputRoutine.Pipe(s.outPipe)

	return s
}

func (s *Script) Chain(r Routine) *Script {
	stepPipe := NewChanPipe()

	s.inPipe.Chain(stepPipe)
	stepPipe.Chain(s.outPipe)

	r.Pipe(stepPipe)
	s.middlewareRoutines = append(s.middlewareRoutines, r)

	return s
}

func (s *Script) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// start routines in reverse order: output, middlewares, input
	err := s.outputRoutine.Run(ctx)
	if err != nil {
		return err
	}

	for _, routine := range s.middlewareRoutines {
		err := routine.Run(ctx)
		if err != nil {
			return err
		}
	}

	err = s.inputRoutine.Run(ctx)
	if err != nil {
		return err
	}

	// wait for input routine to finish
	<-s.inPipe.Done()

	// all routines should exit when context is cancelled
	return nil
}
