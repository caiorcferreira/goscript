package interpreter

import (
	"context"
)

type Script struct {
	inputRoutine memoizedPipeRoutine
	inPipe       Pipe

	outputRoutine memoizedPipeRoutine
	outPipe       Pipe

	middlewareRoutines []memoizedPipeRoutine
}

// NewScript creates a new instance of Script with default values.
func NewScript(in, out Routine) *Script {
	inPipe := NewChanPipe()
	outPipe := NewChanPipe()

	inPipe.Chain(outPipe)

	return &Script{
		inputRoutine: memoizedPipeRoutine{pipe: inPipe, routine: in},
		inPipe:       inPipe,

		outputRoutine: memoizedPipeRoutine{pipe: outPipe, routine: out},
		outPipe:       outPipe,
	}
}

func (s *Script) In(process Routine) *Script {
	s.inputRoutine = memoizedPipeRoutine{pipe: s.inPipe, routine: process}
	//s.inputRoutine.Pipe(s.inPipe)

	return s
}

func (s *Script) Out(process Routine) *Script {
	s.outputRoutine = memoizedPipeRoutine{pipe: s.outPipe, routine: process}
	//s.outputRoutine = process
	//s.outputRoutine.Pipe(s.outPipe)

	return s
}

func (s *Script) Chain(r Routine) *Script {
	stepPipe := NewChanPipe()

	s.inputRoutine.pipe.Chain(stepPipe)
	stepPipe.Chain(s.outputRoutine.pipe)

	//r.Pipe(stepPipe)
	s.middlewareRoutines = append(s.middlewareRoutines, memoizedPipeRoutine{
		pipe:    stepPipe,
		routine: r,
	})

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

type memoizedPipeRoutine struct {
	routine Routine
	pipe    Pipe
}

func (m memoizedPipeRoutine) Run(ctx context.Context) error {
	return m.routine.Run(ctx, m.pipe)
}
