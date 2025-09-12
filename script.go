package goscript

import (
	"context"
	"github.com/caiorcferreira/goscript/internal/pipeline"
	"github.com/caiorcferreira/goscript/internal/routines"
	"log/slog"
	"time"
)

type Script struct {
	inPipe       pipeline.Pipe
	inputRoutine pipeline.Routine

	outPipe       pipeline.Pipe
	outputRoutine pipeline.Routine

	pipelinePipe pipeline.Pipe
	pipeline     *pipeline.Pipeline
}

func New() *Script {
	inPipe := pipeline.NewChanPipe()
	pipelinePipe := pipeline.NewChanPipe()
	outPipe := pipeline.NewChanPipe()

	inPipe.Chain(pipelinePipe)
	pipelinePipe.Chain(outPipe)

	p := pipeline.New2()

	return &Script{
		inPipe:       inPipe,
		inputRoutine: routines.NewStdInRoutine(),

		outPipe:       outPipe,
		outputRoutine: routines.NewStdOutRoutine(),

		pipelinePipe: pipelinePipe,
		pipeline:     p,
	}
}

//func (s *Script) In(routine pipeline.Routine) *Script {
//	s.pipeline.In(routine)
//	return s
//}
//
//func (s *Script) Out(routine pipeline.Routine) *Script {
//	s.pipeline.Out(routine)
//	return s
//}

func (s *Script) In(r pipeline.Routine) *Script {
	s.inputRoutine = r

	return s
}

func (s *Script) Out(r pipeline.Routine) *Script {
	s.outputRoutine = r

	return s
}

func (s *Script) Chain(routine pipeline.Routine) *Script {
	s.pipeline.Chain(routine)
	return s
}

func (s *Script) File(routine pipeline.Routine) *Script {
	s.pipeline.Chain(routine)
	return s
}

func (s *Script) Parallel(r pipeline.Routine, maxConcurrency int) *Script {
	s.pipeline.Chain(routines.Parallel(r, maxConcurrency))

	return s
}

func (s *Script) Debounce(delay time.Duration) *Script {
	s.pipeline.Chain(routines.Debounce(delay))

	return s
}

func (s *Script) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// start routines in reverse order: output, middlewares, input
	go func() {
		err := s.outputRoutine.Start(ctx, s.outPipe)
		if err != nil {
			slog.Error("output routine error", "error", err)
		}
	}()

	go func() {
		err := s.pipeline.Start(ctx, s.pipelinePipe)
		if err != nil {
			slog.Error("pipeline routine error", "error", err)
		}
	}()

	go func() {
		err := s.inputRoutine.Start(ctx, s.inPipe)
		if err != nil {
			slog.Error("input routine error", "error", err)
		}
	}()

	// wait for input routine to finish
	<-s.outPipe.Done()

	// all routines should exit when context is cancelled
	return nil

	//return s.pipeline.Run(ctx)
}
