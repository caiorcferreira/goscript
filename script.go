package goscript

import (
	"context"
	"github.com/caiorcferreira/goscript/internal/pipeline"
	"github.com/caiorcferreira/goscript/internal/routines"
	"github.com/caiorcferreira/goscript/internal/routines/filesystem"
	"log/slog"
	"time"
)

type Script struct {
	inPipe       pipeline.Pipe
	inputRoutine pipeline.Routine

	outPipe       pipeline.Pipe
	outputRoutine pipeline.Routine

	hasPipeline bool
	pipeline    *pipeline.Pipeline
}

func New() *Script {
	inPipe := pipeline.NewChanPipe()
	outPipe := pipeline.NewChanPipe()

	inPipe.Chain(outPipe)

	p := pipeline.New()

	return &Script{
		inPipe:       inPipe,
		inputRoutine: routines.NewStdInRoutine(),

		outPipe:       outPipe,
		outputRoutine: routines.NewStdOutRoutine(),

		pipeline: p,
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
	s.hasPipeline = true
	s.pipeline.Chain(routine)
	return s
}

func (s *Script) JSONIn(path string) *Script {
	s.In(filesystem.File(path).Read().WithJSONCodec())
	return s
}

func (s *Script) JSONOut(path string) *Script {
	s.Out(filesystem.File(path).Write().WithJSONCodec())
	return s
}

func (s *Script) CSVIn(path string) *Script {
	s.In(filesystem.File(path).Read().WithCSVCodec())
	return s
}

func (s *Script) CSVOut(path string) *Script {
	s.Out(filesystem.File(path).Write().WithCSVCodec())
	return s
}

func (s *Script) FileIn(path string) *Script {
	s.In(filesystem.File(path).Read())
	return s
}

func (s *Script) FileOut(path string) *Script {
	s.Out(filesystem.File(path).Write())
	return s
}

func (s *Script) Parallel(r pipeline.Routine, maxConcurrency int) *Script {
	s.Chain(routines.Parallel(r, maxConcurrency))

	return s
}

func (s *Script) Debounce(delay time.Duration) *Script {
	s.Chain(routines.Debounce(delay))

	return s
}

func (s *Script) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if s.hasPipeline {
		slog.Debug("Starting pipeline...")

		pipelinePipe := pipeline.NewChanPipe()

		s.inPipe.Chain(pipelinePipe)
		pipelinePipe.Chain(s.outPipe)

		go func() {
			err := s.pipeline.Start(ctx, pipelinePipe)
			if err != nil {
				slog.Error("pipeline routine error", "error", err)
			}
		}()
	}

	// start routines in reverse order: output, middlewares, input
	go func() {
		err := s.outputRoutine.Start(ctx, s.outPipe)
		if err != nil {
			slog.Error("output routine error", "error", err)
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
