package goscript

import (
	"context"
	"github.com/caiorcferreira/goscript/internal/pipeline"
	"github.com/caiorcferreira/goscript/internal/routines"
	"time"
)

type Script struct {
	pipeline *pipeline.Pipeline
}

func New() *Script {
	p := pipeline.New(
		routines.NewStdInRoutine(),
		routines.NewStdOutRoutine(),
	)

	return &Script{pipeline: p}
}

func (s *Script) In(routine pipeline.Routine) *Script {
	s.pipeline.In(routine)
	return s
}

func (s *Script) Out(routine pipeline.Routine) *Script {
	s.pipeline.Out(routine)
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
	return s.pipeline.Run(ctx)
}
