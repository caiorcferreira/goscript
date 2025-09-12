package pipeline

import (
	"context"
	"io"
)

type Msg struct {
	ID   string
	Data any
}

type Pipe interface {
	In() chan Msg
	Out() chan Msg
	Done() <-chan struct{}
	Chain(p Pipe)
	io.Closer
}

type Routine interface {
	Run(ctx context.Context, pipe Pipe) error
}
