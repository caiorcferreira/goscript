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

//go:generate go run go.uber.org/mock/mockgen -source=$GOFILE -destination=mocks/mock_routine.go -package=mocks Routine
type Routine interface {
	Start(ctx context.Context, pipe Pipe) error
}
