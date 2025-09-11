package interpreter

import (
	"context"
	"io"
)

type Msg struct {
	ID   string
	Data any
}

type ACK struct {
	ID    string
	MsgID string
}

type Pipe interface {
	In() chan any
	Out() chan any
	Done() <-chan struct{}
	Chain(p Pipe)
	ACK(ack ACK)
	RecACK() <-chan ACK
	io.Closer
}

type Routine interface {
	Run(ctx context.Context, pipe Pipe) error
}
