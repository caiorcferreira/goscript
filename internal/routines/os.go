package routines

import (
	"context"
	"fmt"
	"github.com/caiorcferreira/goscript/internal/interpreter"
	"io"
	"log/slog"
	"os"
	"time"
)

type StdInRoutine struct {
	pipe interpreter.Pipe
}

func NewStdInRoutine() *StdInRoutine {
	return &StdInRoutine{}
}

func (p *StdInRoutine) Pipe(pipe interpreter.Pipe) {
	p.pipe = pipe
}

func (p *StdInRoutine) Run(ctx context.Context, pipe interpreter.Pipe) error {
	w := &stdinWriter{pipe: pipe}

	for {
		time.Sleep(1 * time.Second) //todo: avoid busy waiting
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			//todo: handle error
			io.Copy(w, os.Stdin)
		}
	}
}

type stdinWriter struct {
	pipe interpreter.Pipe
}

func (p *stdinWriter) Write(data []byte) (n int, err error) {
	msg := interpreter.Msg{
		ID:   "",
		Data: data,
	}
	p.pipe.Out() <- msg
	return len(data), nil
}

type StdOutRoutine struct{}

func NewStdOutRoutine() *StdOutRoutine {
	return &StdOutRoutine{}
}

func (p *StdOutRoutine) Run(ctx context.Context, pipe interpreter.Pipe) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-pipe.In():
			switch v := msg.Data.(type) {
			case string:
				os.Stdout.Write([]byte(v))
			case []byte:
				os.Stdout.Write(v)
			default:
				slog.Warn("stdout unknown type", "type", fmt.Sprintf("%T", msg.Data))
			}
		}
	}
}
