package routines

import (
	"context"
	"fmt"
	"github.com/caiorcferreira/goscript/internal/interpreter"
	"io"
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
	p.pipe.Out() <- data
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
		case data := <-pipe.In():
			switch v := data.(type) {
			case string:
				os.Stdout.Write([]byte(v))
			case []byte:
				os.Stdout.Write(v)
			default:
				fmt.Printf("stdout: unknown type: %T\n", data)
			}
		}
	}
}
