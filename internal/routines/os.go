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

func (p *StdInRoutine) Run(ctx context.Context) error {
	go func() {
		for {
			time.Sleep(1 * time.Second) //todo: avoid busy waiting
			select {
			case <-ctx.Done():
				return
			default:
				//todo: handle error
				io.Copy(p, os.Stdin)
			}
		}
	}()

	return nil
}

func (p *StdInRoutine) Write(data []byte) (n int, err error) {
	p.pipe.Out() <- data
	return len(data), nil
}

type StdOutRoutine struct {
	pipe interpreter.Pipe
}

func NewStdOutRoutine() *StdOutRoutine {
	return &StdOutRoutine{}
}

func (p *StdOutRoutine) Pipe(pipe interpreter.Pipe) {
	p.pipe = pipe
}

func (p *StdOutRoutine) Run(ctx context.Context) error {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case data := <-p.pipe.In():
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
	}()

	return nil
}
