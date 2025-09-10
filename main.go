package goscript

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

type Pipe interface {
	In() chan any
	Out() chan any
	Done() <-chan struct{}
	io.Closer
}

type Routine interface {
	Run(ctx context.Context, pipe Pipe) error
}

type stdInRoutine struct {
	pipe Pipe
}

func newStdInRoutine() *stdInRoutine {
	return &stdInRoutine{}
}

func (p *stdInRoutine) Run(ctx context.Context, pipe Pipe) error {
	p.pipe = pipe

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

func (p *stdInRoutine) Write(data []byte) (n int, err error) {
	p.pipe.Out() <- data
	return len(data), nil
}

type stdOutRoutine struct {
	in  chan any
	out chan any
}

func newStdOutRoutine() *stdOutRoutine {
	return &stdOutRoutine{}
}

func (p *stdOutRoutine) Run(ctx context.Context, pipe Pipe) error {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
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
	}()

	return nil
}

type Script struct {
	inputRoutine  Routine
	outputRoutine Routine
}

func New() *Script {
	return &Script{
		inputRoutine:  newStdInRoutine(),
		outputRoutine: newStdOutRoutine(),
	}
}

func (s *Script) In(process Routine) *Script {
	s.inputRoutine = process
	return s
}

func (s *Script) Out(process Routine) *Script {
	s.outputRoutine = process
	return s
}

func (s *Script) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	inPipe := newChanPipe()
	outPipe := newChanPipe()

	inPipe.Chain(outPipe)

	err := s.inputRoutine.Run(ctx, inPipe)
	if err != nil {
		return err
	}

	err = s.outputRoutine.Run(ctx, outPipe)
	if err != nil {
		return err
	}

	// wait for input routine to finish
	<-inPipe.Done()

	// all routines should exit when context is cancelled
	return nil
}

type chanPipe struct {
	in   chan any
	out  chan any
	done chan struct{}
}

func (c *chanPipe) Done() <-chan struct{} {
	return c.done
}

func (c *chanPipe) In() chan any {
	return c.in
}

func (c *chanPipe) SetInChan(cin chan any) {
	c.in = cin
}

func (c *chanPipe) Out() chan any {
	return c.out
}

func (c *chanPipe) SetOutChan(cout chan any) {
	c.out = cout
}

func (c *chanPipe) Chain(p Pipe) {
	c.out = p.In()
}

func (c *chanPipe) Close() error {
	close(c.done)

	return nil
}

func newChanPipe() *chanPipe {
	return &chanPipe{
		in:   make(chan any, 1),
		out:  make(chan any, 1),
		done: make(chan struct{}),
	}
}

func File(path string) FileRoutineBuilder {
	return FileRoutineBuilder{path: path}
}

type FileRoutineBuilder struct {
	path string
}

func (f FileRoutineBuilder) Read() *FileRoutine {
	return &FileRoutine{path: f.path, mode: modeRead}
}

func (f FileRoutineBuilder) Write() *FileRoutine {
	return &FileRoutine{path: f.path, mode: modeWrite}
}

func (f FileRoutineBuilder) Append() *FileRoutine {
	return &FileRoutine{path: f.path, mode: modeAppend}
}

const (
	modeRead   = os.O_RDONLY
	modeWrite  = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	modeAppend = os.O_WRONLY | os.O_CREATE | os.O_APPEND
)

type FileRoutine struct {
	path string
	mode int
}

func (f *FileRoutine) Run(ctx context.Context, pipe Pipe) error {
	switch f.mode {
	case modeRead:
		return f.read(ctx, pipe)
	case modeWrite:
		return f.write(ctx, pipe)
	}

	return errors.New("invalid file mode")
}

func (f *FileRoutine) read(ctx context.Context, pipe Pipe) error {
	go func() {
		fmt.Printf("reading file: %s\n", f.path)
		defer func() {
			fmt.Printf("finished reading file: %s\n", f.path)
		}()

		file, err := os.OpenFile(f.path, f.mode, 0)
		if err != nil {
			//return err
			fmt.Printf("error opening file: %s\n", err)
			panic(err) //todo: handle error properly
		}

		defer pipe.Close()
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				fmt.Println("file read: cancelled")
				return
			case pipe.Out() <- scanner.Text():
				fmt.Println("file read: sent line")
			}
		}
	}()

	return nil
}

func (f *FileRoutine) write(ctx context.Context, pipe Pipe) error {
	go func() {
		fmt.Printf("writing file: %s\n", f.path)
		defer func() {
			fmt.Printf("finished writing file: %s\n", f.path)
		}()

		file, err := os.OpenFile(f.path, f.mode, 0644)
		if err != nil {
			//return err
			fmt.Printf("error opening file: %s\n", err)
			panic(err) //todo: handle error properly
		}

		defer pipe.Close()
		defer file.Close()

		for {
			select {
			case <-ctx.Done():
				fmt.Println("file write: cancelled")
				return
			case data := <-pipe.In():
				fmt.Println("file write: recv line")

				switch v := data.(type) {
				case string:
					file.WriteString(v + "\n")
				case []byte:
					file.Write(v)
				default:
					fmt.Printf("file write: unknown type: %T\n", v)
				}
			}
		}
	}()

	return nil
}
