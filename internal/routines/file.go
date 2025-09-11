package routines

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/caiorcferreira/goscript/internal/interpreter"
	"os"
)

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

func (f *FileRoutine) Run(ctx context.Context, pipe interpreter.Pipe) error {
	switch f.mode {
	case modeRead:
		return f.read(ctx, pipe)
	case modeWrite:
		return f.write(ctx, pipe)
	}

	return errors.New("invalid file mode")
}

func (f *FileRoutine) read(ctx context.Context, pipe interpreter.Pipe) error {
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
		text := scanner.Text()

		select {
		case <-ctx.Done():
			fmt.Println("file read: cancelled")
			return ctx.Err()
		case <-pipe.Done():
			fmt.Println("file read: pipe done")
			return nil
		case pipe.Out() <- text:
			fmt.Printf("file read: sent line: %s\n", text)
		}
	}

	return nil
}

func (f *FileRoutine) write(ctx context.Context, pipe interpreter.Pipe) error {
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

	defer file.Close()
	defer pipe.Close()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("file write: cancelled")
			return ctx.Err()
		case <-pipe.Done():
			fmt.Println("file write: pipe done")
			return nil
		case data, open := <-pipe.In():
			if !open {
				fmt.Printf("file write: pipe closed: %v\n", data)
				//return nil
			}

			fmt.Printf("file write: recv line: %v\n", data)

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

	return nil
}
