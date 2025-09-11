package routines

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/caiorcferreira/goscript/internal/interpreter"
	"github.com/google/uuid"
	"log/slog"
	"os"
	"path/filepath"
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
	case modeWrite, modeAppend:
		return f.write(ctx, pipe)
	}

	return errors.New("invalid file mode")
}

func (f *FileRoutine) read(ctx context.Context, pipe interpreter.Pipe) error {
	slog.Info("reading file", "path", f.path)
	defer func() {
		slog.Info("finished reading file", "path", f.path)
	}()

	file, err := os.OpenFile(f.path, f.mode, 0)
	if err != nil {
		return fmt.Errorf("failed to open file for read: %w", err)
	}

	defer pipe.Close()
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		text := scanner.Text()
		msg := interpreter.Msg{
			ID:   uuid.NewString(),
			Data: text,
		}

		select {
		case <-ctx.Done():
			slog.Info("file read cancelled")
			return ctx.Err()
		case pipe.Out() <- msg:
			slog.Debug("file read sent line", "text", text)
		}
	}

	return nil
}

func (f *FileRoutine) write(ctx context.Context, pipe interpreter.Pipe) error {
	slog.Info("writing file", "path", f.path)
	defer func() {
		slog.Info("finished writing file", "path", f.path)
	}()

	file, err := openWritingFile(f.path, f.mode)
	if err != nil {
		return fmt.Errorf("failed to open file for write: %w", err)
	}

	defer file.Close()
	defer pipe.Close()

	for msg := range pipe.In() {
		select {
		case <-ctx.Done():
			slog.Info("file write cancelled")
		default:
			slog.Debug("file write received line", "msg", msg)

			switch v := msg.Data.(type) {
			case string:
				file.WriteString(v + "\n")
			case []byte:
				file.Write(v)
			default:
				slog.Warn("file write unknown type", "type", fmt.Sprintf("%T", v))
			}
		}
	}

	return nil
}

func openWritingFile(path string, mode int) (*os.File, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	file, err := os.OpenFile(path, mode, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}

	return file, nil
}
