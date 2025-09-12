package routines

import (
	"context"
	"errors"
	"fmt"
	"github.com/caiorcferreira/goscript/internal/pipeline"
	"log/slog"
	"os"
	"path/filepath"
)

func File(path string) FileRoutineBuilder {
	return FileRoutineBuilder{path: path}
}

type FileRoutineBuilder struct {
	path  string
	codec Codec
}

func (f FileRoutineBuilder) Read() *FileRoutine {
	codec := f.codec
	if codec == nil {
		codec = NewLineCodec() // Default to line-by-line reading
	}
	return &FileRoutine{path: f.path, mode: modeRead, codec: codec}
}

func (f FileRoutineBuilder) Write() *FileRoutine {
	return &FileRoutine{path: f.path, mode: modeWrite}
}

func (f FileRoutineBuilder) Append() *FileRoutine {
	return &FileRoutine{path: f.path, mode: modeAppend}
}

// WithCodec sets the codec for reading files
func (f FileRoutineBuilder) WithCodec(codec Codec) FileRoutineBuilder {
	f.codec = codec
	return f
}

// WithLineCodec sets the codec to LineCodec for line-by-line reading
func (f FileRoutineBuilder) WithLineCodec() FileRoutineBuilder {
	f.codec = NewLineCodec()
	return f
}

// WithCSVCodec sets the codec to CSVCodec for CSV parsing
func (f FileRoutineBuilder) WithCSVCodec() FileRoutineBuilder {
	f.codec = NewCSVCodec()
	return f
}

// WithJSONCodec sets the codec to JSONCodec for JSON parsing
func (f FileRoutineBuilder) WithJSONCodec() FileRoutineBuilder {
	f.codec = NewJSONCodec()
	return f
}

// WithBlobCodec sets the codec to BlobCodec for entire file reading
func (f FileRoutineBuilder) WithBlobCodec() FileRoutineBuilder {
	f.codec = NewBlobCodec()
	return f
}

const (
	modeRead   = os.O_RDONLY
	modeWrite  = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	modeAppend = os.O_WRONLY | os.O_CREATE | os.O_APPEND
)

type FileRoutine struct {
	path  string
	mode  int
	codec Codec
}

func (f *FileRoutine) Run(ctx context.Context, pipe pipeline.Pipe) error {
	switch f.mode {
	case modeRead:
		return f.read(ctx, pipe)
	case modeWrite, modeAppend:
		return f.write(ctx, pipe)
	}

	return errors.New("invalid file mode")
}

func (f *FileRoutine) read(ctx context.Context, pipe pipeline.Pipe) error {
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

	// Use codec to parse file content and write to pipe with context support
	err = f.codec.Parse(ctx, file, pipe)
	if err != nil {
		return fmt.Errorf("failed to parse file with codec: %w", err)
	}

	return nil
}

func (f *FileRoutine) write(ctx context.Context, pipe pipeline.Pipe) error {
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
