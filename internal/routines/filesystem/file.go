package filesystem

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
	path       string
	readCodec  ReadCodec
	writeCodec WriteCodec
}

func (f FileRoutineBuilder) Read() *FileRoutine {
	readCodec := f.readCodec
	if readCodec == nil {
		readCodec = NewLineCodec()
	}
	return &FileRoutine{path: f.path, mode: modeRead, readCodec: readCodec}
}

func (f FileRoutineBuilder) Write() *FileRoutine {
	writeCodec := f.writeCodec
	if writeCodec == nil {
		writeCodec = NewLineWriteCodec()
	}
	return &FileRoutine{path: f.path, mode: modeWrite, writeCodec: writeCodec}
}

func (f FileRoutineBuilder) Append() *FileRoutine {
	writeCodec := f.writeCodec
	if writeCodec == nil {
		writeCodec = NewLineWriteCodec()
	}
	return &FileRoutine{path: f.path, mode: modeAppend, writeCodec: writeCodec}
}

// WithReadCodec sets the codec for reading files
func (f FileRoutineBuilder) WithReadCodec(codec ReadCodec) FileRoutineBuilder {
	f.readCodec = codec
	return f
}

// WithWriteCodec sets the codec for writing files
func (f FileRoutineBuilder) WithWriteCodec(codec WriteCodec) FileRoutineBuilder {
	f.writeCodec = codec
	return f
}

// WithLineCodec sets the codec to LineCodec for line-by-line reading
func (f FileRoutineBuilder) WithLineCodec() FileRoutineBuilder {
	f.readCodec = NewLineCodec()
	return f
}

// WithLineWriteCodec sets the codec to LineWriteCodec for line-by-line writing
func (f FileRoutineBuilder) WithLineWriteCodec() FileRoutineBuilder {
	f.writeCodec = NewLineWriteCodec()
	return f
}

// WithLineCodecForWrite sets the codec to LineCodec for line-by-line writing
func (f FileRoutineBuilder) WithLineCodecForWrite() FileRoutineBuilder {
	f.writeCodec = NewLineCodec()
	return f
}

// WithCSVCodec sets the codec to CSVCodec for CSV parsing
func (f FileRoutineBuilder) WithCSVCodec() FileRoutineBuilder {
	f.readCodec = NewCSVCodec()
	return f
}

// WithCSVWriteCodec sets the codec to CSVWriteCodec for CSV writing
func (f FileRoutineBuilder) WithCSVWriteCodec() FileRoutineBuilder {
	f.writeCodec = NewCSVWriteCodec()
	return f
}

// WithCSVCodecForWrite sets the codec to CSVCodec for CSV writing
func (f FileRoutineBuilder) WithCSVCodecForWrite() FileRoutineBuilder {
	f.writeCodec = NewCSVCodec()
	return f
}

// WithJSONCodec sets the codec to JSONCodec for JSON parsing
func (f FileRoutineBuilder) WithJSONCodec() FileRoutineBuilder {
	f.readCodec = NewJSONCodec()
	return f
}

// WithJSONWriteCodec sets the codec to JSONWriteCodec for JSON writing
func (f FileRoutineBuilder) WithJSONWriteCodec() FileRoutineBuilder {
	f.writeCodec = NewJSONWriteCodec()
	return f
}

// WithJSONCodecForWrite sets the codec to JSONCodec for JSON writing
func (f FileRoutineBuilder) WithJSONCodecForWrite() FileRoutineBuilder {
	f.writeCodec = NewJSONCodec()
	return f
}

// WithBlobCodec sets the codec to BlobCodec for entire file reading
func (f FileRoutineBuilder) WithBlobCodec() FileRoutineBuilder {
	f.readCodec = NewBlobCodec()
	return f
}

// WithBlobWriteCodec sets the codec to BlobWriteCodec for raw data writing
func (f FileRoutineBuilder) WithBlobWriteCodec() FileRoutineBuilder {
	f.writeCodec = NewBlobWriteCodec()
	return f
}

// WithBlobCodecForWrite sets the codec to BlobCodec for raw data writing
func (f FileRoutineBuilder) WithBlobCodecForWrite() FileRoutineBuilder {
	f.writeCodec = NewBlobCodec()
	return f
}

const (
	modeRead   = os.O_RDONLY
	modeWrite  = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	modeAppend = os.O_WRONLY | os.O_CREATE | os.O_APPEND
)

type FileRoutine struct {
	path       string
	mode       int
	readCodec  ReadCodec
	writeCodec WriteCodec
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
	err = f.readCodec.Parse(ctx, file, pipe)
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

	// Use writeCodec to encode messages and write to file
	err = f.writeCodec.Encode(ctx, pipe, file)
	if err != nil {
		return fmt.Errorf("failed to encode messages with codec: %w", err)
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
