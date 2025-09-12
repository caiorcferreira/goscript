package filesystem

import (
	"context"
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

func (f FileRoutineBuilder) Read() *ReadFileRoutineBuilder {
	readCodec := f.readCodec
	if readCodec == nil {
		readCodec = NewLineCodec()
	}
	return &ReadFileRoutineBuilder{path: f.path, readCodec: readCodec}
}

func (f FileRoutineBuilder) Write() *WriteFileRoutineBuilder {
	writeCodec := f.writeCodec
	if writeCodec == nil {
		writeCodec = NewLineCodec()
	}
	return &WriteFileRoutineBuilder{path: f.path, writeCodec: writeCodec, mode: modeWrite}
}

// ReadFileRoutineBuilder methods

// WithCodec sets the codec for reading files
func (r *ReadFileRoutineBuilder) WithCodec(codec ReadCodec) *ReadFileRoutineBuilder {
	r.readCodec = codec
	return r
}

// WithLineCodec sets the codec to LineCodec for line-by-line reading
func (r *ReadFileRoutineBuilder) WithLineCodec() *ReadFileRoutineBuilder {
	r.readCodec = NewLineCodec()
	return r
}

// WithCSVCodec sets the codec to CSVCodec for CSV parsing
func (r *ReadFileRoutineBuilder) WithCSVCodec() *ReadFileRoutineBuilder {
	r.readCodec = NewCSVCodec()
	return r
}

// WithJSONCodec sets the codec to JSONCodec for JSON parsing
func (r *ReadFileRoutineBuilder) WithJSONCodec() *ReadFileRoutineBuilder {
	r.readCodec = NewJSONCodec()
	return r
}

// WithBlobCodec sets the codec to BlobCodec for entire file reading
func (r *ReadFileRoutineBuilder) WithBlobCodec() *ReadFileRoutineBuilder {
	r.readCodec = NewBlobCodec()
	return r
}

// WriteFileRoutineBuilder methods

// WithCodec sets the codec for writing files
func (w *WriteFileRoutineBuilder) WithCodec(codec WriteCodec) *WriteFileRoutineBuilder {
	w.writeCodec = codec
	return w
}

// WithLineCodec sets the codec to LineCodec for line-by-line writing
func (w *WriteFileRoutineBuilder) WithLineCodec() *WriteFileRoutineBuilder {
	w.writeCodec = NewLineCodec()
	return w
}

// WithCSVCodec sets the codec to CSVCodec for CSV writing
func (w *WriteFileRoutineBuilder) WithCSVCodec() *WriteFileRoutineBuilder {
	w.writeCodec = NewCSVCodec()
	return w
}

// WithJSONCodec sets the codec to JSONCodec for JSON writing
func (w *WriteFileRoutineBuilder) WithJSONCodec() *WriteFileRoutineBuilder {
	w.writeCodec = NewJSONCodec()
	return w
}

// WithBlobCodec sets the codec to BlobCodec for raw data writing
func (w *WriteFileRoutineBuilder) WithBlobCodec() *WriteFileRoutineBuilder {
	w.writeCodec = NewBlobCodec()
	return w
}

const (
	modeRead  = os.O_RDONLY
	modeWrite = os.O_WRONLY | os.O_CREATE | os.O_APPEND
)

// ReadFileRoutineBuilder builds and executes file reading operations
type ReadFileRoutineBuilder struct {
	path      string
	readCodec ReadCodec
}

// Run executes the file reading operation directly
func (r *ReadFileRoutineBuilder) Run(ctx context.Context, pipe pipeline.Pipe) error {
	slog.Info("reading file", "path", r.path)
	defer func() {
		slog.Info("finished reading file", "path", r.path)
	}()

	file, err := os.OpenFile(r.path, modeRead, 0)
	if err != nil {
		return fmt.Errorf("failed to open file for read: %w", err)
	}

	defer pipe.Close()
	defer file.Close()

	// Use codec to parse file content and write to pipe with context support
	err = r.readCodec.Parse(ctx, file, pipe)
	if err != nil {
		return fmt.Errorf("failed to parse file with codec: %w", err)
	}

	return nil
}

// ReadFileRoutine handles file reading operations
type ReadFileRoutine struct {
	path      string
	readCodec ReadCodec
}

func (r *ReadFileRoutine) Run(ctx context.Context, pipe pipeline.Pipe) error {
	slog.Info("reading file", "path", r.path)
	defer func() {
		slog.Info("finished reading file", "path", r.path)
	}()

	file, err := os.OpenFile(r.path, modeRead, 0)
	if err != nil {
		return fmt.Errorf("failed to open file for read: %w", err)
	}

	defer pipe.Close()
	defer file.Close()

	// Use codec to parse file content and write to pipe with context support
	err = r.readCodec.Parse(ctx, file, pipe)
	if err != nil {
		return fmt.Errorf("failed to parse file with codec: %w", err)
	}

	return nil
}

// WriteFileRoutineBuilder builds and executes file writing operations
type WriteFileRoutineBuilder struct {
	path       string
	writeCodec WriteCodec
	mode       int
}

// Run executes the file writing operation directly
func (w *WriteFileRoutineBuilder) Run(ctx context.Context, pipe pipeline.Pipe) error {
	slog.Info("writing file", "path", w.path)
	defer func() {
		slog.Info("finished writing file", "path", w.path)
	}()

	file, err := openWritingFile(w.path, w.mode)
	if err != nil {
		return fmt.Errorf("failed to open file for write: %w", err)
	}

	defer file.Close()

	// Use writeCodec to encode messages and write to file
	err = w.writeCodec.Encode(ctx, pipe, file)
	if err != nil {
		return fmt.Errorf("failed to encode messages with codec: %w", err)
	}

	return nil
}

// WriteFileRoutine handles file writing operations
type WriteFileRoutine struct {
	path       string
	writeCodec WriteCodec
	mode       int
}

func (w *WriteFileRoutine) Run(ctx context.Context, pipe pipeline.Pipe) error {
	slog.Info("writing file", "path", w.path)
	defer func() {
		slog.Info("finished writing file", "path", w.path)
	}()

	file, err := openWritingFile(w.path, w.mode)
	if err != nil {
		return fmt.Errorf("failed to open file for write: %w", err)
	}

	defer file.Close()

	// Use writeCodec to encode messages and write to file
	err = w.writeCodec.Encode(ctx, pipe, file)
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
