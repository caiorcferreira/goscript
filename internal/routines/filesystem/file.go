package filesystem

import (
	"context"
	"fmt"
	"github.com/caiorcferreira/goscript/internal/template"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/caiorcferreira/goscript/internal/pipeline"
)

func File(path string) FileRoutineBuilder {
	return FileRoutineBuilder{path: path}
}

type FileRoutineBuilder struct {
	path       string
	readCodec  ReadCodec
	writeCodec WriteCodec
}

func (f FileRoutineBuilder) Read() *ReadFileRoutine {
	readCodec := f.readCodec
	if readCodec == nil {
		readCodec = buildReadCodec(f.path)
	}
	return &ReadFileRoutine{path: f.path, readCodec: readCodec}
}

func (f FileRoutineBuilder) Write() *WriteFileRoutine {
	writeCodec := f.writeCodec
	if writeCodec == nil {
		writeCodec = buildWriteCodec(f.path)
	}

	return &WriteFileRoutine{
		path:       f.path,
		writeCodec: writeCodec,
		renderer:   template.NewRenderer(),
	}
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
func (r *ReadFileRoutineBuilder) Start(ctx context.Context, pipe pipeline.Pipe) error {
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

func (r *ReadFileRoutine) Start(ctx context.Context, pipe pipeline.Pipe) error {
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

// WithCodec sets the codec for reading files
func (r *ReadFileRoutine) WithCodec(codec ReadCodec) *ReadFileRoutine {
	r.readCodec = codec
	return r
}

// WithLineCodec sets the codec to LineCodec for line-by-line reading
func (r *ReadFileRoutine) WithLineCodec() *ReadFileRoutine {
	r.readCodec = NewLineCodec()
	return r
}

// WithCSVCodec sets the codec to CSVCodec for CSV parsing
func (r *ReadFileRoutine) WithCSVCodec() *ReadFileRoutine {
	r.readCodec = NewCSVCodec()
	return r
}

// WithJSONCodec sets the codec to JSONCodec for JSON parsing
func (r *ReadFileRoutine) WithJSONCodec() *ReadFileRoutine {
	r.readCodec = NewJSONCodec()
	return r
}

// WithBlobCodec sets the codec to BlobCodec for entire file reading
func (r *ReadFileRoutine) WithBlobCodec() *ReadFileRoutine {
	r.readCodec = NewBlobCodec()
	return r
}

// WriteFileRoutine handles file writing operations
type WriteFileRoutine struct {
	path       string
	writeCodec WriteCodec
	renderer   template.Renderer
}

func (w *WriteFileRoutine) Start(ctx context.Context, pipe pipeline.Pipe) error {
	slog.Info("writing file", "path", w.path)
	defer func() {
		slog.Info("finished writing file", "path", w.path)
	}()

	defer pipe.Close()

	for msg := range pipe.In() {
		filePath, err := template.RenderAs[string](w.renderer, w.path, msg.Data)
		if err != nil {
			slog.Error("failed to render file", "path", w.path, "error", err)
			continue
		}

		file, err := openWritingFile(filePath, modeWrite)
		if err != nil {
			return fmt.Errorf("failed to open file for write: %w", err)
		}

		err = w.writeCodec.Encode(ctx, msg, file)
		file.Close() // Close file immediately after writing each message

		if err != nil {
			slog.Error("failed to encode message to file", "path", filePath, "error", err)
			continue
		}

		slog.Debug("message written to file", "path", filePath)
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

// WithCodec sets the codec for writing files
func (w *WriteFileRoutine) WithCodec(codec WriteCodec) *WriteFileRoutine {
	w.writeCodec = codec
	return w
}

// WithLineCodec sets the codec to LineCodec for line-by-line writing
func (w *WriteFileRoutine) WithLineCodec() *WriteFileRoutine {
	w.writeCodec = NewLineCodec()
	return w
}

// WithCSVCodec sets the codec to CSVCodec for CSV writing
func (w *WriteFileRoutine) WithCSVCodec() *WriteFileRoutine {
	w.writeCodec = NewCSVCodec()
	return w
}

// WithJSONCodec sets the codec to JSONCodec for JSON writing
func (w *WriteFileRoutine) WithJSONCodec() *WriteFileRoutine {
	w.writeCodec = NewJSONCodec()
	return w
}

// WithBlobCodec sets the codec to BlobCodec for raw data writing
func (w *WriteFileRoutine) WithBlobCodec() *WriteFileRoutine {
	w.writeCodec = NewBlobCodec()
	return w
}
