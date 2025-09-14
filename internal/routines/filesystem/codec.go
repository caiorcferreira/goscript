package filesystem

import (
	"context"
	"io"
	"path/filepath"
	"strings"

	"github.com/caiorcferreira/goscript/internal/pipeline"
)

// ReadCodec defines the interface for parsing file content into messages
// Reads from a reader and writes messages to a pipe
type ReadCodec interface {
	// Parse reads from the reader and writes messages to the pipe
	Parse(ctx context.Context, reader io.Reader, pipe pipeline.Pipe) error
}

// WriteCodec defines the interface for encoding messages to file content
// Reads messages from a pipe and writes them to a writer
type WriteCodec interface {
	// Encode reads messages from the pipe and writes them to the writer
	Encode(ctx context.Context, pipe pipeline.Pipe, writer io.Writer) error
}

var extensionToCodec = map[string]any{
	".json":  NewJSONCodec(),
	".jsonl": NewJSONCodec().WithJSONLinesMode(),
	".csv":   NewCSVCodec(),
	".txt":   NewLineCodec(),
}

func buildReadCodec(path string) ReadCodec {
	ext := filepath.Ext(path)
	ext = strings.ToLower(ext)

	codec, found := extensionToCodec[ext]
	if !found {
		return NewLineCodec()
	}

	return codec.(ReadCodec)
}

func buildWriteCodec(path string) WriteCodec {
	ext := filepath.Ext(path)
	ext = strings.ToLower(ext)

	codec, found := extensionToCodec[ext]
	if !found {
		return NewLineCodec()
	}

	return codec.(WriteCodec)
}
