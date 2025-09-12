package filesystem

import (
	"context"
	"github.com/caiorcferreira/goscript/internal/pipeline"
	"io"
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
