package filesystem

import (
	"context"
	"github.com/caiorcferreira/goscript/internal/pipeline"
	"io"
)

// LineWriteCodec writes messages as lines to a writer
type LineWriteCodec struct{}

// Ensure LineWriteCodec implements WriteCodec
var _ WriteCodec = (*LineWriteCodec)(nil)

func NewLineWriteCodec() *LineWriteCodec {
	return &LineWriteCodec{}
}

func (c *LineWriteCodec) Encode(ctx context.Context, pipe pipeline.Pipe, writer io.Writer) error {
	defer pipe.Close()

	for msg := range pipe.In() {
		select {
		case <-ctx.Done():
			return nil
		default:
			switch v := msg.Data.(type) {
			case string:
				if _, err := writer.Write([]byte(v + "\n")); err != nil {
					return err
				}
			case []byte:
				if _, err := writer.Write(v); err != nil {
					return err
				}
				// Note: Other types are ignored to maintain backward compatibility
				// The original implementation only handled strings and []byte
			}
		}
	}

	return nil
}
