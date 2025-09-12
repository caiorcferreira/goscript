package filesystem

import (
	"context"
	"fmt"
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
			line := castDataToLine(msg.Data)
			if _, err := writer.Write(line); err != nil {
				return err
			}
		}
	}

	return nil
}

func castDataToLine(data any) []byte {
	switch v := data.(type) {
	case string:
		return []byte(v + "\n")
	case []byte:
		return append(v, '\n')
	default:
		return []byte(fmt.Sprintf("%v\n", v))
	}
}
