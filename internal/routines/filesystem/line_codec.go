package filesystem

import (
	"bufio"
	"context"
	"github.com/caiorcferreira/goscript/internal/pipeline"
	"github.com/google/uuid"
	"io"
)

// LineCodec parses file content line by line
type LineCodec struct{}

// Ensure LineCodec implements all interfaces
var _ ReadCodec = (*LineCodec)(nil)
var _ WriteCodec = (*LineCodec)(nil)
var _ Codec = (*LineCodec)(nil)

func NewLineCodec() *LineCodec {
	return &LineCodec{}
}

func (c *LineCodec) Parse(ctx context.Context, reader io.Reader, pipe pipeline.Pipe) error {
	defer pipe.Close()
	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return nil
		default:
			text := scanner.Text()
			msg := pipeline.Msg{
				ID:   uuid.NewString(),
				Data: text,
			}
			select {
			case pipe.Out() <- msg:
			case <-ctx.Done():
				return nil
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

// Encode implements WriteCodec interface for LineCodec
func (c *LineCodec) Encode(ctx context.Context, pipe pipeline.Pipe, writer io.Writer) error {
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
