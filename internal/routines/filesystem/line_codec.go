package filesystem

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/caiorcferreira/goscript/internal/pipeline"
	"github.com/google/uuid"
)

// LineCodec parses file content line by line
type LineCodec struct{}

// Ensure LineCodec implements all interfaces
var _ ReadCodec = (*LineCodec)(nil)
var _ WriteCodec = (*LineCodec)(nil)

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

			slog.Debug("parsed line", "line", text, "msg_id", msg.ID)

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
			line := castDataToLine(msg.Data)

			slog.Debug("encoded line", "line", line, "msg_id", msg.ID)

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
