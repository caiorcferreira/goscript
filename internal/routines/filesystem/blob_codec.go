package filesystem

import (
	"context"
	"fmt"
	"github.com/caiorcferreira/goscript/internal/pipeline"
	"github.com/google/uuid"
	"io"
)

// BlobCodec returns the entire file content as a single message
type BlobCodec struct {
	// AsString when true, returns content as string, otherwise as []byte
	AsString bool
}

// Ensure BlobCodec implements all interfaces
var _ ReadCodec = (*BlobCodec)(nil)
var _ WriteCodec = (*BlobCodec)(nil)
var _ Codec = (*BlobCodec)(nil)

func NewBlobCodec() *BlobCodec {
	return &BlobCodec{
		AsString: true,
	}
}

func (c *BlobCodec) AsBytes() *BlobCodec {
	c.AsString = false
	return c
}

func (c *BlobCodec) AsStrings() *BlobCodec {
	c.AsString = true
	return c
}

func (c *BlobCodec) Parse(ctx context.Context, reader io.Reader, pipe pipeline.Pipe) error {
	defer pipe.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	var msgData any
	if c.AsString {
		msgData = string(data)
	} else {
		msgData = data
	}

	msg := pipeline.Msg{
		ID:   uuid.NewString(),
		Data: msgData,
	}

	select {
	case pipe.Out() <- msg:
	case <-ctx.Done():
		return nil
	}

	return nil
}

// Encode implements WriteCodec interface for BlobCodec
func (c *BlobCodec) Encode(ctx context.Context, pipe pipeline.Pipe, writer io.Writer) error {
	defer pipe.Close()

	for msg := range pipe.In() {
		select {
		case <-ctx.Done():
			return nil
		default:
			switch v := msg.Data.(type) {
			case string:
				if _, err := writer.Write([]byte(v)); err != nil {
					return err
				}
			case []byte:
				if _, err := writer.Write(v); err != nil {
					return err
				}
			default:
				// Convert other types to string representation
				str := fmt.Sprintf("%v", v)
				if _, err := writer.Write([]byte(str)); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
