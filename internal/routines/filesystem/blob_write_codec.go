package filesystem

import (
	"context"
	"fmt"
	"github.com/caiorcferreira/goscript/internal/pipeline"
	"io"
)

// BlobWriteCodec writes messages as raw data to a writer
type BlobWriteCodec struct{}

// Ensure BlobWriteCodec implements WriteCodec
var _ WriteCodec = (*BlobWriteCodec)(nil)

func NewBlobWriteCodec() *BlobWriteCodec {
	return &BlobWriteCodec{}
}

func (c *BlobWriteCodec) Encode(ctx context.Context, pipe pipeline.Pipe, writer io.Writer) error {
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
