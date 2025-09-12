package filesystem

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/caiorcferreira/goscript/internal/pipeline"
	"io"
)

// CSVWriteCodec writes messages as CSV records to a writer
type CSVWriteCodec struct {
	Separator rune
}

// Ensure CSVWriteCodec implements WriteCodec
var _ WriteCodec = (*CSVWriteCodec)(nil)

func NewCSVWriteCodec() *CSVWriteCodec {
	return &CSVWriteCodec{
		Separator: ',',
	}
}

func (c *CSVWriteCodec) WithSeparator(sep rune) *CSVWriteCodec {
	c.Separator = sep
	return c
}

func (c *CSVWriteCodec) Encode(ctx context.Context, pipe pipeline.Pipe, writer io.Writer) error {
	defer pipe.Close()

	csvWriter := csv.NewWriter(writer)
	csvWriter.Comma = c.Separator
	defer csvWriter.Flush()

	for msg := range pipe.In() {
		select {
		case <-ctx.Done():
			return nil
		default:
			switch v := msg.Data.(type) {
			case []string:
				if err := csvWriter.Write(v); err != nil {
					return err
				}
			case string:
				// Split string by separator and write as CSV record
				record := []string{v}
				if err := csvWriter.Write(record); err != nil {
					return err
				}
			default:
				// Convert to string and write as single field
				record := []string{fmt.Sprintf("%v", v)}
				if err := csvWriter.Write(record); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
