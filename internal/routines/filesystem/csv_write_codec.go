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
	Headers   []string
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
			row := c.castDataToCSVRow(msg.Data)
			if err := csvWriter.Write(row); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *CSVWriteCodec) castDataToCSVRow(data any) []string {
	switch v := data.(type) {
	case []string:
		return v
	case string:
		return []string{v}
	case map[string]any:
		values := make([]string, 0, len(c.Headers))
		for _, header := range c.Headers {
			if val, ok := v[header]; ok {
				values = append(values, fmt.Sprintf("%v", val))
			} else {
				values = append(values, "")
			}
		}

		return values
	case []any:
		values := make([]string, len(v))
		for i, item := range v {
			values[i] = fmt.Sprintf("%v", item)
		}

		return values
	default:
		// Convert to string and write as single field
		return []string{fmt.Sprintf("%v", v)}
	}
}
