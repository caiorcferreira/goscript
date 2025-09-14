package filesystem

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"

	"github.com/caiorcferreira/goscript/internal/pipeline"
	"github.com/google/uuid"
)

// CSVCodec parses CSV file content
type CSVCodec struct {
	Separator rune
	Comment   rune
	Headers   []string
}

// Ensure CSVCodec implements all interfaces
var _ ReadCodec = (*CSVCodec)(nil)
var _ WriteCodec = (*CSVCodec)(nil)

func NewCSVCodec() *CSVCodec {
	return &CSVCodec{
		Separator: ',',
		Comment:   '#',
	}
}

func (c *CSVCodec) WithSeparator(sep rune) *CSVCodec {
	c.Separator = sep
	return c
}

func (c *CSVCodec) WithComment(comment rune) *CSVCodec {
	c.Comment = comment
	return c
}

func (c *CSVCodec) Parse(ctx context.Context, reader io.Reader, pipe pipeline.Pipe) error {
	defer pipe.Close()

	csvReader := csv.NewReader(reader)
	csvReader.Comma = c.Separator
	csvReader.Comment = c.Comment

	records, err := csvReader.ReadAll()
	if err != nil {
		return err
	}

	for _, record := range records {
		select {
		case <-ctx.Done():
			return nil
		default:
			msg := pipeline.Msg{
				ID:   uuid.NewString(),
				Data: record,
			}
			select {
			case pipe.Out() <- msg:
			case <-ctx.Done():
				return nil
			}
		}
	}

	return nil
}

func (c *CSVCodec) Encode(ctx context.Context, pipe pipeline.Pipe, writer io.Writer) error {
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

func (c *CSVCodec) castDataToCSVRow(data any) []string {
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
