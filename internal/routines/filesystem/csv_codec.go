package filesystem

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/caiorcferreira/goscript/internal/pipeline"
	"github.com/google/uuid"
	"io"
)

// CSVCodec parses CSV file content
type CSVCodec struct {
	Separator rune
	Comment   rune
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

// Encode implements WriteCodec interface for CSVCodec
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
