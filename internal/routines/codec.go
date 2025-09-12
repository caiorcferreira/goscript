package routines

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"github.com/caiorcferreira/goscript/internal/pipeline"
	"github.com/google/uuid"
	"io"
)

// Codec defines the interface for parsing file content into messages
// Now writes directly to a Pipe and supports context
type Codec interface {
	// Parse reads from the reader and writes messages to the pipe
	Parse(ctx context.Context, reader io.Reader, pipe pipeline.Pipe) error
}

// LineCodec parses file content line by line
type LineCodec struct{}

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

// CSVCodec parses CSV file content
type CSVCodec struct {
	Separator rune
	Comment   rune
}

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

// JSONCodec parses JSON file content
// Supports both single JSON objects and JSON arrays
type JSONCodec struct {
	// JSONLines when true, treats each line as a separate JSON object (JSONL format)
	JSONLines bool
	JSONArray bool
}

func NewJSONCodec() *JSONCodec {
	return &JSONCodec{
		JSONLines: false,
		JSONArray: false,
	}
}

func (c *JSONCodec) WithJSONLinesMode() *JSONCodec {
	c.JSONLines = true
	return c
}

func (c *JSONCodec) WithJSONArrayMode() *JSONCodec {
	c.JSONArray = true
	return c
}

func (c *JSONCodec) Parse(ctx context.Context, reader io.Reader, pipe pipeline.Pipe) error {
	defer pipe.Close()

	if c.JSONLines {
		return c.parseJSONLines(ctx, reader, pipe)
	}

	if c.JSONArray {
		return c.parseJSONArray(ctx, reader, pipe)
	}

	return c.parseJSON(ctx, reader, pipe)
}

func (c *JSONCodec) parseJSON(ctx context.Context, reader io.Reader, pipe pipeline.Pipe) error {
	decoder := json.NewDecoder(reader)

	var objectData any
	if err := decoder.Decode(&objectData); err != nil {
		return err
	}

	msg := pipeline.Msg{
		ID:   uuid.NewString(),
		Data: objectData,
	}

	select {
	case pipe.Out() <- msg:
	case <-ctx.Done():
		return nil
	}

	return nil
}

func (c *JSONCodec) parseJSONLines(ctx context.Context, reader io.Reader, pipe pipeline.Pipe) error {
	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return nil
		default:
			line := bytes.TrimSpace(scanner.Bytes())
			if len(line) == 0 {
				continue
			}

			var data any
			if err := json.Unmarshal(line, &data); err != nil {
				return err
			}

			msg := pipeline.Msg{
				ID:   uuid.NewString(),
				Data: data,
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

func (c *JSONCodec) parseJSONArray(ctx context.Context, reader io.Reader, pipe pipeline.Pipe) error {
	decoder := json.NewDecoder(reader)

	var arrayData []any
	err := decoder.Decode(&arrayData)
	if err != nil {
		return err
	}

	for _, item := range arrayData {
		select {
		case <-ctx.Done():
			return nil
		default:
			msg := pipeline.Msg{
				ID:   uuid.NewString(),
				Data: item,
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

// BlobCodec returns the entire file content as a single message
type BlobCodec struct {
	// AsString when true, returns content as string, otherwise as []byte
	AsString bool
}

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
