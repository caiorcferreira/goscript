package filesystem

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/caiorcferreira/goscript/internal/pipeline"
	"github.com/google/uuid"
	"io"
)

// ReadCodec defines the interface for parsing file content into messages
// Reads from a reader and writes messages to a pipe
type ReadCodec interface {
	// Parse reads from the reader and writes messages to the pipe
	Parse(ctx context.Context, reader io.Reader, pipe pipeline.Pipe) error
}

// WriteCodec defines the interface for encoding messages to file content
// Reads messages from a pipe and writes them to a writer
type WriteCodec interface {
	// Encode reads messages from the pipe and writes them to the writer
	Encode(ctx context.Context, pipe pipeline.Pipe, writer io.Writer) error
}

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

// JSONCodec parses JSON file content
// Supports both single JSON objects and JSON arrays
type JSONCodec struct {
	// JSONLines when true, treats each line as a separate JSON object (JSONL format)
	JSONLines bool
	JSONArray bool
}

// Ensure JSONCodec implements all interfaces
var _ ReadCodec = (*JSONCodec)(nil)
var _ WriteCodec = (*JSONCodec)(nil)

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

	// Auto-detect arrays and process them as individual elements for backward compatibility
	if arrayData, ok := objectData.([]any); ok {
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
	} else {
		msg := pipeline.Msg{
			ID:   uuid.NewString(),
			Data: objectData,
		}

		select {
		case pipe.Out() <- msg:
		case <-ctx.Done():
			return nil
		}
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

// Encode implements WriteCodec interface for JSONCodec
func (c *JSONCodec) Encode(ctx context.Context, pipe pipeline.Pipe, writer io.Writer) error {
	defer pipe.Close()

	if c.JSONLines {
		return c.encodeJSONLines(ctx, pipe, writer)
	}

	if c.JSONArray {
		return c.encodeJSONArray(ctx, pipe, writer)
	}

	return c.encodeJSON(ctx, pipe, writer)
}

func (c *JSONCodec) encodeJSON(ctx context.Context, pipe pipeline.Pipe, writer io.Writer) error {
	encoder := json.NewEncoder(writer)

	for msg := range pipe.In() {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err := encoder.Encode(msg.Data); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *JSONCodec) encodeJSONLines(ctx context.Context, pipe pipeline.Pipe, writer io.Writer) error {
	encoder := json.NewEncoder(writer)

	for msg := range pipe.In() {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err := encoder.Encode(msg.Data); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *JSONCodec) encodeJSONArray(ctx context.Context, pipe pipeline.Pipe, writer io.Writer) error {
	var messages []any

	// Collect all messages first
	for msg := range pipe.In() {
		select {
		case <-ctx.Done():
			return nil
		default:
			messages = append(messages, msg.Data)
		}
	}

	// Write as JSON array
	encoder := json.NewEncoder(writer)
	return encoder.Encode(messages)
}

// BlobCodec returns the entire file content as a single message
type BlobCodec struct {
	// AsString when true, returns content as string, otherwise as []byte
	AsString bool
}

// Ensure BlobCodec implements all interfaces
var _ ReadCodec = (*BlobCodec)(nil)
var _ WriteCodec = (*BlobCodec)(nil)

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

// WriteCodec implementations

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

// JSONWriteCodec writes messages as JSON to a writer
type JSONWriteCodec struct {
	// JSONArray when true, writes messages as a JSON array
	JSONArray bool
	// JSONLines when true, writes each message as a separate JSON line (JSONL format)
	JSONLines bool
}

// Ensure JSONWriteCodec implements WriteCodec
var _ WriteCodec = (*JSONWriteCodec)(nil)

func NewJSONWriteCodec() *JSONWriteCodec {
	return &JSONWriteCodec{
		JSONArray: false,
		JSONLines: false,
	}
}

func (c *JSONWriteCodec) WithJSONArrayMode() *JSONWriteCodec {
	c.JSONArray = true
	c.JSONLines = false
	return c
}

func (c *JSONWriteCodec) WithJSONLinesMode() *JSONWriteCodec {
	c.JSONLines = true
	c.JSONArray = false
	return c
}

func (c *JSONWriteCodec) Encode(ctx context.Context, pipe pipeline.Pipe, writer io.Writer) error {
	defer pipe.Close()

	if c.JSONLines {
		return c.encodeJSONLines(ctx, pipe, writer)
	}

	if c.JSONArray {
		return c.encodeJSONArray(ctx, pipe, writer)
	}

	return c.encodeJSON(ctx, pipe, writer)
}

func (c *JSONWriteCodec) encodeJSON(ctx context.Context, pipe pipeline.Pipe, writer io.Writer) error {
	encoder := json.NewEncoder(writer)

	for msg := range pipe.In() {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err := encoder.Encode(msg.Data); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *JSONWriteCodec) encodeJSONLines(ctx context.Context, pipe pipeline.Pipe, writer io.Writer) error {
	encoder := json.NewEncoder(writer)

	for msg := range pipe.In() {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err := encoder.Encode(msg.Data); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *JSONWriteCodec) encodeJSONArray(ctx context.Context, pipe pipeline.Pipe, writer io.Writer) error {
	var messages []any

	// Collect all messages first
	for msg := range pipe.In() {
		select {
		case <-ctx.Done():
			return nil
		default:
			messages = append(messages, msg.Data)
		}
	}

	// Write as JSON array
	encoder := json.NewEncoder(writer)
	return encoder.Encode(messages)
}

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
