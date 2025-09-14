package filesystem

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"github.com/caiorcferreira/goscript/internal/pipeline"
	"github.com/google/uuid"
	"io"
)

// JSONCodec parses JSON file content
// Supports both single JSON objects and JSON arrays
type JSONCodec struct {
	//todo: create an enum for modes
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
func (c *JSONCodec) Encode(ctx context.Context, msg pipeline.Msg, writer io.Writer) error {
	encoder := json.NewEncoder(writer)

	// For regular JSON, just encode the single message
	if err := encoder.Encode(msg.Data); err != nil {
		return err
	}

	return nil
}
