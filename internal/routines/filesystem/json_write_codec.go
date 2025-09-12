package filesystem

import (
	"context"
	"encoding/json"
	"github.com/caiorcferreira/goscript/internal/pipeline"
	"io"
	"log/slog"
)

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

	// Ensure we write the JSON array at the end
	defer func() {
		encoder := json.NewEncoder(writer)
		err := encoder.Encode(messages)
		if err != nil {
			slog.Error("failed to encode JSON array", "error", err)
		}
	}()

	// Collect all messages first
	for msg := range pipe.In() {
		select {
		case <-ctx.Done():
			return nil
		default:
			messages = append(messages, msg.Data)
		}
	}

	return nil
}
