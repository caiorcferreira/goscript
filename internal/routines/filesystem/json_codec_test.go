package filesystem_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/caiorcferreira/goscript/internal/pipeline"
	"github.com/caiorcferreira/goscript/internal/routines/filesystem"
	"github.com/stretchr/testify/assert"
)

func TestJSONCodec_Parse(t *testing.T) {
	t.Run("parses single JSON object", func(t *testing.T) {
		codec := filesystem.NewJSONCodec()
		content := `{"name": "John", "age": 30}`
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		var results []map[string]any
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			for msg := range pipe.Out() {
				results = append(results, msg.Data.(map[string]any))
			}
		}()

		ctx := context.Background()
		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err)

		wg.Wait()

		assert.Len(t, results, 1)
		assert.Equal(t, "John", results[0]["name"])
		assert.Equal(t, float64(30), results[0]["age"])
	})

	t.Run("parses JSON array as individual items by default", func(t *testing.T) {
		codec := filesystem.NewJSONCodec()
		content := `[{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]`
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		var results []map[string]any
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			for msg := range pipe.Out() {
				results = append(results, msg.Data.(map[string]any))
			}
		}()

		ctx := context.Background()
		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err)

		wg.Wait()

		assert.Len(t, results, 2)
		assert.Equal(t, "John", results[0]["name"])
		assert.Equal(t, "Jane", results[1]["name"])
	})

	t.Run("parses JSON array in array mode", func(t *testing.T) {
		codec := filesystem.NewJSONCodec().WithJSONArrayMode()
		content := `[{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]`
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		var results []map[string]any
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			for msg := range pipe.Out() {
				results = append(results, msg.Data.(map[string]any))
			}
		}()

		ctx := context.Background()
		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err)

		wg.Wait()

		assert.Len(t, results, 2)
		assert.Equal(t, "John", results[0]["name"])
		assert.Equal(t, "Jane", results[1]["name"])
	})

	t.Run("parses JSON lines", func(t *testing.T) {
		codec := filesystem.NewJSONCodec().WithJSONLinesMode()
		content := `{"name": "John", "age": 30}
{"name": "Jane", "age": 25}
{"name": "Bob", "age": 35}`
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		var results []map[string]any
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			for msg := range pipe.Out() {
				results = append(results, msg.Data.(map[string]any))
			}
		}()

		ctx := context.Background()
		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err)

		wg.Wait()

		assert.Len(t, results, 3)
		assert.Equal(t, "John", results[0]["name"])
		assert.Equal(t, "Jane", results[1]["name"])
		assert.Equal(t, "Bob", results[2]["name"])
	})

	t.Run("handles empty lines in JSON lines mode", func(t *testing.T) {
		codec := filesystem.NewJSONCodec().WithJSONLinesMode()
		content := `{"name": "John", "age": 30}

{"name": "Jane", "age": 25}

`
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		var results []map[string]any
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			for msg := range pipe.Out() {
				results = append(results, msg.Data.(map[string]any))
			}
		}()

		ctx := context.Background()
		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err)

		wg.Wait()

		assert.Len(t, results, 2)
		assert.Equal(t, "John", results[0]["name"])
		assert.Equal(t, "Jane", results[1]["name"])
	})

	t.Run("handles context cancellation during parsing", func(t *testing.T) {
		codec := filesystem.NewJSONCodec().WithJSONLinesMode()
		content := `{"name": "John", "age": 30}
{"name": "Jane", "age": 25}
{"name": "Bob", "age": 35}`
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err) // Should handle gracefully
	})

	t.Run("returns error for invalid JSON", func(t *testing.T) {
		codec := filesystem.NewJSONCodec()
		content := `{"invalid": json}`
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		ctx := context.Background()
		err := codec.Parse(ctx, reader, pipe)
		assert.Error(t, err)
	})

	t.Run("returns error for invalid JSON in lines mode", func(t *testing.T) {
		codec := filesystem.NewJSONCodec().WithJSONLinesMode()
		content := `{"name": "John", "age": 30}
{"invalid": json}
{"name": "Jane", "age": 25}`
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		ctx := context.Background()
		err := codec.Parse(ctx, reader, pipe)
		assert.Error(t, err)
	})

	t.Run("returns error for invalid JSON in array mode", func(t *testing.T) {
		codec := filesystem.NewJSONCodec().WithJSONArrayMode()
		content := `[{"name": "John"}, invalid]`
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		ctx := context.Background()
		err := codec.Parse(ctx, reader, pipe)
		assert.Error(t, err)
	})
}

func TestJSONCodec_Encode(t *testing.T) {
	t.Run("encodes messages as JSON lines by default", func(t *testing.T) {
		codec := filesystem.NewJSONCodec().WithJSONLinesMode()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: map[string]any{"name": "John", "age": 30}},
			{ID: "2", Data: map[string]any{"name": "Jane", "age": 25}},
		}

		ctx := context.Background()
		for _, msg := range messages {
			err := codec.Encode(ctx, msg, &buffer)
			assert.NoError(t, err)
		}

		result := buffer.String()
		lines := strings.Split(strings.TrimSpace(result), "\n")
		assert.Len(t, lines, 2)
		assert.Contains(t, lines[0], `"name":"John"`)
		assert.Contains(t, lines[1], `"name":"Jane"`)
	})

	t.Run("encodes as JSON array", func(t *testing.T) {
		codec := filesystem.NewJSONCodec()
		var buffer bytes.Buffer

		msg := pipeline.Msg{
			ID:   "1",
			Data: []map[string]any{{"name": "John"}, {"name": "Jane"}},
		}

		ctx := context.Background()
		err := codec.Encode(ctx, msg, &buffer)
		assert.NoError(t, err)

		result := buffer.String()
		var data []map[string]any
		err = json.Unmarshal([]byte(result), &data)
		assert.NoError(t, err)
		assert.Len(t, data, 2)
		assert.Equal(t, "John", data[0]["name"])
		assert.Equal(t, "Jane", data[1]["name"])
	})

	t.Run("encodes complex data structures", func(t *testing.T) {
		codec := filesystem.NewJSONCodec()
		var buffer bytes.Buffer

		complexData := map[string]any{
			"string": "hello",
			"number": 42,
			"bool":   true,
			"array":  []int{1, 2, 3},
			"nested": map[string]any{"key": "value"},
		}

		msg := pipeline.Msg{ID: "1", Data: complexData}

		ctx := context.Background()
		err := codec.Encode(ctx, msg, &buffer)
		assert.NoError(t, err)

		result := buffer.String()
		var decoded map[string]any
		err = json.Unmarshal([]byte(result), &decoded)
		assert.NoError(t, err)
		assert.Equal(t, "hello", decoded["string"])
		assert.Equal(t, float64(42), decoded["number"])
		assert.Equal(t, true, decoded["bool"])
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		codec := filesystem.NewJSONCodec()
		var buffer bytes.Buffer

		msg := pipeline.Msg{ID: "1", Data: map[string]string{"test": "data"}}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		err := codec.Encode(ctx, msg, &buffer)
		// Should still encode the message since cancellation is checked during processing
		assert.NoError(t, err)
	})

	t.Run("encodes string messages as JSON strings", func(t *testing.T) {
		codec := filesystem.NewJSONCodec()
		var buffer bytes.Buffer

		msg := pipeline.Msg{ID: "1", Data: "hello world"}

		ctx := context.Background()
		err := codec.Encode(ctx, msg, &buffer)
		assert.NoError(t, err)

		result := buffer.String()
		var decoded string
		err = json.Unmarshal([]byte(result), &decoded)
		assert.NoError(t, err)
		assert.Equal(t, "hello world", decoded)
	})

	t.Run("encodes various data types", func(t *testing.T) {
		codec := filesystem.NewJSONCodec().WithJSONLinesMode()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: 42},
			{ID: "2", Data: true},
			{ID: "3", Data: 3.14},
			{ID: "4", Data: "string"},
			{ID: "5", Data: []int{1, 2, 3}},
		}

		ctx := context.Background()
		for _, msg := range messages {
			err := codec.Encode(ctx, msg, &buffer)
			assert.NoError(t, err)
		}

		result := buffer.String()
		lines := strings.Split(strings.TrimSpace(result), "\n")
		assert.Len(t, lines, 5)
		assert.Equal(t, "42", lines[0])
		assert.Equal(t, "true", lines[1])
		assert.Equal(t, "3.14", lines[2])
		assert.Equal(t, `"string"`, lines[3])
		assert.Equal(t, "[1,2,3]", lines[4])
	})
}

func TestJSONCodec_Interfaces(t *testing.T) {
	t.Run("implements ReadCodec interface", func(t *testing.T) {
		var codec filesystem.ReadCodec = filesystem.NewJSONCodec()
		assert.NotNil(t, codec)
	})

	t.Run("implements WriteCodec interface", func(t *testing.T) {
		var codec filesystem.WriteCodec = filesystem.NewJSONCodec()
		assert.NotNil(t, codec)
	})
}
