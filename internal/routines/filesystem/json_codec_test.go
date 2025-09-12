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
	"github.com/stretchr/testify/require"
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
	t.Run("encodes as separate JSON objects by default", func(t *testing.T) {
		codec := filesystem.NewJSONCodec()
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: map[string]any{"name": "John", "age": 30}},
			{ID: "2", Data: map[string]any{"name": "Jane", "age": 25}},
		}

		go func() {
			for _, msg := range messages {
				pipe.In() <- msg
			}
			close(pipe.In())
		}()

		ctx := context.Background()
		err := codec.Encode(ctx, pipe, &buffer)
		assert.NoError(t, err)

		result := buffer.String()
		assert.Contains(t, result, `{"age":30,"name":"John"}`)
		assert.Contains(t, result, `{"age":25,"name":"Jane"}`)
	})

	t.Run("encodes as JSON lines", func(t *testing.T) {
		codec := filesystem.NewJSONCodec().WithJSONLinesMode()
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: map[string]any{"name": "John", "age": 30}},
			{ID: "2", Data: map[string]any{"name": "Jane", "age": 25}},
		}

		go func() {
			for _, msg := range messages {
				pipe.In() <- msg
			}
			close(pipe.In())
		}()

		ctx := context.Background()
		err := codec.Encode(ctx, pipe, &buffer)
		assert.NoError(t, err)

		result := buffer.String()
		lines := strings.Split(strings.TrimSpace(result), "\n")
		assert.Len(t, lines, 2)
		assert.Contains(t, lines[0], `"name":"John"`)
		assert.Contains(t, lines[1], `"name":"Jane"`)
	})

	t.Run("encodes as JSON array", func(t *testing.T) {
		codec := filesystem.NewJSONCodec().WithJSONArrayMode()
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: map[string]any{"name": "John", "age": 30}},
			{ID: "2", Data: map[string]any{"name": "Jane", "age": 25}},
		}

		go func() {
			for _, msg := range messages {
				pipe.In() <- msg
			}
			close(pipe.In())
		}()

		ctx := context.Background()
		err := codec.Encode(ctx, pipe, &buffer)
		assert.NoError(t, err)

		result := buffer.String()
		// Should be a valid JSON array
		var decoded []map[string]any
		err = json.Unmarshal([]byte(result), &decoded)
		require.NoError(t, err)
		assert.Len(t, decoded, 2)
		assert.Equal(t, "John", decoded[0]["name"])
		assert.Equal(t, "Jane", decoded[1]["name"])
	})

	t.Run("handles empty input pipe", func(t *testing.T) {
		codec := filesystem.NewJSONCodec()
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		close(pipe.In())

		ctx := context.Background()
		err := codec.Encode(ctx, pipe, &buffer)
		assert.NoError(t, err)

		assert.Equal(t, "", buffer.String())
	})

	t.Run("handles empty input pipe in array mode", func(t *testing.T) {
		codec := filesystem.NewJSONCodec().WithJSONArrayMode()
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		close(pipe.In())

		ctx := context.Background()
		err := codec.Encode(ctx, pipe, &buffer)
		assert.NoError(t, err)

		result := strings.TrimSpace(buffer.String())
		// Empty array mode results in null when no messages
		assert.Equal(t, "null", result)
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		codec := filesystem.NewJSONCodec()
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: map[string]any{"name": "John"}},
			{ID: "2", Data: map[string]any{"name": "Jane"}},
		}

		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			pipe.In() <- messages[0]
			cancel() // Cancel after first message
			pipe.In() <- messages[1]
			close(pipe.In())
		}()

		err := codec.Encode(ctx, pipe, &buffer)
		assert.NoError(t, err)
	})

	t.Run("encodes various data types", func(t *testing.T) {
		codec := filesystem.NewJSONCodec()
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: "string"},
			{ID: "2", Data: 123},
			{ID: "3", Data: true},
			{ID: "4", Data: []int{1, 2, 3}},
		}

		go func() {
			for _, msg := range messages {
				pipe.In() <- msg
			}
			close(pipe.In())
		}()

		ctx := context.Background()
		err := codec.Encode(ctx, pipe, &buffer)
		assert.NoError(t, err)

		result := buffer.String()
		assert.Contains(t, result, `"string"`)
		assert.Contains(t, result, `123`)
		assert.Contains(t, result, `true`)
		assert.Contains(t, result, `[1,2,3]`)
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
