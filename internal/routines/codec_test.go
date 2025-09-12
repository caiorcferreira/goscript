package routines_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/caiorcferreira/goscript/internal/pipeline"
	"github.com/caiorcferreira/goscript/internal/routines"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to collect messages from pipe
func collectMessages(pipe *pipeline.ChannelPipe, timeout time.Duration) []pipeline.Msg {
	var messages []pipeline.Msg
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for msg := range pipe.Out() {
		messages = append(messages, msg)
	}

	return messages
}

func TestLineCodec(t *testing.T) {
	t.Run("parses lines successfully", func(t *testing.T) {
		content := "line1\nline2\nline3"
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()
		ctx := context.Background()

		codec := routines.NewLineCodec()

		// Run codec in goroutine
		go func() {
			err := codec.Parse(ctx, reader, pipe)
			require.NoError(t, err)
		}()

		messages := collectMessages(pipe, 100*time.Millisecond)

		require.Len(t, messages, 3)

		assert.Equal(t, "line1", messages[0].Data)
		assert.Equal(t, "line2", messages[1].Data)
		assert.Equal(t, "line3", messages[2].Data)

		// Check that IDs are set
		assert.NotEmpty(t, messages[0].ID)
		assert.NotEmpty(t, messages[1].ID)
		assert.NotEmpty(t, messages[2].ID)
	})

	t.Run("handles empty file", func(t *testing.T) {
		reader := strings.NewReader("")
		pipe := pipeline.NewChanPipe()
		ctx := context.Background()

		codec := routines.NewLineCodec()

		go func() {
			err := codec.Parse(ctx, reader, pipe)
			require.NoError(t, err)
		}()

		messages := collectMessages(pipe, 100*time.Millisecond)

		assert.Empty(t, messages)
	})

	t.Run("handles single line without newline", func(t *testing.T) {
		content := "single line"
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()
		ctx := context.Background()

		codec := routines.NewLineCodec()

		go func() {
			err := codec.Parse(ctx, reader, pipe)
			require.NoError(t, err)
		}()

		messages := collectMessages(pipe, 100*time.Millisecond)

		require.Len(t, messages, 1)
		assert.Equal(t, "single line", messages[0].Data)
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		content := "line1\nline2\nline3"
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()
		ctx, cancel := context.WithCancel(context.Background())

		codec := routines.NewLineCodec()

		// Cancel context immediately
		cancel()

		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err) // Should return nil when context is cancelled
	})
}

func TestCSVCodec(t *testing.T) {
	t.Run("parses CSV with default settings", func(t *testing.T) {
		content := "name,age,city\nJohn,30,NYC\nJane,25,LA"
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()
		ctx := context.Background()

		codec := routines.NewCSVCodec()

		go func() {
			err := codec.Parse(ctx, reader, pipe)
			require.NoError(t, err)
		}()

		messages := collectMessages(pipe, 100*time.Millisecond)

		require.Len(t, messages, 3)

		// Check header row
		headerRow := messages[0].Data.([]string)
		assert.Equal(t, []string{"name", "age", "city"}, headerRow)

		// Check data rows
		dataRow1 := messages[1].Data.([]string)
		assert.Equal(t, []string{"John", "30", "NYC"}, dataRow1)

		dataRow2 := messages[2].Data.([]string)
		assert.Equal(t, []string{"Jane", "25", "LA"}, dataRow2)

		// Check that IDs are set
		assert.NotEmpty(t, messages[0].ID)
		assert.NotEmpty(t, messages[1].ID)
		assert.NotEmpty(t, messages[2].ID)
	})

	t.Run("parses CSV with custom separator", func(t *testing.T) {
		content := "name;age;city\nJohn;30;NYC"
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()
		ctx := context.Background()

		codec := routines.NewCSVCodec().WithSeparator(';')

		go func() {
			err := codec.Parse(ctx, reader, pipe)
			require.NoError(t, err)
		}()

		messages := collectMessages(pipe, 100*time.Millisecond)

		require.Len(t, messages, 2)

		headerRow := messages[0].Data.([]string)
		assert.Equal(t, []string{"name", "age", "city"}, headerRow)

		dataRow := messages[1].Data.([]string)
		assert.Equal(t, []string{"John", "30", "NYC"}, dataRow)
	})

	t.Run("handles CSV comments", func(t *testing.T) {
		content := "# This is a comment\nname,age\nJohn,30"
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()
		ctx := context.Background()

		codec := routines.NewCSVCodec().WithComment('#')

		go func() {
			err := codec.Parse(ctx, reader, pipe)
			require.NoError(t, err)
		}()

		messages := collectMessages(pipe, 100*time.Millisecond)

		require.Len(t, messages, 2) // Comment line should be ignored

		headerRow := messages[0].Data.([]string)
		assert.Equal(t, []string{"name", "age"}, headerRow)

		dataRow := messages[1].Data.([]string)
		assert.Equal(t, []string{"John", "30"}, dataRow)
	})

	t.Run("handles empty CSV", func(t *testing.T) {
		reader := strings.NewReader("")
		pipe := pipeline.NewChanPipe()
		ctx := context.Background()

		codec := routines.NewCSVCodec()

		go func() {
			err := codec.Parse(ctx, reader, pipe)
			require.NoError(t, err)
		}()

		messages := collectMessages(pipe, 100*time.Millisecond)

		assert.Empty(t, messages)
	})

	t.Run("handles malformed CSV", func(t *testing.T) {
		content := "name,age\nJohn,30,extra"
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()
		ctx := context.Background()

		codec := routines.NewCSVCodec()

		err := codec.Parse(ctx, reader, pipe)

		// CSV reader is strict by default, should return error for mismatched fields
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of fields")
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		content := "name,age\nJohn,30\nJane,25"
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()
		ctx, cancel := context.WithCancel(context.Background())

		codec := routines.NewCSVCodec()

		// Cancel context immediately
		cancel()

		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err) // Should return nil when context is cancelled
	})
}

func TestJSONCodec(t *testing.T) {
	t.Run("parses JSON array", func(t *testing.T) {
		content := `[{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]`
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()
		ctx := context.Background()

		codec := routines.NewJSONCodec().WithJSONArrayMode()

		go func() {
			err := codec.Parse(ctx, reader, pipe)
			require.NoError(t, err)
		}()

		messages := collectMessages(pipe, 100*time.Millisecond)

		require.Len(t, messages, 2)

		// Check first object
		obj1 := messages[0].Data.(map[string]any)
		assert.Equal(t, "John", obj1["name"])
		assert.Equal(t, float64(30), obj1["age"]) // JSON numbers are float64

		// Check second object
		obj2 := messages[1].Data.(map[string]any)
		assert.Equal(t, "Jane", obj2["name"])
		assert.Equal(t, float64(25), obj2["age"])

		// Check that IDs are set
		assert.NotEmpty(t, messages[0].ID)
		assert.NotEmpty(t, messages[1].ID)
	})

	t.Run("parses single JSON object", func(t *testing.T) {
		content := `{"name": "John", "age": 30}`
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()
		ctx := context.Background()

		codec := routines.NewJSONCodec()

		go func() {
			err := codec.Parse(ctx, reader, pipe)
			require.NoError(t, err)
		}()

		messages := collectMessages(pipe, 100*time.Millisecond)

		require.Len(t, messages, 1)

		obj := messages[0].Data.(map[string]any)
		assert.Equal(t, "John", obj["name"])
		assert.Equal(t, float64(30), obj["age"])
		assert.NotEmpty(t, messages[0].ID)
	})

	t.Run("parses JSON lines (stream mode)", func(t *testing.T) {
		content := `{"name": "John", "age": 30}
{"name": "Jane", "age": 25}`
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()
		ctx := context.Background()

		codec := routines.NewJSONCodec().WithJSONLinesMode()

		go func() {
			err := codec.Parse(ctx, reader, pipe)
			require.NoError(t, err)
		}()

		messages := collectMessages(pipe, 100*time.Millisecond)

		require.Len(t, messages, 2)

		obj1 := messages[0].Data.(map[string]any)
		assert.Equal(t, "John", obj1["name"])

		obj2 := messages[1].Data.(map[string]any)
		assert.Equal(t, "Jane", obj2["name"])
	})

	t.Run("handles empty lines in stream mode", func(t *testing.T) {
		content := `{"name": "John"}

{"name": "Jane"}`
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()
		ctx := context.Background()

		codec := routines.NewJSONCodec().WithJSONLinesMode()

		go func() {
			err := codec.Parse(ctx, reader, pipe)
			require.NoError(t, err)
		}()

		messages := collectMessages(pipe, 100*time.Millisecond)

		require.Len(t, messages, 2) // Empty line should be skipped

		obj1 := messages[0].Data.(map[string]any)
		assert.Equal(t, "John", obj1["name"])

		obj2 := messages[1].Data.(map[string]any)
		assert.Equal(t, "Jane", obj2["name"])
	})

	t.Run("handles invalid JSON", func(t *testing.T) {
		content := `{"name": "John", "age": }`
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()
		ctx := context.Background()

		codec := routines.NewJSONCodec()

		err := codec.Parse(ctx, reader, pipe)

		assert.Error(t, err)
	})

	t.Run("handles invalid JSON in stream mode", func(t *testing.T) {
		content := `{"name": "John"}
invalid json line`
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()
		ctx := context.Background()

		codec := routines.NewJSONCodec().WithJSONLinesMode()

		err := codec.Parse(ctx, reader, pipe)

		assert.Error(t, err)
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		content := `[{"name": "John"}, {"name": "Jane"}, {"name": "Bob"}]`
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()
		ctx, cancel := context.WithCancel(context.Background())

		codec := routines.NewJSONCodec()

		// Cancel context immediately
		cancel()

		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err) // Should return nil when context is cancelled
	})
}

func TestBlobCodec(t *testing.T) {
	t.Run("reads entire file as string", func(t *testing.T) {
		content := "line1\nline2\nline3"
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()
		ctx := context.Background()

		codec := routines.NewBlobCodec()

		go func() {
			err := codec.Parse(ctx, reader, pipe)
			require.NoError(t, err)
		}()

		messages := collectMessages(pipe, 100*time.Millisecond)

		require.Len(t, messages, 1)

		assert.Equal(t, content, messages[0].Data.(string))
		assert.NotEmpty(t, messages[0].ID)
	})

	t.Run("reads entire file as bytes", func(t *testing.T) {
		content := "line1\nline2\nline3"
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()
		ctx := context.Background()

		codec := routines.NewBlobCodec().AsBytes()

		go func() {
			err := codec.Parse(ctx, reader, pipe)
			require.NoError(t, err)
		}()

		messages := collectMessages(pipe, 100*time.Millisecond)

		require.Len(t, messages, 1)

		assert.Equal(t, []byte(content), messages[0].Data.([]byte))
		assert.NotEmpty(t, messages[0].ID)
	})

	t.Run("handles empty file as string", func(t *testing.T) {
		reader := strings.NewReader("")
		pipe := pipeline.NewChanPipe()
		ctx := context.Background()

		codec := routines.NewBlobCodec()

		go func() {
			err := codec.Parse(ctx, reader, pipe)
			require.NoError(t, err)
		}()

		messages := collectMessages(pipe, 100*time.Millisecond)

		require.Len(t, messages, 1)

		assert.Equal(t, "", messages[0].Data.(string))
	})

	t.Run("handles empty file as bytes", func(t *testing.T) {
		reader := strings.NewReader("")
		pipe := pipeline.NewChanPipe()
		ctx := context.Background()

		codec := routines.NewBlobCodec().AsBytes()

		go func() {
			err := codec.Parse(ctx, reader, pipe)
			require.NoError(t, err)
		}()

		messages := collectMessages(pipe, 100*time.Millisecond)

		require.Len(t, messages, 1)

		assert.Equal(t, []byte{}, messages[0].Data.([]byte))
	})

	t.Run("can switch between string and bytes", func(t *testing.T) {
		content := "test content"

		// Test as string
		codec := routines.NewBlobCodec().AsStrings()
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()
		ctx := context.Background()

		go func() {
			err := codec.Parse(ctx, reader, pipe)
			require.NoError(t, err)
		}()

		messages := collectMessages(pipe, 100*time.Millisecond)

		require.Len(t, messages, 1)
		assert.Equal(t, content, messages[0].Data.(string))

		// Test as bytes with same codec
		codec = codec.AsBytes()
		reader = strings.NewReader(content)
		pipe = pipeline.NewChanPipe()

		go func() {
			err := codec.Parse(ctx, reader, pipe)
			require.NoError(t, err)
		}()

		messages = collectMessages(pipe, 100*time.Millisecond)

		require.Len(t, messages, 1)
		assert.Equal(t, []byte(content), messages[0].Data.([]byte))
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		content := "test content"
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()
		ctx, cancel := context.WithCancel(context.Background())

		codec := routines.NewBlobCodec()

		// Cancel context immediately
		cancel()

		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err) // Should return nil when context is cancelled
	})
}
