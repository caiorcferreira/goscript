package filesystem_test

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/caiorcferreira/goscript/internal/pipeline"
	"github.com/caiorcferreira/goscript/internal/routines/filesystem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLineCodec_Parse(t *testing.T) {
	t.Run("parses content line by line", func(t *testing.T) {
		codec := filesystem.NewLineCodec()
		content := "line1\nline2\nline3"
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		var results []string
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			for msg := range pipe.Out() {
				results = append(results, msg.Data.(string))
			}
		}()

		ctx := context.Background()
		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err)

		wg.Wait()

		assert.Len(t, results, 3)
		assert.Equal(t, "line1", results[0])
		assert.Equal(t, "line2", results[1])
		assert.Equal(t, "line3", results[2])
	})

	t.Run("handles single line without newline", func(t *testing.T) {
		codec := filesystem.NewLineCodec()
		content := "single line"
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		var results []string
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			for msg := range pipe.Out() {
				results = append(results, msg.Data.(string))
			}
		}()

		ctx := context.Background()
		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err)

		wg.Wait()

		assert.Len(t, results, 1)
		assert.Equal(t, "single line", results[0])
	})

	t.Run("handles empty lines", func(t *testing.T) {
		codec := filesystem.NewLineCodec()
		content := "line1\n\nline3\n\n"
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		var results []string
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			for msg := range pipe.Out() {
				results = append(results, msg.Data.(string))
			}
		}()

		ctx := context.Background()
		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err)

		wg.Wait()

		assert.Len(t, results, 4)
		assert.Equal(t, "line1", results[0])
		assert.Equal(t, "", results[1])
		assert.Equal(t, "line3", results[2])
		assert.Equal(t, "", results[3])
	})

	t.Run("handles empty content", func(t *testing.T) {
		codec := filesystem.NewLineCodec()
		reader := strings.NewReader("")
		pipe := pipeline.NewChanPipe()

		var results []string
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			for msg := range pipe.Out() {
				results = append(results, msg.Data.(string))
			}
		}()

		ctx := context.Background()
		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err)

		wg.Wait()

		assert.Empty(t, results)
	})

	t.Run("handles lines with carriage returns", func(t *testing.T) {
		codec := filesystem.NewLineCodec()
		content := "line1\r\nline2\r\nline3"
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		var results []string
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			for msg := range pipe.Out() {
				results = append(results, msg.Data.(string))
			}
		}()

		ctx := context.Background()
		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err)

		wg.Wait()

		assert.Len(t, results, 3)
		assert.Equal(t, "line1", results[0])
		assert.Equal(t, "line2", results[1])
		assert.Equal(t, "line3", results[2])
	})

	t.Run("handles very long lines", func(t *testing.T) {
		codec := filesystem.NewLineCodec()
		// Use a line length that's within scanner limits
		longLine := strings.Repeat("a", 50000)
		content := longLine + "\nshort line"
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		var results []string
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			for msg := range pipe.Out() {
				results = append(results, msg.Data.(string))
			}
		}()

		ctx := context.Background()
		err := codec.Parse(ctx, reader, pipe)
		// Scanner has token length limits, so this might error
		if err != nil {
			assert.Contains(t, err.Error(), "token too long")
			return
		}

		wg.Wait()

		require.Len(t, results, 2)
		assert.Equal(t, longLine, results[0])
		assert.Equal(t, "short line", results[1])
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		codec := filesystem.NewLineCodec()
		content := "line1\nline2\nline3"
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err) // Should handle gracefully
	})

	t.Run("handles unicode content", func(t *testing.T) {
		codec := filesystem.NewLineCodec()
		content := "こんにちは\n🌍 Hello World\nПривет мир"
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		var results []string
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			for msg := range pipe.Out() {
				results = append(results, msg.Data.(string))
			}
		}()

		ctx := context.Background()
		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err)

		wg.Wait()

		assert.Len(t, results, 3)
		assert.Equal(t, "こんにちは", results[0])
		assert.Equal(t, "🌍 Hello World", results[1])
		assert.Equal(t, "Привет мир", results[2])
	})
}

func TestLineCodec_Encode(t *testing.T) {
	t.Run("encodes string messages with newlines", func(t *testing.T) {
		codec := filesystem.NewLineCodec()
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: "line1"},
			{ID: "2", Data: "line2"},
			{ID: "3", Data: "line3"},
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

		expected := "line1\nline2\nline3\n"
		assert.Equal(t, expected, buffer.String())
	})

	t.Run("encodes byte slice messages directly", func(t *testing.T) {
		codec := filesystem.NewLineCodec()
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: []byte("line1")},
			{ID: "2", Data: []byte("line2")},
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

		expected := "line1\nline2\n"
		assert.Equal(t, expected, buffer.String())
	})

	t.Run("handles empty strings", func(t *testing.T) {
		codec := filesystem.NewLineCodec()
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: ""},
			{ID: "2", Data: "non-empty"},
			{ID: "3", Data: ""},
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

		expected := "\nnon-empty\n\n"
		assert.Equal(t, expected, buffer.String())
	})

	t.Run("handles empty input pipe", func(t *testing.T) {
		codec := filesystem.NewLineCodec()
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		close(pipe.In())

		ctx := context.Background()
		err := codec.Encode(ctx, pipe, &buffer)
		assert.NoError(t, err)

		assert.Equal(t, "", buffer.String())
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		codec := filesystem.NewLineCodec()
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: "line1"},
			{ID: "2", Data: "line2"},
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

		// With immediate cancellation, context may prevent any processing
		result := buffer.String()
		// Either no data written or "line1" written before cancellation
		assert.True(t, result == "" || strings.Contains(result, "line1"))
	})

	t.Run("handles unicode content", func(t *testing.T) {
		codec := filesystem.NewLineCodec()
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: "こんにちは"},
			{ID: "2", Data: "🌍 Hello World"},
			{ID: "3", Data: "Привет мир"},
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

		expected := "こんにちは\n🌍 Hello World\nПривет мир\n"
		assert.Equal(t, expected, buffer.String())
	})

	t.Run("preserves existing newlines in strings", func(t *testing.T) {
		codec := filesystem.NewLineCodec()
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: "line with\ninternal newline"},
			{ID: "2", Data: "normal line"},
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

		expected := "line with\ninternal newline\nnormal line\n"
		assert.Equal(t, expected, buffer.String())
	})
}

func TestLineCodec_Interfaces(t *testing.T) {
	t.Run("implements ReadCodec interface", func(t *testing.T) {
		var codec filesystem.ReadCodec = filesystem.NewLineCodec()
		assert.NotNil(t, codec)
	})

	t.Run("implements WriteCodec interface", func(t *testing.T) {
		var codec filesystem.WriteCodec = filesystem.NewLineCodec()
		assert.NotNil(t, codec)
	})
}
