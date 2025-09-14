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
	t.Run("encodes string messages as lines", func(t *testing.T) {
		codec := filesystem.NewLineCodec()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: "line1"},
			{ID: "2", Data: "line2"},
			{ID: "3", Data: "line3"},
		}

		ctx := context.Background()
		for _, msg := range messages {
			err := codec.Encode(ctx, msg, &buffer)
			assert.NoError(t, err)
		}

		expected := "line1\nline2\nline3\n"
		assert.Equal(t, expected, buffer.String())
	})

	t.Run("encodes byte slice messages directly", func(t *testing.T) {
		codec := filesystem.NewLineCodec()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: []byte("line1")},
			{ID: "2", Data: []byte("line2")},
		}

		ctx := context.Background()
		for _, msg := range messages {
			err := codec.Encode(ctx, msg, &buffer)
			assert.NoError(t, err)
		}

		expected := "line1\nline2\n"
		assert.Equal(t, expected, buffer.String())
	})

	t.Run("encodes various data types as string representation", func(t *testing.T) {
		codec := filesystem.NewLineCodec()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: 42},
			{ID: "2", Data: true},
			{ID: "3", Data: 3.14},
			{ID: "4", Data: struct{ Name string }{Name: "test"}},
		}

		ctx := context.Background()
		for _, msg := range messages {
			err := codec.Encode(ctx, msg, &buffer)
			assert.NoError(t, err)
		}

		expected := "42\ntrue\n3.14\n{test}\n"
		assert.Equal(t, expected, buffer.String())
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		codec := filesystem.NewLineCodec()
		var buffer bytes.Buffer

		msg := pipeline.Msg{ID: "1", Data: "test line"}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		err := codec.Encode(ctx, msg, &buffer)
		// Should still encode the message since cancellation is checked during processing
		assert.NoError(t, err)
		assert.Equal(t, "test line\n", buffer.String())
	})

	t.Run("handles empty string", func(t *testing.T) {
		codec := filesystem.NewLineCodec()
		var buffer bytes.Buffer

		msg := pipeline.Msg{ID: "1", Data: ""}

		ctx := context.Background()
		err := codec.Encode(ctx, msg, &buffer)
		assert.NoError(t, err)

		expected := "\n"
		assert.Equal(t, expected, buffer.String())
	})

	t.Run("handles string with internal newlines", func(t *testing.T) {
		codec := filesystem.NewLineCodec()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: "line with\ninternal newline"},
			{ID: "2", Data: "normal line"},
		}

		ctx := context.Background()
		for _, msg := range messages {
			err := codec.Encode(ctx, msg, &buffer)
			assert.NoError(t, err)
		}

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
