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
)

func TestBlobCodec_Parse(t *testing.T) {
	t.Run("parses entire content as string by default", func(t *testing.T) {
		codec := filesystem.NewBlobCodec()
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

		assert.Len(t, results, 1)
		assert.Equal(t, content, results[0])
	})

	t.Run("parses content as bytes when configured", func(t *testing.T) {
		codec := filesystem.NewBlobCodec().AsBytes()
		content := "line1\nline2\nline3"
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		var results [][]byte
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			for msg := range pipe.Out() {
				results = append(results, msg.Data.([]byte))
			}
		}()

		ctx := context.Background()
		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err)

		wg.Wait()

		assert.Len(t, results, 1)
		assert.Equal(t, []byte(content), results[0])
	})

	t.Run("handles empty content", func(t *testing.T) {
		codec := filesystem.NewBlobCodec()
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

		assert.Len(t, results, 1)
		assert.Equal(t, "", results[0])
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		codec := filesystem.NewBlobCodec()
		content := "some content"
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err) // Should gracefully handle cancellation
	})

	t.Run("AsStrings returns same codec with string mode", func(t *testing.T) {
		codec := filesystem.NewBlobCodec().AsBytes().AsStrings()
		content := "test content"
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
		assert.Equal(t, content, results[0])
	})
}

func TestBlobCodec_Encode(t *testing.T) {
	t.Run("encodes string messages", func(t *testing.T) {
		codec := filesystem.NewBlobCodec()
		var buffer bytes.Buffer

		msg := pipeline.Msg{
			ID:   "test-id",
			Data: "hello world",
		}

		ctx := context.Background()
		err := codec.Encode(ctx, msg, &buffer)
		assert.NoError(t, err)

		expected := "hello world"
		assert.Equal(t, expected, buffer.String())
	})

	t.Run("encodes other types as string representation", func(t *testing.T) {
		codec := filesystem.NewBlobCodec()
		var buffer bytes.Buffer

		msg := pipeline.Msg{
			ID:   "test-id",
			Data: 12345,
		}

		ctx := context.Background()
		err := codec.Encode(ctx, msg, &buffer)
		assert.NoError(t, err)

		expected := "12345"
		assert.Equal(t, expected, buffer.String())
	})

	t.Run("encodes byte slice data", func(t *testing.T) {
		codec := filesystem.NewBlobCodec()
		var buffer bytes.Buffer

		msg := pipeline.Msg{
			ID:   "test-id",
			Data: []byte("hello world"),
		}

		ctx := context.Background()
		err := codec.Encode(ctx, msg, &buffer)
		assert.NoError(t, err)

		expected := "hello world"
		assert.Equal(t, expected, buffer.String())
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		codec := filesystem.NewBlobCodec()
		var buffer bytes.Buffer

		msg := pipeline.Msg{
			ID:   "test-id",
			Data: "hello world",
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		err := codec.Encode(ctx, msg, &buffer)
		// Should still encode the message since cancellation is checked during processing
		assert.NoError(t, err)
		assert.Equal(t, "hello world", buffer.String())
	})
}

func TestBlobCodec_Interfaces(t *testing.T) {
	t.Run("implements ReadCodec interface", func(t *testing.T) {
		var codec filesystem.ReadCodec = filesystem.NewBlobCodec()
		assert.NotNil(t, codec)
	})

	t.Run("implements WriteCodec interface", func(t *testing.T) {
		var codec filesystem.WriteCodec = filesystem.NewBlobCodec()
		assert.NotNil(t, codec)
	})
}
