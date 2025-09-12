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

func TestCSVCodec_Parse(t *testing.T) {
	t.Run("parses CSV content with default settings", func(t *testing.T) {
		codec := filesystem.NewCSVCodec()
		content := "name,age,city\nJohn,30,NYC\nJane,25,LA"
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		var results [][]string
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			for msg := range pipe.Out() {
				results = append(results, msg.Data.([]string))
			}
		}()

		ctx := context.Background()
		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err)

		wg.Wait()

		require.Len(t, results, 3)
		assert.Equal(t, []string{"name", "age", "city"}, results[0])
		assert.Equal(t, []string{"John", "30", "NYC"}, results[1])
		assert.Equal(t, []string{"Jane", "25", "LA"}, results[2])
	})

	t.Run("parses CSV content with custom separator", func(t *testing.T) {
		codec := filesystem.NewCSVCodec().WithSeparator(';')
		content := "name;age;city\nJohn;30;NYC\nJane;25;LA"
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		var results [][]string
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			for msg := range pipe.Out() {
				results = append(results, msg.Data.([]string))
			}
		}()

		ctx := context.Background()
		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err)

		wg.Wait()

		require.Len(t, results, 3)
		assert.Equal(t, []string{"name", "age", "city"}, results[0])
		assert.Equal(t, []string{"John", "30", "NYC"}, results[1])
		assert.Equal(t, []string{"Jane", "25", "LA"}, results[2])
	})

	t.Run("handles CSV with comments", func(t *testing.T) {
		codec := filesystem.NewCSVCodec().WithComment('#')
		content := `# This is a header comment
name,age,city
John,30,NYC
# This is a mid-file comment  
Jane,25,LA`
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		var results [][]string
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			for msg := range pipe.Out() {
				results = append(results, msg.Data.([]string))
			}
		}()

		ctx := context.Background()
		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err)

		wg.Wait()

		require.Len(t, results, 3)
		assert.Equal(t, []string{"name", "age", "city"}, results[0])
		assert.Equal(t, []string{"John", "30", "NYC"}, results[1])
		assert.Equal(t, []string{"Jane", "25", "LA"}, results[2])
	})

	t.Run("handles CSV with quoted fields", func(t *testing.T) {
		codec := filesystem.NewCSVCodec()
		content := `name,description,age
John,"Software Engineer, Senior",30
Jane,"Product Manager, Lead",25`
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		var results [][]string
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			for msg := range pipe.Out() {
				results = append(results, msg.Data.([]string))
			}
		}()

		ctx := context.Background()
		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err)

		wg.Wait()

		require.Len(t, results, 3)
		assert.Equal(t, []string{"name", "description", "age"}, results[0])
		assert.Equal(t, []string{"John", "Software Engineer, Senior", "30"}, results[1])
		assert.Equal(t, []string{"Jane", "Product Manager, Lead", "25"}, results[2])
	})

	t.Run("handles empty CSV", func(t *testing.T) {
		codec := filesystem.NewCSVCodec()
		reader := strings.NewReader("")
		pipe := pipeline.NewChanPipe()

		var results [][]string
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			for msg := range pipe.Out() {
				results = append(results, msg.Data.([]string))
			}
		}()

		ctx := context.Background()
		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err)

		wg.Wait()

		assert.Empty(t, results)
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		codec := filesystem.NewCSVCodec()
		content := "name,age\nJohn,30\nJane,25"
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		err := codec.Parse(ctx, reader, pipe)
		assert.NoError(t, err) // Should handle gracefully
	})

	t.Run("returns error for malformed CSV", func(t *testing.T) {
		codec := filesystem.NewCSVCodec()
		content := `name,age
John,30,"extra quote`
		reader := strings.NewReader(content)
		pipe := pipeline.NewChanPipe()

		ctx := context.Background()
		err := codec.Parse(ctx, reader, pipe)
		assert.Error(t, err)
	})
}

func TestCSVCodec_Encode(t *testing.T) {
	t.Run("encodes string slice messages", func(t *testing.T) {
		codec := filesystem.NewCSVCodec()
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: []string{"name", "age", "city"}},
			{ID: "2", Data: []string{"John", "30", "NYC"}},
			{ID: "3", Data: []string{"Jane", "25", "LA"}},
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
		assert.Len(t, lines, 3)
		assert.Equal(t, "name,age,city", lines[0])
		assert.Equal(t, "John,30,NYC", lines[1])
		assert.Equal(t, "Jane,25,LA", lines[2])
	})

	t.Run("encodes with custom separator", func(t *testing.T) {
		codec := filesystem.NewCSVCodec().WithSeparator(';')
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: []string{"name", "age", "city"}},
			{ID: "2", Data: []string{"John", "30", "NYC"}},
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
		assert.Equal(t, "name;age;city", lines[0])
		assert.Equal(t, "John;30;NYC", lines[1])
	})

	t.Run("encodes string messages as single field", func(t *testing.T) {
		codec := filesystem.NewCSVCodec()
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: "hello"},
			{ID: "2", Data: "world"},
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
		assert.Equal(t, "hello", lines[0])
		assert.Equal(t, "world", lines[1])
	})

	t.Run("encodes other types as string representation", func(t *testing.T) {
		codec := filesystem.NewCSVCodec()
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: 123},
			{ID: "2", Data: true},
			{ID: "3", Data: 3.14},
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
		assert.Len(t, lines, 3)
		assert.Equal(t, "123", lines[0])
		assert.Equal(t, "true", lines[1])
		assert.Equal(t, "3.14", lines[2])
	})

	t.Run("handles fields with commas and quotes", func(t *testing.T) {
		codec := filesystem.NewCSVCodec()
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: []string{"John", "Software Engineer, Senior", "30"}},
			{ID: "2", Data: []string{"Jane", "Product \"Manager\"", "25"}},
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
		assert.Contains(t, lines[0], `"Software Engineer, Senior"`)
		assert.Contains(t, lines[1], `"Product ""Manager"""`)
	})

	t.Run("handles empty input pipe", func(t *testing.T) {
		codec := filesystem.NewCSVCodec()
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		close(pipe.In())

		ctx := context.Background()
		err := codec.Encode(ctx, pipe, &buffer)
		assert.NoError(t, err)

		assert.Equal(t, "", buffer.String())
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		codec := filesystem.NewCSVCodec()
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: []string{"name", "age"}},
			{ID: "2", Data: []string{"John", "30"}},
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
}

func TestCSVCodec_Interfaces(t *testing.T) {
	t.Run("implements ReadCodec interface", func(t *testing.T) {
		var codec filesystem.ReadCodec = filesystem.NewCSVCodec()
		assert.NotNil(t, codec)
	})

	t.Run("implements WriteCodec interface", func(t *testing.T) {
		var codec filesystem.WriteCodec = filesystem.NewCSVCodec()
		assert.NotNil(t, codec)
	})

	t.Run("method chaining returns same instance", func(t *testing.T) {
		codec := filesystem.NewCSVCodec()
		same := codec.WithSeparator(';').WithComment('*')
		assert.Same(t, codec, same)
	})
}
