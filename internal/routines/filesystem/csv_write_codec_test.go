package filesystem_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/caiorcferreira/goscript/internal/pipeline"
	"github.com/caiorcferreira/goscript/internal/routines/filesystem"
	"github.com/stretchr/testify/assert"
)

func TestCSVWriteCodec_Encode(t *testing.T) {
	t.Run("encodes multiple messages with different data types", func(t *testing.T) {
		codec := filesystem.NewCSVWriteCodec().WithSeparator(';')
		codec.Headers = []string{"id", "name", "value"}

		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: []string{"header1", "header2", "header3"}},
			{ID: "2", Data: map[string]any{"id": 1, "name": "John", "value": 100}},
			{ID: "3", Data: []any{2, "Jane", 200}},
			{ID: "4", Data: "simple string"},
			{ID: "5", Data: 42},
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
		assert.Len(t, lines, 5)

		// Verify each line uses semicolon separator
		assert.Equal(t, "header1;header2;header3", lines[0])
		assert.Equal(t, "1;John;100", lines[1])
		assert.Equal(t, "2;Jane;200", lines[2])
		assert.Equal(t, "simple string", lines[3])
		assert.Equal(t, "42", lines[4])
	})

	t.Run("handles empty input pipe", func(t *testing.T) {
		codec := filesystem.NewCSVWriteCodec()
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		close(pipe.In())

		ctx := context.Background()
		err := codec.Encode(ctx, pipe, &buffer)
		assert.NoError(t, err)

		assert.Equal(t, "", buffer.String())
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		codec := filesystem.NewCSVWriteCodec()
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: []string{"col1", "col2"}},
			{ID: "2", Data: []string{"val1", "val2"}},
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

func TestCSVWriteCodec_Configuration(t *testing.T) {
	t.Run("WithSeparator sets custom separator", func(t *testing.T) {
		codec := filesystem.NewCSVWriteCodec().WithSeparator('|')

		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		go func() {
			pipe.In() <- pipeline.Msg{ID: "1", Data: []string{"a", "b", "c"}}
			close(pipe.In())
		}()

		ctx := context.Background()
		err := codec.Encode(ctx, pipe, &buffer)
		assert.NoError(t, err)

		result := buffer.String()
		expected := "a|b|c\n"
		assert.Equal(t, expected, result)
	})

	t.Run("method chaining returns same instance", func(t *testing.T) {
		codec := filesystem.NewCSVWriteCodec()
		same := codec.WithSeparator(';')
		assert.Same(t, codec, same)
	})

	t.Run("converts []string to []string unchanged", func(t *testing.T) {
		codec := filesystem.NewCSVWriteCodec()
		input := []string{"col1", "col2", "col3"}

		// Use reflection to access private method via Encode behavior
		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		go func() {
			pipe.In() <- pipeline.Msg{ID: "1", Data: input}
			close(pipe.In())
		}()

		ctx := context.Background()
		err := codec.Encode(ctx, pipe, &buffer)
		assert.NoError(t, err)

		result := buffer.String()
		expected := "col1,col2,col3\n"
		assert.Equal(t, expected, result)
	})

	t.Run("converts string to single-element []string", func(t *testing.T) {
		codec := filesystem.NewCSVWriteCodec()
		input := "single value"

		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		go func() {
			pipe.In() <- pipeline.Msg{ID: "1", Data: input}
			close(pipe.In())
		}()

		ctx := context.Background()
		err := codec.Encode(ctx, pipe, &buffer)
		assert.NoError(t, err)

		result := buffer.String()
		expected := "single value\n"
		assert.Equal(t, expected, result)
	})

	t.Run("converts map[string]any using headers", func(t *testing.T) {
		codec := filesystem.NewCSVWriteCodec()
		// Set headers to control map conversion order
		codec.Headers = []string{"name", "age", "city"}

		input := map[string]any{
			"name": "John",
			"age":  30,
			"city": "NYC",
		}

		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		go func() {
			pipe.In() <- pipeline.Msg{ID: "1", Data: input}
			close(pipe.In())
		}()

		ctx := context.Background()
		err := codec.Encode(ctx, pipe, &buffer)
		assert.NoError(t, err)

		result := buffer.String()
		expected := "John,30,NYC\n"
		assert.Equal(t, expected, result)
	})

	t.Run("handles missing map values with empty strings", func(t *testing.T) {
		codec := filesystem.NewCSVWriteCodec()
		codec.Headers = []string{"name", "age", "city", "country"}

		input := map[string]any{
			"name": "Jane",
			"age":  25,
			// city missing
			// country missing
		}

		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		go func() {
			pipe.In() <- pipeline.Msg{ID: "1", Data: input}
			close(pipe.In())
		}()

		ctx := context.Background()
		err := codec.Encode(ctx, pipe, &buffer)
		assert.NoError(t, err)

		result := buffer.String()
		expected := "Jane,25,,\n"
		assert.Equal(t, expected, result)
	})

	t.Run("converts []any to []string using fmt.Sprintf", func(t *testing.T) {
		codec := filesystem.NewCSVWriteCodec()
		input := []any{"string", 42, true, 3.14, nil}

		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		go func() {
			pipe.In() <- pipeline.Msg{ID: "1", Data: input}
			close(pipe.In())
		}()

		ctx := context.Background()
		err := codec.Encode(ctx, pipe, &buffer)
		assert.NoError(t, err)

		result := buffer.String()
		expected := "string,42,true,3.14,<nil>\n"
		assert.Equal(t, expected, result)
	})

	t.Run("converts various data types to string representation", func(t *testing.T) {
		codec := filesystem.NewCSVWriteCodec()

		testCases := []struct {
			name     string
			input    any
			expected string
		}{
			{"int", 123, "123\n"},
			{"int64", int64(456), "456\n"},
			{"float64", 3.14159, "3.14159\n"},
			{"bool true", true, "true\n"},
			{"bool false", false, "false\n"},
			{"nil", nil, "<nil>\n"},
			{"struct", struct{ Name string }{Name: "test"}, "{test}\n"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				pipe := pipeline.NewChanPipe()
				var buffer bytes.Buffer

				go func() {
					pipe.In() <- pipeline.Msg{ID: "1", Data: tc.input}
					close(pipe.In())
				}()

				ctx := context.Background()
				err := codec.Encode(ctx, pipe, &buffer)
				assert.NoError(t, err)

				result := buffer.String()
				assert.Equal(t, tc.expected, result)
			})
		}
	})

	t.Run("handles complex nested data structures", func(t *testing.T) {
		codec := filesystem.NewCSVWriteCodec()
		input := []any{
			map[string]any{"nested": "value"},
			[]int{1, 2, 3},
			struct{ Field string }{Field: "test"},
		}

		pipe := pipeline.NewChanPipe()
		var buffer bytes.Buffer

		go func() {
			pipe.In() <- pipeline.Msg{ID: "1", Data: input}
			close(pipe.In())
		}()

		ctx := context.Background()
		err := codec.Encode(ctx, pipe, &buffer)
		assert.NoError(t, err)

		result := buffer.String()
		// Should convert each element to string representation
		assert.Contains(t, result, "map[nested:value]")
		assert.Contains(t, result, "[1 2 3]")
		assert.Contains(t, result, "{test}")
	})
}
