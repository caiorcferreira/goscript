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
		var buffer bytes.Buffer

		// Test multiple messages by calling Encode multiple times
		messages := []pipeline.Msg{
			{ID: "1", Data: []string{"name", "age", "city"}},
			{ID: "2", Data: []string{"John", "30", "NYC"}},
			{ID: "3", Data: []string{"Jane", "25", "LA"}},
		}

		ctx := context.Background()
		for _, msg := range messages {
			err := codec.Encode(ctx, msg, &buffer)
			assert.NoError(t, err)
		}

		result := buffer.String()
		lines := strings.Split(strings.TrimSpace(result), "\n")
		assert.Len(t, lines, 3)
		assert.Equal(t, "name,age,city", lines[0])
		assert.Equal(t, "John,30,NYC", lines[1])
		assert.Equal(t, "Jane,25,LA", lines[2])
	})

	t.Run("encodes with custom separator", func(t *testing.T) {
		codec := filesystem.NewCSVCodec().WithSeparator(';')
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: []string{"name", "age", "city"}},
			{ID: "2", Data: []string{"John", "30", "NYC"}},
		}

		ctx := context.Background()
		for _, msg := range messages {
			err := codec.Encode(ctx, msg, &buffer)
			assert.NoError(t, err)
		}

		result := buffer.String()
		lines := strings.Split(strings.TrimSpace(result), "\n")
		assert.Equal(t, "name;age;city", lines[0])
		assert.Equal(t, "John;30;NYC", lines[1])
	})

	t.Run("encodes string messages as single field", func(t *testing.T) {
		codec := filesystem.NewCSVCodec()
		var buffer bytes.Buffer

		msg := pipeline.Msg{
			ID:   "1",
			Data: "hello world",
		}

		ctx := context.Background()
		err := codec.Encode(ctx, msg, &buffer)
		assert.NoError(t, err)

		expected := "hello world\n"
		assert.Equal(t, expected, buffer.String())
	})

	t.Run("encodes map messages with headers", func(t *testing.T) {
		codec := filesystem.NewCSVCodec()
		codec.Headers = []string{"name", "age", "city"}
		var buffer bytes.Buffer

		msg := pipeline.Msg{
			ID: "1",
			Data: map[string]any{
				"name": "John",
				"age":  30,
				"city": "NYC",
			},
		}

		ctx := context.Background()
		err := codec.Encode(ctx, msg, &buffer)
		assert.NoError(t, err)

		expected := "John,30,NYC\n"
		assert.Equal(t, expected, buffer.String())
	})

	t.Run("encodes any slice messages", func(t *testing.T) {
		codec := filesystem.NewCSVCodec()
		var buffer bytes.Buffer

		msg := pipeline.Msg{
			ID:   "1",
			Data: []any{"John", 30, "NYC"},
		}

		ctx := context.Background()
		err := codec.Encode(ctx, msg, &buffer)
		assert.NoError(t, err)

		expected := "John,30,NYC\n"
		assert.Equal(t, expected, buffer.String())
	})

	t.Run("handles fields with commas and quotes", func(t *testing.T) {
		codec := filesystem.NewCSVCodec()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: []string{"John", "Software Engineer, Senior", "30"}},
			{ID: "2", Data: []string{"Jane", "Product \"Manager\"", "25"}},
		}

		ctx := context.Background()
		for _, msg := range messages {
			err := codec.Encode(ctx, msg, &buffer)
			assert.NoError(t, err)
		}

		result := buffer.String()
		lines := strings.Split(strings.TrimSpace(result), "\n")
		assert.Len(t, lines, 2)
		assert.Contains(t, lines[0], `"Software Engineer, Senior"`)
		assert.Contains(t, lines[1], `"Product ""Manager"""`)
	})

	t.Run("encodes multiple messages with different data types", func(t *testing.T) {
		codec := filesystem.NewCSVCodec().WithSeparator(';')
		codec.Headers = []string{"id", "name", "value"}
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: []string{"header1", "header2", "header3"}},
			{ID: "2", Data: map[string]any{"id": 1, "name": "John", "value": 100}},
			{ID: "3", Data: []any{2, "Jane", 200}},
			{ID: "4", Data: "simple string"},
			{ID: "5", Data: 42},
		}

		ctx := context.Background()
		for _, msg := range messages {
			err := codec.Encode(ctx, msg, &buffer)
			assert.NoError(t, err)
		}

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

	t.Run("converts map with headers and handles missing values", func(t *testing.T) {
		codec := filesystem.NewCSVCodec()
		codec.Headers = []string{"name", "age", "city", "country"}
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: map[string]any{
				"name": "John",
				"age":  30,
				"city": "NYC",
			}},
			{ID: "2", Data: map[string]any{
				"name": "Jane",
				"age":  25,
				// city and country missing
			}},
		}

		ctx := context.Background()
		for _, msg := range messages {
			err := codec.Encode(ctx, msg, &buffer)
			assert.NoError(t, err)
		}

		result := buffer.String()
		lines := strings.Split(strings.TrimSpace(result), "\n")
		assert.Len(t, lines, 2)
		assert.Equal(t, "John,30,NYC,", lines[0])
		assert.Equal(t, "Jane,25,,", lines[1])
	})

	t.Run("converts []any to string representations", func(t *testing.T) {
		codec := filesystem.NewCSVCodec()
		var buffer bytes.Buffer

		messages := []pipeline.Msg{
			{ID: "1", Data: []any{"string", 42, true, 3.14, nil}},
			{ID: "2", Data: []any{
				map[string]any{"nested": "value"},
				[]int{1, 2, 3},
				struct{ Field string }{Field: "test"},
			}},
		}

		ctx := context.Background()
		for _, msg := range messages {
			err := codec.Encode(ctx, msg, &buffer)
			assert.NoError(t, err)
		}

		result := buffer.String()
		lines := strings.Split(strings.TrimSpace(result), "\n")
		assert.Len(t, lines, 2)
		assert.Equal(t, "string,42,true,3.14,<nil>", lines[0])
		assert.Contains(t, lines[1], "map[nested:value]")
		assert.Contains(t, lines[1], "[1 2 3]")
		assert.Contains(t, lines[1], "{test}")
	})

	t.Run("WithSeparator configuration", func(t *testing.T) {
		codec := filesystem.NewCSVCodec().WithSeparator('|')
		var buffer bytes.Buffer

		msg := pipeline.Msg{ID: "1", Data: []string{"a", "b", "c"}}

		ctx := context.Background()
		err := codec.Encode(ctx, msg, &buffer)
		assert.NoError(t, err)

		result := buffer.String()
		expected := "a|b|c\n"
		assert.Equal(t, expected, result)

		// Verify method chaining returns same instance
		same := codec.WithSeparator(';')
		assert.Same(t, codec, same)
	})

	t.Run("various data type conversions", func(t *testing.T) {
		codec := filesystem.NewCSVCodec()

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
				var buffer bytes.Buffer

				msg := pipeline.Msg{ID: "1", Data: tc.input}

				ctx := context.Background()
				err := codec.Encode(ctx, msg, &buffer)
				assert.NoError(t, err)

				result := buffer.String()
				assert.Equal(t, tc.expected, result)
			})
		}
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		codec := filesystem.NewCSVCodec()
		var buffer bytes.Buffer

		msg := pipeline.Msg{ID: "1", Data: []string{"name", "age"}}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		err := codec.Encode(ctx, msg, &buffer)
		// Should still encode the message since cancellation is checked during processing
		assert.NoError(t, err)
	})
}
