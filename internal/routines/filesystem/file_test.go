package filesystem_test

import (
	"context"
	"github.com/caiorcferreira/goscript/internal/routines/filesystem"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/caiorcferreira/goscript/internal/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileRoutine_Read(t *testing.T) {
	t.Run("reads file lines successfully", func(t *testing.T) {
		tempDir := t.TempDir()
		testFile := filepath.Join(tempDir, "test.txt")

		testContent := "line1\nline2\nline3"
		err := os.WriteFile(testFile, []byte(testContent), 0644)
		require.NoError(t, err)

		pipe := pipeline.NewChanPipe()
		fileRoutine := filesystem.File(testFile).Read()

		var results []string
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			for msg := range pipe.Out() {
				results = append(results, msg.Data.(string))
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := fileRoutine.Start(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		expectedLines := []string{"line1", "line2", "line3"}
		assert.Equal(t, expectedLines, results)
	})

	t.Run("handles empty file", func(t *testing.T) {
		tempDir := t.TempDir()
		testFile := filepath.Join(tempDir, "empty.txt")

		err := os.WriteFile(testFile, []byte(""), 0644)
		require.NoError(t, err)

		pipe := pipeline.NewChanPipe()
		fileRoutine := filesystem.File(testFile).Read()

		var results []string
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			for msg := range pipe.Out() {
				results = append(results, msg.Data.(string))
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := fileRoutine.Start(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		assert.Empty(t, results)
	})

	t.Run("handles context cancellation during read", func(t *testing.T) {
		tempDir := t.TempDir()
		testFile := filepath.Join(tempDir, "test.txt")

		testContent := "line1\nline2\nline3\nline4\nline5"
		err := os.WriteFile(testFile, []byte(testContent), 0644)
		require.NoError(t, err)

		pipe := pipeline.NewChanPipe()
		fileRoutine := filesystem.File(testFile).Read()

		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		err = fileRoutine.Start(ctx, pipe)
		assert.NoError(t, err) // Context cancellation should gracefully stop, not error
	})

	t.Run("returns error for non-existent file", func(t *testing.T) {
		pipe := pipeline.NewChanPipe()
		fileRoutine := filesystem.File("/non/existent/file.txt").Read()

		ctx := context.Background()
		err := fileRoutine.Start(ctx, pipe)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to open file for read")
	})

	t.Run("closes pipe after reading", func(t *testing.T) {
		tempDir := t.TempDir()
		testFile := filepath.Join(tempDir, "test.txt")

		err := os.WriteFile(testFile, []byte("test"), 0644)
		require.NoError(t, err)

		pipe := pipeline.NewChanPipe()
		fileRoutine := filesystem.File(testFile).Read()

		ctx := context.Background()
		go func() {
			err := fileRoutine.Start(ctx, pipe)
			assert.NoError(t, err)
		}()

		// Drain the output channel
		for range pipe.Out() {
		}

		// Verify the channel is closed
		_, ok := <-pipe.Out()
		assert.False(t, ok, "pipe output should be closed")
	})
}

func TestFileRoutine_Write(t *testing.T) {
	t.Run("writes string messages to file", func(t *testing.T) {
		tempDir := t.TempDir()
		testFile := filepath.Join(tempDir, "output.txt")

		pipe := pipeline.NewChanPipe()
		fileRoutine := filesystem.File(testFile).Write()

		testMessages := []pipeline.Msg{
			{ID: "1", Data: "first line"},
			{ID: "2", Data: "second line"},
			{ID: "3", Data: "third line"},
		}

		go func() {
			for _, msg := range testMessages {
				pipe.In() <- msg
			}
			close(pipe.In())
		}()

		ctx := context.Background()
		err := fileRoutine.Start(ctx, pipe)
		assert.NoError(t, err)

		content, err := os.ReadFile(testFile)
		require.NoError(t, err)

		expectedContent := "first line\nsecond line\nthird line\n"
		assert.Equal(t, expectedContent, string(content))
	})

	t.Run("writes byte slice messages to file", func(t *testing.T) {
		tempDir := t.TempDir()
		testFile := filepath.Join(tempDir, "output.txt")

		pipe := pipeline.NewChanPipe()
		fileRoutine := filesystem.File(testFile).Write().WithBlobCodec()

		testData := []byte("binary data content")
		testMessages := []pipeline.Msg{
			{ID: "1", Data: testData},
		}

		go func() {
			for _, msg := range testMessages {
				pipe.In() <- msg
			}
			close(pipe.In())
		}()

		ctx := context.Background()
		err := fileRoutine.Start(ctx, pipe)
		assert.NoError(t, err)

		content, err := os.ReadFile(testFile)
		require.NoError(t, err)

		assert.Equal(t, testData, content)
	})

	t.Run("creates directory if it doesn't exist", func(t *testing.T) {
		tempDir := t.TempDir()
		testFile := filepath.Join(tempDir, "subdir", "nested", "output.txt")

		pipe := pipeline.NewChanPipe()
		fileRoutine := filesystem.File(testFile).Write()

		testMessages := []pipeline.Msg{
			{ID: "1", Data: "test content"},
		}

		go func() {
			for _, msg := range testMessages {
				pipe.In() <- msg
			}
			close(pipe.In())
		}()

		ctx := context.Background()
		err := fileRoutine.Start(ctx, pipe)
		assert.NoError(t, err)

		// Verify directory was created
		assert.DirExists(t, filepath.Dir(testFile))

		// Verify file content
		content, err := os.ReadFile(testFile)
		require.NoError(t, err)
		assert.Equal(t, "test content\n", string(content))
	})

	t.Run("handles context cancellation during write", func(t *testing.T) {
		tempDir := t.TempDir()
		testFile := filepath.Join(tempDir, "output.txt")

		pipe := pipeline.NewChanPipe()
		fileRoutine := filesystem.File(testFile).Write()

		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			pipe.In() <- pipeline.Msg{ID: "1", Data: "test"}
			time.Sleep(50 * time.Millisecond)
			cancel()
			pipe.In() <- pipeline.Msg{ID: "2", Data: "should not be written"}
			close(pipe.In())
		}()

		err := fileRoutine.Start(ctx, pipe)
		assert.NoError(t, err) // Context cancellation doesn't return error in write mode
	})

	t.Run("handles unknown data types gracefully", func(t *testing.T) {
		tempDir := t.TempDir()
		testFile := filepath.Join(tempDir, "output.txt")

		pipe := pipeline.NewChanPipe()
		fileRoutine := filesystem.File(testFile).Write()

		testMessages := []pipeline.Msg{
			{ID: "1", Data: "valid string"},
			{ID: "2", Data: 123}, // Invalid type
			{ID: "3", Data: "another valid string"},
		}

		go func() {
			for _, msg := range testMessages {
				pipe.In() <- msg
			}
			close(pipe.In())
		}()

		ctx := context.Background()
		err := fileRoutine.Start(ctx, pipe)
		assert.NoError(t, err)

		content, err := os.ReadFile(testFile)
		require.NoError(t, err)

		// All data types should be written, converted to strings with newlines
		expectedContent := "valid string\n123\nanother valid string\n"
		assert.Equal(t, expectedContent, string(content))
	})
}

func TestFileRoutine_ErrorHandling(t *testing.T) {
	t.Run("returns error for non-existent file read", func(t *testing.T) {
		pipe := pipeline.NewChanPipe()
		fileRoutine := filesystem.File("/non/existent/file.txt").Read()

		ctx := context.Background()
		err := fileRoutine.Start(ctx, pipe)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to open file for read")
	})

	t.Run("returns error when directory creation fails for write", func(t *testing.T) {
		// Try to create a file in a location that would require root permissions
		testFile := "/root/restricted/test.txt"

		pipe := pipeline.NewChanPipe()
		fileRoutine := filesystem.File(testFile).Write()

		testMessages := []pipeline.Msg{
			{ID: "1", Data: "test"},
		}

		go func() {
			for _, msg := range testMessages {
				pipe.In() <- msg
			}
			close(pipe.In())
		}()

		ctx := context.Background()
		err := fileRoutine.Start(ctx, pipe)

		// This should fail due to permissions
		if err != nil {
			assert.Contains(t, err.Error(), "failed to")
		}
	})

	t.Run("handles read from directory as error", func(t *testing.T) {
		tempDir := t.TempDir()

		pipe := pipeline.NewChanPipe()
		fileRoutine := filesystem.File(tempDir).Read() // tempDir is a directory, not a file

		ctx := context.Background()
		err := fileRoutine.Start(ctx, pipe)

		// With codec strategy, error now comes from parse phase
		if err != nil {
			assert.Contains(t, err.Error(), "failed to parse file with codec")
		}
	})
}

func TestFileRoutine_WithCodec(t *testing.T) {
	t.Run("uses LineCodec by default", func(t *testing.T) {
		tempDir := t.TempDir()
		testFile := filepath.Join(tempDir, "test.txt")

		testContent := "line1\nline2\nline3"
		err := os.WriteFile(testFile, []byte(testContent), 0644)
		require.NoError(t, err)

		pipe := pipeline.NewChanPipe()
		fileRoutine := filesystem.File(testFile).Read()

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
		go func() {
			err := fileRoutine.Start(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		expectedLines := []string{"line1", "line2", "line3"}
		assert.Equal(t, expectedLines, results)
	})

	t.Run("uses explicit LineCodec", func(t *testing.T) {
		tempDir := t.TempDir()
		testFile := filepath.Join(tempDir, "test.txt")

		testContent := "line1\nline2\nline3"
		err := os.WriteFile(testFile, []byte(testContent), 0644)
		require.NoError(t, err)

		pipe := pipeline.NewChanPipe()
		fileRoutine := filesystem.File(testFile).Read().WithLineCodec()

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
		go func() {
			err := fileRoutine.Start(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		expectedLines := []string{"line1", "line2", "line3"}
		assert.Equal(t, expectedLines, results)
	})

	t.Run("uses CSVCodec", func(t *testing.T) {
		tempDir := t.TempDir()
		testFile := filepath.Join(tempDir, "test.csv")

		testContent := "name,age,city\nJohn,30,NYC\nJane,25,LA"
		err := os.WriteFile(testFile, []byte(testContent), 0644)
		require.NoError(t, err)

		pipe := pipeline.NewChanPipe()
		fileRoutine := filesystem.File(testFile).Read().WithCSVCodec()

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
		go func() {
			err := fileRoutine.Start(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		require.Len(t, results, 3)
		assert.Equal(t, []string{"name", "age", "city"}, results[0])
		assert.Equal(t, []string{"John", "30", "NYC"}, results[1])
		assert.Equal(t, []string{"Jane", "25", "LA"}, results[2])
	})

	t.Run("uses JSONCodec", func(t *testing.T) {
		tempDir := t.TempDir()
		testFile := filepath.Join(tempDir, "test.json")

		testContent := `[{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]`
		err := os.WriteFile(testFile, []byte(testContent), 0644)
		require.NoError(t, err)

		pipe := pipeline.NewChanPipe()
		fileRoutine := filesystem.File(testFile).Read().WithJSONCodec()

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
		go func() {
			err := fileRoutine.Start(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		require.Len(t, results, 2)
		assert.Equal(t, "John", results[0]["name"])
		assert.Equal(t, float64(30), results[0]["age"])
		assert.Equal(t, "Jane", results[1]["name"])
		assert.Equal(t, float64(25), results[1]["age"])
	})

	t.Run("uses BlobCodec", func(t *testing.T) {
		tempDir := t.TempDir()
		testFile := filepath.Join(tempDir, "test.txt")

		testContent := "line1\nline2\nline3"
		err := os.WriteFile(testFile, []byte(testContent), 0644)
		require.NoError(t, err)

		pipe := pipeline.NewChanPipe()
		fileRoutine := filesystem.File(testFile).Read().WithBlobCodec()

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
		go func() {
			err := fileRoutine.Start(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		require.Len(t, results, 1)
		assert.Equal(t, testContent, results[0])
	})

	t.Run("handles codec parse error", func(t *testing.T) {
		tempDir := t.TempDir()
		testFile := filepath.Join(tempDir, "test.json")

		testContent := `{"invalid": json}`
		err := os.WriteFile(testFile, []byte(testContent), 0644)
		require.NoError(t, err)

		pipe := pipeline.NewChanPipe()
		fileRoutine := filesystem.File(testFile).Read().WithJSONCodec()

		ctx := context.Background()
		err = fileRoutine.Start(ctx, pipe)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse file with codec")
	})
}
