package routines_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/caiorcferreira/goscript/internal/pipeline"
	"github.com/caiorcferreira/goscript/internal/routines"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDebounceRoutine_Run(t *testing.T) {
	t.Run("debounces messages with correct delay", func(t *testing.T) {
		debounceTime := 100 * time.Millisecond
		debounce := routines.Debounce(debounceTime)

		pipe := pipeline.NewChanPipe()

		testData := generateTestMsgs(1, 3)

		start := time.Now()

		go func() {
			for _, data := range testData {
				pipe.In() <- data
			}
			close(pipe.In())
		}()

		var wg sync.WaitGroup
		wg.Add(1)

		var results []pipeline.Msg
		var timestamps []time.Time

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				results = append(results, result)
				timestamps = append(timestamps, time.Now())
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := debounce.Start(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		elapsed := time.Since(start)

		assert.Len(t, results, len(testData))
		assert.ElementsMatch(t, testData, results)

		// Check that total execution time includes debounce delays
		expectedMinTime := time.Duration(len(testData)) * debounceTime
		assert.GreaterOrEqual(t, elapsed, expectedMinTime)

		// Check that each message was delayed by at least debounceTime
		for i, timestamp := range timestamps {
			minExpectedTime := start.Add(time.Duration(i+1) * debounceTime)
			assert.True(t, timestamp.After(minExpectedTime) || timestamp.Equal(minExpectedTime),
				"Message %d should be delayed by at least %v", i, debounceTime)
		}
	})

	t.Run("handles empty input", func(t *testing.T) {
		debounceTime := 50 * time.Millisecond
		debounce := routines.Debounce(debounceTime)

		pipe := pipeline.NewChanPipe()

		testData := generateTestMsgs(1, 0)

		go func() {
			for _, data := range testData {
				pipe.In() <- data
			}
			close(pipe.In())
		}()

		var wg sync.WaitGroup
		wg.Add(1)

		var results []pipeline.Msg

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				results = append(results, result)
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := debounce.Start(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		assert.Empty(t, results)
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		debounceTime := 100 * time.Millisecond
		debounce := routines.Debounce(debounceTime)

		pipe := pipeline.NewChanPipe()

		testData := generateTestMsgs(1, 10)
		stopAfter := 3

		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(1)

		var results []int

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				results = append(results, result.Data.(int))
			}
		}()

		go func() {
			err := debounce.Start(ctx, pipe)
			assert.NoError(t, err)
		}()

		go func() {
			for i, data := range testData {
				if i <= stopAfter {
					pipe.In() <- data
				} else {
					cancel()
					break
				}
			}
			close(pipe.In())
		}()

		wg.Wait()

		// We can get less messages if debounce was processing when context was cancelled
		// but at most we should get `stopAfter + 1` messages (0-indexed)
		if len(results) > 0 {
			require.LessOrEqual(t, slices.Max(results), stopAfter+1)
		}
	})

	t.Run("closes output pipe after completion", func(t *testing.T) {
		debounceTime := 50 * time.Millisecond
		debounce := routines.Debounce(debounceTime)

		pipe := pipeline.NewChanPipe()

		testData := generateTestMsgs(1, 2)

		go func() {
			for _, data := range testData {
				pipe.In() <- data
			}
			close(pipe.In())
		}()

		var wg sync.WaitGroup
		wg.Add(1)

		var results []pipeline.Msg

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				results = append(results, result)
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := debounce.Start(ctx, pipe)
			require.NoError(t, err)
		}()

		wg.Wait()

		assert.Len(t, results, len(testData))
		assert.ElementsMatch(t, testData, results)

		// Verify pipe is closed
		_, ok := <-pipe.Out()
		assert.False(t, ok, "pipe output should be closed")
	})

	t.Run("handles single message", func(t *testing.T) {
		debounceTime := 75 * time.Millisecond
		debounce := routines.Debounce(debounceTime)

		pipe := pipeline.NewChanPipe()

		testData := generateTestMsgs(42, 1)

		start := time.Now()

		go func() {
			for _, data := range testData {
				pipe.In() <- data
			}
			close(pipe.In())
		}()

		var wg sync.WaitGroup
		wg.Add(1)

		var results []pipeline.Msg

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				results = append(results, result)
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := debounce.Start(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		elapsed := time.Since(start)

		assert.Len(t, results, 1)
		assert.Equal(t, testData[0], results[0])
		assert.GreaterOrEqual(t, elapsed, debounceTime)
	})

	t.Run("preserves message order", func(t *testing.T) {
		debounceTime := 30 * time.Millisecond
		debounce := routines.Debounce(debounceTime)

		pipe := pipeline.NewChanPipe()

		testData := generateTestMsgs(1, 10)

		go func() {
			for _, data := range testData {
				pipe.In() <- data
			}
			close(pipe.In())
		}()

		var wg sync.WaitGroup
		wg.Add(1)

		var results []pipeline.Msg

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				results = append(results, result)
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := debounce.Start(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		assert.Len(t, results, len(testData))

		// Verify order is preserved
		for i, expected := range testData {
			assert.Equal(t, expected, results[i], "Message at index %d should maintain order", i)
		}
	})

	t.Run("handles zero debounce time", func(t *testing.T) {
		debounceTime := 0 * time.Millisecond
		debounce := routines.Debounce(debounceTime)

		pipe := pipeline.NewChanPipe()

		testData := generateTestMsgs(1, 5)

		start := time.Now()

		go func() {
			for _, data := range testData {
				pipe.In() <- data
			}
			close(pipe.In())
		}()

		var wg sync.WaitGroup
		wg.Add(1)

		var results []pipeline.Msg

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				results = append(results, result)
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := debounce.Start(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		elapsed := time.Since(start)

		assert.Len(t, results, len(testData))
		assert.ElementsMatch(t, testData, results)

		// Should complete quickly with zero debounce
		assert.Less(t, elapsed, 50*time.Millisecond)
	})

	t.Run("handles large debounce time", func(t *testing.T) {
		debounceTime := 500 * time.Millisecond
		debounce := routines.Debounce(debounceTime)

		pipe := pipeline.NewChanPipe()

		testData := generateTestMsgs(1, 2)

		start := time.Now()

		go func() {
			for _, data := range testData {
				pipe.In() <- data
			}
			close(pipe.In())
		}()

		var wg sync.WaitGroup
		wg.Add(1)

		var results []pipeline.Msg

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				results = append(results, result)
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := debounce.Start(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		elapsed := time.Since(start)

		assert.Len(t, results, len(testData))
		assert.ElementsMatch(t, testData, results)

		// Should take at least the total debounce time
		expectedMinTime := time.Duration(len(testData)) * debounceTime
		assert.GreaterOrEqual(t, elapsed, expectedMinTime)
	})

	t.Run("concurrent message processing", func(t *testing.T) {
		debounceTime := 100 * time.Millisecond
		debounce := routines.Debounce(debounceTime)

		pipe := pipeline.NewChanPipe()

		numMessages := 50
		testData := generateTestMsgs(1, numMessages)

		start := time.Now()

		go func() {
			for _, data := range testData {
				pipe.In() <- data
			}
			close(pipe.In())
		}()

		var wg sync.WaitGroup
		wg.Add(1)

		var results []pipeline.Msg
		var mu sync.Mutex

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				mu.Lock()
				results = append(results, result)
				mu.Unlock()
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := debounce.Start(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		elapsed := time.Since(start)

		mu.Lock()
		resultCount := len(results)
		mu.Unlock()

		assert.Equal(t, numMessages, resultCount)

		// Should take at least the total debounce time for all messages
		expectedMinTime := time.Duration(numMessages) * debounceTime
		assert.GreaterOrEqual(t, elapsed, expectedMinTime)
	})
}
