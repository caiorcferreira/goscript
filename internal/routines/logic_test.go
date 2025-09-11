package routines_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/caiorcferreira/goscript/internal/interpreter"
	"github.com/caiorcferreira/goscript/internal/routines"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransformRoutine_Run(t *testing.T) {
	t.Run("transforms integers to doubles", func(t *testing.T) {
		doubleTransform := routines.Transform(func(x int) int {
			return x * 2
		})

		pipe := interpreter.NewChanPipe()

		testData := generateTestMsgs(1, 5)

		go func() {
			for _, data := range testData {
				pipe.In() <- data
			}
			close(pipe.In())
		}()

		var wg sync.WaitGroup
		wg.Add(1)

		var results []int

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				results = append(results, result.(int))
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := doubleTransform.Run(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		expectedResults := make([]int, len(testData))
		for i, data := range testData {
			expectedResults[i] = data.(int) * 2
		}

		assert.Len(t, results, len(testData))
		assert.ElementsMatch(t, expectedResults, results)
	})

	t.Run("transforms integers to strings", func(t *testing.T) {
		stringTransform := routines.Transform(func(x int) string {
			return fmt.Sprintf("number_%d", x)
		})

		pipe := interpreter.NewChanPipe()

		testData := generateTestMsgs(1, 3)

		go func() {
			for _, data := range testData {
				pipe.In() <- data
			}
			close(pipe.In())
		}()

		var wg sync.WaitGroup
		wg.Add(1)

		var results []string

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				results = append(results, result.(string))
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := stringTransform.Run(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		expectedResults := []string{"number_1", "number_2", "number_3"}

		assert.Len(t, results, len(testData))
		assert.ElementsMatch(t, expectedResults, results)
	})

	t.Run("handles empty input", func(t *testing.T) {
		identityTransform := routines.Transform(func(x int) int {
			return x
		})

		pipe := interpreter.NewChanPipe()

		testData := generateTestMsgs(1, 0)

		go func() {
			for _, data := range testData {
				pipe.In() <- data
			}
			close(pipe.In())
		}()

		var wg sync.WaitGroup
		wg.Add(1)

		var results []int

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				results = append(results, result.(int))
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := identityTransform.Run(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		assert.Empty(t, results)
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		identityTransform := routines.Transform(func(x int) int {
			return x
		})

		pipe := interpreter.NewChanPipe()

		testData := generateTestMsgs(1, 10)
		stopAfter := 3

		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(1)

		var results []int

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				results = append(results, result.(int))
			}
		}()

		go func() {
			err := identityTransform.Run(ctx, pipe)
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

		// We can get less messages if transform was processing when context was cancelled
		// but at most we should get `stopAfter + 1` messages (0-indexed)
		if len(results) > 0 {
			require.LessOrEqual(t, slices.Max(results), stopAfter+1)
		}
	})

	t.Run("passes through type assertion failures", func(t *testing.T) {
		intTransform := routines.Transform(func(x int) int {
			return x * 2
		})

		pipe := interpreter.NewChanPipe()

		// Mix different types - ints should be transformed, others passed through
		mixedData := []any{1, "string", 2, 3.14, 3, true, 4}

		go func() {
			for _, data := range mixedData {
				pipe.In() <- data
			}
			close(pipe.In())
		}()

		var wg sync.WaitGroup
		wg.Add(1)

		var results []any

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				results = append(results, result)
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := intTransform.Run(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		// Integers should be transformed, non-integers passed through unchanged
		expectedResults := []any{2, "string", 4, 3.14, 6, true, 8}

		assert.Len(t, results, len(mixedData))
		assert.ElementsMatch(t, expectedResults, results)
	})

	t.Run("handles only type assertion failures", func(t *testing.T) {
		intTransform := routines.Transform(func(x int) int {
			return x * 2
		})

		pipe := interpreter.NewChanPipe()

		// Only non-int types - all should be passed through unchanged
		nonIntData := []any{"hello", 3.14, true, []int{1, 2, 3}}

		go func() {
			for _, data := range nonIntData {
				pipe.In() <- data
			}
			close(pipe.In())
		}()

		var wg sync.WaitGroup
		wg.Add(1)

		var results []any

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				results = append(results, result)
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := intTransform.Run(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		// All non-int data should be passed through unchanged
		assert.Len(t, results, len(nonIntData))
		assert.ElementsMatch(t, nonIntData, results)
	})

	t.Run("closes output pipe after completion", func(t *testing.T) {
		identityTransform := routines.Transform(func(x int) int {
			return x
		})

		pipe := interpreter.NewChanPipe()

		testData := generateTestMsgs(1, 2)

		go func() {
			for _, data := range testData {
				pipe.In() <- data
			}
			close(pipe.In())
		}()

		var wg sync.WaitGroup
		wg.Add(1)

		var results []int

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				results = append(results, result.(int))
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := identityTransform.Run(ctx, pipe)
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
		squareTransform := routines.Transform(func(x int) int {
			return x * x
		})

		pipe := interpreter.NewChanPipe()

		testData := generateTestMsgs(5, 1)

		go func() {
			for _, data := range testData {
				pipe.In() <- data
			}
			close(pipe.In())
		}()

		var wg sync.WaitGroup
		wg.Add(1)

		var results []int

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				results = append(results, result.(int))
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := squareTransform.Run(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		assert.Len(t, results, 1)
		assert.Equal(t, 25, results[0]) // 5 * 5
	})

	t.Run("preserves message order", func(t *testing.T) {
		incrementTransform := routines.Transform(func(x int) int {
			return x + 1
		})

		pipe := interpreter.NewChanPipe()

		testData := generateTestMsgs(1, 10)

		go func() {
			for _, data := range testData {
				pipe.In() <- data
			}
			close(pipe.In())
		}()

		var wg sync.WaitGroup
		wg.Add(1)

		var results []int

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				results = append(results, result.(int))
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := incrementTransform.Run(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		assert.Len(t, results, len(testData))

		// Verify order is preserved and transformation is applied
		for i, expected := range testData {
			assert.Equal(t, expected.(int)+1, results[i], "Message at index %d should maintain order and be transformed", i)
		}
	})

	t.Run("handles complex transformation", func(t *testing.T) {
		// Transform int to struct
		type Result struct {
			Original int
			Doubled  int
			IsEven   bool
		}

		complexTransform := routines.Transform(func(x int) Result {
			return Result{
				Original: x,
				Doubled:  x * 2,
				IsEven:   x%2 == 0,
			}
		})

		pipe := interpreter.NewChanPipe()

		testData := generateTestMsgs(1, 4)

		go func() {
			for _, data := range testData {
				pipe.In() <- data
			}
			close(pipe.In())
		}()

		var wg sync.WaitGroup
		wg.Add(1)

		var results []Result

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				results = append(results, result.(Result))
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := complexTransform.Run(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		expectedResults := []Result{
			{Original: 1, Doubled: 2, IsEven: false},
			{Original: 2, Doubled: 4, IsEven: true},
			{Original: 3, Doubled: 6, IsEven: false},
			{Original: 4, Doubled: 8, IsEven: true},
		}

		assert.Len(t, results, len(testData))
		assert.ElementsMatch(t, expectedResults, results)
	})

	t.Run("concurrent message processing", func(t *testing.T) {
		doubleTransform := routines.Transform(func(x int) int {
			return x * 2
		})

		pipe := interpreter.NewChanPipe()

		numMessages := 100
		testData := generateTestMsgs(1, numMessages)

		go func() {
			for _, data := range testData {
				pipe.In() <- data
			}
			close(pipe.In())
		}()

		var wg sync.WaitGroup
		wg.Add(1)

		var results []int
		var mu sync.Mutex

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				mu.Lock()
				results = append(results, result.(int))
				mu.Unlock()
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := doubleTransform.Run(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		mu.Lock()
		resultCount := len(results)
		mu.Unlock()

		assert.Equal(t, numMessages, resultCount)

		// Verify all results are correctly transformed
		expectedSum := 0
		actualSum := 0
		for i := 1; i <= numMessages; i++ {
			expectedSum += i * 2
		}
		for _, result := range results {
			actualSum += result
		}
		assert.Equal(t, expectedSum, actualSum)
	})
}
