package routines_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/caiorcferreira/goscript/internal/pipeline"
	"github.com/caiorcferreira/goscript/internal/routines"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransformRoutine_Run(t *testing.T) {
	t.Run("transforms integers to doubles", func(t *testing.T) {
		doubleTransform := routines.Transform(func(x int) int {
			return x * 2
		})

		pipe := pipeline.NewChanPipe()

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
				results = append(results, result.Data.(int))
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := doubleTransform.Start(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		expectedResults := make([]int, len(testData))
		for i, data := range testData {
			expectedResults[i] = data.Data.(int) * 2
		}

		assert.Len(t, results, len(testData))
		assert.ElementsMatch(t, expectedResults, results)
	})

	t.Run("transforms integers to strings", func(t *testing.T) {
		stringTransform := routines.Transform(func(x int) string {
			return fmt.Sprintf("number_%d", x)
		})

		pipe := pipeline.NewChanPipe()

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
				results = append(results, result.Data.(string))
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := stringTransform.Start(ctx, pipe)
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

		var results []int

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				results = append(results, result.Data.(int))
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := identityTransform.Start(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		assert.Empty(t, results)
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		identityTransform := routines.Transform(func(x int) int {
			return x
		})

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
			err := identityTransform.Start(ctx, pipe)
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

		pipe := pipeline.NewChanPipe()

		// Mix different types - ints should be transformed, others passed through
		mixedData := []pipeline.Msg{
			{ID: "", Data: 1},
			{ID: "", Data: "string"},
			{ID: "", Data: 2},
			{ID: "", Data: 3.14},
			{ID: "", Data: 3},
			{ID: "", Data: true},
			{ID: "", Data: 4},
		}

		go func() {
			for _, data := range mixedData {
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
			err := intTransform.Start(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		// Integers should be transformed, non-integers passed through unchanged
		expectedResults := []pipeline.Msg{
			{ID: "", Data: 2},
			{ID: "", Data: "string"},
			{ID: "", Data: 4},
			{ID: "", Data: 3.14},
			{ID: "", Data: 6},
			{ID: "", Data: true},
			{ID: "", Data: 8},
		}

		assert.Len(t, results, len(mixedData))
		assert.ElementsMatch(t, expectedResults, results)
	})

	t.Run("handles only type assertion failures", func(t *testing.T) {
		intTransform := routines.Transform(func(x int) int {
			return x * 2
		})

		pipe := pipeline.NewChanPipe()

		// Only non-int types - all should be passed through unchanged
		nonIntData := []pipeline.Msg{
			{ID: "", Data: "hello"},
			{ID: "", Data: 3.14},
			{ID: "", Data: true},
			{ID: "", Data: []int{1, 2, 3}},
		}

		go func() {
			for _, data := range nonIntData {
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
			err := intTransform.Start(ctx, pipe)
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

		var results []int

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				results = append(results, result.Data.(int))
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := identityTransform.Start(ctx, pipe)
			require.NoError(t, err)
		}()

		wg.Wait()

		assert.Len(t, results, len(testData))
		// Convert testData to ints for comparison
		testDataInts := make([]int, len(testData))
		for i, t := range testData {
			testDataInts[i] = t.Data.(int)
		}
		assert.ElementsMatch(t, testDataInts, results)

		// Verify pipe is closed
		_, ok := <-pipe.Out()
		assert.False(t, ok, "pipe output should be closed")
	})

	t.Run("handles single message", func(t *testing.T) {
		squareTransform := routines.Transform(func(x int) int {
			return x * x
		})

		pipe := pipeline.NewChanPipe()

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
				results = append(results, result.Data.(int))
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := squareTransform.Start(ctx, pipe)
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

		var results []int

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				results = append(results, result.Data.(int))
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := incrementTransform.Start(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		assert.Len(t, results, len(testData))

		// Verify order is preserved and transformation is applied
		for i, expected := range testData {
			assert.Equal(t, expected.Data.(int)+1, results[i], "Message at index %d should maintain order and be transformed", i)
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

		pipe := pipeline.NewChanPipe()

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
				results = append(results, result.Data.(Result))
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := complexTransform.Start(ctx, pipe)
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

		pipe := pipeline.NewChanPipe()

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
				results = append(results, result.Data.(int))
				mu.Unlock()
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := doubleTransform.Start(ctx, pipe)
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
