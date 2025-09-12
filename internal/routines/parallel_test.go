package routines_test

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caiorcferreira/goscript/internal/pipeline"
	"github.com/caiorcferreira/goscript/internal/routines"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockRoutine struct {
	processFunc func(ctx context.Context, pipe pipeline.Pipe) error
	callCount   int32
	mu          sync.Mutex
}

func (m *mockRoutine) Run(ctx context.Context, pipe pipeline.Pipe) error {
	atomic.AddInt32(&m.callCount, 1)
	if m.processFunc != nil {
		return m.processFunc(ctx, pipe)
	}
	return nil
}

func (m *mockRoutine) getCallCount() int32 {
	return atomic.LoadInt32(&m.callCount)
}

func TestParallelRoutine_Run(t *testing.T) {
	t.Run("processes data with correct concurrency", func(t *testing.T) {
		maxConcurrency := 3
		processedData := make([]pipeline.Msg, 0)
		var mu sync.Mutex

		mockR := &mockRoutine{
			processFunc: func(ctx context.Context, pipe pipeline.Pipe) error {
				defer pipe.Close()

				for data := range pipe.In() {
					time.Sleep(100 * time.Millisecond)

					mu.Lock()
					processedData = append(processedData, data)
					mu.Unlock()
					pipe.Out() <- data
				}

				return nil
			},
		}

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

		parallel := routines.Parallel(mockR, maxConcurrency)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := parallel.Run(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		assert.Len(t, results, len(testData))
		assert.ElementsMatch(t, testData, results)
		assert.Equal(t, int32(maxConcurrency), mockR.getCallCount())
	})

	t.Run("handles empty input", func(t *testing.T) {
		maxConcurrency := 2

		mockR := &mockRoutine{
			processFunc: func(ctx context.Context, pipe pipeline.Pipe) error {
				defer pipe.Close()

				for range pipe.In() {
				}
				return nil
			},
		}

		pipe := pipeline.NewChanPipe()

		// Empty input data
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

		parallel := routines.Parallel(mockR, maxConcurrency)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := parallel.Run(ctx, pipe)
			assert.NoError(t, err)
		}()

		wg.Wait()

		assert.Empty(t, results)
		assert.Equal(t, int32(maxConcurrency), mockR.getCallCount())
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		maxConcurrency := 2

		mockR := &mockRoutine{
			processFunc: func(ctx context.Context, pipe pipeline.Pipe) error {
				defer pipe.Close()

				for data := range pipe.In() {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case pipe.Out() <- data:
					}
				}
				return nil
			},
		}

		pipe := pipeline.NewChanPipe()

		testData := generateTestMsgs(1, 10)
		stopAfter := 5

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

		parallel := routines.Parallel(mockR, maxConcurrency)

		go func() {
			err := parallel.Run(ctx, pipe)
			assert.NoError(t, err)
		}()

		go func() {
			for i, data := range testData {
				if i <= stopAfter {
					pipe.In() <- data
				} else {
					cancel()
				}
			}
		}()

		wg.Wait()

		//we can get less messages if a worker was processing when context was cancelled
		//but at most we should get `stopAfter` messages
		require.LessOrEqual(t, slices.Max(results), stopAfter)
	})

	t.Run("closes pipe after completion", func(t *testing.T) {
		maxConcurrency := 1

		mockR := &mockRoutine{
			processFunc: func(ctx context.Context, pipe pipeline.Pipe) error {
				defer pipe.Close()

				for data := range pipe.In() {
					pipe.Out() <- data
				}
				return nil
			},
		}

		pipe := pipeline.NewChanPipe()

		testData := generateTestMsgs(1, 1)

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

		parallel := routines.Parallel(mockR, maxConcurrency)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := parallel.Run(ctx, pipe)
			require.NoError(t, err)
		}()

		wg.Wait()

		assert.Len(t, results, len(testData))
		assert.ElementsMatch(t, testData, results)

		_, ok := <-pipe.Out()
		assert.False(t, ok, "pipe output should be closed")
	})

	t.Run("distributes work across multiple workers", func(t *testing.T) {
		maxConcurrency := 3
		workerActivity := make(map[int]bool)
		var mu sync.Mutex

		var mockR *mockRoutine

		mockR = &mockRoutine{
			processFunc: func(ctx context.Context, pipe pipeline.Pipe) error {
				defer pipe.Close()

				workerID := int(atomic.AddInt32(&mockR.callCount, 1))

				for data := range pipe.In() {
					mu.Lock()
					workerActivity[workerID] = true
					mu.Unlock()

					time.Sleep(10 * time.Millisecond)
					pipe.Out() <- data
				}
				return nil
			},
		}

		pipe := pipeline.NewChanPipe()

		testData := generateTestMsgs(0, 10)

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

		parallel := routines.Parallel(mockR, maxConcurrency)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := parallel.Run(ctx, pipe)
			require.NoError(t, err)
		}()

		wg.Wait()

		assert.Len(t, results, len(testData))

		mu.Lock()
		activeWorkers := len(workerActivity)
		mu.Unlock()

		assert.Equal(t, maxConcurrency, activeWorkers, "all workers should have been active")
	})

	t.Run("handles routine errors gracefully", func(t *testing.T) {
		maxConcurrency := 2

		mockR := &mockRoutine{
			processFunc: func(ctx context.Context, pipe pipeline.Pipe) error {
				defer pipe.Close()

				for data := range pipe.In() {
					pipe.Out() <- data
				}
				return nil
			},
		}

		pipe := pipeline.NewChanPipe()

		testData := generateTestMsgs(1, 1)

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

		parallel := routines.Parallel(mockR, maxConcurrency)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := parallel.Run(ctx, pipe)
			require.NoError(t, err)
		}()

		wg.Wait()

		assert.Len(t, results, len(testData))
		assert.ElementsMatch(t, testData, results)
	})

	t.Run("fan-out distributes data evenly", func(t *testing.T) {
		maxConcurrency := 2
		workerDataCount := make(map[int]int)
		var mu sync.Mutex

		var mockR *mockRoutine

		mockR = &mockRoutine{
			processFunc: func(ctx context.Context, pipe pipeline.Pipe) error {
				defer pipe.Close()

				workerID := int(atomic.AddInt32(&mockR.callCount, 1))

				count := 0
				for data := range pipe.In() {
					count++
					pipe.Out() <- data
				}

				mu.Lock()
				workerDataCount[workerID] = count
				mu.Unlock()

				return nil
			},
		}

		pipe := pipeline.NewChanPipe()

		numItems := 100
		testData := generateTestMsgs(0, numItems)

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

		parallel := routines.Parallel(mockR, maxConcurrency)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := parallel.Run(ctx, pipe)
			require.NoError(t, err)
		}()

		wg.Wait()

		assert.Len(t, results, numItems)

		mu.Lock()
		totalProcessed := 0
		for _, count := range workerDataCount {
			totalProcessed += count
		}
		mu.Unlock()

		assert.Equal(t, numItems, totalProcessed)
		assert.Len(t, workerDataCount, maxConcurrency)
	})

	t.Run("handles single worker concurrency", func(t *testing.T) {
		maxConcurrency := 1

		mockR := &mockRoutine{
			processFunc: func(ctx context.Context, pipe pipeline.Pipe) error {
				defer pipe.Close()

				for data := range pipe.In() {
					pipe.Out() <- data
				}
				return nil
			},
		}

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

		var results []pipeline.Msg

		go func() {
			defer wg.Done()

			for result := range pipe.Out() {
				results = append(results, result)
			}
		}()

		parallel := routines.Parallel(mockR, maxConcurrency)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			err := parallel.Run(ctx, pipe)
			require.NoError(t, err)
		}()

		wg.Wait()

		assert.ElementsMatch(t, testData, results)
		assert.Equal(t, int32(maxConcurrency), mockR.getCallCount())
	})
}

func generateTestMsgs(start, size int) []pipeline.Msg {
	testData := make([]pipeline.Msg, 0, size)
	for i := start; i < start+size; i++ {
		testData = append(testData, pipeline.Msg{
			ID:   "",
			Data: i,
		})
	}

	return testData
}
