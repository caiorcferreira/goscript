package pipeline_test

import (
	"context"
	"fmt"
	"github.com/caiorcferreira/goscript/internal/pipeline"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"

	pipelinemocks "github.com/caiorcferreira/goscript/internal/pipeline/mocks"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestPipeline_Start_SingleRoutine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ctrl := gomock.NewController(t)
	routine := pipelinemocks.NewMockRoutine(ctrl)
	routine.EXPECT().Start(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, pipe pipeline.Pipe) error {
			defer pipe.Close()

			for msg := range pipe.In() {
				select {
				case <-ctx.Done():
				case pipe.Out() <- msg:
				}
			}

			return nil
		},
	)

	sourcePipe := pipeline.NewChanPipe()
	ppl := pipeline.New().Chain(routine)

	// Send test message
	testMsg := pipeline.Msg{ID: "1", Data: "hello"}
	go func() {
		defer func() {
			close(sourcePipe.In())
		}()
		sourcePipe.In() <- testMsg
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	// Collect output
	var outputMsgs []pipeline.Msg
	go func() {
		defer wg.Done()

		for msg := range sourcePipe.Out() {
			fmt.Println("Received message:", msg)
			outputMsgs = append(outputMsgs, msg)
		}
	}()

	assert.NoError(t, ppl.Start(ctx, sourcePipe))

	wg.Wait()

	// Verify results
	require.Len(t, outputMsgs, 1)
	require.Equal(t, "1", outputMsgs[0].ID)
	require.Equal(t, "hello", outputMsgs[0].Data)
}

func TestPipeline_Start_MultipleRoutines(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ctrl := gomock.NewController(t)

	// Create mock routines
	routine1 := pipelinemocks.NewMockRoutine(ctrl)
	routine2 := pipelinemocks.NewMockRoutine(ctrl)
	routine3 := pipelinemocks.NewMockRoutine(ctrl)

	// Set up expectations for routine1 (adds -step1)
	routine1.EXPECT().Start(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, pipe pipeline.Pipe) error {
			defer pipe.Close()
			for msg := range pipe.In() {
				newMsg := pipeline.Msg{
					ID:   msg.ID,
					Data: msg.Data.(string) + "-step1",
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case pipe.Out() <- newMsg:
				}
			}
			return nil
		},
	)

	// Set up expectations for routine2 (adds -step2)
	routine2.EXPECT().Start(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, pipe pipeline.Pipe) error {
			defer pipe.Close()
			for msg := range pipe.In() {
				newMsg := pipeline.Msg{
					ID:   msg.ID,
					Data: msg.Data.(string) + "-step2",
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case pipe.Out() <- newMsg:
				}
			}
			return nil
		},
	)

	// Set up expectations for routine3 (adds -step3)
	routine3.EXPECT().Start(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, pipe pipeline.Pipe) error {
			defer pipe.Close()
			for msg := range pipe.In() {
				newMsg := pipeline.Msg{
					ID:   msg.ID,
					Data: msg.Data.(string) + "-step3",
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case pipe.Out() <- newMsg:
				}
			}
			return nil
		},
	)

	ppl := pipeline.New()
	ppl.Chain(routine1).Chain(routine2).Chain(routine3)

	inputPipe := pipeline.NewChanPipe()

	// Send test messages
	testMsgs := []pipeline.Msg{
		{ID: "1", Data: "msg1"},
		{ID: "2", Data: "msg2"},
	}

	go func() {
		defer close(inputPipe.In())
		for _, msg := range testMsgs {
			inputPipe.In() <- msg
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	// Collect output
	var outputMsgs []pipeline.Msg
	go func() {
		defer wg.Done()
		for msg := range inputPipe.Out() {
			outputMsgs = append(outputMsgs, msg)
		}
	}()

	assert.NoError(t, ppl.Start(ctx, inputPipe))
	wg.Wait()

	// Verify results
	require.Len(t, outputMsgs, 2)

	// Messages should be processed through all three routines
	for i, msg := range outputMsgs {
		expectedData := testMsgs[i].Data.(string) + "-step1-step2-step3"
		assert.Equal(t, testMsgs[i].ID, msg.ID)
		assert.Equal(t, expectedData, msg.Data)
	}
}

func TestPipeline_Start_EmptyPipeline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	ppl := pipeline.New()
	inputPipe := pipeline.NewChanPipe()

	// Send test message
	testMsg := pipeline.Msg{ID: "1", Data: "hello"}
	go func() {
		defer close(inputPipe.In())
		inputPipe.In() <- testMsg
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	// Collect output
	var outputMsgs []pipeline.Msg
	go func() {
		defer wg.Done()
		for msg := range inputPipe.Out() {
			outputMsgs = append(outputMsgs, msg)
		}
	}()

	assert.NoError(t, ppl.Start(ctx, inputPipe))
	wg.Wait()

	// Empty pipeline should pass messages through unchanged
	assert.Len(t, outputMsgs, 1)
	assert.Equal(t, testMsg, outputMsgs[0])
}

func TestPipeline_Start_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	ctrl := gomock.NewController(t)
	blockingRoutine := pipelinemocks.NewMockRoutine(ctrl)

	blockingRoutine.EXPECT().Start(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, pipe pipeline.Pipe) error {
			defer pipe.Close()
			<-ctx.Done() // Block until context is cancelled
			return ctx.Err()
		},
	)

	ppl := pipeline.New()
	ppl.Chain(blockingRoutine)
	inputPipe := pipeline.NewChanPipe()

	// Start pipeline in goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- ppl.Start(ctx, inputPipe)
	}()

	// Cancel context after short delay
	time.Sleep(100 * time.Millisecond)
	cancel()

	// Pipeline should complete quickly after cancellation
	select {
	case err := <-errChan:
		require.NoError(t, err)
	case <-time.After(1 * time.Second):
		t.Fatal("pipeline did not respond to context cancellation")
	}
}

func TestPipeline_Start_RoutineError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ctrl := gomock.NewController(t)
	errorRoutine := pipelinemocks.NewMockRoutine(ctrl)

	errorRoutine.EXPECT().Start(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, pipe pipeline.Pipe) error {
			defer pipe.Close()
			// Simulate an error in the routine
			return fmt.Errorf("test error")
		},
	)

	ppl := pipeline.New()
	ppl.Chain(errorRoutine)

	inputPipe := pipeline.NewChanPipe()

	// Send test message
	go func() {
		defer close(inputPipe.In())
		inputPipe.In() <- pipeline.Msg{ID: "1", Data: "hello"}
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	// Collect output
	var outputMsgs []pipeline.Msg
	go func() {
		defer wg.Done()
		for msg := range inputPipe.Out() {
			outputMsgs = append(outputMsgs, msg)
		}
	}()

	// Pipeline should complete without error even if routine errors
	// (errors are logged, not returned)
	assert.NoError(t, ppl.Start(ctx, inputPipe))
	wg.Wait()
}

func TestPipeline_Start_MessageTransformation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ctrl := gomock.NewController(t)
	transformRoutine := pipelinemocks.NewMockRoutine(ctrl)

	transformRoutine.EXPECT().Start(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, pipe pipeline.Pipe) error {
			defer pipe.Close()
			for msg := range pipe.In() {
				newMsg := pipeline.Msg{
					ID:   msg.ID,
					Data: "transformed_" + msg.Data.(string),
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case pipe.Out() <- newMsg:
				}
			}
			return nil
		},
	)

	ppl := pipeline.New()
	ppl.Chain(transformRoutine)
	inputPipe := pipeline.NewChanPipe()

	// Send test message
	testMsg := pipeline.Msg{ID: "1", Data: "original"}
	go func() {
		defer close(inputPipe.In())
		inputPipe.In() <- testMsg
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	// Collect output
	var outputMsgs []pipeline.Msg
	go func() {
		defer wg.Done()
		for msg := range inputPipe.Out() {
			outputMsgs = append(outputMsgs, msg)
		}
	}()

	assert.NoError(t, ppl.Start(ctx, inputPipe))
	wg.Wait()

	// Verify transformation
	require.Len(t, outputMsgs, 1)
	assert.Equal(t, "1", outputMsgs[0].ID)
	assert.Equal(t, "transformed_original", outputMsgs[0].Data)
}

func TestPipeline_Start_ConcurrentExecution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ctrl := gomock.NewController(t)
	routine1 := pipelinemocks.NewMockRoutine(ctrl)
	routine2 := pipelinemocks.NewMockRoutine(ctrl)

	// Create routines with processing delays
	routine1.EXPECT().Start(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, pipe pipeline.Pipe) error {
			defer pipe.Close()
			for msg := range pipe.In() {
				time.Sleep(100 * time.Millisecond) // Simulate processing time
				newMsg := pipeline.Msg{
					ID:   msg.ID,
					Data: msg.Data.(string) + "-slow1",
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case pipe.Out() <- newMsg:
				}
			}
			return nil
		},
	)

	routine2.EXPECT().Start(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, pipe pipeline.Pipe) error {
			defer pipe.Close()
			for msg := range pipe.In() {
				time.Sleep(100 * time.Millisecond) // Simulate processing time
				newMsg := pipeline.Msg{
					ID:   msg.ID,
					Data: msg.Data.(string) + "-slow2",
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case pipe.Out() <- newMsg:
				}
			}
			return nil
		},
	)

	ppl := pipeline.New()
	ppl.Chain(routine1).Chain(routine2)
	inputPipe := pipeline.NewChanPipe()

	// Send multiple messages
	numMsgs := 3
	go func() {
		defer close(inputPipe.In())
		for i := 0; i < numMsgs; i++ {
			msg := pipeline.Msg{ID: string(rune(i + '1')), Data: "msg" + string(rune(i+'1'))}
			inputPipe.In() <- msg
		}
	}()

	// Collect output with timing
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(1)

	var outputMsgs []pipeline.Msg
	go func() {
		defer wg.Done()
		for msg := range inputPipe.Out() {
			outputMsgs = append(outputMsgs, msg)
		}
	}()

	assert.NoError(t, ppl.Start(ctx, inputPipe))
	wg.Wait()
	duration := time.Since(start)

	// Verify all messages processed
	assert.Len(t, outputMsgs, numMsgs)

	// Should complete faster than sequential execution due to pipelining
	// Sequential would be: numMsgs * (delay1 + delay2) = 3 * 200ms = 600ms
	// Pipelined should be closer to: delay1 + delay2 + (numMsgs-1) * max(delay1, delay2)
	assert.Less(t, duration, 500*time.Millisecond, "pipeline should execute concurrently")
}

func TestPipeline_Start_MessageOrdering(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ctrl := gomock.NewController(t)
	routine := pipelinemocks.NewMockRoutine(ctrl)

	routine.EXPECT().Start(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, pipe pipeline.Pipe) error {
			defer pipe.Close()
			for msg := range pipe.In() {
				newMsg := pipeline.Msg{
					ID:   msg.ID,
					Data: msg.Data.(string) + "-order",
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case pipe.Out() <- newMsg:
				}
			}
			return nil
		},
	)

	ppl := pipeline.New()
	ppl.Chain(routine)

	inputPipe := pipeline.NewChanPipe()

	// Send messages in specific order
	expectedOrder := []string{"first", "second", "third"}
	go func() {
		defer close(inputPipe.In())
		for i, data := range expectedOrder {
			msg := pipeline.Msg{ID: string(rune(i + '1')), Data: data}
			inputPipe.In() <- msg
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	// Collect output
	var outputMsgs []pipeline.Msg
	go func() {
		defer wg.Done()
		for msg := range inputPipe.Out() {
			outputMsgs = append(outputMsgs, msg)
		}
	}()

	assert.NoError(t, ppl.Start(ctx, inputPipe))
	wg.Wait()

	// Verify order preservation
	require.Len(t, outputMsgs, len(expectedOrder))
	for i, msg := range outputMsgs {
		expectedData := expectedOrder[i] + "-order"
		assert.Equal(t, expectedData, msg.Data)
	}
}

func TestPipeline_Start_NoInputMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	ctrl := gomock.NewController(t)
	routine := pipelinemocks.NewMockRoutine(ctrl)

	routine.EXPECT().Start(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, pipe pipeline.Pipe) error {
			defer pipe.Close()
			// Just wait for input channel to close
			for range pipe.In() {
				// No messages expected
			}
			return nil
		},
	)

	ppl := pipeline.New()
	ppl.Chain(routine)

	inputPipe := pipeline.NewChanPipe()

	// Close input immediately without sending messages
	go func() {
		close(inputPipe.In())
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	// Collect output (should be empty)
	var outputMsgs []pipeline.Msg
	go func() {
		defer wg.Done()
		for msg := range inputPipe.Out() {
			outputMsgs = append(outputMsgs, msg)
		}
	}()

	assert.NoError(t, ppl.Start(ctx, inputPipe))
	wg.Wait()

	// Should have no output messages
	assert.Empty(t, outputMsgs)
}

func TestPipeline_WithMockRoutine(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Create mock routine
	mockRoutine := pipelinemocks.NewMockRoutine(ctrl)

	// Set up expectations
	mockRoutine.EXPECT().Start(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, pipe pipeline.Pipe) error {
			defer pipe.Close()
			// Just consume any input and close
			for range pipe.In() {
			}
			return nil
		},
	).Times(1)

	ppl := pipeline.New()
	ppl.Chain(mockRoutine)

	inputPipe := pipeline.NewChanPipe()

	// Close input immediately to trigger completion
	go func() {
		close(inputPipe.In())
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	// Collect output
	go func() {
		defer wg.Done()
		for range inputPipe.Out() {
		}
	}()

	assert.NoError(t, ppl.Start(ctx, inputPipe))
	wg.Wait()

	// Mock expectations are verified automatically by gomock
}
