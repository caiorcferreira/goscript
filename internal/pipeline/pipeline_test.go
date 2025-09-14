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

//func TestPipeline_Start_MultipleRoutines(t *testing.T) {
//	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
//	defer cancel()
//
//	ppl := pipeline.New()
//	routine1 := NewTestRoutine("step1")
//	routine2 := NewTestRoutine("step2")
//	routine3 := NewTestRoutine("step3")
//
//	ppl.Chain(routine1).Chain(routine2).Chain(routine3)
//
//	inputPipe := pipeline.NewChanPipe()
//
//	// Start pipeline in goroutine
//	errChan := make(chan error, 1)
//	go func() {
//		errChan <- ppl.Start(ctx, inputPipe)
//	}()
//
//	// Send test messages
//	testMsgs := []pipeline.Msg{
//		{ID: "1", Data: "msg1"},
//		{ID: "2", Data: "msg2"},
//	}
//
//	go func() {
//		for _, msg := range testMsgs {
//			inputPipe.In() <- msg
//		}
//		_ = inputPipe.Close()
//	}()
//
//	// Collect output
//	var outputMsgs []pipeline.Msg
//	for msg := range inputPipe.Out() {
//		outputMsgs = append(outputMsgs, msg)
//	}
//
//	// Wait for pipeline to complete
//	select {
//	case err := <-errChan:
//		require.NoError(t, err)
//	case <-ctx.Done():
//		t.Fatal("pipeline did not complete in time")
//	}
//
//	// Verify results
//	require.Len(t, outputMsgs, 2)
//
//	// Messages should be processed through all three routines
//	for i, msg := range outputMsgs {
//		expectedData := testMsgs[i].Data.(string) + "-step1-step2-step3"
//		assert.Equal(t, testMsgs[i].ID, msg.ID)
//		assert.Equal(t, expectedData, msg.Data)
//	}
//}
//
//func TestPipeline_Start_EmptyPipeline(t *testing.T) {
//	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
//	defer cancel()
//
//	ppl := pipeline.New()
//	inputPipe := pipeline.NewChanPipe()
//
//	// Start pipeline in goroutine
//	errChan := make(chan error, 1)
//	go func() {
//		errChan <- ppl.Start(ctx, inputPipe)
//	}()
//
//	// Send test message
//	testMsg := pipeline.Msg{ID: "1", Data: "hello"}
//	go func() {
//		inputPipe.In() <- testMsg
//		_ = inputPipe.Close()
//	}()
//
//	// Collect output
//	var outputMsgs []pipeline.Msg
//	for msg := range inputPipe.Out() {
//		outputMsgs = append(outputMsgs, msg)
//	}
//
//	// Wait for pipeline to complete
//	select {
//	case err := <-errChan:
//		require.NoError(t, err)
//	case <-ctx.Done():
//		t.Fatal("pipeline did not complete in time")
//	}
//
//	// Empty pipeline should pass messages through unchanged
//	assert.Len(t, outputMsgs, 1)
//	assert.Equal(t, testMsg, outputMsgs[0])
//}
//
//func TestPipeline_Start_ContextCancellation(t *testing.T) {
//	ctx, cancel := context.WithCancel(context.Background())
//
//	ppl := pipeline.New()
//
//	// Create a routine that will block
//	blockingRoutine := NewTestRoutine("blocking").WithStartFunc(func(ctx context.Context, pipe pipeline.Pipe) error {
//		<-ctx.Done() // Block until context is cancelled
//		return ctx.Err()
//	})
//
//	ppl.Chain(blockingRoutine)
//	inputPipe := pipeline.NewChanPipe()
//
//	// Start pipeline in goroutine
//	errChan := make(chan error, 1)
//	go func() {
//		errChan <- ppl.Start(ctx, inputPipe)
//	}()
//
//	// Cancel context after short delay
//	time.Sleep(100 * time.Millisecond)
//	cancel()
//
//	// Pipeline should complete quickly after cancellation
//	select {
//	case err := <-errChan:
//		require.NoError(t, err)
//	case <-time.After(1 * time.Second):
//		t.Fatal("pipeline did not respond to context cancellation")
//	}
//}
//
//func TestPipeline_Start_RoutineError(t *testing.T) {
//	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
//	defer cancel()
//
//	ppl := pipeline.New()
//	errorRoutine := NewTestRoutine("error").WithError("test error")
//	ppl.Chain(errorRoutine)
//
//	inputPipe := pipeline.NewChanPipe()
//
//	// Start pipeline in goroutine
//	errChan := make(chan error, 1)
//	go func() {
//		errChan <- ppl.Start(ctx, inputPipe)
//	}()
//
//	// Send test message
//	go func() {
//		inputPipe.In() <- pipeline.Msg{ID: "1", Data: "hello"}
//		_ = inputPipe.Close()
//	}()
//
//	// Wait for pipeline to complete
//	select {
//	case err := <-errChan:
//		// Pipeline should complete without error even if routine errors
//		// (errors are logged, not returned)
//		require.NoError(t, err)
//	case <-ctx.Done():
//		t.Fatal("pipeline did not complete in time")
//	}
//}
//
//func TestPipeline_Start_MessageTransformation(t *testing.T) {
//	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
//	defer cancel()
//
//	ppl := pipeline.New()
//
//	// Create routine that transforms messages
//	transformRoutine := NewTestRoutine("transform").WithProcessFunc(func(msg pipeline.Msg) (pipeline.Msg, error) {
//		return pipeline.Msg{
//			ID:   msg.ID,
//			Data: "transformed_" + msg.Data.(string),
//		}, nil
//	})
//
//	ppl.Chain(transformRoutine)
//	inputPipe := pipeline.NewChanPipe()
//
//	// Start pipeline in goroutine
//	errChan := make(chan error, 1)
//	go func() {
//		errChan <- ppl.Start(ctx, inputPipe)
//	}()
//
//	// Send test message
//	testMsg := pipeline.Msg{ID: "1", Data: "original"}
//	go func() {
//		inputPipe.In() <- testMsg
//		_ = inputPipe.Close()
//	}()
//
//	// Collect output
//	var outputMsgs []pipeline.Msg
//	for msg := range inputPipe.Out() {
//		outputMsgs = append(outputMsgs, msg)
//	}
//
//	// Wait for pipeline to complete
//	select {
//	case err := <-errChan:
//		require.NoError(t, err)
//	case <-ctx.Done():
//		t.Fatal("pipeline did not complete in time")
//	}
//
//	// Verify transformation
//	require.Len(t, outputMsgs, 1)
//	assert.Equal(t, "1", outputMsgs[0].ID)
//	assert.Equal(t, "transformed_original", outputMsgs[0].Data)
//}
//
//func TestPipeline_Start_ConcurrentExecution(t *testing.T) {
//	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
//	defer cancel()
//
//	ppl := pipeline.New()
//
//	// Create routines with processing delays
//	routine1 := NewTestRoutine("slow1").WithProcessingTime(100 * time.Millisecond)
//	routine2 := NewTestRoutine("slow2").WithProcessingTime(100 * time.Millisecond)
//
//	ppl.Chain(routine1).Chain(routine2)
//	inputPipe := pipeline.NewChanPipe()
//
//	// Start pipeline in goroutine
//	errChan := make(chan error, 1)
//	go func() {
//		errChan <- ppl.Start(ctx, inputPipe)
//	}()
//
//	// Send multiple messages
//	numMsgs := 3
//	go func() {
//		for i := 0; i < numMsgs; i++ {
//			msg := pipeline.Msg{ID: string(rune(i + '1')), Data: "msg" + string(rune(i+'1'))}
//			inputPipe.In() <- msg
//		}
//		_ = inputPipe.Close()
//	}()
//
//	// Collect output with timing
//	start := time.Now()
//	var outputMsgs []pipeline.Msg
//	for msg := range inputPipe.Out() {
//		outputMsgs = append(outputMsgs, msg)
//	}
//	duration := time.Since(start)
//
//	// Wait for pipeline to complete
//	select {
//	case err := <-errChan:
//		require.NoError(t, err)
//	case <-ctx.Done():
//		t.Fatal("pipeline did not complete in time")
//	}
//
//	// Verify all messages processed
//	assert.Len(t, outputMsgs, numMsgs)
//
//	// Should complete faster than sequential execution due to pipelining
//	// Sequential would be: numMsgs * (delay1 + delay2) = 3 * 200ms = 600ms
//	// Pipelined should be closer to: delay1 + delay2 + (numMsgs-1) * max(delay1, delay2)
//	assert.Less(t, duration, 500*time.Millisecond, "pipeline should execute concurrently")
//}
//
//func TestPipeline_Start_MessageOrdering(t *testing.T) {
//	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
//	defer cancel()
//
//	ppl := pipeline.New()
//	routine := NewTestRoutine("order")
//	ppl.Chain(routine)
//
//	inputPipe := pipeline.NewChanPipe()
//
//	// Start pipeline in goroutine
//	errChan := make(chan error, 1)
//	go func() {
//		errChan <- ppl.Start(ctx, inputPipe)
//	}()
//
//	// Send messages in specific order
//	expectedOrder := []string{"first", "second", "third"}
//	go func() {
//		for i, data := range expectedOrder {
//			msg := pipeline.Msg{ID: string(rune(i + '1')), Data: data}
//			inputPipe.In() <- msg
//		}
//		_ = inputPipe.Close()
//	}()
//
//	// Collect output
//	var outputMsgs []pipeline.Msg
//	for msg := range inputPipe.Out() {
//		outputMsgs = append(outputMsgs, msg)
//	}
//
//	// Wait for pipeline to complete
//	select {
//	case err := <-errChan:
//		require.NoError(t, err)
//	case <-ctx.Done():
//		t.Fatal("pipeline did not complete in time")
//	}
//
//	// Verify order preservation
//	require.Len(t, outputMsgs, len(expectedOrder))
//	for i, msg := range outputMsgs {
//		expectedData := expectedOrder[i] + "-order"
//		assert.Equal(t, expectedData, msg.Data)
//	}
//}
//
//func TestPipeline_Start_NoInputMessages(t *testing.T) {
//	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
//	defer cancel()
//
//	ppl := pipeline.New()
//	routine := NewTestRoutine("test")
//	ppl.Chain(routine)
//
//	inputPipe := pipeline.NewChanPipe()
//
//	// Start pipeline in goroutine
//	errChan := make(chan error, 1)
//	go func() {
//		errChan <- ppl.Start(ctx, inputPipe)
//	}()
//
//	// Close input immediately without sending messages
//	go func() {
//		inputPipe.Close()
//	}()
//
//	// Collect output (should be empty)
//	var outputMsgs []pipeline.Msg
//	for msg := range inputPipe.Out() {
//		outputMsgs = append(outputMsgs, msg)
//	}
//
//	// Wait for pipeline to complete
//	select {
//	case err := <-errChan:
//		require.NoError(t, err)
//	case <-ctx.Done():
//		t.Fatal("pipeline did not complete in time")
//	}
//
//	// Should have no output messages
//	assert.Empty(t, outputMsgs)
//	assert.Empty(t, routine.GetReceivedMsgs())
//}
//
//func TestPipeline_WithMockRoutine(t *testing.T) {
//	ctrl := gomock.NewController(t)
//	defer ctrl.Finish()
//
//	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
//	defer cancel()
//
//	// Create mock routine
//	mockRoutine := mocks.NewMockRoutine(ctrl)
//
//	// Set up expectations
//	mockRoutine.EXPECT().Start(gomock.Any(), gomock.Any()).Return(nil).Times(1)
//
//	ppl := pipeline.New()
//	ppl.Chain(mockRoutine)
//
//	inputPipe := pipeline.NewChanPipe()
//
//	// Start pipeline in goroutine
//	errChan := make(chan error, 1)
//	go func() {
//		errChan <- ppl.Start(ctx, inputPipe)
//	}()
//
//	// Close input immediately to trigger completion
//	go func() {
//		inputPipe.Close()
//	}()
//
//	// Wait for pipeline to complete
//	select {
//	case err := <-errChan:
//		require.NoError(t, err)
//	case <-ctx.Done():
//		t.Fatal("pipeline did not complete in time")
//	}
//
//	// Mock expectations are verified automatically by gomock
//}
