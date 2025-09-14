package goscript

import (
	"context"
	"fmt"
	"github.com/caiorcferreira/goscript/internal/pipeline"
	"github.com/caiorcferreira/goscript/internal/routines"
	"github.com/caiorcferreira/goscript/internal/routines/filesystem"
	"log/slog"
	"time"
)

// Script represents a pipeline-based data processing script with concurrent execution support.
// It provides a fluent API for chaining data processing routines with features like debouncing,
// parallel execution, and file I/O operations.
//
// The Script follows a pipeline architecture where data flows through a chain of routines
// connected by pipes. Each routine runs in its own goroutine, managed by the Script coordinator.
type Script struct {
	inPipe       pipeline.Pipe
	inputRoutine pipeline.Routine

	outPipe       pipeline.Pipe
	outputRoutine pipeline.Routine

	hasPipeline bool
	pipeline    *pipeline.Pipeline
}

// New creates a new Script instance with default input (stdin) and output (stdout) routines.
// The returned Script is ready to be configured with additional routines and executed.
//
// Example:
//
//	script := goscript.New()
//	err := script.Run(context.Background())
func New() *Script {
	inPipe := pipeline.NewChanPipe()
	outPipe := pipeline.NewChanPipe()

	inPipe.Chain(outPipe)

	p := pipeline.New()

	return &Script{
		inPipe:       inPipe,
		inputRoutine: routines.NewStdInRoutine(),

		outPipe:       outPipe,
		outputRoutine: routines.NewStdOutRoutine(),

		pipeline: p,
	}
}

// In sets the input routine for the script. The input routine is responsible for generating
// data that will flow through the pipeline.
//
// Parameters:
//   - r: The routine that will serve as the data source for the pipeline
//
// Returns the Script instance for method chaining.
//
// Example:
//
//	script.In(customInputRoutine)
func (s *Script) In(r pipeline.Routine) *Script {
	s.inputRoutine = r

	return s
}

// Out sets the output routine for the script. The output routine is responsible for consuming
// the final processed data from the pipeline.
//
// Parameters:
//   - r: The routine that will handle the final output of the pipeline
//
// Returns the Script instance for method chaining.
//
// Example:
//
//	script.Out(customOutputRoutine)
func (s *Script) Out(r pipeline.Routine) *Script {
	s.outputRoutine = r

	return s
}

// Chain adds a processing routine to the pipeline. Multiple routines can be chained together
// to create complex data processing workflows.
//
// Parameters:
//   - routine: The processing routine to add to the pipeline
//
// Returns the Script instance for method chaining.
//
// Example:
//
//	script.Chain(filterRoutine).Chain(transformRoutine)
func (s *Script) Chain(routine pipeline.Routine) *Script {
	s.hasPipeline = true
	s.pipeline.Chain(routine)
	return s
}

// FileIn configures the script to read input from a file, processing it line by line.
// Each line is treated as a separate data item in the pipeline.
//
// Parameters:
//   - path: The file path to read from
//
// Returns the Script instance for method chaining.
//
// Example:
//
//	script.FileIn("input.txt").Chain(processLine).Run(ctx)
func (s *Script) FileIn(path string) *Script {
	s.In(filesystem.File(path).Read())
	return s
}

// FileOut configures the script to write output to a file, with each data item written as a separate line.
//
// Parameters:
//   - path: The file path to write to
//
// Returns the Script instance for method chaining.
//
// Example:
//
//	script.FileIn("input.txt").Chain(processLine).FileOut("output.txt").Run(ctx)
func (s *Script) FileOut(path string) *Script {
	s.Out(filesystem.File(path).Write())
	return s
}

// JSONIn configures the script to read input from a JSON file.
// The file content is parsed as JSON and made available to the pipeline.
//
// Parameters:
//   - path: The JSON file path to read from
//
// Returns the Script instance for method chaining.
//
// Example:
//
//	script.JSONIn("data.json").Chain(processJSON).Run(ctx)
func (s *Script) JSONIn(path string) *Script {
	s.In(filesystem.File(path).Read().WithJSONCodec())
	return s
}

// JSONOut configures the script to write output to a JSON file.
// The pipeline output is serialized as JSON before writing.
//
// Parameters:
//   - path: The JSON file path to write to
//
// Returns the Script instance for method chaining.
//
// Example:
//
//	script.Chain(generateData).JSONOut("output.json").Run(ctx)
func (s *Script) JSONOut(path string) *Script {
	s.Out(filesystem.File(path).Write().WithJSONCodec())
	return s
}

// CSVIn configures the script to read input from a CSV file.
// Each row is processed as a separate data item in the pipeline.
//
// Parameters:
//   - path: The CSV file path to read from
//
// Returns the Script instance for method chaining.
//
// Example:
//
//	script.CSVIn("data.csv").Chain(processRow).Run(ctx)
func (s *Script) CSVIn(path string) *Script {
	s.In(filesystem.File(path).Read().WithCSVCodec())
	return s
}

// CSVOut configures the script to write output to a CSV file.
// Each data item is formatted as a CSV row.
//
// Parameters:
//   - path: The CSV file path to write to
//
// Returns the Script instance for method chaining.
//
// Example:
//
//	script.Chain(generateRows).CSVOut("output.csv").Run(ctx)
func (s *Script) CSVOut(path string) *Script {
	s.Out(filesystem.File(path).Write().WithCSVCodec())
	return s
}

// BlobFileIn configures the script to read a file as a single binary blob.
// The entire file content is treated as one data item in the pipeline.
//
// Parameters:
//   - path: The file path to read as a blob
//
// Returns the Script instance for method chaining.
//
// Example:
//
//	script.BlobFileIn("image.jpg").Chain(processBlob).Run(ctx)
func (s *Script) BlobFileIn(path string) *Script {
	s.In(filesystem.File(path).Read().WithBlobCodec())
	return s
}

// BlobFileOut configures the script to write output as a single binary blob to a file.
// All pipeline output is combined and written as binary data.
//
// Parameters:
//   - path: The file path to write the blob to
//
// Returns the Script instance for method chaining.
//
// Example:
//
//	script.Chain(generateBlob).BlobFileOut("output.bin").Run(ctx)
func (s *Script) BlobFileOut(path string) *Script {
	s.Out(filesystem.File(path).Write().WithBlobCodec())
	return s
}

// Parallel adds a routine to the pipeline that will process data items concurrently.
// The routine will be executed in parallel up to the specified maximum concurrency limit.
//
// Parameters:
//   - r: The routine to execute in parallel
//   - maxConcurrency: Maximum number of concurrent executions of the routine
//
// Returns the Script instance for method chaining.
//
// Example:
//
//	script.FileIn("input.txt").Parallel(expensiveProcessing, 4).Run(ctx)
func (s *Script) Parallel(r pipeline.Routine, maxConcurrency int) *Script {
	s.Chain(routines.Parallel(r, maxConcurrency))

	return s
}

// Debounce adds a debouncing mechanism to the pipeline that delays processing until
// no new data has been received for the specified duration. This is useful for
// batch processing or reducing noise from rapidly changing data.
//
// Parameters:
//   - delay: Duration to wait for no new data before proceeding
//
// Returns the Script instance for method chaining.
//
// Example:
//
//	script.FileIn("input.txt").Debounce(100*time.Millisecond).Chain(batchProcess).Run(ctx)
func (s *Script) Debounce(delay time.Duration) *Script {
	s.Chain(routines.Debounce(delay))

	return s
}

// ToString executes the script and returns all output as a concatenated string.
// This is a convenience method that replaces the output routine with a string accumulator
// and runs the script to completion.
//
// Parameters:
//   - ctx: Context for execution control and cancellation
//
// Returns:
//   - string: All pipeline output concatenated as a single string
//   - error: Any error that occurred during execution
//
// Example:
//
//	result, err := script.FileIn("input.txt").Chain(processLines).ToString(ctx)
func (s *Script) ToString(ctx context.Context) (string, error) {
	s.outputRoutine = routines.Reduce(
		func(v string, t string) string {
			return v + t
		},
		"",
	)

	err := s.Run(ctx)
	if err != nil {
		return "", err
	}

	result := <-s.outPipe.Out()

	str, ok := result.Data.(string)
	if !ok {
		return "", fmt.Errorf("failed to convert result to string: %v", result.Data)
	}

	return str, nil
}

// Run executes the configured script pipeline. This method starts all routines in the
// proper order (output → middlewares → input) and manages their lifecycle through
// goroutines. The execution follows the concurrency model where only routines that
// write to a Pipe.Out() may call Close() on the Pipe.
//
// The execution pattern is:
// 1. Routines start in reverse order: output → middlewares → input
// 2. Data flows forward through the pipeline
// 3. Context cancellation propagates to all routines
// 4. Input routine completion triggers pipeline shutdown
//
// Parameters:
//   - ctx: Context for execution control and cancellation
//
// Returns:
//   - error: Any error that occurred during execution (currently always returns nil)
//
// Example:
//
//	err := script.FileIn("input.txt").Chain(processData).FileOut("output.txt").Run(ctx)
func (s *Script) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if s.hasPipeline {
		slog.Debug("Starting pipeline...")

		pipelinePipe := pipeline.NewChanPipe()

		s.inPipe.Chain(pipelinePipe)
		pipelinePipe.Chain(s.outPipe)

		go func() {
			err := s.pipeline.Start(ctx, pipelinePipe)
			if err != nil {
				slog.Error("pipeline routine error", "error", err)
			}
		}()
	}

	// start routines in reverse order: output, middlewares, input
	go func() {
		err := s.outputRoutine.Start(ctx, s.outPipe)
		if err != nil {
			slog.Error("output routine error", "error", err)
		}
	}()

	go func() {
		err := s.inputRoutine.Start(ctx, s.inPipe)
		if err != nil {
			slog.Error("input routine error", "error", err)
		}
	}()

	// wait for input routine to finish
	<-s.outPipe.Done()

	// all routines should exit when context is cancelled
	return nil
}
