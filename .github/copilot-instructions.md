# AI Instructions for GoScript Repository

This file provides guidance to LLM agents when working with code in this repository.

## Project Overview

GoScript is a Go library for building pipeline-based data processing scripts with concurrent execution support. It provides a fluent API for chaining data processing routines with features like debouncing, parallel execution, and file I/O operations.

## Architecture

### Core Components

- **Script** (`internal/interpreter/script.go`): Main orchestrator that manages the pipeline of routines
- **Pipe** (`internal/interpreter/channel.go`): Channel-based communication system between routines  
- **Routine** (`internal/interpreter/types.go`): Interface for data processing units that can be chained together
- **Execution Options** (`internal/interpreter/execution.go`): Decorators for adding concurrency and debouncing behavior

### Key Patterns

- **Pipeline Architecture**: Data flows through a chain of routines connected by pipes
- **Goroutine Management**: Each routine runs in its own goroutine, managed by the Script coordinator
- **Decorator Pattern**: Execution options (WithConcurrency, WithDebounce) wrap routines to add behavior
- **Channel Communication**: Pipes use Go channels for thread-safe data passing between routines

### Package Structure

```
internal/
├── interpreter/    # Core pipeline execution engine
│   ├── script.go      # Main Script type and orchestration
│   ├── types.go       # Core interfaces (Pipe, Routine)
│   ├── channel.go     # Channel-based Pipe implementation
│   ├── execution.go   # Execution options (concurrency, debouncing)
│   ├── parallel.go    # Parallel execution wrapper
│   └── debounce.go    # Debouncing wrapper
└── routines/       # Built-in routine implementations
    ├── file.go        # File I/O routines
    ├── logic.go       # Data transformation routines
    └── os.go          # OS interaction routines
```

## Development Commands

### Task Runner (Recommended)
This project uses [Task](https://taskfile.dev/) for build automation:

```bash
# Display available tasks
task help

# Run the application
task run

# Build binary
task build

# Run all tests
task test

# Run unit tests only
task test/unit

# Run e2e tests only  
task test/e2e

# Format code
task format

# Lint code
task lint

# Generate code
task gen

# Vendor dependencies
task vendor
```

### Direct Go Commands
```bash
# Run example
go run examples/example.go

# Build
go build -o goscript

# Test
go test ./...

# Format
go fmt ./...
goimports -w .

# Lint (requires golangci-lint)
golangci-lint run -c .golangci.yml
```

## Concurrency Model

**Critical Rule**: Only routines that write to a `Pipe.Out()` may call `Close()` on the Pipe.

The system follows this execution pattern:
1. Routines start in reverse order: output → middlewares → input
2. Data flows forward through the pipeline
3. Context cancellation propagates to all routines
4. Input routine completion triggers pipeline shutdown

## Testing

- Use `task test/unit` for unit tests only
- Use `testify` for assertions and `go.uber.org/mock/gomock` for mocking. 

## Code Style

- Strict linting with golangci-lint using custom configuration
- Go 1.24.3+ required
- Follows standard Go formatting with goimports
- Function length limit: 100 lines
- Cyclomatic complexity limit: 30
- Always use `any` instead of `interface{}`