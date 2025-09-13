package main

import (
	"context"
	"github.com/caiorcferreira/goscript"
	"github.com/caiorcferreira/goscript/internal/pipeline"
	"github.com/caiorcferreira/goscript/internal/routines"
	"log/slog"
	"os"
	"strings"
)

func main() {
	println("This is an example function.")

	// Set slog to debug level
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	textFormatting := pipeline.New().
		Chain(routines.Transform(strings.ToUpper)).
		Chain(routines.Transform(func(t string) string {
			return strings.ReplaceAll(t, " ", "_")
		}))

	script := goscript.New()
	script.
		FileIn("data/example.txt").
		Chain(textFormatting).
		FileOut("data/output/subpipe_5.txt")

	ctx := context.Background()
	err := script.Run(ctx)
	if err != nil {
		panic(err)
	}
}
