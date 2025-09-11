package main

import (
	"context"
	"github.com/caiorcferreira/goscript"
	"github.com/caiorcferreira/goscript/internal/interpreter"
	"github.com/caiorcferreira/goscript/internal/routines"
	"strings"
	"time"
)

func main() {
	println("This is an example function.")

	script := goscript.New()

	//, interpreter.WithConcurrency(3)

	script.
		In(routines.File("data/example.txt").Read()).
		Chain(interpreter.Debounce(10 * time.Second)).
		Chain(interpreter.Parallel(routines.Transform(strings.ToUpper), 3)).
		//Chain(routines.Transform(strings.ToUpper)).
		Out(routines.File("data/output_parallel_10.txt").Write())
	//Chain(routines.Transform(strings.ToUpper), interpreter.WithDebounce(1*time.Second), interpreter.WithConcurrency(3)).
	//Chain(routines.Transform(strings.ToUpper)).
	//Out(routines.File("data/output.txt").Write())
	//ExecForEach(goscript.HTTP().Get("https://httpbun.com/get?param={{.}}"))

	ctx := context.Background()
	err := script.Run(ctx)
	if err != nil {
		panic(err)
	}
}
