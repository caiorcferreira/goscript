package main

import (
	"context"
	"github.com/caiorcferreira/goscript"
	"github.com/caiorcferreira/goscript/internal/routines"
	"strings"
	"time"
)

func main() {
	println("This is an example function.")

	script := goscript.New()

	script.
		In(routines.File("data/example.txt").Read()).
		Parallel(routines.Transform(strings.ToUpper), 3).
		Debounce(time.Millisecond * 100).
		Out(routines.File("data/output/parallel_15.txt").Write())

	//Chain(routines.Parallel(routines.Transform(strings.ToUpper), 3)).
	//Chain(routines.Debounce(5 * time.Second)).
	//ExecForEach(goscript.HTTP().Get("https://httpbun.com/get?param={{.}}"))

	ctx := context.Background()
	err := script.Run(ctx)
	if err != nil {
		panic(err)
	}
}
