package examples_test

import (
	"context"
	"strings"
	"time"

	"github.com/caiorcferreira/goscript"
	"github.com/caiorcferreira/goscript/internal/routines"
)

func ExampleScript_FileOut() {
	println("This is an example function.")

	script := goscript.New()

	script.
		FileIn("../data/example.txt").
		Parallel(routines.Transform(strings.ToUpper), 3).
		Debounce(time.Millisecond * 10).
		FileOut("../data/output/parallel_17.txt")

	//Chain(routines.Parallel(routines.Transform(strings.ToUpper), 3)).
	//Chain(routines.Debounce(5 * time.Second)).
	//ExecForEach(goscript.HTTP().Get("https://httpbun.com/get?param={{.}}"))

	ctx := context.Background()
	err := script.Run(ctx)
	if err != nil {
		panic(err)
	}

	// Output:
}
