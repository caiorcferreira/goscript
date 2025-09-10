package main

import (
	"context"
	"github.com/caiorcferreira/goscript"
	"github.com/caiorcferreira/goscript/internal/routines"
	"strings"
)

func main() {
	println("This is an example function.")

	script := goscript.New()

	script.
		In(routines.File("data/example.txt").Read()).
		Chain(routines.Transform(strings.ToUpper)).
		Out(routines.File("data/output.txt").Write())
	//ExecForEach(goscript.HTTP().Get("https://httpbun.com/get?param={{.}}"))

	ctx := context.Background()
	err := script.Run(ctx)
	if err != nil {
		panic(err)
	}
}
