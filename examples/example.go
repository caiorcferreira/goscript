package main

import (
	"context"
	"github.com/caiorcferreira/goscript"
	"strings"
)

func main() {
	println("This is an example function.")

	script := goscript.New()

	script.
		In(goscript.File("data/example.txt").Read()).
		Chain(goscript.Transform(strings.ToUpper)).
		Out(goscript.File("data/output.txt").Write())
	//ExecForEach(goscript.HTTP().Get("https://httpbun.com/get?param={{.}}"))

	ctx := context.Background()
	err := script.Run(ctx)
	if err != nil {
		panic(err)
	}
}
