package goscript

import (
	"github.com/caiorcferreira/goscript/internal/interpreter"
	"github.com/caiorcferreira/goscript/internal/routines"
)

func New() *interpreter.Script {
	return interpreter.NewScript(
		routines.NewStdInRoutine(),
		routines.NewStdOutRoutine(),
	)
}
