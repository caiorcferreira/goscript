package routines

import (
	"context"
	"fmt"
	"github.com/caiorcferreira/goscript/internal/interpreter"
)

type TransformRoutine[T, V any] struct {
	transform func(T) V
}

func Transform[T, V any](f func(T) V) *TransformRoutine[T, V] {
	return &TransformRoutine[T, V]{transform: f}
}

func (t *TransformRoutine[T, V]) Run(ctx context.Context, pipe interpreter.Pipe) error {
	defer pipe.Close()

	for msg := range pipe.In() {
		fmt.Printf("transform: received message: %v\n", msg)

		// type assertion to T
		val, ok := msg.Data.(T)
		if !ok {
			//todo: log error
			pipe.Out() <- msg
			continue
		}

		transformedMsg := interpreter.Msg{
			ID:   msg.ID,
			Data: t.transform(val),
		}

		select {
		case <-ctx.Done():
			return nil
		case pipe.Out() <- transformedMsg:
		}
	}

	return nil
}
