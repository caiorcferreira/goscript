package routines

import (
	"context"
	"github.com/caiorcferreira/goscript/internal/interpreter"
)

type TransformRoutine[T, V any] struct {
	transform func(T) V
}

func Transform[T, V any](f func(T) V) *TransformRoutine[T, V] {
	return &TransformRoutine[T, V]{transform: f}
}

func (t *TransformRoutine[T, V]) Run(ctx context.Context, pipe interpreter.Pipe) error {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case data := <-pipe.In():
				// type assertion to T
				val, ok := data.(T)
				if !ok {
					// handle type assertion failure, skip or log error
					continue
				}
				pipe.Out() <- t.transform(val)
			}
		}
	}()

	return nil
}
