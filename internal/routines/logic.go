package routines

import (
	"context"
	"github.com/caiorcferreira/goscript/internal/interpreter"
)

type TransformRoutine[T, V any] struct {
	transform func(T) V
	pipe      interpreter.Pipe
}

func Transform[T, V any](f func(T) V) *TransformRoutine[T, V] {
	return &TransformRoutine[T, V]{transform: f}
}

func (t *TransformRoutine[T, V]) Pipe(pipe interpreter.Pipe) {
	t.pipe = pipe
}

func (t *TransformRoutine[T, V]) Run(ctx context.Context) error {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case data := <-t.pipe.In():
				// type assertion to T
				val, ok := data.(T)
				if !ok {
					// handle type assertion failure, skip or log error
					continue
				}
				t.pipe.Out() <- t.transform(val)
			}
		}
	}()

	return nil
}
