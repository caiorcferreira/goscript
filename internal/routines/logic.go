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
	//defer pipe.Close()
	defer func() {
		fmt.Printf("CLOSING TRANSFORM PIPE OUT\n")
		close(pipe.Out())
	}()

	for msg := range pipe.In() {
		fmt.Printf("transform: received message: %v\n", msg)

		data := msg

		// type assertion to T
		val, ok := data.(T)
		if !ok {
			// handle type assertion failure, skip or log error
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case pipe.Out() <- t.transform(val):
		}
	}

	return nil

	//for {
	//	select {
	//	case <-ctx.Done():
	//		return ctx.Err()
	//	case <-pipe.Done():
	//		fmt.Println("transform: pipe done")
	//		return nil
	//	case data, open := <-pipe.In():
	//		if !open {
	//			return nil
	//		}
	//
	//		// type assertion to T
	//		val, ok := data.(T)
	//		if !ok {
	//			// handle type assertion failure, skip or log error
	//			continue
	//		}
	//		pipe.Out() <- t.transform(val)
	//	}
	//}
}
