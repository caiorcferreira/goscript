package routines

import (
	"context"
	"log/slog"
	"reflect"

	"github.com/caiorcferreira/goscript/internal/pipeline"
	"github.com/google/uuid"
)

type TransformRoutine[T, V any] struct {
	transform func(T) V
}

func Transform[T, V any](f func(T) V) *TransformRoutine[T, V] {
	return &TransformRoutine[T, V]{transform: f}
}

func (t *TransformRoutine[T, V]) Start(ctx context.Context, pipe pipeline.Pipe) error {
	defer pipe.Close()

	slog.Debug("starting transform routine")

	for msg := range pipe.In() {
		slog.Debug("transform received message", "msg", msg)

		// type assertion to T
		val, ok := msg.Data.(T)
		if !ok {
			//todo: log error
			pipe.Out() <- msg
			continue
		}

		transformedMsg := pipeline.Msg{
			ID:   msg.ID,
			Data: t.transform(val),
		}

		slog.Debug("transformed message", "msg", transformedMsg)

		select {
		case <-ctx.Done():
			return nil
		case pipe.Out() <- transformedMsg:
		}
	}

	return nil
}

type ReduceRoutine[T, V any] struct {
	reduce       func(V, T) V
	currentValue V
}

func Reduce[T, V any](f func(V, T) V, initialValue V) *ReduceRoutine[T, V] {
	return &ReduceRoutine[T, V]{reduce: f, currentValue: initialValue}
}

func (t *ReduceRoutine[T, V]) Start(ctx context.Context, pipe pipeline.Pipe) error {
	defer pipe.Close()

	slog.Debug("starting reduce routine")

	for msg := range pipe.In() {
		slog.Debug("reduce received message", "msg", msg)

		// type assertion to T
		val, ok := msg.Data.(T)
		if !ok {
			slog.Error("reduce received message with invalid type", "type", reflect.TypeOf(msg.Data))

			continue
		}

		t.currentValue = t.reduce(t.currentValue, val)

		slog.Debug("reduced message", "msg", msg, "currentValue", t.currentValue)
	}

	reducedMsg := pipeline.Msg{
		ID:   uuid.NewString(),
		Data: t.currentValue,
	}

	select {
	case pipe.Out() <- reducedMsg:
	case <-ctx.Done():
		return nil
	}

	return nil
}
