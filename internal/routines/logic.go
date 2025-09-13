package routines

import (
	"context"
	"github.com/caiorcferreira/goscript/internal/pipeline"
	"log/slog"
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
