package platform

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"
)

func TestWarmupRunsWithTimeoutAndNeverFails(t *testing.T) {
	var deadlineSet, untraced bool
	Warmup(context.Background(), nil, "ok", func(ctx context.Context) error {
		_, deadlineSet = ctx.Deadline()
		sc := trace.SpanContextFromContext(ctx)
		untraced = sc.IsValid() && !sc.IsSampled()
		return nil
	})
	if !deadlineSet {
		t.Fatal("warm-up must be bounded by a timeout")
	}
	if !untraced {
		t.Fatal("warm-up must run under an unsampled span context (no root trace at startup)")
	}
	// A failing dependency must not panic or block startup.
	done := make(chan struct{})
	go func() {
		Warmup(context.Background(), nil, "bad", func(context.Context) error { return errors.New("down") })
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("warm-up blocked on a failing dependency")
	}
}

func TestConsumeRunsTheLoopUntilItReturns(t *testing.T) {
	ran := false
	Consume(context.Background(), nil, "test", func(context.Context) error {
		ran = true
		return nil
	})
	if !ran {
		t.Fatal("Consume must run the loop")
	}
}

func TestConsumeSurvivesAPanicAndAnError(t *testing.T) {
	Consume(context.Background(), nil, "panics", func(context.Context) error { panic("boom") })
	Consume(context.Background(), nil, "fails", func(context.Context) error { return errors.New("down") })
}

func TestConsumeStopsWithTheContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		Consume(ctx, nil, "loop", func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Consume did not return after the context was canceled")
	}
}
