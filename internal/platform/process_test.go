package platform

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWarmupRunsWithTimeoutAndNeverFails(t *testing.T) {
	var deadlineSet bool
	Warmup(context.Background(), nil, "ok", func(ctx context.Context) error {
		_, deadlineSet = ctx.Deadline()
		return nil
	})
	if !deadlineSet {
		t.Fatal("warm-up must be bounded by a timeout")
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
