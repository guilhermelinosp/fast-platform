package listeners

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

type ctxKey struct{}

type fakeStore struct {
	pending   []Event
	queryCtx  context.Context
	recorded  []string
	recordCtx context.Context
}

func (s *fakeStore) QueryRow(context.Context, string) (Event, bool, error) {
	return Event{}, false, nil
}

func (s *fakeStore) QueryPending(ctx context.Context) ([]Event, error) {
	s.queryCtx = ctx
	return s.pending, nil
}

func (s *fakeStore) RecordPublished(ctx context.Context, id string) error {
	s.recordCtx = ctx
	s.recorded = append(s.recorded, id)
	return nil
}

func (s *fakeStore) RecordFailure(context.Context, string, string) error { return nil }

type fakePublisher struct{ ctx context.Context }

func (p *fakePublisher) Publish(ctx context.Context, _ Event) error {
	p.ctx = ctx
	return nil
}

func TestReconcilePropagatesContext(t *testing.T) {
	ctx := context.WithValue(context.Background(), ctxKey{}, "caller")
	store := &fakeStore{pending: []Event{{ID: "e1", EventType: "order.requested"}}}
	pub := &fakePublisher{}
	l := &Listener{store: store, publisher: pub, ctx: ctx, memo: newOutboxMemo()}

	l.reconcileOnce()

	for name, got := range map[string]context.Context{"query": store.queryCtx, "publish": pub.ctx, "record": store.recordCtx} {
		if got == nil || got.Value(ctxKey{}) != "caller" {
			t.Fatalf("%s did not receive the listener context", name)
		}
	}
	if sc := trace.SpanContextFromContext(store.queryCtx); !sc.IsValid() || sc.IsSampled() {
		t.Fatal("the polling query must run under an unsampled span context")
	}
	if trace.SpanContextFromContext(pub.ctx).IsValid() {
		t.Fatal("publishing real events must stay traceable, not suppressed")
	}
	if len(store.recorded) != 1 || store.recorded[0] != "e1" {
		t.Fatalf("recorded = %v, want [e1]", store.recorded)
	}
}
