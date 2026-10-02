package listeners

import (
	"context"
	"testing"
	"time"

	"github.com/guilhermelinosp/fast-platform/platform"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type ctxKey struct{}

type fakeStore struct {
	byID      map[string]Event
	rowCtx    context.Context
	pending   []Event
	queryCtx  context.Context
	sinceArgs []time.Time
	recorded  []string
	recordCtx context.Context
}

func (s *fakeStore) QueryRow(ctx context.Context, id string) (Event, bool, error) {
	s.rowCtx = ctx
	event, ok := s.byID[id]
	return event, ok, nil
}

func (s *fakeStore) QueryPending(ctx context.Context, since time.Time) ([]Event, error) {
	s.queryCtx = ctx
	s.sinceArgs = append(s.sinceArgs, since)
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

func TestPublishContinuesTraceStoredInPayload(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()
	reqCtx, reqSpan := tp.Tracer("api").Start(context.Background(), "request")
	defer reqSpan.End()
	payload := platform.InjectTraceContext(reqCtx, []byte(`{"order_id":"o1"}`))

	store := &fakeStore{pending: []Event{{ID: "e1", EventType: "order.requested", Payload: payload}}}
	pub := &fakePublisher{}
	l := &Listener{store: store, publisher: pub, ctx: context.Background(), memo: newOutboxMemo()}

	l.reconcileOnce()

	want := reqSpan.SpanContext().TraceID()
	for name, ctx := range map[string]context.Context{"publish": pub.ctx, "audit": store.recordCtx} {
		got := trace.SpanContextFromContext(ctx)
		if !got.IsValid() || got.TraceID() != want {
			t.Fatalf("%s trace id = %s, want the request trace %s (the audit insert must stay in the trace)", name, got.TraceID(), want)
		}
	}
}

func TestNotificationSelectIsNotTraced(t *testing.T) {
	store := &fakeStore{byID: map[string]Event{"e1": {ID: "e1", EventType: "order.requested"}}}
	l := &Listener{store: store, publisher: &fakePublisher{}, ctx: context.Background(), memo: newOutboxMemo()}

	l.onNotification("e1")
	l.workers.Wait()

	if sc := trace.SpanContextFromContext(store.rowCtx); !sc.IsValid() || sc.IsSampled() {
		t.Fatal("the event SELECT must run under an unsampled span context (no orphan trace per event)")
	}
	if len(store.recorded) != 1 || store.recorded[0] != "e1" {
		t.Fatalf("recorded = %v, want [e1]", store.recorded)
	}
}

func TestCorrelationHeaders(t *testing.T) {
	got := correlationHeaders(Event{ID: "e1", AggregateID: "o1", EventType: "order.requested"})
	want := map[string]string{"event_id": "e1", "order_id": "o1", "event_type": "order.requested"}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("header %s = %q, want %q", k, got[k], v)
		}
	}
}

func TestReconcileScansEverythingOnceThenOnlyRecentEvents(t *testing.T) {
	store := &fakeStore{}
	l := &Listener{store: store, publisher: &fakePublisher{}, ctx: context.Background(), memo: newOutboxMemo()}

	l.reconcileOnce() // startup: full scan
	l.reconcileOnce() // next ticks: bounded by the lookback window
	l.reconcileOnce()
	if len(store.sinceArgs) != 3 {
		t.Fatalf("queries = %d, want 3", len(store.sinceArgs))
	}
	if !store.sinceArgs[0].IsZero() {
		t.Fatalf("first reconcile must be a full scan, since = %v", store.sinceArgs[0])
	}
	for i, since := range store.sinceArgs[1:] {
		if age := time.Since(since); since.IsZero() || age < outboxLookback-time.Minute || age > outboxLookback+time.Minute {
			t.Fatalf("tick %d since = %v (age %v), want about the %v lookback", i+1, since, age, outboxLookback)
		}
	}

	l.lastFullScan = time.Now().Add(-outboxFullScanEvery - time.Minute)
	l.reconcileOnce() // periodic full scan picks up anything older than the window
	if !store.sinceArgs[3].IsZero() {
		t.Fatal("the periodic full scan must use a zero since")
	}
}
