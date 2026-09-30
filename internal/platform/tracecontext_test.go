package platform

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/instrument"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestTraceContextRoundTripsThroughPayload(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()
	ctx, span := tp.Tracer("test").Start(context.Background(), "request")
	defer span.End()

	payload := InjectTraceContext(ctx, []byte(`{"order_id":"o1"}`))

	var event struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(payload, &event); err != nil || event.OrderID != "o1" {
		t.Fatalf("event fields must survive injection: %v %+v", err, event)
	}
	got := trace.SpanContextFromContext(ExtractTraceContext(context.Background(), payload))
	want := span.SpanContext()
	if !got.IsValid() || got.TraceID() != want.TraceID() || got.SpanID() != want.SpanID() || !got.IsRemote() {
		t.Fatalf("extracted span context = %+v, want remote copy of %+v", got, want)
	}
}

func TestTraceContextIsNoopWithoutSpanOrJSONObject(t *testing.T) {
	plain := []byte(`{"order_id":"o1"}`)
	if got := InjectTraceContext(context.Background(), plain); string(got) != string(plain) {
		t.Fatalf("no span: payload changed to %s", got)
	}
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()
	ctx, span := tp.Tracer("test").Start(context.Background(), "request")
	defer span.End()
	for _, bad := range []string{`not json`, `[1,2]`, `null`} {
		if got := InjectTraceContext(ctx, []byte(bad)); string(got) != bad {
			t.Fatalf("payload %q must be left unchanged, got %s", bad, got)
		}
	}
	if got := ExtractTraceContext(context.Background(), []byte(`{"order_id":"o1"}`)); trace.SpanContextFromContext(got).IsValid() {
		t.Fatal("payload without trace_context must not yield a span context")
	}
}

// Consumer loops run under instrument.WithoutTracing so the loop itself is not
// traced; each message must still be traced once the Kafka library restores the
// producer's sampled context from the record headers.
func TestSampledRemoteParentOverridesUntracedLoop(t *testing.T) {
	tp := sdktrace.NewTracerProvider() // default sampler: ParentBased(AlwaysSample)
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer("test")

	producerCtx, producer := tracer.Start(context.Background(), "send")
	defer producer.End()
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(producerCtx, carrier)

	loop := instrument.WithoutTracing(context.Background())
	if _, span := tracer.Start(loop, "loop"); span.IsRecording() {
		t.Fatal("the loop itself must not be recorded")
	}
	restored := propagation.TraceContext{}.Extract(loop, carrier)
	_, process := tracer.Start(restored, "process")
	defer process.End()
	if !process.IsRecording() || process.SpanContext().TraceID() != producer.SpanContext().TraceID() {
		t.Fatal("a message with a sampled traceparent must be traced under the producer's trace")
	}
}
