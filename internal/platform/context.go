package platform

import (
	"context"
	"encoding/json"

	"go.opentelemetry.io/otel/propagation"
)

// traceContextKey is the reserved payload key that carries the W3C trace
// context through the outbox. Decoding the payload into an event struct ignores
// it, so it is never forwarded to Kafka; the Kafka library propagates the
// context through message headers from the listener onwards.
const traceContextKey = "trace_context"

var traceContextPropagator = propagation.TraceContext{}

// InjectTraceContext stores the trace context of ctx in a JSON object payload.
// It returns payload unchanged when it is not a JSON object or ctx has no span.
func InjectTraceContext(ctx context.Context, payload []byte) []byte {
	carrier := propagation.MapCarrier{}
	traceContextPropagator.Inject(ctx, carrier)
	if len(carrier) == 0 {
		return payload
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil || fields == nil {
		return payload
	}
	encoded, err := json.Marshal(map[string]string(carrier))
	if err != nil {
		return payload
	}
	fields[traceContextKey] = encoded
	out, err := json.Marshal(fields)
	if err != nil {
		return payload
	}
	return out
}

// ExtractTraceContext returns ctx carrying the remote span context stored in
// payload by InjectTraceContext, or ctx itself when there is none.
func ExtractTraceContext(ctx context.Context, payload []byte) context.Context {
	var envelope struct {
		TraceContext map[string]string `json:"trace_context"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil || len(envelope.TraceContext) == 0 {
		return ctx
	}
	return traceContextPropagator.Extract(ctx, propagation.MapCarrier(envelope.TraceContext))
}
