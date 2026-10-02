package sockets

import (
	"context"
	"net/http"

	"github.com/guilhermelinosp/fast-platform/env"
	"github.com/guilhermelinosp/fast-platform/events"
	"github.com/guilhermelinosp/fast-platform/platform"
	"github.com/guilhermelinosp/hellnet-lib-kafka/kafka"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func requestedHandlerSpec() kafka.HandlerSpec {
	return kafka.HandlerSpec{Group: env.String("KAFKA_TOPIC_ORDER_REQUESTED", "br.com.hellnet.fast.order.requested.v1")}
}

func acceptedHandlerSpec() kafka.HandlerSpec {
	return kafka.HandlerSpec{Group: env.String("KAFKA_TOPIC_ORDER_ACCEPTED", "br.com.hellnet.fast.order.accepted.v1")}
}

// RequestedEmitter publishes requested riders to connected driver clients.
type RequestedEmitter interface {
	EmitRequested(context.Context, events.OrderRequested) error
}

// AcceptedEmitter publishes accepted riders to the subscribed rider client.
type AcceptedEmitter interface {
	EmitAccepted(context.Context, events.OrderAccepted) error
}

// NewOrderRequestConsumer consumes the order-requested topic and emits each event to the
// driver Socket.IO namespace.
func NewOrderRequestConsumer(ctx context.Context, ops *telemetry.Telemetry, emitter RequestedEmitter) (*kafka.Consumer[events.OrderRequested], error) {
	if emitter == nil {
		return nil, platform.NewError(http.StatusInternalServerError, "INTERNAL", "sockets: requested emitter is nil")
	}
	var handler kafka.HandlerFunc[events.OrderRequested] = func(ctx context.Context, event events.OrderRequested, kctx kafka.Ctx) error {
		// The Kafka library's process span already covers this handler; enrich it.
		trace.SpanFromContext(ctx).SetAttributes(attribute.String("order_id", event.OrderID), attribute.String("event_id", event.EventID))
		ops.Log(ctx).Info("kafka.consume.order_requested", "order_id", event.OrderID, "event_id", event.EventID, "event_type", kctx.Headers["event_type"])
		_ = ops.Metric(ctx).Counter("socket.kafka.consume.order_requested.total", 1)
		return emitter.EmitRequested(ctx, event)
	}
	consumer, err := kafka.NewConsumer[events.OrderRequested](ctx, ops)
	if err != nil {
		return nil, err
	}
	if err := consumer.Configure(handler, requestedHandlerSpec()); err != nil {
		return nil, err
	}
	return consumer, nil
}

// NewOrderAcceptedConsumer consumes the order-accepted topic and emits each event to
// its Socket.IO order room.
func NewOrderAcceptedConsumer(ctx context.Context, ops *telemetry.Telemetry, emitter AcceptedEmitter) (*kafka.Consumer[events.OrderAccepted], error) {
	if emitter == nil {
		return nil, platform.NewError(http.StatusInternalServerError, "INTERNAL", "sockets: accepted emitter is nil")
	}
	var handler kafka.HandlerFunc[events.OrderAccepted] = func(ctx context.Context, event events.OrderAccepted, kctx kafka.Ctx) error {
		// The Kafka library's process span already covers this handler; enrich it.
		trace.SpanFromContext(ctx).SetAttributes(attribute.String("order_id", event.OrderID), attribute.String("driver_id", event.DriverID), attribute.String("event_id", event.EventID))
		ops.Log(ctx).Info("kafka.consume.order_accepted", "order_id", event.OrderID, "driver_id", event.DriverID, "event_id", event.EventID, "event_type", kctx.Headers["event_type"])
		_ = ops.Metric(ctx).Counter("socket.kafka.consume.order_accepted.total", 1)
		return emitter.EmitAccepted(ctx, event)
	}
	consumer, err := kafka.NewConsumer[events.OrderAccepted](ctx, ops)
	if err != nil {
		return nil, err
	}
	if err := consumer.Configure(handler, acceptedHandlerSpec()); err != nil {
		return nil, err
	}
	return consumer, nil
}
