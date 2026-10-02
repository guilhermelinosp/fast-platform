package listeners

import (
	"context"
	"encoding/json"

	"github.com/guilhermelinosp/fast-platform/events"
	"github.com/guilhermelinosp/fast-platform/platform"
	"github.com/guilhermelinosp/hellnet-lib-kafka/kafka"
)

// Producer handles Kafka publishing of outbox events.
type Producer struct {
	requested *kafka.Producer[events.OrderRequested]
	accepted  *kafka.Producer[events.OrderAccepted]
}

// NewProducer creates a producer that publishes outbox events to Kafka.
func NewProducer(requested *kafka.Producer[events.OrderRequested], accepted *kafka.Producer[events.OrderAccepted]) *Producer {
	return &Producer{
		requested: requested,
		accepted:  accepted,
	}
}

// Publish decodes and publishes a single outbox event to Kafka.
func (p *Producer) Publish(ctx context.Context, event Event) error {
	ctx = kafka.ContextWithHeaders(ctx, correlationHeaders(event))
	switch event.EventType {
	case (events.OrderRequested{}).MessageType():
		var message events.OrderRequested
		if err := json.Unmarshal(event.Payload, &message); err != nil {
			return platform.WrapError(platform.NewError(500, "OUTBOX_DECODE", "decode order requested event"), err)
		}
		return p.requested.PublishContext(ctx, message)
	case (events.OrderAccepted{}).MessageType():
		var message events.OrderAccepted
		if err := json.Unmarshal(event.Payload, &message); err != nil {
			return platform.WrapError(platform.NewError(500, "OUTBOX_DECODE", "decode order accepted event"), err)
		}
		return p.accepted.PublishContext(ctx, message)
	default:
		return platform.NewError(500, "UNSUPPORTED_EVENT", "unsupported outbox event type "+event.EventType)
	}
}

// correlationHeaders are the Kafka record headers that correlate a message with
// its outbox event and order; the trace context is added by the Kafka library.
func correlationHeaders(event Event) map[string]string {
	return map[string]string{
		"event_id":   event.ID,
		"event_type": event.EventType,
		"order_id":   event.AggregateID,
	}
}
