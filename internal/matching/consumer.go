package matching

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

// MatchService matches an order to an available driver.
type MatchService interface {
	Match(context.Context, string) (OfferOutput, error)
}

// NewConsumer builds a Kafka consumer that matches incoming ride requests.
func NewConsumer(ctx context.Context, ops *telemetry.Telemetry, service MatchService) (*kafka.Consumer[events.OrderRequested], error) {
	if service == nil {
		return nil, platform.NewError(http.StatusInternalServerError, "INTERNAL", "matching: service is nil")
	}
	handler := kafka.HandlerFunc[events.OrderRequested](func(ctx context.Context, event events.OrderRequested, _ kafka.Ctx) error {
		// Correlaciona o consume do Kafka com um span OTel (kafka.consume),
		// filho do ctx fornecido pelo consumidor.
		if ops == nil {
			return matchEvent(ctx, ops, event, service)
		}
		return ops.Trace(ctx).Span("kafka.consume.order_requested", func(ctx context.Context) error {
			trace.SpanFromContext(ctx).SetAttributes(attribute.String("order_id", event.OrderID))
			ops.Log(ctx).Info("kafka.consume.order_requested", "order_id", event.OrderID, "event_id", event.EventID)
			return matchEvent(ctx, ops, event, service)
		})
	})
	consumer, err := kafka.NewConsumer[events.OrderRequested](ctx, ops)
	if err != nil {
		return nil, err
	}
	if err := consumer.Configure(handler, kafka.HandlerSpec{Group: env.String("KAFKA_MATCHING_CONSUMER_GROUP", "fast-matching")}); err != nil {
		return nil, err
	}
	return consumer, nil
}

func matchEvent(ctx context.Context, ops *telemetry.Telemetry, event events.OrderRequested, service MatchService) error {
	_, err := service.Match(ctx, event.OrderID)
	switch {
	case err == nil:
		logOutcome(ctx, ops, "ride matched", event)
		return nil
	case hasCode(err, codeNoDriverAvailable):
		// Acknowledge: retrying the message cannot produce a driver by itself.
		logOutcome(ctx, ops, "no driver available; event acknowledged", event)
		return nil
	case hasCode(err, codeRideAlreadyMatched):
		logOutcome(ctx, ops, "ride already matched; event acknowledged", event)
		return nil
	default:
		return err
	}
}

func logOutcome(ctx context.Context, ops *telemetry.Telemetry, msg string, event events.OrderRequested) {
	if ops != nil {
		ops.Log(ctx).Info(msg, "order_id", event.OrderID, "event_id", event.EventID)
	}
}

const (
	codeNoDriverAvailable  = "NO_DRIVER_AVAILABLE"
	codeRideAlreadyMatched = "RIDE_ALREADY_MATCHED"
)

// hasCode reports whether err carries the platform error code.
func hasCode(err error, code string) bool { return platform.HasCode(err, code) }
