package matching

import (
	"context"
	"net/http"

	"github.com/guilhermelinosp/fast-platform-modular/internal/orders"
	"github.com/guilhermelinosp/fast-platform-modular/internal/platform"
	"github.com/guilhermelinosp/hellnet-lib-environments/environments"
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
func NewConsumer(ctx context.Context, ops telemetry.Client, service MatchService) (*kafka.Consumer[orders.OrderRequested], error) {
	if service == nil {
		return nil, platform.NewError(http.StatusInternalServerError, "INTERNAL", "matching: service is nil")
	}
	handler := kafka.HandlerFunc[orders.OrderRequested](func(ctx context.Context, event orders.OrderRequested, _ kafka.Ctx) error {
		// Correlaciona o consume do Kafka com um span OTel (kafka.consume),
		// filho do ctx fornecido pelo consumidor.
		if ops == nil {
			return matchEvent(ctx, event, service)
		}
		return ops.Span(ctx, "kafka.consume.order_requested", func(ctx context.Context) error {
			trace.SpanFromContext(ctx).SetAttributes(attribute.String("order_id", event.OrderID))
			return matchEvent(ctx, event, service)
		})
	})
	consumer, err := kafka.NewConsumer[orders.OrderRequested](ctx, ops)
	if err != nil {
		return nil, err
	}
	if err := consumer.Configure(handler, kafka.HandlerSpec{Group: environments.GetString("KAFKA_MATCHING_CONSUMER_GROUP", "fast-matching")}); err != nil {
		return nil, err
	}
	return consumer, nil
}

func matchEvent(ctx context.Context, event orders.OrderRequested, service MatchService) error {
	_, err := service.Match(ctx, event.OrderID)
	switch {
	case err == nil:
		return nil
	case isErrNoDriverAvailable(err):
		return nil // Info log handled by service
	case isErrRideAlreadyMatched(err):
		return nil // Info log handled by service
	default:
		return err
	}
}

func isErrNoDriverAvailable(err error) bool {
	return err != nil && err.Error() == "matching: no driver available"
}

func isErrRideAlreadyMatched(err error) bool {
	return err != nil && err.Error() == "matching: ride already matched"
}
