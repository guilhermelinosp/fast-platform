package matching

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/guilhermelinosp/fast-platform/events"
	"github.com/guilhermelinosp/fast-platform/platform"
)

type noopService struct{}

func (noopService) Match(context.Context, string) (OfferOutput, error) { return OfferOutput{}, nil }

func TestNewConsumerRejectsNilService(t *testing.T) {
	if _, err := NewConsumer(context.Background(), nil, nil); err == nil {
		t.Fatal("NewConsumer must reject a nil service")
	}
}

// A nil *telemetry.Telemetry must not reach kafka.WithInstrumentation: a typed
// nil inside the interface would panic when the libraries resolve providers.
func TestNewConsumerWithNilTelemetryDoesNotPanic(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:19092")
	t.Setenv("KAFKA_SECURITY_PROTOCOL", "plaintext")
	consumer, err := NewConsumer(context.Background(), nil, noopService{})
	if err == nil {
		_ = consumer.Close()
	}
}

type errService struct{ err error }

func (s errService) Match(context.Context, string) (OfferOutput, error) { return OfferOutput{}, s.err }

func TestMatchEventAcknowledgesDomainOutcomes(t *testing.T) {
	noDriver := platform.NewError(503, codeNoDriverAvailable, "matching: no driver available")
	already := platform.NewError(409, codeRideAlreadyMatched, "matching: ride already matched")
	cases := map[string]error{
		"typed":     noDriver,
		"wrapped":   fmt.Errorf("cache: %w", noDriver),
		"flattened": errors.New(noDriver.Error()),
		"already":   already,
	}
	for name, err := range cases {
		if got := matchEvent(context.Background(), nil, events.OrderRequested{OrderID: "o"}, errService{err}); got != nil {
			t.Errorf("%s: matchEvent = %v, want nil (acknowledge, no Kafka retry)", name, got)
		}
	}
	boom := errors.New("database down")
	if got := matchEvent(context.Background(), nil, events.OrderRequested{OrderID: "o"}, errService{boom}); !errors.Is(got, boom) {
		t.Fatalf("unexpected errors must be returned for retry, got %v", got)
	}
	if got := matchEvent(context.Background(), nil, events.OrderRequested{OrderID: "o"}, noopService{}); got != nil {
		t.Fatalf("success must return nil, got %v", got)
	}
}
