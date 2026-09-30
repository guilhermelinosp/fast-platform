package matching

import (
	"context"
	"testing"
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
