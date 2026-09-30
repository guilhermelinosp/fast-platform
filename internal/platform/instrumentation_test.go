package platform

import (
	"testing"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/instrument"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
)

type instrumentedClient struct {
	telemetry.Client
	instrument.Instrumentation
}

func TestInstrumentation(t *testing.T) {
	if got := Instrumentation(nil); got != nil {
		t.Fatalf("Instrumentation(nil) = %v, want nil", got)
	}
	if got := Instrumentation(telemetry.Client(nil)); got != nil {
		t.Fatalf("Instrumentation(nil client) = %v, want nil", got)
	}
	if got := Instrumentation(instrumentedClient{Instrumentation: instrument.Noop()}); got == nil {
		t.Fatal("Instrumentation did not return the client as the contract")
	}
}
