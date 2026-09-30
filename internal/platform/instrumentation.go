package platform

import (
	"github.com/guilhermelinosp/hellnet-lib-telemetry/instrument"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
)

// Instrumentation returns ops as the Hellnet observability contract, or nil
// (treated as a no-op by the libraries) when ops does not implement it.
func Instrumentation(ops telemetry.Client) instrument.Instrumentation {
	inst, _ := ops.(instrument.Instrumentation)
	return inst
}
