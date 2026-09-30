package platform

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/guilhermelinosp/fast-platform-modular/internal/env"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/instrument"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
)

// Context creates the process context and loads the process-local environment.
// Applications call it once at startup; libraries only consume the resulting
// environment and never own process lifecycle.
func Context() (context.Context, context.CancelFunc, error) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	if err := env.Environment(); err != nil {
		stop()
		return nil, nil, fmt.Errorf("load environment: %w", err)
	}
	return ctx, stop, nil
}

// Fatal writes a process error using the same compact format for every binary.
func Fatal(name string, err error) {
	_, _ = fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
}

// warmupTimeout bounds each startup warm-up so a slow dependency cannot stall
// the process; the dependency is then simply used cold.
const warmupTimeout = 5 * time.Second

// Warmup opens a dependency connection (database pool, Kafka broker) before the
// first request, so connection, TLS and SASL setup does not land in the first
// trace. It is best effort: a failure is logged and never stops startup.
func Warmup(ctx context.Context, ops *telemetry.Telemetry, name string, fn func(context.Context) error) {
	// Warm-up traffic is not a request: keep it out of the traces.
	ctx, cancel := context.WithTimeout(instrument.WithoutTracing(ctx), warmupTimeout)
	defer cancel()
	started := time.Now()
	err := fn(ctx)
	if ops == nil {
		return
	}
	if err != nil {
		ops.Log(ctx).Warn("warm-up failed; the dependency will be used cold", "dependency", name, "error", err)
		return
	}
	ops.Log(ctx).Info("warm-up completed", "dependency", name, "duration_ms", time.Since(started).Milliseconds())
}
