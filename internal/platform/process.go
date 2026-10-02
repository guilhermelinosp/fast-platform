package platform

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/guilhermelinosp/fast-platform/internal/env"
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
	// Warm-up traffic is not a request: keep it out of the traces. Only the call
	// runs untraced; the logs use the caller's context, because the untraced one
	// carries a made-up trace id that no backend has (a dead link in Grafana).
	callCtx, cancel := context.WithTimeout(instrument.WithoutTracing(ctx), warmupTimeout)
	defer cancel()
	started := time.Now()
	err := fn(callCtx)
	if ops == nil {
		return
	}
	if err != nil {
		ops.Log(ctx).Warn("warm-up failed; the dependency will be used cold", "dependency", name, "error", err)
		return
	}
	ops.Log(ctx).Info("warm-up completed", "dependency", name, "duration_ms", time.Since(started).Milliseconds())
}

// Consume runs a long-lived consumer loop (for example a Kafka Consumer's
// RunContext) until ctx is done. A loop is not a job: wrapping it in
// Telemetry.WorkerContext kept a root span open for the life of the process and
// recorded a single worker_job_duration sample of that length at shutdown. Each
// message is already traced by the Kafka process span; Consume only logs the
// start and the end and keeps a panic from taking the process down silently.
func Consume(ctx context.Context, ops *telemetry.Telemetry, name string, run func(context.Context) error) {
	log := func(level, msg string, args ...any) {
		if ops == nil {
			return
		}
		l := ops.Log(ctx)
		args = append([]any{"consumer", name}, args...)
		if level == "error" {
			l.Error(msg, args...)
			return
		}
		l.Info(msg, args...)
	}
	defer func() {
		if r := recover(); r != nil {
			log("error", "consumer panicked", "panic", fmt.Sprint(r))
		}
	}()
	log("info", "consumer started")
	if err := run(ctx); err != nil && ctx.Err() == nil {
		log("error", "consumer stopped with an error", "error", err.Error())
		return
	}
	log("info", "consumer stopped")
}
