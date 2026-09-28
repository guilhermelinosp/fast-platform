package process

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/guilhermelinosp/hellnet-lib-environments/environments"
)

// Context creates the process context and loads the process-local environment.
// Applications call it once at startup; libraries only consume the resulting
// environment and never own process lifecycle.
func Context() (context.Context, context.CancelFunc, error) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	if err := environments.LoadDotEnv(); err != nil {
		stop()
		return nil, nil, fmt.Errorf("load environment: %w", err)
	}
	return ctx, stop, nil
}

// Fatal writes a process error using the same compact format for every binary.
func Fatal(name string, err error) {
	_, _ = fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
}
