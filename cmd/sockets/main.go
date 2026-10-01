package main

import (
	"context"
	"errors"
	"net/http"
	"os"

	"github.com/guilhermelinosp/fast-platform-modular/internal/platform"
	"github.com/guilhermelinosp/fast-platform-modular/internal/sockets"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
)

func main() {
	if err := run(); err != nil {
		platform.Fatal("fast-sockets", err)
		os.Exit(1)
	}
}

// run starts the Socket.IO gateway and its Kafka notification consumers.
func run() error {
	ctx, stop, err := platform.Context()
	if err != nil {
		return err
	}
	defer stop()

	cfg, err := platform.NewConfig()
	if err != nil {
		return err
	}
	ops, err := telemetry.New(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = ops.Close(ctx) }()

	socket := sockets.NewServer(ops)
	orderRequestConsumer, err := sockets.NewOrderRequestConsumer(ctx, ops, socket)
	if err != nil {
		return err
	}
	defer func() { _ = orderRequestConsumer.Close() }()
	orderAcceptedConsumer, err := sockets.NewOrderAcceptedConsumer(ctx, ops, socket)
	if err != nil {
		return err
	}
	defer func() { _ = orderAcceptedConsumer.Close() }()

	go platform.Consume(ctx, ops, "order-requested", orderRequestConsumer.RunContext)
	go platform.Consume(ctx, ops, "order-accepted", orderAcceptedConsumer.RunContext)

	mux := http.NewServeMux()
	mux.Handle("/socket.io/", socket.Handler())
	mux.Handle("/live", ops.Live())
	mux.Handle("/ready", ops.Ready())
	mux.Handle("/health", ops.Health())

	ops.Log(ctx).Info("fast-sockets started", "port", cfg.Port)
	err = platform.Run(ctx, cfg, platform.NewServer(cfg, ops, mux))
	if err != nil && !errors.Is(err, context.Canceled) {
		ops.Log(ctx).Error("runtime error", "error", err)
		return err
	}
	return nil
}
