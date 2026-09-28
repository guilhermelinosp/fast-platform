package main

import (
	"context"
	"errors"
	"net/http"
	"os"

	"github.com/guilhermelinosp/fast-platform-modular/internal/platform"
	"github.com/guilhermelinosp/fast-platform-modular/internal/process"
	"github.com/guilhermelinosp/fast-platform-modular/internal/sockets"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
	"go.opentelemetry.io/otel/attribute"
)

func main() {
	if err := run(); err != nil {
		process.Fatal("fast-sockets", err)
		os.Exit(1)
	}
}

// run starts the Socket.IO gateway and its Kafka notification consumers.
func run() error {
	ctx, stop, err := process.Context()
	if err != nil {
		return err
	}
	defer stop()

	cfg, err := platform.NewConfig()
	if err != nil {
		return err
	}
	ops, err := telemetry.New()
	if err != nil {
		return err
	}
	defer func() { _ = ops.Close() }()

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

	go func() {
		_ = ops.Worker("socket.consume.order_requested", func(ctx context.Context) error {
			return orderRequestConsumer.RunContext(ctx)
		}, attribute.String("consumer", "order-requested"))
	}()
	go func() {
		_ = ops.Worker("socket.consume.order_accepted", func(ctx context.Context) error {
			return orderAcceptedConsumer.RunContext(ctx)
		}, attribute.String("consumer", "order-accepted"))
	}()

	mux := http.NewServeMux()
	mux.Handle("/socket.io/", socket.Handler())
	mux.Handle("/live", ops.Live())
	mux.Handle("/ready", ops.Ready())
	mux.Handle("/health", ops.Health())

	ops.Info("fast-sockets started", "port", cfg.Port)
	err = platform.Run(ctx, cfg, platform.NewServer(cfg, ops, telemetry.Middleware(ops, mux)))
	if err != nil && !errors.Is(err, context.Canceled) {
		ops.Error("runtime error", "error", err)
		return err
	}
	return nil
}
