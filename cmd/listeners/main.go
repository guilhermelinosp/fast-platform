package main

import (
	"context"
	"os"

	"github.com/guilhermelinosp/fast-platform-modular/internal/listeners"
	"github.com/guilhermelinosp/fast-platform-modular/internal/matching"
	"github.com/guilhermelinosp/fast-platform-modular/internal/orders"
	"github.com/guilhermelinosp/fast-platform-modular/internal/process"
	"github.com/guilhermelinosp/hellnet-lib-cache/cache"
	"github.com/guilhermelinosp/hellnet-lib-database/database"
	"github.com/guilhermelinosp/hellnet-lib-kafka/kafka"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
	"go.opentelemetry.io/otel/attribute"
)

func main() {
	if err := run(); err != nil {
		process.Fatal("fast-listeners", err)
		os.Exit(1)
	}
}

// run starts durable event publication and matching consumers. It does not
// open an HTTP listener; cmd/api and cmd/sockets own network servers.
func run() error {
	ctx, stop, err := process.Context()
	if err != nil {
		return err
	}
	defer stop()

	ops, err := telemetry.New()
	if err != nil {
		return err
	}
	defer func() { _ = ops.Close() }()

	db, err := database.New(ctx, ops)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	c, err := cache.New(ctx, ops)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()

	orderRequestedProducer, err := kafka.NewProducer[orders.OrderRequested](ctx, ops)
	if err != nil {
		return err
	}
	defer func() { _ = orderRequestedProducer.Close() }()
	orderAcceptedProducer, err := kafka.NewProducer[orders.OrderAccepted](ctx, ops)
	if err != nil {
		return err
	}
	defer func() { _ = orderAcceptedProducer.Close() }()

	producer := listeners.NewProducer(orderRequestedProducer, orderAcceptedProducer)
	listener, err := listeners.NewListener(ops, db, producer)
	if err != nil {
		return err
	}
	defer listener.Close()

	matchingConsumer, err := matching.NewConsumer(ctx, ops, matching.NewService(matching.NewRepository(db), c))
	if err != nil {
		return err
	}
	defer func() { _ = matchingConsumer.Close() }()

	go func() {
		_ = ops.Worker("matching.consume.order_requested", func(ctx context.Context) error {
			return matchingConsumer.RunContext(ctx)
		}, attribute.String("consumer", "matching"))
	}()

	ops.Info("fast-listeners started")
	<-ctx.Done()
	return nil
}
