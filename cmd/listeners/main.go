package main

import (
	"context"
	"os"

	"github.com/guilhermelinosp/fast-platform-modular/internal/listeners"
	"github.com/guilhermelinosp/fast-platform-modular/internal/matching"
	"github.com/guilhermelinosp/fast-platform-modular/internal/orders"
	"github.com/guilhermelinosp/fast-platform-modular/internal/platform"
	"github.com/guilhermelinosp/hellnet-lib-cache/cache"
	"github.com/guilhermelinosp/hellnet-lib-database/database"
	"github.com/guilhermelinosp/hellnet-lib-kafka/kafka"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/instrument"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
	"go.opentelemetry.io/otel/attribute"
)

func main() {
	if err := run(); err != nil {
		platform.Fatal("fast-listeners", err)
		os.Exit(1)
	}
}

// run starts durable event publication and matching consumers. It does not
// open an HTTP listener; cmd/api and cmd/sockets own network servers.
func run() error {
	ctx, stop, err := platform.Context()
	if err != nil {
		return err
	}
	defer stop()

	ops, err := telemetry.New(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = ops.Close(ctx) }()

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
	defer func() { _ = orderRequestedProducer.Shutdown(context.WithoutCancel(ctx)) }()
	orderAcceptedProducer, err := kafka.NewProducer[orders.OrderAccepted](ctx, ops)
	if err != nil {
		return err
	}
	defer func() { _ = orderAcceptedProducer.Shutdown(context.WithoutCancel(ctx)) }()

	producer := listeners.NewProducer(orderRequestedProducer, orderAcceptedProducer)
	listener, err := listeners.NewListener(ctx, ops, db, producer)
	if err != nil {
		return err
	}
	defer listener.Close()

	matchingConsumer, err := matching.NewConsumer(ctx, ops, matching.NewService(matching.NewRepository(db), c))
	if err != nil {
		return err
	}
	defer func() { _ = matchingConsumer.Close() }()

	// The consumer loop lives as long as the process: tracing it as one job would keep a
	// root span open for minutes. Each message is traced by the Kafka process span.
	go func() {
		_ = ops.WorkerContext(instrument.WithoutTracing(ctx), "matching.consume.order_requested", func(ctx context.Context) error {
			return matchingConsumer.RunContext(ctx)
		}, attribute.String("consumer", "matching"))
	}()

	ops.Log(ctx).Info("fast-listeners started")
	<-ctx.Done()
	return nil
}
