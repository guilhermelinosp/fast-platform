package main

import (
	"context"
	"os"

	"github.com/guilhermelinosp/fast-platform/internal/listeners"
	"github.com/guilhermelinosp/fast-platform/internal/orders"
	"github.com/guilhermelinosp/fast-platform/internal/platform"
	"github.com/guilhermelinosp/hellnet-lib-cache/cache"
	"github.com/guilhermelinosp/hellnet-lib-database/database"
	"github.com/guilhermelinosp/hellnet-lib-kafka/kafka"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
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
	platform.Warmup(ctx, ops, "database", db.PingContext)

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
	platform.Warmup(ctx, ops, "kafka.order_requested", orderRequestedProducer.Ping)
	orderAcceptedProducer, err := kafka.NewProducer[orders.OrderAccepted](ctx, ops)
	if err != nil {
		return err
	}
	defer func() { _ = orderAcceptedProducer.Shutdown(context.WithoutCancel(ctx)) }()
	platform.Warmup(ctx, ops, "kafka.order_accepted", orderAcceptedProducer.Ping)

	producer := listeners.NewProducer(orderRequestedProducer, orderAcceptedProducer)
	listener, err := listeners.NewListener(ctx, ops, db, producer)
	if err != nil {
		return err
	}
	defer listener.Close()

	//matchingConsumer, err := matching.NewConsumer(ctx, ops, matching.NewService(matching.NewRepository(db), c))
	//if err != nil {
	//	return err
	//}
	//defer func() { _ = matchingConsumer.Close() }()

	//go platform.Consume(ctx, ops, "matching", matchingConsumer.RunContext)

	ops.Log(ctx).Info("fast-listeners started")
	<-ctx.Done()
	return nil
}
