package main

import (
	"context"
	"errors"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/guilhermelinosp/fast-platform/internal/drivers"
	"github.com/guilhermelinosp/fast-platform/internal/orders"
	"github.com/guilhermelinosp/hellnet-lib-cache/cache"
	"github.com/guilhermelinosp/hellnet-lib-core/platform"
	"github.com/guilhermelinosp/hellnet-lib-database/database"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
)

func main() {
	if err := run(); err != nil {
		platform.Fatal("fast-platform", err)
		os.Exit(1)
	}
}

// run starts the HTTP API process. Background consumers and the outbox
// publisher are owned by the fast-listeners repository and the Socket.IO gateway by
// the fast-sockets repository.
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

	db, err := database.New(ctx, ops)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	platform.Warmup(ctx, ops, "database", db.PingContext)

	orderCache, err := cache.New(ctx, ops)
	if err != nil {
		return err
	}
	defer func() { _ = orderCache.Close() }()

	router := platform.NewRouter(cfg, ops)
	riderService := orders.NewService(ops, orders.NewRepository(db), orderCache)
	driverService := drivers.NewService(ops, drivers.NewRepository(db), riderService)
	v1 := router.Group("/api/v1")
	orders.NewHandler(riderService).Register(v1)
	drivers.NewHandler(driverService).Register(v1)

	router.GET("/live", gin.WrapH(ops.Live()))
	router.GET("/ready", gin.WrapH(ops.Ready()))
	router.GET("/health", gin.WrapH(ops.Health()))

	ops.Log(ctx).Info("fast-platform started", "port", cfg.Port)
	err = platform.Run(ctx, cfg, platform.NewServer(cfg, ops, router))
	if err != nil && !errors.Is(err, context.Canceled) {
		ops.Log(ctx).Error("runtime error", "error", err)
		return err
	}
	return nil
}
