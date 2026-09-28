package main

import (
	"context"
	"errors"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/guilhermelinosp/fast-platform-modular/internal/drivers"
	"github.com/guilhermelinosp/fast-platform-modular/internal/orders"
	"github.com/guilhermelinosp/fast-platform-modular/internal/platform"
	"github.com/guilhermelinosp/fast-platform-modular/internal/process"
	"github.com/guilhermelinosp/hellnet-lib-database/database"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
)

func main() {
	if err := run(); err != nil {
		process.Fatal("fast-platform", err)
		os.Exit(1)
	}
}

// run starts the HTTP API process. Background consumers and the outbox
// publisher are owned by cmd/listeners and the Socket.IO gateway by
// cmd/sockets.
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

	db, err := database.New(ctx, ops)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	router := platform.NewRouter(cfg, ops)
	riderService := orders.NewService(ops, orders.NewRepository(db))
	driverService := drivers.NewService(ops, drivers.NewRepository(db))
	v1 := router.Group("/api/v1")
	orders.NewHandler(riderService).Register(v1)
	drivers.NewHandler(driverService).Register(v1)

	router.GET("/live", ginHandler(ops.Live()))
	router.GET("/ready", ginHandler(ops.Ready()))
	router.GET("/health", ginHandler(ops.Health()))

	ops.Info("fast-platform started", "port", cfg.Port)
	err = platform.Run(ctx, cfg, platform.NewServer(cfg, ops, telemetry.Middleware(ops, router)))
	if err != nil && !errors.Is(err, context.Canceled) {
		ops.Error("runtime error", "error", err)
		return err
	}
	return nil
}

func ginHandler(h http.Handler) gin.HandlerFunc {
	return func(c *gin.Context) {
		h.ServeHTTP(c.Writer, c.Request)
	}
}
