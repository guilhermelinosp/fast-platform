package drivers

import (
	"context"
	"net/http"

	"uuid"

	"github.com/gin-gonic/gin"
	"github.com/guilhermelinosp/fast-platform-modular/internal/platform"
)

// Handler exposes driver HTTP routes.
type Handler struct {
	service interface {
		Accepted(context.Context, AcceptedInput) (OrderOutput, error)
	}
}

// NewHandler creates a driver HTTP handler.
func NewHandler(service interface {
	Accepted(context.Context, AcceptedInput) (OrderOutput, error)
}) *Handler {
	return &Handler{service: service}
}

// Register mounts the driver routes on the gin engine.
func (h *Handler) Register(r *gin.RouterGroup) {
	r.POST("/orders/:orderId/accept", h.accept)
}

// accept handles POST /api/v1/orders/:orderId/accept
func (h *Handler) accept(c *gin.Context) {
	driverID := c.GetHeader("driver_id")
	if _, err := uuid.Parse(driverID); err != nil {
		platform.AbortError(c, platform.ValidationError("driver_id", "must be a UUID"))
		return
	}

	order, err := h.service.Accepted(c.Request.Context(), AcceptedInput{
		OrderID:         c.Param("orderId"),
		DriverID:        driverID,
		AcceptanceID:    uuid.New().String(),
		StatusHistoryID: uuid.New().String(),
		OutboxID:        uuid.New().String(),
	})
	if err != nil {
		platform.AbortError(c, err)
		return
	}
	c.JSON(http.StatusCreated, order)
}
