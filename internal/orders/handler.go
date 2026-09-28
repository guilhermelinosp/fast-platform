package orders

import (
	"context"
	"net/http"

	"uuid"

	"github.com/gin-gonic/gin"
	"github.com/guilhermelinosp/fast-platform-modular/internal/platform"
)

// Handler exposes the rider HTTP routes.
type Handler struct {
	service interface {
		Requested(context.Context, OrderRequestedInput) (OrderOutput, error)
	}
}

type requestInput struct {
	ID                   string  `json:"id"`
	RiderID              string  `json:"rider_id"`
	PickupLatitude       float64 `json:"pickup_latitude"`
	PickupLongitude      float64 `json:"pickup_longitude"`
	DestinationLatitude  float64 `json:"destination_latitude"`
	DestinationLongitude float64 `json:"destination_longitude"`
}

// NewHandler creates a rider HTTP handler.
func NewHandler(service interface {
	Requested(context.Context, OrderRequestedInput) (OrderOutput, error)
}) *Handler {
	return &Handler{service: service}
}

// Register mounts the rider routes on the gin engine.
func (h *Handler) Register(r *gin.RouterGroup) {
	r.POST("/orders", h.request)
}

// request handles POST /api/v1/orders
func (h *Handler) request(c *gin.Context) {
	var in requestInput
	if err := c.ShouldBindJSON(&in); err != nil {
		platform.AbortError(c, platform.ValidationError("body", err.Error()))
		return
	}
	if in.ID == "" {
		in.ID = uuid.New().String()
	}
	riderID := c.GetHeader("rider_id")
	if riderID == "" {
		riderID = in.RiderID
	}
	if riderID == "" {
		riderID = uuid.New().String()
	}
	if _, err := uuid.Parse(in.ID); err != nil {
		platform.AbortError(c, platform.ValidationError("id", "must be a UUID"))
		return
	}
	if _, err := uuid.Parse(riderID); err != nil {
		platform.AbortError(c, platform.ValidationError("rider_id", "must be a UUID"))
		return
	}
	input := OrderRequestedInput{
		ID:                   in.ID,
		RiderID:              riderID,
		PickupLatitude:       in.PickupLatitude,
		PickupLongitude:      in.PickupLongitude,
		DestinationLatitude:  in.DestinationLatitude,
		DestinationLongitude: in.DestinationLongitude,
	}
	order, err := h.service.Requested(c.Request.Context(), input)
	if err != nil {
		platform.AbortError(c, err)
		return
	}
	c.JSON(http.StatusCreated, order)
}
