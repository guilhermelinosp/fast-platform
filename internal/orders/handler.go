package orders

import (
	"context"
	"net/http"

	"uuid"

	"github.com/gin-gonic/gin"
	"github.com/guilhermelinosp/hellnet-lib-core/platform"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Handler exposes the rider HTTP routes.
type Handler struct {
	service interface {
		Requested(context.Context, OrderRequestedInput) (OrderOutput, error)
		Get(context.Context, string) (OrderView, error)
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
	Get(context.Context, string) (OrderView, error)
}) *Handler {
	return &Handler{service: service}
}

// Register mounts the rider routes on the gin engine.
func (h *Handler) Register(r *gin.RouterGroup) {
	r.POST("/orders", h.request)
	r.GET("/orders/:orderId", h.get)
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
	trace.SpanFromContext(c.Request.Context()).SetAttributes(attribute.String("order_id", in.ID), attribute.String("rider_id", riderID))
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

// get handles GET /api/v1/orders/:orderId. Like the writes, the caller is
// identified by the rider_id header, and an order is only visible to the rider
// who requested it: anyone else gets the same 404 as for a missing order, so the
// response does not reveal that an id exists.
func (h *Handler) get(c *gin.Context) {
	orderID := c.Param("orderId")
	riderID := c.GetHeader("rider_id")
	if _, err := uuid.Parse(riderID); err != nil {
		platform.AbortError(c, platform.ValidationError("rider_id", "must be a UUID"))
		return
	}
	trace.SpanFromContext(c.Request.Context()).SetAttributes(attribute.String("order_id", orderID), attribute.String("rider_id", riderID))
	view, err := h.service.Get(c.Request.Context(), orderID)
	if err != nil {
		platform.AbortError(c, err)
		return
	}
	if view.RiderID != riderID {
		platform.AbortError(c, platform.NewError(http.StatusNotFound, "ORDER_NOT_FOUND", "order not found"))
		return
	}
	c.JSON(http.StatusOK, view)
}
