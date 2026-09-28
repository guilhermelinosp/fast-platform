package orders

import (
	"context"
	"encoding/json"
	"strings"
	"time"
	"uuid"

	"github.com/guilhermelinosp/fast-platform-modular/internal/platform"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Service implements rider use cases.
type Service struct {
	tel        telemetry.Client
	repository interface {
		Requested(context.Context, OrderRequestedInput) (Order, error)
	}

	// Metrics
	requestedTotal    metric.Int64Counter
	requestedDuration metric.Int64Histogram
}

// NewService creates the rider service.
func NewService(tel telemetry.Client, repository interface {
	Requested(context.Context, OrderRequestedInput) (Order, error)
}) *Service {
	if tel == nil {
		tel, _ = telemetry.New()
	}
	s := &Service{tel: tel, repository: repository}

	// Initialize metrics
	if tel != nil && tel.Metric() != nil {
		s.requestedTotal, _ = tel.Metric().Counter("orders.requested.total")
		s.requestedDuration, _ = tel.Metric().Histogram("orders.request.duration_seconds")
	}

	return s
}

// Requested handles the ride request use case.
func (s *Service) Requested(ctx context.Context, input OrderRequestedInput) (OrderOutput, error) {
	var result OrderOutput
	var err error

	if s.tel != nil {
		err = s.tel.Worker("orders.requested", func(ctx context.Context) error {
			result, err = s.doRequested(ctx, input)
			return err
		}, attribute.String("rider_id", input.RiderID))
	} else {
		result, err = s.doRequested(ctx, input)
	}

	return result, err
}

func (s *Service) doRequested(ctx context.Context, input OrderRequestedInput) (OrderOutput, error) {
	start := time.Now()
	status := "success"
	defer func() {
		if s.requestedTotal != nil {
			s.requestedTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("status", status)))
		}
		if s.requestedDuration != nil {
			s.requestedDuration.Record(ctx, time.Since(start).Milliseconds(), metric.WithAttributes(attribute.String("status", status)))
		}
	}()

	if strings.TrimSpace(input.ID) == "" || strings.TrimSpace(input.RiderID) == "" {
		status = "validation_error"
		return OrderOutput{}, platform.ValidationError("ride", "id and rider_id are required")
	}

	if input.StatusHistoryID == "" {
		input.StatusHistoryID = uuid.New().String()
	}

	if input.OutboxID == "" {
		input.OutboxID = uuid.New().String()
	}

	input.Payload, _ = json.Marshal(OrderRequested{
		EventID:              input.OutboxID,
		EventVersion:         1,
		OccurredAt:           time.Now().UnixMilli(),
		OrderID:              input.ID,
		RiderID:              input.RiderID,
		PickupLatitude:       input.PickupLatitude,
		PickupLongitude:      input.PickupLongitude,
		DestinationLatitude:  input.DestinationLatitude,
		DestinationLongitude: input.DestinationLongitude,
	})
	input.EventType = (OrderRequested{}).MessageType()

	var order Order
	err := s.tel.Span(ctx, "db.orders.requested", func(ctx context.Context) error {
		var dbErr error
		order, dbErr = s.repository.Requested(ctx, input)
		return dbErr
	})
	if err != nil {
		status = "error"
		return OrderOutput{}, err
	}

	s.tel.Info("order requested", "order_id", input.ID, "rider_id", input.RiderID)
	return OrderOutput(order), nil
}
