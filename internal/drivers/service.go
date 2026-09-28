package drivers

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
	"uuid"

	"github.com/guilhermelinosp/fast-platform-modular/internal/orders"
	"github.com/guilhermelinosp/fast-platform-modular/internal/platform"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/jackc/pgx/v5/pgconn"
)

// Sentinel errors for service-level error handling

// Service implements driver use cases.
type Service struct {
	tel        telemetry.Client
	repository interface {
		Accepted(context.Context, AcceptedInput) (Order, error)
	}

	// Metrics
	acceptedTotal    metric.Int64Counter
	acceptedDuration metric.Int64Histogram
}

// NewService creates a driver service.
func NewService(tel telemetry.Client, repository interface {
	Accepted(context.Context, AcceptedInput) (Order, error)
}) *Service {
	if tel == nil {
		tel, _ = telemetry.New()
	}
	s := &Service{tel: tel, repository: repository}

	// Initialize metrics
	if tel != nil && tel.Metric() != nil {
		s.acceptedTotal, _ = tel.Metric().Counter("drivers.accepted.total")
		s.acceptedDuration, _ = tel.Metric().Histogram("drivers.accepted.duration_seconds")
	}

	return s
}

// Accepted handles the order acceptance use case.
func (s *Service) Accepted(ctx context.Context, input AcceptedInput) (OrderOutput, error) {
	var result OrderOutput
	var err error

	if s.tel != nil {
		err = s.tel.Worker("drivers.accepted", func(ctx context.Context) error {
			result, err = s.doAccepted(ctx, input)
			return err
		}, attribute.String("driver_id", input.DriverID), attribute.String("order_id", input.OrderID))
	} else {
		result, err = s.doAccepted(ctx, input)
	}

	return result, err
}

func (s *Service) doAccepted(ctx context.Context, input AcceptedInput) (OrderOutput, error) {
	start := time.Now()
	status := "success"
	defer func() {
		if s.acceptedTotal != nil {
			s.acceptedTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("status", status)))
		}
		if s.acceptedDuration != nil {
			s.acceptedDuration.Record(ctx, time.Since(start).Milliseconds(), metric.WithAttributes(attribute.String("status", status)))
		}
	}()

	if _, err := uuid.Parse(input.OrderID); err != nil {
		status = "validation_error"
		return OrderOutput{}, platform.ValidationError("order_id", "must be a UUID")
	}
	if _, err := uuid.Parse(input.DriverID); err != nil {
		status = "validation_error"
		return OrderOutput{}, platform.ValidationError("driver_id", "must be a UUID")
	}
	input.Payload, _ = json.Marshal(orders.OrderAccepted{EventID: input.OutboxID, EventVersion: 1, OccurredAt: time.Now().UnixMilli(), OrderID: input.OrderID, DriverID: input.DriverID})
	input.EventType = (orders.OrderAccepted{}).MessageType()
	var order Order
	err := s.tel.Span(ctx, "db.drivers.accepted", func(ctx context.Context) error {
		var dbErr error
		order, dbErr = s.repository.Accepted(ctx, input)
		return dbErr
	})
	if err != nil {
		status = "error"
		// Check if error is from our errors package by type assertion
		if err != nil {
			if e, ok := err.(*platform.HTTPError); ok {
				if e.Code == "ORDER_NOT_ACCEPTABLE" {
					return OrderOutput{}, platform.NewError(http.StatusConflict, "ORDER_NOT_ACCEPTABLE", "order is not in the requested state")
				}
			}
		}
		pgErr, isPgError := err.(*pgconn.PgError)
		if isPgError && pgErr.Code == "23505" && pgErr.ConstraintName == "uq_order_acceptances_order" {
			return OrderOutput{}, platform.NewError(http.StatusConflict, "ORDER_ALREADY_ACCEPTED", "order has already been accepted")
		}
		return OrderOutput{}, err
	}

	s.tel.Info("order accepted", "order_id", input.OrderID, "driver_id", input.DriverID)
	return OrderOutput(order), nil
}
