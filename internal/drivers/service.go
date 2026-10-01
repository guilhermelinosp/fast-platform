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
	"go.opentelemetry.io/otel/trace"

	"github.com/jackc/pgx/v5/pgconn"
)

// Sentinel errors for service-level error handling

// Service implements driver use cases.
type Service struct {
	tel        *telemetry.Telemetry
	repository interface {
		Accepted(context.Context, AcceptedInput) (Order, error)
	}
}

// NewService creates a driver service.
func NewService(tel *telemetry.Telemetry, repository interface {
	Accepted(context.Context, AcceptedInput) (Order, error)
}) *Service {
	return &Service{tel: tel, repository: repository}
}

// Accepted handles the order acceptance use case.
func (s *Service) Accepted(ctx context.Context, input AcceptedInput) (OrderOutput, error) {
	var result OrderOutput
	var err error

	if s.tel != nil {
		work := func(ctx context.Context) error {
			// Identifiers go on the span only: WorkerContext attributes become metric
			// labels, and order/driver ids would create one series per request.
			trace.SpanFromContext(ctx).SetAttributes(attribute.String("driver_id", input.DriverID), attribute.String("order_id", input.OrderID))
			result, err = s.doAccepted(ctx, input)
			return err
		}
		err = s.tel.WorkerContext(ctx, "drivers.accepted", work)
	} else {
		result, err = s.doAccepted(ctx, input)
	}

	return result, err
}

func (s *Service) doAccepted(ctx context.Context, input AcceptedInput) (OrderOutput, error) {
	start := time.Now()
	status := "success"
	defer func() {
		if s.tel != nil {
			m := s.tel.Metric(ctx)
			_ = m.Counter("drivers.accepted.total", 1, attribute.String("status", status))
			_ = m.Histogram("drivers.accepted.duration_seconds", time.Since(start).Seconds(), attribute.String("status", status))
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
	input.Payload = platform.InjectTraceContext(ctx, input.Payload)
	input.EventType = (orders.OrderAccepted{}).MessageType()
	order, err := s.repository.Accepted(ctx, input)
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

	if s.tel != nil {
		s.tel.Log(ctx).Info("order accepted", "order_id", input.OrderID, "driver_id", input.DriverID)
	}
	return OrderOutput(order), nil
}
