package drivers

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
	"uuid"

	"github.com/guilhermelinosp/fast-platform/internal/orders"
	"github.com/guilhermelinosp/hellnet-lib-core/events"
	"github.com/guilhermelinosp/hellnet-lib-core/platform"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/jackc/pgx/v5/pgconn"
)

// Sentinel errors for service-level error handling

// orderReader reads an order and its current status, through the cache in the
// API; *orders.Service satisfies it.
type orderReader interface {
	Get(context.Context, string) (orders.OrderView, error)
}

// Service implements driver use cases.
type Service struct {
	tel        *telemetry.Telemetry
	repository interface {
		Accepted(context.Context, AcceptedInput) (Order, error)
	}
	orders orderReader
}

// NewService creates a driver service. reader may be nil, which skips the
// early check and relies only on the guard inside the SQL statement.
func NewService(tel *telemetry.Telemetry, repository interface {
	Accepted(context.Context, AcceptedInput) (Order, error)
}, reader orderReader) *Service {
	return &Service{tel: tel, repository: repository, orders: reader}
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
			if platform.IsClientError(err) {
				// Expected rejection: the caller gets the 4xx, the job did not fail.
				return nil
			}
			return err
		}
		// WorkerContext only reports whether the *job* failed; keep the domain error (a 4xx the
		// work func swallowed so it is not counted as a failure) instead of overwriting it with nil.
		workErr := s.tel.WorkerContext(ctx, "drivers.accepted", work)
		if err == nil {
			err = workErr
		}
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
	// A status only moves forward: once an order is no longer "requested" it can
	// never be accepted, so a (possibly cached) answer saying so is safe to act
	// on. A stale "requested", a missing order or a read failure fall through to
	// the guard inside the SQL statement, which stays the authority.
	if s.orders != nil {
		if view, err := s.orders.Get(ctx, input.OrderID); err == nil && view.Status != "requested" {
			status = "conflict"
			return OrderOutput{}, platform.NewError(http.StatusConflict, "ORDER_NOT_ACCEPTABLE", "order is not in the requested state")
		}
	}
	input.Payload, _ = json.Marshal(events.OrderAccepted{EventID: input.OutboxID, EventVersion: 1, OccurredAt: time.Now().UnixMilli(), OrderID: input.OrderID, DriverID: input.DriverID})
	input.Payload = platform.InjectTraceContext(ctx, input.Payload)
	input.EventType = (events.OrderAccepted{}).MessageType()
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
