package orders

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
	"uuid"

	"github.com/guilhermelinosp/fast-platform-modular/internal/platform"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Service implements rider use cases.
type Service struct {
	tel        *telemetry.Telemetry
	repository interface {
		Requested(context.Context, OrderRequestedInput) (Order, error)
	}
}

// NewService creates the rider service.
func NewService(tel *telemetry.Telemetry, repository interface {
	Requested(context.Context, OrderRequestedInput) (Order, error)
}) *Service {
	return &Service{tel: tel, repository: repository}
}

// Requested handles the ride request use case.
func (s *Service) Requested(ctx context.Context, input OrderRequestedInput) (OrderOutput, error) {
	var result OrderOutput
	var err error

	if s.tel != nil {
		work := func(ctx context.Context) error {
			// Identifiers go on the span only: WorkerContext attributes become metric
			// labels, and order/rider ids would create one series per request.
			trace.SpanFromContext(ctx).SetAttributes(attribute.String("order_id", input.ID), attribute.String("rider_id", input.RiderID))
			result, err = s.doRequested(ctx, input)
			if platform.IsClientError(err) {
				// Expected rejection: the caller gets the 4xx, the job did not fail.
				return nil
			}
			return err
		}
		err = s.tel.WorkerContext(ctx, "orders.requested", work)
	} else {
		result, err = s.doRequested(ctx, input)
	}

	return result, err
}

func (s *Service) doRequested(ctx context.Context, input OrderRequestedInput) (OrderOutput, error) {
	start := time.Now()
	status := "success"
	defer func() {
		if s.tel != nil {
			m := s.tel.Metric(ctx)
			_ = m.Counter("orders.requested.total", 1, attribute.String("status", status))
			_ = m.Histogram("orders.requested.duration_seconds", time.Since(start).Seconds(), attribute.String("status", status))
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
	input.Payload = platform.InjectTraceContext(ctx, input.Payload)
	input.EventType = (OrderRequested{}).MessageType()

	order, err := s.repository.Requested(ctx, input)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "orders_pkey" {
			// Same id sent twice: the order already exists, which is the caller's
			// conflict and not a failure of the service.
			status = "conflict"
			return OrderOutput{}, platform.NewError(http.StatusConflict, "ORDER_ALREADY_EXISTS", "an order with this id already exists")
		}
		status = "error"
		return OrderOutput{}, err
	}

	if s.tel != nil {
		s.tel.Log(ctx).Info("order requested", "order_id", input.ID, "rider_id", input.RiderID)
	}
	return OrderOutput(order), nil
}
