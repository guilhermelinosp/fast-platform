package orders

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
	"uuid"

	"github.com/guilhermelinosp/fast-platform/platform"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Repository is the orders persistence port.
type Repository interface {
	Requested(context.Context, OrderRequestedInput) (Order, error)
	Get(context.Context, string) (OrderView, bool, error)
}

// viewCache is the context-first cache port used for order reads;
// *cache.HybridCache satisfies it.
type viewCache interface {
	GetOrSetContext(ctx context.Context, key string, out any, factory func(context.Context) (any, error), ttl time.Duration) error
}

// viewTTL is how long an order read may be stale. The status changes when a
// driver accepts, so it is kept short.
const viewTTL = 10 * time.Second

// Service implements rider use cases.
type Service struct {
	tel        *telemetry.Telemetry
	repository Repository
	cache      viewCache
}

// NewService creates the rider service. cache may be nil, which reads straight
// from the repository.
func NewService(tel *telemetry.Telemetry, repository Repository, cache viewCache) *Service {
	return &Service{tel: tel, repository: repository, cache: cache}
}

// Get returns an order and its current status, read through the cache (L1
// memory, L2 Redis) with stampede protection. A missing order is not cached.
func (s *Service) Get(ctx context.Context, id string) (OrderView, error) {
	if _, err := uuid.Parse(id); err != nil {
		return OrderView{}, platform.ValidationError("order_id", "must be a UUID")
	}
	load := func(ctx context.Context) (OrderView, error) {
		view, found, err := s.repository.Get(ctx, id)
		if err != nil {
			return OrderView{}, err
		}
		if !found {
			return OrderView{}, platform.NewError(http.StatusNotFound, "ORDER_NOT_FOUND", "order not found")
		}
		return view, nil
	}
	if s.cache == nil {
		return load(ctx)
	}
	var out OrderView
	err := s.cache.GetOrSetContext(ctx, "orders:view:"+id, &out, func(ctx context.Context) (any, error) {
		view, err := load(ctx)
		if err != nil {
			return nil, err
		}
		return view, nil
	}, viewTTL)
	if err != nil {
		if platform.HasCode(err, "ORDER_NOT_FOUND") {
			return OrderView{}, platform.NewError(http.StatusNotFound, "ORDER_NOT_FOUND", "order not found")
		}
		return OrderView{}, err
	}
	return out, nil
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
		// WorkerContext only reports whether the *job* failed; keep the domain error (a 4xx the
		// work func swallowed so it is not counted as a failure) instead of overwriting it with nil.
		workErr := s.tel.WorkerContext(ctx, "orders.requested", work)
		if err == nil {
			err = workErr
		}
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
		if errors.As(err, &pgErr) && isDuplicateOrder(pgErr) {
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

// isDuplicateOrder reports whether a unique violation means "this order id already exists". The
// order is written by one CTE statement (orders + first status + outbox), so PostgreSQL reports
// whichever unique constraint it hits first: the orders primary key or the (order_id, sequence)
// key of the status history.
func isDuplicateOrder(e *pgconn.PgError) bool {
	if e.Code != "23505" {
		return false
	}
	switch e.ConstraintName {
	case "orders_pkey", "order_status_history_order_id_sequence_key":
		return true
	}
	return false
}
