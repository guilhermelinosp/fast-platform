package orders

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/guilhermelinosp/fast-platform-modular/internal/platform"
	"github.com/jackc/pgx/v5/pgconn"
)

type failingRepository struct{ err error }

func (r failingRepository) Get(context.Context, string) (OrderView, bool, error) {
	return OrderView{}, false, r.err
}

func (r failingRepository) Requested(context.Context, OrderRequestedInput) (Order, error) {
	return Order{}, r.err
}

func requestedInput() OrderRequestedInput {
	return OrderRequestedInput{
		ID:      "0d0a3d4e-37a4-4b0e-9d8e-3d9a7f8d1c11",
		RiderID: "7b0a1d4e-37a4-4b0e-9d8e-3d9a7f8d1c22",
	}
}

func TestRequestedDuplicateIDIsConflict(t *testing.T) {
	dup := &pgconn.PgError{Code: "23505", ConstraintName: "orders_pkey"}
	_, err := NewService(nil, failingRepository{err: dup}, nil).Requested(context.Background(), requestedInput())

	var httpErr *platform.HTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != http.StatusConflict || httpErr.Code != "ORDER_ALREADY_EXISTS" {
		t.Fatalf("err = %v, want 409 ORDER_ALREADY_EXISTS", err)
	}
}

func TestRequestedOtherDatabaseErrorsPropagate(t *testing.T) {
	boom := errors.New("connection reset")
	_, err := NewService(nil, failingRepository{err: boom}, nil).Requested(context.Background(), requestedInput())
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the original error", err)
	}
	other := &pgconn.PgError{Code: "23505", ConstraintName: "some_other_key"}
	_, err = NewService(nil, failingRepository{err: other}, nil).Requested(context.Background(), requestedInput())
	if platform.IsClientError(err) {
		t.Fatalf("a different unique violation must not be reported as a client conflict: %v", err)
	}
}

// memoryCache is a minimal GetOrSet that stores JSON-free values by key, to
// observe how many times the factory (the database read) runs.
type memoryCache struct{ store map[string]OrderView }

func (c *memoryCache) GetOrSetContext(ctx context.Context, key string, out any, factory func(context.Context) (any, error), _ time.Duration) error {
	if v, ok := c.store[key]; ok {
		*(out.(*OrderView)) = v
		return nil
	}
	v, err := factory(ctx)
	if err != nil {
		// A real single-flight flattens the error to a string.
		return errors.New(err.Error())
	}
	c.store[key] = v.(OrderView)
	*(out.(*OrderView)) = c.store[key]
	return nil
}

type countingRepository struct {
	failingRepository
	view  OrderView
	found bool
	reads int
}

func (r *countingRepository) Get(context.Context, string) (OrderView, bool, error) {
	r.reads++
	return r.view, r.found, nil
}

const viewOrderID = "0d0a3d4e-37a4-4b0e-9d8e-3d9a7f8d1c11"

func TestGetReadsThroughTheCache(t *testing.T) {
	repo := &countingRepository{view: OrderView{ID: viewOrderID, Status: "requested"}, found: true}
	svc := NewService(nil, repo, &memoryCache{store: map[string]OrderView{}})

	for i := 0; i < 3; i++ {
		got, err := svc.Get(context.Background(), viewOrderID)
		if err != nil || got.Status != "requested" {
			t.Fatalf("Get #%d = %+v, %v", i, got, err)
		}
	}
	if repo.reads != 1 {
		t.Fatalf("repository reads = %d, want 1 (the other two are cache hits)", repo.reads)
	}
}

func TestGetMissingOrderIs404EvenWhenTheCacheFlattensTheError(t *testing.T) {
	repo := &countingRepository{found: false}
	svc := NewService(nil, repo, &memoryCache{store: map[string]OrderView{}})

	_, err := svc.Get(context.Background(), viewOrderID)
	var httpErr *platform.HTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != http.StatusNotFound || httpErr.Code != "ORDER_NOT_FOUND" {
		t.Fatalf("err = %v, want 404 ORDER_NOT_FOUND", err)
	}
}

func TestGetRejectsANonUUID(t *testing.T) {
	repo := &countingRepository{}
	_, err := NewService(nil, repo, nil).Get(context.Background(), "not-a-uuid")
	if !platform.IsClientError(err) || repo.reads != 0 {
		t.Fatalf("err = %v, reads = %d; want a validation error before any read", err, repo.reads)
	}
}

func TestGetWithoutCacheReadsTheRepository(t *testing.T) {
	repo := &countingRepository{view: OrderView{ID: viewOrderID}, found: true}
	svc := NewService(nil, repo, nil)
	for i := 0; i < 2; i++ {
		if _, err := svc.Get(context.Background(), viewOrderID); err != nil {
			t.Fatal(err)
		}
	}
	if repo.reads != 2 {
		t.Fatalf("repository reads = %d, want 2", repo.reads)
	}
}
