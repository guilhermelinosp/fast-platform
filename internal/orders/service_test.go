package orders

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/guilhermelinosp/fast-platform-modular/internal/platform"
	"github.com/jackc/pgx/v5/pgconn"
)

type failingRepository struct{ err error }

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
	_, err := NewService(nil, failingRepository{err: dup}).Requested(context.Background(), requestedInput())

	var httpErr *platform.HTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != http.StatusConflict || httpErr.Code != "ORDER_ALREADY_EXISTS" {
		t.Fatalf("err = %v, want 409 ORDER_ALREADY_EXISTS", err)
	}
}

func TestRequestedOtherDatabaseErrorsPropagate(t *testing.T) {
	boom := errors.New("connection reset")
	_, err := NewService(nil, failingRepository{err: boom}).Requested(context.Background(), requestedInput())
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the original error", err)
	}
	other := &pgconn.PgError{Code: "23505", ConstraintName: "some_other_key"}
	_, err = NewService(nil, failingRepository{err: other}).Requested(context.Background(), requestedInput())
	if platform.IsClientError(err) {
		t.Fatalf("a different unique violation must not be reported as a client conflict: %v", err)
	}
}
