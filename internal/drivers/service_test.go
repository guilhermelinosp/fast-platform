package drivers

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/guilhermelinosp/fast-platform-modular/internal/orders"
	"github.com/guilhermelinosp/fast-platform-modular/internal/platform"
)

const (
	acceptOrderID  = "0d0a3d4e-37a4-4b0e-9d8e-3d9a7f8d1c11"
	acceptDriverID = "7b0a1d4e-37a4-4b0e-9d8e-3d9a7f8d1c22"
)

type fakeAcceptRepository struct{ calls int }

func (r *fakeAcceptRepository) Accepted(context.Context, AcceptedInput) (Order, error) {
	r.calls++
	return Order{}, nil
}

type fakeReader struct {
	view  orders.OrderView
	err   error
	reads int
}

func (r *fakeReader) Get(context.Context, string) (orders.OrderView, error) {
	r.reads++
	return r.view, r.err
}

func accept(t *testing.T, reader orderReader) (*fakeAcceptRepository, error) {
	t.Helper()
	repo := &fakeAcceptRepository{}
	_, err := NewService(nil, repo, reader).Accepted(context.Background(), AcceptedInput{OrderID: acceptOrderID, DriverID: acceptDriverID})
	return repo, err
}

func TestAcceptedStopsEarlyWhenTheOrderIsNoLongerRequested(t *testing.T) {
	reader := &fakeReader{view: orders.OrderView{Status: "accepted"}}
	repo, err := accept(t, reader)

	var httpErr *platform.HTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != http.StatusConflict || httpErr.Code != "ORDER_NOT_ACCEPTABLE" {
		t.Fatalf("err = %v, want 409 ORDER_NOT_ACCEPTABLE", err)
	}
	if repo.calls != 0 {
		t.Fatalf("repository calls = %d, want 0: the early check must save the write", repo.calls)
	}
}

func TestAcceptedWritesWhenTheOrderIsRequested(t *testing.T) {
	reader := &fakeReader{view: orders.OrderView{Status: "requested"}}
	repo, err := accept(t, reader)
	if err != nil || repo.calls != 1 || reader.reads != 1 {
		t.Fatalf("err = %v, repository calls = %d, reads = %d; want nil, 1, 1", err, repo.calls, reader.reads)
	}
}

func TestAcceptedFallsThroughToTheSQLGuardWhenTheReadFails(t *testing.T) {
	for _, readErr := range []error{
		platform.NewError(http.StatusNotFound, "ORDER_NOT_FOUND", "order not found"),
		errors.New("redis down"),
	} {
		repo, err := accept(t, &fakeReader{err: readErr})
		if err != nil || repo.calls != 1 {
			t.Fatalf("read error %v: err = %v, repository calls = %d; want the write to be attempted", readErr, err, repo.calls)
		}
	}
}

func TestAcceptedWithoutReaderWritesDirectly(t *testing.T) {
	repo, err := accept(t, nil)
	if err != nil || repo.calls != 1 {
		t.Fatalf("err = %v, repository calls = %d; want nil, 1", err, repo.calls)
	}
}
