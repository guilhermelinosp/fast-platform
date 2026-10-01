package orders

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

const (
	ownerRider = "7b0a1d4e-37a4-4b0e-9d8e-3d9a7f8d1c22"
	otherRider = "9c1b2e5f-48b5-4c1f-8e9f-4e0b8a9e2d33"
)

type getOnlyService struct{ view OrderView }

func (getOnlyService) Requested(context.Context, OrderRequestedInput) (OrderOutput, error) {
	return OrderOutput{}, nil
}

func (s getOnlyService) Get(context.Context, string) (OrderView, error) { return s.view, nil }

func getOrder(t *testing.T, riderHeader string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	NewHandler(getOnlyService{view: OrderView{ID: viewOrderID, RiderID: ownerRider, Status: "requested"}}).Register(router.Group("/api/v1"))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/orders/"+viewOrderID, nil)
	if riderHeader != "" {
		req.Header.Set("rider_id", riderHeader)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func TestGetOrderIsVisibleToItsRider(t *testing.T) {
	if rec := getOrder(t, ownerRider); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body)
	}
}

func TestGetOrderOfAnotherRiderLooksLikeAMissingOrder(t *testing.T) {
	rec := getOrder(t, otherRider)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: %s", rec.Code, rec.Body)
	}
	if body := rec.Body.String(); !strings.Contains(body, "ORDER_NOT_FOUND") || strings.Contains(body, ownerRider) {
		t.Fatalf("body must say not found and leak nothing about the order: %s", body)
	}
}

func TestGetOrderRequiresAValidRiderHeader(t *testing.T) {
	for _, header := range []string{"", "not-a-uuid"} {
		if rec := getOrder(t, header); rec.Code != http.StatusBadRequest {
			t.Fatalf("rider_id %q: status = %d, want 400", header, rec.Code)
		}
	}
}
