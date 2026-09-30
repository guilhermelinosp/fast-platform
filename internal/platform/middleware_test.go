package platform

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
)

func TestTelemetryMiddlewarePreservesRoutePattern(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tel, err := telemetry.NewWithOptions(context.Background(), telemetry.Options{ServiceName: "platform-test"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tel.Close(context.Background()) }()

	router := gin.New()
	router.Use(telemetryMiddleware(tel))
	router.GET("/orders/:id", func(c *gin.Context) {
		if got := c.Request.Pattern; got != "/orders/:id" {
			t.Fatalf("request pattern = %q", got)
		}
		if c.Request.Context() == nil {
			t.Fatal("request context is nil")
		}
		c.Status(204)
	})

	req := httptest.NewRequest("GET", "/orders/123", nil)
	res := httptest.NewRecorder()
	router.ServeHTTP(res, req)
	if res.Code != 204 {
		t.Fatalf("status = %d, want 204", res.Code)
	}
}

type statusSpy struct {
	http.ResponseWriter
	codes []int
}

func (s *statusSpy) WriteHeader(code int) { s.codes = append(s.codes, code) }

func TestReportStatusForwardsTheStatusGinWrote(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, want := range []int{http.StatusCreated, http.StatusBadRequest, http.StatusInternalServerError} {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Status(want)
		spy := &statusSpy{ResponseWriter: rec}
		reportStatus(spy, c)
		if len(spy.codes) != 1 || spy.codes[0] != want {
			t.Fatalf("reported %v, want [%d]", spy.codes, want)
		}
	}
}

func TestTelemetryMiddlewareKeepsTheResponseStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tel, err := telemetry.NewWithOptions(context.Background(), telemetry.Options{ServiceName: "platform-test"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tel.Close(context.Background()) }()

	router := gin.New()
	router.Use(telemetryMiddleware(tel))
	router.POST("/orders", func(c *gin.Context) { c.JSON(http.StatusCreated, gin.H{"ok": true}) })
	router.GET("/bad", func(c *gin.Context) { c.AbortWithStatus(http.StatusBadRequest) })

	for path, want := range map[string]int{"/orders": http.StatusCreated, "/bad": http.StatusBadRequest} {
		method := http.MethodPost
		if path == "/bad" {
			method = http.MethodGet
		}
		res := httptest.NewRecorder()
		router.ServeHTTP(res, httptest.NewRequest(method, path, nil))
		if res.Code != want {
			t.Fatalf("%s status = %d, want %d", path, res.Code, want)
		}
	}
}
