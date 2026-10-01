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

type writerSpy struct {
	http.ResponseWriter
	codes []int
	bytes int
}

func (s *writerSpy) WriteHeader(code int) {
	s.codes = append(s.codes, code)
	s.ResponseWriter.WriteHeader(code)
}

func (s *writerSpy) Write(b []byte) (int, error) {
	s.bytes += len(b)
	return s.ResponseWriter.Write(b)
}

func newObserved(t *testing.T) (*gin.Context, *httptest.ResponseRecorder, *writerSpy) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	spy := &writerSpy{ResponseWriter: c.Writer}
	c.Writer = &observedWriter{ResponseWriter: c.Writer, w: spy}
	return c, rec, spy
}

func TestObservedWriterReportsStatusAndBodySize(t *testing.T) {
	c, rec, spy := newObserved(t)
	c.JSON(http.StatusCreated, gin.H{"ok": true})
	if len(spy.codes) != 1 || spy.codes[0] != http.StatusCreated {
		t.Fatalf("reported status codes = %v, want [201]", spy.codes)
	}
	if spy.bytes == 0 || spy.bytes != rec.Body.Len() {
		t.Fatalf("reported %d bytes, response body has %d", spy.bytes, rec.Body.Len())
	}
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rec.Code)
	}
}

func TestObservedWriterReportsHandlersThatNeverWrite(t *testing.T) {
	c, rec, spy := newObserved(t)
	c.Status(http.StatusNoContent)
	c.Writer.WriteHeaderNow()
	if len(spy.codes) != 1 || spy.codes[0] != http.StatusNoContent || spy.bytes != 0 {
		t.Fatalf("codes=%v bytes=%d, want [204] and 0", spy.codes, spy.bytes)
	}
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
}

func TestObservedWriterCountsStringBodies(t *testing.T) {
	c, rec, spy := newObserved(t)
	c.String(http.StatusAccepted, "hello %s", "world")
	if spy.bytes != len("hello world") || rec.Body.String() != "hello world" || rec.Code != http.StatusAccepted {
		t.Fatalf("bytes=%d body=%q status=%d", spy.bytes, rec.Body.String(), rec.Code)
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
		if path == "/orders" && res.Body.String() != `{"ok":true}` {
			t.Fatalf("body through the telemetry middleware = %q, want the handler's JSON", res.Body.String())
		}
	}
}

func TestTelemetryMiddlewareSkipsHealthProbes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tel, err := telemetry.NewWithOptions(context.Background(), telemetry.Options{ServiceName: "platform-test"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tel.Close(context.Background()) }()

	patterns := map[string]string{}
	router := gin.New()
	router.Use(telemetryMiddleware(tel))
	for _, path := range []string{"/live", "/ready", "/health", "/orders"} {
		router.GET(path, func(c *gin.Context) {
			patterns[c.Request.URL.Path] = c.Request.Pattern // set only when the telemetry wrapper ran
			c.Status(http.StatusOK)
		})
	}
	for _, path := range []string{"/live", "/ready", "/health", "/orders"} {
		res := httptest.NewRecorder()
		router.ServeHTTP(res, httptest.NewRequest(http.MethodGet, path, nil))
		if res.Code != http.StatusOK {
			t.Fatalf("%s status = %d", path, res.Code)
		}
	}
	for _, probe := range []string{"/live", "/ready", "/health"} {
		if patterns[probe] != "" {
			t.Errorf("%s went through the telemetry wrapper (pattern %q); probes must not be traced or logged", probe, patterns[probe])
		}
	}
	if patterns["/orders"] != "/orders" {
		t.Errorf("a normal route must still be instrumented, pattern = %q", patterns["/orders"])
	}
}
