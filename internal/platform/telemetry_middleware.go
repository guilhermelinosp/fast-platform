package platform

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
)

type telemetryContextKey struct{}

// telemetryMiddleware adapts the library's net/http instrumentation to Gin.
// Keeping this adapter in the application avoids coupling the telemetry
// library to a specific HTTP framework.
func telemetryMiddleware(tel *telemetry.Telemetry) gin.HandlerFunc {
	if tel == nil {
		return func(c *gin.Context) { c.Next() }
	}

	handler := telemetry.Middleware(tel, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, ok := r.Context().Value(telemetryContextKey{}).(*gin.Context)
		if !ok || c == nil {
			return
		}
		c.Request = r
		c.Next()
	}))

	return func(c *gin.Context) {
		r := c.Request
		if pattern := c.FullPath(); pattern != "" {
			r = r.Clone(r.Context())
			r.Pattern = pattern
		}
		r = r.WithContext(context.WithValue(r.Context(), telemetryContextKey{}, c))
		handler.ServeHTTP(c.Writer, r)
	}
}
