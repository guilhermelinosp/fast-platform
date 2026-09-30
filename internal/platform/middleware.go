package platform

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
)

// RequestIDHeader is the canonical request ID header name.
const RequestIDHeader = "X-Request-ID"

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
		reportStatus(w, c)
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

// reportStatus tells the telemetry wrapper the status Gin actually wrote. Gin
// writes through its own ResponseWriter, not w, so without this the library
// sees no WriteHeader and records 200 for every request (hiding 4xx/5xx from
// spans, logs and http_server_errors_total). Gin ignores the repeated
// WriteHeader because the code equals the one already set.
func reportStatus(w http.ResponseWriter, c *gin.Context) {
	w.WriteHeader(c.Writer.Status())
}

// telemetryFromContext returns the telemetry client set by the router.
func telemetryFromContext(c *gin.Context) *telemetry.Telemetry {
	if v, ok := c.Get("telemetry"); ok {
		if t, ok := v.(*telemetry.Telemetry); ok {
			return t
		}
	}
	return nil
}

// SanitizeRequestID validates and normalizes an incoming request ID header.
func SanitizeRequestID(raw string) string {
	raw = strings.TrimSpace(raw)
	if len(raw) == 0 || len(raw) > 64 {
		return ""
	}
	for _, r := range raw {
		if !validIDChar(r) {
			return ""
		}
	}
	return raw
}

func validIDChar(r rune) bool {
	return r >= '0' && r <= '9' ||
		r >= 'a' && r <= 'z' ||
		r >= 'A' && r <= 'Z' ||
		r == '-' || r == '_' || r == '.'
}

// GenerateRequestID produces a random 128-bit hex request ID.
func GenerateRequestID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "unavailable"
	}
	return hex.EncodeToString(b)
}

func requestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := SanitizeRequestID(c.GetHeader(RequestIDHeader))
		if id == "" {
			id = GenerateRequestID()
		}
		c.Header(RequestIDHeader, id)
		c.Next()
	}
}

func securityHeaders() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("X-Content-Type-Options", "nosniff")
		c.Header("X-Frame-Options", "DENY")
		c.Header("Referrer-Policy", "strict-origin-when-cross-origin")
		c.Header("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		c.Header("Cross-Origin-Resource-Policy", "same-origin")
		c.Next()
	}
}

func cors(origins []string) gin.HandlerFunc {
	allowed := map[string]struct{}{}
	wildcard := false
	for _, origin := range origins {
		if origin == "*" {
			wildcard = true
		}
		allowed[origin] = struct{}{}
	}
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if origin != "" && (wildcard || hasOrigin(allowed, origin)) {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Vary", "Origin")
			c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
			c.Header("Access-Control-Allow-Headers", "Authorization, Content-Type, "+RequestIDHeader)
			c.Header("Access-Control-Max-Age", "600")
		}
		if c.Request.Method == http.MethodOptions && c.GetHeader("Access-Control-Request-Method") != "" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}

func hasOrigin(allowed map[string]struct{}, origin string) bool { _, ok := allowed[origin]; return ok }

func recovery(ops *telemetry.Telemetry) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				if ops != nil {
					ops.Log(c.Request.Context()).Error("panic recovered", "method", c.Request.Method, "path", sanitizeForLog(c.Request.URL.Path), "panic", fmt.Sprint(r))
				}
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": gin.H{"code": "INTERNAL_ERROR", "message": "internal server error"}})
			}
		}()
		c.Next()
	}
}

func sanitizeForLog(value string) string {
	value = strings.ReplaceAll(value, "\n", "")
	value = strings.ReplaceAll(value, "\r", "")
	return value
}
