// Package platform is the thin HTTP layer for the modular service.
//
// It wires the gin engine (with telemetry instrumentation, request ID,
// security headers, CORS and recovery), the env-driven config and the HTTP
// server in one place. Handlers use gin directly — no extra abstractions.
package platform

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/guilhermelinosp/fast-platform-modular/internal/env"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
)

// ─────────────────────────────────────────────────────────────────────────
// Config (env-driven)
// ─────────────────────────────────────────────────────────────────────────

// Config holds runtime settings derived from environment variables.
type Config struct {
	Name               string
	Env                string
	Port               string
	ShutdownTimeout    time.Duration
	ReadTimeout        time.Duration
	WriteTimeout       time.Duration
	IdleTimeout        time.Duration
	ReadHeaderTimeout  time.Duration
	CORSAllowedOrigins []string
	BodyLimit          int64
	ReleaseMode        bool
	TrustedProxies     []string
}

// NewConfig builds runtime configuration from environment variables.
func NewConfig() (*Config, error) {
	environmentName := strings.TrimSpace(env.String("HELLNET_ENVIRONMENT", "Development"))
	c := &Config{
		Name:               strings.TrimSpace(env.String("HELLNET_SERVICE", "")),
		Env:                environmentName,
		Port:               env.String("HELLNET_PORT", "8080"),
		ShutdownTimeout:    env.Duration("SHUTDOWN_TIMEOUT", 10*time.Second),
		ReadTimeout:        env.Duration("READ_TIMEOUT", 15*time.Second),
		WriteTimeout:       env.Duration("WRITE_TIMEOUT", 30*time.Second),
		IdleTimeout:        env.Duration("IDLE_TIMEOUT", 120*time.Second),
		ReadHeaderTimeout:  env.Duration("READ_HEADER_TIMEOUT", 10*time.Second),
		CORSAllowedOrigins: env.Slice("CORS_ALLOWED_ORIGINS"),
		BodyLimit:          int64(env.Int("BODY_LIMIT", 1048576)),
		ReleaseMode:        !strings.EqualFold(environmentName, "Development"),
		TrustedProxies:     env.Slice("TRUSTED_PROXIES"),
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// Validate checks the configuration for invalid or inconsistent values.
func (c *Config) Validate() error {
	port, err := strconv.Atoi(c.Port)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("config: HELLNET_PORT %q is invalid", c.Port)
	}
	if strings.TrimSpace(c.Name) == "" {
		return fmt.Errorf("config: HELLNET_SERVICE cannot be empty")
	}
	if c.ShutdownTimeout <= 0 || c.ReadTimeout <= 0 || c.WriteTimeout <= 0 || c.IdleTimeout <= 0 || c.ReadHeaderTimeout <= 0 {
		return fmt.Errorf("config: timeouts must be positive")
	}
	if c.BodyLimit <= 0 {
		return fmt.Errorf("config: BODY_LIMIT must be positive")
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────
// Typed HTTP errors
// ─────────────────────────────────────────────────────────────────────────

// HTTPError is a typed HTTP error carrying a status code, machine-readable
// code and message, with optional cause wrapping.
type HTTPError struct {
	Status  int
	Code    string
	Message string
	cause   error
}

// Error implements the error interface.
func (e *HTTPError) Error() string { return e.Code + ": " + e.Message }

// Unwrap returns the wrapped cause, if any.
func (e *HTTPError) Unwrap() error { return e.cause }

// NewError returns a new HTTPError with the given status, code and message.
func NewError(s int, c, m string) *HTTPError { return &HTTPError{Status: s, Code: c, Message: m} }

// WrapError returns a new HTTPError wrapping the given cause.
func WrapError(e *HTTPError, c error) *HTTPError {
	return &HTTPError{Status: e.Status, Code: e.Code, Message: e.Message, cause: c}
}

// ErrorCause returns the wrapped cause of e, or nil.
func ErrorCause(e *HTTPError) error {
	if e == nil {
		return nil
	}
	return e.cause
}

// MapError converts any error into an *HTTPError.
func MapError(e error) *HTTPError {
	if e == nil {
		return nil
	}
	if a, ok := errors.AsType[*HTTPError](e); ok {
		return a
	}
	return WrapError(InternalError(), e)
}

// ValidationError returns a 400 Bad Request HTTPError for a field+reason.
func ValidationError(f, r string) *HTTPError {
	return NewError(http.StatusBadRequest, "VALIDATION_ERROR", fmt.Sprintf("field %q %s", f, r))
}

// InternalError returns a 500 Internal Server Error.
func InternalError() *HTTPError {
	return NewError(http.StatusInternalServerError, "INTERNAL_ERROR", "internal server error")
}

// AbortError writes the JSON error envelope for err to the gin context and
// aborts. Use from handlers:
//
//	order, err := s.service.Requested(c.Request.Context(), in)
//	if err != nil { platform.AbortError(c, err); return }
func AbortError(c *gin.Context, err error) {
	mapped := MapError(err)
	if mapped == nil {
		mapped = InternalError()
	}
	ops := telemetryFromContext(c)
	path := sanitizeForLog(c.Request.URL.Path)
	if cause := ErrorCause(mapped); cause != nil && !errors.Is(cause, context.Canceled) && ops != nil {
		ops.Error("request failed", "method", c.Request.Method, "path", path, "status", mapped.Status, "code", mapped.Code, "error", cause)
	} else if ops != nil {
		ops.Warn("request rejected", "method", c.Request.Method, "path", path, "status", mapped.Status, "code", mapped.Code, "message", mapped.Message)
	}
	c.AbortWithStatusJSON(mapped.Status, gin.H{"error": gin.H{"code": mapped.Code, "message": mapped.Message}})
}

// ─────────────────────────────────────────────────────────────────────────
// Gin engine
// ─────────────────────────────────────────────────────────────────────────

// telemetryFromContext returns the telemetry client set by the router.
func telemetryFromContext(c *gin.Context) *telemetry.Telemetry {
	if v, ok := c.Get("telemetry"); ok {
		if t, ok := v.(*telemetry.Telemetry); ok {
			return t
		}
	}
	return nil
}

// NewRouter builds a gin engine with request ID, security headers, CORS,
// recovery and trusted-proxy handling. It exposes the telemetry client so
// AbortError can log rejected/failed requests.
func NewRouter(cfg *Config, ops *telemetry.Telemetry) *gin.Engine {
	if cfg.ReleaseMode {
		gin.SetMode(gin.ReleaseMode)
	} else {
		gin.SetMode(gin.DebugMode)
	}
	limit := cfg.BodyLimit
	if limit <= 0 {
		limit = 1 << 20
	}

	engine := gin.New()
	engine.HandleMethodNotAllowed = true
	engine.Use(requestID())
	engine.Use(securityHeaders())
	if len(cfg.CORSAllowedOrigins) > 0 {
		engine.Use(cors(cfg.CORSAllowedOrigins))
	}
	engine.Use(recovery(ops))
	if len(cfg.TrustedProxies) == 0 {
		_ = engine.SetTrustedProxies(nil)
	} else {
		_ = engine.SetTrustedProxies(cfg.TrustedProxies)
	}
	engine.MaxMultipartMemory = limit
	engine.NoRoute(func(c *gin.Context) {
		AbortError(c, NewError(http.StatusNotFound, "NOT_FOUND", "route not found"))
	})
	engine.NoMethod(func(c *gin.Context) {
		AbortError(c, NewError(http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed for this resource"))
	})
	return engine
}

// ─────────────────────────────────────────────────────────────────────────
// Request middlewares
// ─────────────────────────────────────────────────────────────────────────

// RequestIDHeader is the canonical request ID header name.
const RequestIDHeader = "X-Request-ID"

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
					ops.Error("panic recovered", "method", c.Request.Method, "path", sanitizeForLog(c.Request.URL.Path), "panic", fmt.Sprint(r))
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

// ─────────────────────────────────────────────────────────────────────────
// HTTP server (graceful shutdown)
// ─────────────────────────────────────────────────────────────────────────

// NewServer builds an *http.Server from a Config, a telemetry client and the
// application's handler.
func NewServer(cfg *Config, ops *telemetry.Telemetry, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           handler,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		MaxHeaderBytes:    1 << 20,
	}
}

// Run serves HTTP until the server fails or ctx is cancelled, then drains
// existing connections gracefully within the configured shutdown timeout.
func Run(ctx context.Context, cfg *Config, srv *http.Server) error {
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.ListenAndServe()
	}()
	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("server: listenAndServe failed: %w", err)
	case <-ctx.Done():
		drainCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(drainCtx); err != nil {
			return fmt.Errorf("server: graceful shutdown incomplete: %w", err)
		}
		return nil
	}
}
