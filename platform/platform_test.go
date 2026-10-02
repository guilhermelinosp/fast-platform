package platform

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestIsClientError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"conflict", NewError(http.StatusConflict, "ORDER_NOT_ACCEPTABLE", "x"), true},
		{"wrapped validation", fmt.Errorf("ctx: %w", ValidationError("id", "must be a UUID")), true},
		{"server error", NewError(http.StatusInternalServerError, "INTERNAL", "x"), false},
		{"plain error", errors.New("boom"), false},
	}
	for _, c := range cases {
		if got := IsClientError(c.err); got != c.want {
			t.Errorf("%s: IsClientError = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestNewRouterDebugModeKeepsGinOutputOffStdout(t *testing.T) {
	prevWriter, prevRoute := gin.DefaultWriter, gin.DebugPrintRouteFunc
	t.Cleanup(func() {
		gin.DefaultWriter, gin.DebugPrintRouteFunc = prevWriter, prevRoute
		gin.SetMode(gin.TestMode)
	})

	NewRouter(&Config{ReleaseMode: false}, nil)

	if gin.DefaultWriter != io.Discard {
		t.Fatal("debug mode must not let gin print plain text to stdout")
	}
	if gin.DebugPrintRouteFunc == nil {
		t.Fatal("debug mode must route the route listing through the structured logger")
	}
}
