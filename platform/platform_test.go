package platform

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
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
