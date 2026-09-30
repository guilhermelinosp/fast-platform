package drivers

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/guilhermelinosp/fast-platform-modular/internal/platform"
	"github.com/guilhermelinosp/hellnet-lib-database/database"
)

func stubExecute(t *testing.T, rows int64, err error) (sqls *[]string, args *[][]any) {
	t.Helper()
	var gotSQL []string
	var gotArgs [][]any
	prev := execute
	execute = func(_ context.Context, _ *database.DB, sql string, a ...any) (int64, error) {
		gotSQL = append(gotSQL, sql)
		gotArgs = append(gotArgs, a)
		return rows, err
	}
	t.Cleanup(func() { execute = prev })
	return &gotSQL, &gotArgs
}

func TestAcceptedIsOneGuardedInsertOnlyStatement(t *testing.T) {
	sqls, args := stubExecute(t, 1, nil)
	in := AcceptedInput{OrderID: "o1", DriverID: "d1", AcceptanceID: "a1", StatusHistoryID: "h1", OutboxID: "x1", Payload: []byte(`{}`), EventType: "order.accepted.v1"}
	order, err := (&Database{}).Accepted(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if len(*sqls) != 1 {
		t.Fatalf("statements = %d, want 1 (one round trip)", len(*sqls))
	}
	upper := strings.ToUpper((*sqls)[0])
	for _, forbidden := range []string{"UPDATE ", "DELETE ", "ON CONFLICT"} {
		if strings.Contains(upper, forbidden) {
			t.Fatalf("append-only tables: statement contains %q", forbidden)
		}
	}
	if !strings.Contains(upper, "STATUS_ID = 1") || !strings.Contains(upper, "NOT EXISTS") {
		t.Fatal("the statement must guard on the requested state and on not yet accepted")
	}
	if got := (*args)[0]; len(got) != 7 || got[0] != "a1" || got[1] != "o1" || got[2] != "d1" || got[3] != "h1" || got[4] != "x1" || got[6] != "order.accepted.v1" {
		t.Fatalf("arguments = %v", got)
	}
	if order.ID != "o1" {
		t.Fatalf("order = %+v", order)
	}
}

func TestAcceptedZeroRowsIsOrderNotAcceptable(t *testing.T) {
	stubExecute(t, 0, nil)
	_, err := (&Database{}).Accepted(context.Background(), AcceptedInput{OrderID: "o1"})
	var httpErr *platform.HTTPError
	if !errors.As(err, &httpErr) || httpErr.Code != "ORDER_NOT_ACCEPTABLE" || httpErr.Status != 409 {
		t.Fatalf("err = %v, want 409 ORDER_NOT_ACCEPTABLE", err)
	}
}

func TestAcceptedPropagatesDatabaseErrors(t *testing.T) {
	boom := errors.New("db down")
	stubExecute(t, 0, boom)
	if _, err := (&Database{}).Accepted(context.Background(), AcceptedInput{OrderID: "o1"}); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}
