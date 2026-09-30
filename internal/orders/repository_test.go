package orders

import (
	"context"
	"errors"
	"strings"
	"testing"

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

func TestRequestedIsOneInsertOnlyStatement(t *testing.T) {
	sqls, args := stubExecute(t, 1, nil)
	in := OrderRequestedInput{
		ID: "o1", RiderID: "r1", StatusHistoryID: "h1", OutboxID: "x1",
		PickupLatitude: 1, PickupLongitude: 2, DestinationLatitude: 3, DestinationLongitude: 4,
		Payload: []byte(`{}`), EventType: "order.requested.v1",
	}
	order, err := (&Database{}).Requested(context.Background(), in)
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
	for _, table := range []string{"INTO ORDERS", "INTO ORDER_STATUS_HISTORY", "INTO OUTBOX_EVENTS"} {
		if !strings.Contains(upper, table) {
			t.Fatalf("statement must insert %s", table)
		}
	}
	if got := (*args)[0]; len(got) != 10 || got[0] != "o1" || got[6] != "h1" || got[7] != "x1" || got[9] != "order.requested.v1" {
		t.Fatalf("arguments = %v", got)
	}
	if order.ID != "o1" || order.RiderID != "r1" {
		t.Fatalf("order = %+v", order)
	}
}

func TestRequestedPropagatesErrors(t *testing.T) {
	boom := errors.New("db down")
	stubExecute(t, 0, boom)
	if _, err := (&Database{}).Requested(context.Background(), OrderRequestedInput{ID: "o1"}); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}
