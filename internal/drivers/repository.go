package drivers

import (
	"context"

	"github.com/guilhermelinosp/fast-platform-modular/internal/platform"
	"github.com/guilhermelinosp/hellnet-lib-database/database"
)

// Database persists driver availability and order acceptances in PostgreSQL.
type Database struct{ db *database.DB }

// execFn is a single SQL execution inside a transaction. It is the unit the
// repository depends on, so tests can capture statements without a database.
type execFn func(sql string, args ...any) (int64, error)

// transactional runs fn inside a database transaction. It is a package
// variable so tests can replace it with a fake that records statements.
var transactional = func(db *database.DB, fn func(execute execFn) error) error {
	return db.Transactional(func(tx *database.Tx) error {
		return fn(tx.Execute)
	})
}

// NewRepository creates a driver availability repository.
func NewRepository(db *database.DB) *Database { return &Database{db: db} }

// Accepted persists an order acceptance and its outbox event atomically, but
// only when the order is still in the requested state.
//
// The guard is enforced by the SQL itself: the status-history INSERT is a
// SELECT guarded by the current state. A zero-row result means the guard
// failed, and the transaction aborts with the matching domain error.
func (r *Database) Accepted(ctx context.Context, input AcceptedInput) (Order, error) {
	_ = ctx
	var order Order
	err := transactional(r.db, func(execute execFn) error {
		if _, err := execute("INSERT INTO order_acceptances (id, order_id, driver_id) VALUES ($1::uuid, $2::uuid, $3::uuid)", input.AcceptanceID, input.OrderID, input.DriverID); err != nil {
			return err
		}

		rows, err := execute("INSERT INTO order_status_history (id, order_id, sequence, status_id) SELECT $1::uuid, $2::uuid, COALESCE((SELECT MAX(sequence) FROM order_status_history WHERE order_id = $2::uuid), 0) + 1, 3 WHERE EXISTS (SELECT 1 FROM order_status_history WHERE order_id = $2::uuid AND status_id = 1) AND NOT EXISTS (SELECT 1 FROM order_status_history WHERE order_id = $2::uuid AND status_id = 3)", input.StatusHistoryID, input.OrderID)
		if err != nil {
			return err
		}
		if rows == 0 {
			return platform.NewError(409, "ORDER_NOT_ACCEPTABLE", "order is not in the requested state")
		}

		if _, err := execute("INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, event_version, payload) VALUES ($1::uuid, 'order', $2::uuid, $4, 1, $3::jsonb)", input.OutboxID, input.OrderID, input.Payload, input.EventType); err != nil {
			return err
		}
		order.ID = input.OrderID
		return nil
	})
	return order, err
}
