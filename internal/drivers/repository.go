package drivers

import (
	"context"

	"github.com/guilhermelinosp/fast-platform/internal/platform"
	"github.com/guilhermelinosp/hellnet-lib-database/database"
)

// Database persists driver availability and order acceptances in PostgreSQL.
type Database struct{ db *database.DB }

// execute runs one SQL statement. It is a package variable so tests can capture
// the statement and its arguments without a database.
var execute = func(ctx context.Context, db *database.DB, sql string, args ...any) (int64, error) {
	return db.ExecuteContext(ctx, sql, args...)
}

// NewRepository creates a driver availability repository.
func NewRepository(db *database.DB) *Database { return &Database{db: db} }

// acceptedSQL appends the acceptance, the "accepted" status and the outbox event
// in a single INSERT-only statement, all guarded by the current state: the order
// must be in the requested state (status 1) and not yet accepted (status 3). The
// affected-row count is that of the outbox INSERT, so 0 means the guard failed
// (order not requested, already accepted, or unknown) and nothing was written.
const acceptedSQL = `
WITH guard AS (
  SELECT 1
  WHERE EXISTS (SELECT 1 FROM order_status_history WHERE order_id = $2::uuid AND status_id = 1)
    AND NOT EXISTS (SELECT 1 FROM order_status_history WHERE order_id = $2::uuid AND status_id = 3)
), a AS (
  INSERT INTO order_acceptances (id, order_id, driver_id) SELECT $1::uuid, $2::uuid, $3::uuid FROM guard
), h AS (
  INSERT INTO order_status_history (id, order_id, sequence, status_id)
  SELECT $4::uuid, $2::uuid, COALESCE((SELECT MAX(sequence) FROM order_status_history WHERE order_id = $2::uuid), 0) + 1, 3 FROM guard
)
INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, event_version, payload)
SELECT $5::uuid, 'order', $2::uuid, $7, 1, $6::jsonb FROM guard`

// Accepted persists an order acceptance and its outbox event atomically, but
// only when the order is still in the requested state. A zero-row result means
// the guard failed and is reported as the matching domain error.
func (r *Database) Accepted(ctx context.Context, input AcceptedInput) (Order, error) {
	rows, err := execute(ctx, r.db, acceptedSQL,
		input.AcceptanceID, input.OrderID, input.DriverID, input.StatusHistoryID, input.OutboxID, input.Payload, input.EventType)
	if err != nil {
		return Order{}, err
	}
	if rows == 0 {
		return Order{}, platform.NewError(409, "ORDER_NOT_ACCEPTABLE", "order is not in the requested state")
	}
	return Order{ID: input.OrderID}, nil
}
