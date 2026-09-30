package orders

import (
	"context"

	"github.com/guilhermelinosp/hellnet-lib-database/database"
)

// Database persists rider data in PostgreSQL.
type Database struct{ db *database.DB }

// execFn is a single SQL execution inside a transaction. It is the unit the
// repository depends on, so tests can capture statements without a database.
type execFn func(sql string, args ...any) (int64, error)

// transactional runs fn inside a database transaction. It is a package
// variable so tests can replace it with a fake that records statements.
var transactional = func(ctx context.Context, db *database.DB, fn func(execute execFn) error) error {
	return db.TransactionalContext(ctx, func(ctx context.Context, tx *database.Tx) error {
		return fn(func(sql string, args ...any) (int64, error) {
			return tx.ExecuteContext(ctx, sql, args...)
		})
	})
}

// NewRepository creates a rider database repository.
func NewRepository(db *database.DB) *Database {
	return &Database{db: db}
}

// Requested persists a requested ride and its outbox event.
func (r *Database) Requested(ctx context.Context, input OrderRequestedInput) (Order, error) {
	var order Order
	err := transactional(ctx, r.db, func(execute execFn) error {
		if _, err := execute(
			"INSERT INTO orders (id, rider_id, pickup_latitude, pickup_longitude, destination_latitude, destination_longitude) VALUES ($1, $2, $3, $4, $5, $6)",
			input.ID,
			input.RiderID,
			input.PickupLatitude,
			input.PickupLongitude,
			input.DestinationLatitude,
			input.DestinationLongitude); err != nil {
			return err
		}

		if _, err := execute(
			"INSERT INTO order_status_history (id, order_id, sequence, status_id) VALUES ($1, $2, 1, 1)",
			input.StatusHistoryID,
			input.ID); err != nil {
			return err
		}

		if _, err := execute(
			"INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, event_version, payload) VALUES ($1, 'order', $2, $4, 1, $3::jsonb)",
			input.OutboxID,
			input.ID,
			input.Payload, input.EventType); err != nil {
			return err
		}

		order = Order{
			ID:                   input.ID,
			RiderID:              input.RiderID,
			PickupLatitude:       input.PickupLatitude,
			PickupLongitude:      input.PickupLongitude,
			DestinationLatitude:  input.DestinationLatitude,
			DestinationLongitude: input.DestinationLongitude,
		}

		return nil
	})
	return order, err
}
